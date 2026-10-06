/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

//! Worker lifecycle: opens the control, data and logging streams with the runner and runs
//! the loops that serve instructions and data.

use std::collections::HashMap;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::mpsc;
use tokio_stream::wrappers::{ReceiverStream, UnboundedReceiverStream};
use tonic::transport::Channel;
use tracing::{debug, error, info, warn};

use beam::options::HarnessOptions;

use crate::bundle_processor::{BundleProcessor, TransformFn};
use crate::control::ControlClient;
use crate::data::{
    DataChannelState, DataError, DataManager, DataStreamConnector, OUTBOUND_QUEUE_MESSAGES, deliver,
};
use crate::grpc::{self, attach_header, attach_worker_id};
use crate::logging::{LOG_QUEUE_BATCHES, LoggingClient};
use model::fn_execution::{
    Elements, InstructionRequest, InstructionResponse, beam_fn_control_client::BeamFnControlClient,
    beam_fn_data_client::BeamFnDataClient, beam_fn_logging_client::BeamFnLoggingClient, log_entry,
};

#[derive(Error, Debug)]
pub enum WorkerError {
    #[error("Failed to connect to gRPC endpoint: {0}")]
    Connection(#[from] tonic::transport::Error),
    #[error(transparent)]
    Dial(#[from] grpc::ChannelError),
    #[error("Invalid endpoint URI: {0}")]
    Uri(#[from] tonic::codegen::http::uri::InvalidUri),
    #[error("gRPC status error: {0}")]
    Status(Box<tonic::Status>),
    #[error("Channel error")]
    Channel,
}

impl From<tonic::Status> for WorkerError {
    fn from(status: tonic::Status) -> Self {
        Self::Status(Box::new(status))
    }
}

/// The top-level worker harness coordinator.
pub struct Worker {
    args: HarnessOptions,
    transform_handlers: HashMap<String, TransformFn>,
    metrics: crate::status::WorkerMetrics,
}

impl Worker {
    pub fn new(args: HarnessOptions) -> Self {
        Self {
            args,
            transform_handlers: HashMap::new(),
            metrics: crate::status::WorkerMetrics::new(),
        }
    }

    pub fn with_handlers(
        args: HarnessOptions,
        transform_handlers: HashMap<String, TransformFn>,
    ) -> Self {
        Self {
            args,
            transform_handlers,
            metrics: crate::status::WorkerMetrics::new(),
        }
    }

    pub fn metrics(&self) -> &crate::status::WorkerMetrics {
        &self.metrics
    }

    /// Connects to runner endpoints and runs the worker harness execution loop.
    pub async fn run(&self) -> Result<(), WorkerError> {
        let worker_id = self.args.id.clone().ok_or_else(|| {
            WorkerError::Status(Box::new(tonic::Status::invalid_argument(
                "Worker started without --id. The runner must supply the worker identity it \
                 expects on the control stream; defaulting it would make the harness register \
                 under a name the runner never asked for.",
            )))
        })?;

        info!("Starting Apache Beam Rust Worker Harness (id: '{worker_id}')");

        // The graph is rebuilt from these. If a transform later has no handler, this line
        // tells "the runner sent wrong arguments" apart from "the pipeline code is wrong".
        info!(
            "Replayed pipeline arguments: {:?}",
            std::env::args().skip(1).collect::<Vec<_>>()
        );

        let endpoints = self.resolve_endpoints(&worker_id).await?;

        // Set up logging first, so later failures show in the runner's console and not only
        // on the container's stderr.
        let logging_client = match &endpoints.logging {
            Some(endpoint) => Some(connect_logging(endpoint, &worker_id)),
            None => {
                warn!("No logging endpoint supplied; worker logs stay local to the container");
                None
            }
        };

        let status_handler = endpoints.status.as_ref().map(|ep| {
            crate::status::WorkerStatusHandler::connect(ep, &worker_id, self.metrics.clone())
        });

        if let Some(client) = &logging_client {
            crate::logging::set_global_client(client.clone());
            let _ = client.info(format!(
                "Rust SDK worker '{worker_id}' starting with {} registered transform handlers",
                self.transform_handlers.len()
            ));
        }

        // The Fn API requires the control stream (and worker_id) before data or state.
        let control = connect_control(&endpoints.control, &worker_id).await?;
        let data_manager = connect_data(&endpoints.control, &worker_id).await?;

        // The handlers are prototypes; each descriptor gets its own copies on its first bundle.
        let bundle_processor = Arc::new(
            BundleProcessor::with_handlers(data_manager, self.transform_handlers.clone())
                .with_worker_id(worker_id.clone()),
        );
        let control_client =
            ControlClient::with_client(bundle_processor.clone(), control.client, worker_id.clone())
                .with_metrics(self.metrics.clone());

        // A control stream error still runs the shutdown, so the last logs reach the runner.
        let control_result =
            serve_control(control.requests, control.responses, control_client).await;

        info!("Control stream terminated. Shutting down worker harness.");
        bundle_processor.shutdown();
        if let Some(handler) = &status_handler {
            handler.stop();
        }
        if let Some(client) = &logging_client {
            client.flush().await;
        }
        control_result
    }

    /// Resolves the control, logging and status endpoints from the arguments or the
    /// ProvisionService.
    async fn resolve_endpoints(&self, worker_id: &str) -> Result<Endpoints, WorkerError> {
        let from_args = |endpoint: &Option<String>| endpoint.as_deref().map(grpc::with_scheme);
        let (control, logging, status) =
            match (&self.args.control_endpoint, &self.args.provision_endpoint) {
                (Some(ctrl), _) => (grpc::with_scheme(ctrl), None, None),
                (None, Some(prov)) => provision_endpoints(prov, worker_id).await?,
                (None, None) => (grpc::with_scheme("127.0.0.1:50001"), None, None),
            };
        Ok(Endpoints {
            control,
            logging: from_args(&self.args.logging_endpoint).or(logging),
            status: from_args(&self.args.status_endpoint).or(status),
        })
    }
}

/// Where the runner's services are.
struct Endpoints {
    control: String,
    logging: Option<String>,
    status: Option<String>,
}

/// Asks the ProvisionService for the control, logging and status endpoints.
async fn provision_endpoints(
    provision_endpoint: &str,
    worker_id: &str,
) -> Result<(String, Option<String>, Option<String>), WorkerError> {
    info!("Connecting to ProvisionService at '{provision_endpoint}'...");
    let mut client = model::fn_execution::provision_service_client::ProvisionServiceClient::new(
        grpc::channel(provision_endpoint).await?,
    )
    .max_decoding_message_size(grpc::MAX_MESSAGE_BYTES);
    let mut request = tonic::Request::new(model::fn_execution::GetProvisionInfoRequest {});
    attach_worker_id(&mut request, worker_id);
    let internal = |msg: &str| WorkerError::Status(Box::new(tonic::Status::internal(msg)));
    let info = client
        .get_provision_info(request)
        .await?
        .into_inner()
        .info
        .ok_or_else(|| internal("Empty provision info"))?;
    let control = info
        .control_endpoint
        .ok_or_else(|| internal("No control endpoint in provision info"))?;
    let logging = info.logging_endpoint.map(|e| grpc::with_scheme(&e.url));
    let status = info.status_endpoint.map(|e| grpc::with_scheme(&e.url));
    info!(
        "Resolved from ProvisionService: control='{}', logging={:?}, status={:?}",
        control.url, logging, status
    );
    Ok((grpc::with_scheme(&control.url), logging, status))
}

/// The control stream: the client, the runner's instructions, and where responses go.
struct Control {
    client: BeamFnControlClient<Channel>,
    requests: tonic::Streaming<InstructionRequest>,
    responses: mpsc::UnboundedSender<InstructionResponse>,
}

/// Opens the `BeamFnControl` stream, retrying for up to 30s while the runner comes up.
async fn connect_control(endpoint: &str, worker_id: &str) -> Result<Control, WorkerError> {
    let mut retries = 0;
    let channel = loop {
        match grpc::channel(endpoint).await {
            Ok(channel) => break channel,
            Err(e) if retries < 60 => {
                retries += 1;
                warn!("Retrying connection to control endpoint '{endpoint}' ({retries}/60): {e}");
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            }
            Err(e) => return Err(WorkerError::Dial(e)),
        }
    };
    let (responses, responses_rx) = mpsc::unbounded_channel::<InstructionResponse>();
    let mut client = BeamFnControlClient::new(channel)
        .max_decoding_message_size(grpc::MAX_MESSAGE_BYTES)
        .max_encoding_message_size(grpc::MAX_MESSAGE_BYTES);
    let mut request = tonic::Request::new(UnboundedReceiverStream::new(responses_rx));
    attach_worker_id(&mut request, worker_id);
    let requests = client.control(request).await?.into_inner();
    info!("Connected to BeamFnControl at '{endpoint}' (worker_id: '{worker_id}')");
    Ok(Control {
        client,
        requests,
        responses,
    })
}

/// Opens the default `BeamFnData` stream, retrying while the runner registers the worker,
/// and routes what arrives. Data has its own connection so bulk elements never queue behind
/// (or ahead of) control messages on one HTTP/2 connection.
async fn connect_data(endpoint: &str, worker_id: &str) -> Result<DataManager, WorkerError> {
    let channel = grpc::channel(endpoint).await.map_err(WorkerError::Dial)?;
    let mut client = BeamFnDataClient::new(channel)
        .max_decoding_message_size(grpc::MAX_MESSAGE_BYTES)
        .max_encoding_message_size(grpc::MAX_MESSAGE_BYTES);
    let mut retries = 0;
    let (outbound, mut inbound) = loop {
        let (tx, rx) = mpsc::channel::<Elements>(OUTBOUND_QUEUE_MESSAGES);
        let mut request = tonic::Request::new(ReceiverStream::new(rx));
        attach_worker_id(&mut request, worker_id);
        match client.data(request).await {
            Ok(response) => {
                info!("Connected to BeamFnData");
                break (tx, response.into_inner());
            }
            Err(e) if retries < 20 => {
                retries += 1;
                warn!("Retrying BeamFnData connection ({retries}/20): {e}");
                tokio::time::sleep(std::time::Duration::from_millis(250)).await;
            }
            Err(e) => return Err(WorkerError::Status(Box::new(e))),
        }
    };
    let data_manager = DataManager::new(outbound);
    data_manager.set_connector(Arc::new(GrpcDataStreamConnector::new(
        client,
        worker_id.to_string(),
        data_manager.channel_state(),
    )));

    let state = data_manager.channel_state();
    tokio::spawn(async move {
        let reason = loop {
            match inbound.message().await {
                Ok(Some(elements)) => deliver(&state, elements).await,
                Ok(None) => break "BeamFnData stream closed by runner".to_string(),
                Err(e) => break format!("BeamFnData stream error: {e}"),
            }
        };
        error!("{reason}");
        state.lock().await.close(reason);
    });
    Ok(data_manager)
}

/// Handles each instruction on its own task until the control stream ends.
async fn serve_control(
    mut requests: tonic::Streaming<InstructionRequest>,
    responses: mpsc::UnboundedSender<InstructionResponse>,
    control_client: ControlClient,
) -> Result<(), WorkerError> {
    loop {
        match requests.message().await {
            Ok(Some(request)) => {
                let client = control_client.clone();
                let responses = responses.clone();
                tokio::spawn(async move {
                    let _ = responses.send(client.handle_instruction(request).await);
                });
            }
            Ok(None) => return Ok(()),
            Err(e) => {
                error!("Control stream failed: {e}");
                return Err(WorkerError::Status(Box::new(e)));
            }
        }
    }
}

/// First wait before reconnecting a failed logging stream; it doubles up to
/// [`LOG_RECONNECT_MAX`].
const LOG_RECONNECT_MIN: std::time::Duration = std::time::Duration::from_millis(500);
const LOG_RECONNECT_MAX: std::time::Duration = std::time::Duration::from_secs(10);

/// Opens the `BeamFnLogging` stream, and reopens it whenever it fails.
///
/// The stream opens on a background task and the client returns at once. Awaiting it would
/// deadlock: the server never sends on `BeamFnLogging`, so it never flushes its response
/// headers and the call never resolves. The outbound half still sends log entries.
///
/// Logging is best-effort: a worker that cannot log can still process bundles, so failures
/// are logged locally and never propagate. Entries logged while disconnected wait in the
/// client's bounded queue.
fn connect_logging(endpoint: &str, worker_id: &str) -> LoggingClient {
    let (tx, mut rx) = mpsc::channel(LOG_QUEUE_BATCHES);
    let endpoint = endpoint.to_string();
    let worker_id = worker_id.to_string();

    tokio::spawn(async move {
        let mut backoff = LOG_RECONNECT_MIN;
        loop {
            let started = std::time::Instant::now();
            let Err(e) = stream_logs(&endpoint, &worker_id, &mut rx).await else {
                return;
            };
            // A stream that stayed up a while starts the backoff over.
            if started.elapsed() > LOG_RECONNECT_MAX {
                backoff = LOG_RECONNECT_MIN;
            }
            warn!("BeamFnLogging stream to '{endpoint}' failed: {e}; reconnecting in {backoff:?}");
            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(LOG_RECONNECT_MAX);
        }
    });

    LoggingClient::new(tx)
}

/// Forwards `batches` over one `BeamFnLogging` stream: `Ok` once the client is gone, `Err`
/// when the stream fails. Unsent batches stay in `batches` for the next stream.
async fn stream_logs(
    endpoint: &str,
    worker_id: &str,
    batches: &mut mpsc::Receiver<log_entry::List>,
) -> Result<(), String> {
    let channel = grpc::channel(endpoint).await.map_err(|e| e.to_string())?;
    let mut client = BeamFnLoggingClient::new(channel)
        .max_decoding_message_size(grpc::MAX_MESSAGE_BYTES)
        .max_encoding_message_size(grpc::MAX_MESSAGE_BYTES);
    let (stream_tx, stream_rx) = mpsc::channel(1);
    let mut request = tonic::Request::new(ReceiverStream::new(stream_rx));
    attach_worker_id(&mut request, worker_id);

    info!("Streaming worker logs to BeamFnLogging at '{endpoint}'");
    let call = async {
        let mut inbound = client
            .logging(request)
            .await
            .map_err(|e| format!("rejected by runner: {e}"))?
            .into_inner();
        // The runner may push back on the log stream; draining keeps it healthy.
        while inbound
            .message()
            .await
            .map_err(|e| e.to_string())?
            .is_some()
        {}
        Err::<(), String>("closed by runner".to_string())
    };
    tokio::pin!(call);
    loop {
        tokio::select! {
            ended = &mut call => return ended,
            sent = async {
                let permit = stream_tx
                    .reserve()
                    .await
                    .map_err(|_| "request stream closed".to_string())?;
                Ok::<_, String>(batches.recv().await.map(|batch| permit.send(batch)))
            } => match sent? {
                Some(()) => {}
                None => return Ok(()),
            },
        }
    }
}

/// gRPC connector establishing dynamic named data streams for `BeamFnData`.
pub struct GrpcDataStreamConnector {
    client: BeamFnDataClient<Channel>,
    worker_id: String,
    state: Arc<tokio::sync::Mutex<DataChannelState>>,
}

impl GrpcDataStreamConnector {
    pub fn new(
        client: BeamFnDataClient<Channel>,
        worker_id: String,
        state: Arc<tokio::sync::Mutex<DataChannelState>>,
    ) -> Self {
        Self {
            client,
            worker_id,
            state,
        }
    }
}

#[async_trait::async_trait]
impl DataStreamConnector for GrpcDataStreamConnector {
    async fn connect(&self, data_stream_id: &str) -> Result<mpsc::Sender<Elements>, DataError> {
        let (tx, rx) = mpsc::channel::<Elements>(OUTBOUND_QUEUE_MESSAGES);
        let mut req = tonic::Request::new(ReceiverStream::new(rx));
        attach_worker_id(&mut req, &self.worker_id);
        attach_header(&mut req, "data_stream_id", data_stream_id);

        let mut client = self.client.clone();
        let response = client.data(req).await.map_err(DataError::Connection)?;

        let mut inbound_stream = response.into_inner();
        let state = self.state.clone();
        let stream_id = data_stream_id.to_string();

        tokio::spawn(async move {
            debug!("Inbound listener started for named data stream '{stream_id}'");
            let reason = loop {
                match inbound_stream.message().await {
                    Ok(Some(elements)) => deliver(&state, elements).await,
                    Ok(None) => break format!("Named data stream '{stream_id}' closed by runner"),
                    Err(e) => break format!("Named data stream '{stream_id}' error: {e}"),
                }
            };
            error!("{reason}");
            // Inbound queues are keyed by instruction only, so this stream's bundles cannot be
            // told apart; fail them all and let the runner retry.
            state.lock().await.fail_all(&reason);
        });

        Ok(tx)
    }
}
