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

//! External Worker Pool gRPC service for running SDK workers in loopback mode.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::{Mutex, oneshot};
use tokio_stream::wrappers::TcpListenerStream;
use tracing::{info, warn};

use beam::internals::TransformFn;
use beam::options::HarnessOptions;
use harness::worker::Worker;
use model::fn_execution::beam_fn_external_worker_pool_server::{
    BeamFnExternalWorkerPool, BeamFnExternalWorkerPoolServer,
};
use model::fn_execution::{
    StartWorkerRequest, StartWorkerResponse, StopWorkerRequest, StopWorkerResponse,
};

#[derive(Error, Debug)]
pub enum WorkerPoolError {
    #[error("Failed to bind TCP listener: {0}")]
    Bind(#[from] std::io::Error),
    #[error("gRPC server error: {0}")]
    Tonic(#[from] tonic::transport::Error),
}

/// Internal gRPC service implementation for BeamFnExternalWorkerPool.
struct WorkerPoolService {
    workers: Arc<Mutex<HashMap<String, tokio::task::JoinHandle<()>>>>,
    transform_handlers: Arc<HashMap<String, TransformFn>>,
}

#[tonic::async_trait]
impl BeamFnExternalWorkerPool for WorkerPoolService {
    async fn start_worker(
        &self,
        request: tonic::Request<StartWorkerRequest>,
    ) -> Result<tonic::Response<StartWorkerResponse>, tonic::Status> {
        let req = request.into_inner();
        let worker_id = req.worker_id;
        let control_endpoint = req.control_endpoint.map(|e| e.url).unwrap_or_default();
        let logging_endpoint = req.logging_endpoint.map(|e| e.url);

        info!(
            "WorkerPool received StartWorker for '{}' (control: '{}')",
            worker_id, control_endpoint
        );

        let artifact_endpoint = req.artifact_endpoint.map(|e| e.url);
        let provision_endpoint = req.provision_endpoint.map(|e| e.url);

        let args = HarnessOptions {
            worker: true,
            id: Some(worker_id.clone()),
            control_endpoint: Some(control_endpoint),
            logging_endpoint,
            artifact_endpoint,
            provision_endpoint,
            ..Default::default()
        };

        let worker = Worker::with_handlers(args, (*self.transform_handlers).clone());

        let w_id = worker_id.clone();
        let handle = tokio::spawn(async move {
            if let Err(e) = worker.run().await {
                warn!("Worker '{}' terminated with error: {:?}", w_id, e);
            } else {
                info!("Worker '{}' exited cleanly", w_id);
            }
        });

        self.workers.lock().await.insert(worker_id, handle);

        Ok(tonic::Response::new(StartWorkerResponse {
            error: String::new(),
        }))
    }

    async fn stop_worker(
        &self,
        request: tonic::Request<StopWorkerRequest>,
    ) -> Result<tonic::Response<StopWorkerResponse>, tonic::Status> {
        let req = request.into_inner();
        info!("WorkerPool received StopWorker for '{}'", req.worker_id);

        if let Some(handle) = self.workers.lock().await.remove(&req.worker_id) {
            handle.abort();
        }

        Ok(tonic::Response::new(StopWorkerResponse {
            error: String::new(),
        }))
    }
}

/// Managing handle for a running external worker pool gRPC server.
pub struct WorkerPool {
    endpoint: String,
    port: u16,
    workers: Arc<Mutex<HashMap<String, tokio::task::JoinHandle<()>>>>,
    shutdown_tx: Option<oneshot::Sender<()>>,
}

impl WorkerPool {
    /// Starts the loopback worker pool server on an ephemeral or requested port.
    pub async fn start(
        requested_port: Option<u16>,
        transform_handlers: Arc<HashMap<String, TransformFn>>,
    ) -> Result<Self, WorkerPoolError> {
        let port = requested_port.unwrap_or(0);
        let bind_addr = SocketAddr::from(([127, 0, 0, 1], port));
        let listener = tokio::net::TcpListener::bind(bind_addr).await?;
        let actual_port = listener.local_addr()?.port();
        let endpoint = format!("127.0.0.1:{actual_port}");

        info!("WorkerPool listening on {}", endpoint);

        let workers = Arc::new(Mutex::new(HashMap::new()));
        let service = BeamFnExternalWorkerPoolServer::new(WorkerPoolService {
            workers: workers.clone(),
            transform_handlers,
        });

        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

        tokio::spawn(async move {
            let incoming = TcpListenerStream::new(listener);
            let _ = tonic::transport::Server::builder()
                .add_service(service)
                .serve_with_incoming_shutdown(incoming, async {
                    let _ = shutdown_rx.await;
                })
                .await;
        });

        Ok(Self {
            endpoint,
            port: actual_port,
            workers,
            shutdown_tx: Some(shutdown_tx),
        })
    }

    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    pub async fn stop(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        let mut map = self.workers.lock().await;
        map.drain().for_each(|(_, handle)| handle.abort());
    }
}

impl Drop for WorkerPool {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
    }
}
