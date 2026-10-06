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

//! Beam Fn Control instruction dispatch: caches registered descriptors, runs bundles with
//! [`BundleProcessor`] and runs bundle finalization callbacks.

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{Instrument, debug, error, warn};

use crate::bundle_processor::BundleProcessor;
use model::fn_execution::{
    FinalizeBundleResponse, GetProcessBundleDescriptorRequest, HarnessMonitoringInfosResponse,
    InstructionRequest, InstructionResponse, MonitoringInfosMetadataResponse,
    ProcessBundleDescriptor, RegisterResponse, SampleDataResponse,
    beam_fn_control_client::BeamFnControlClient, instruction_request::Request,
    instruction_response::Response,
};
use tonic::transport::Channel;

/// Client managing the Beam Fn Control plane.
#[derive(Clone)]
pub struct ControlClient {
    descriptors: Arc<Mutex<HashMap<String, ProcessBundleDescriptor>>>,
    bundle_processor: Arc<BundleProcessor>,
    ctrl_client: Option<BeamFnControlClient<Channel>>,
    worker_id: String,
    short_id_cache: beam::metrics::ShortIdCache,
    metrics: crate::status::WorkerMetrics,
}

impl ControlClient {
    pub fn new(bundle_processor: Arc<BundleProcessor>) -> Self {
        let short_id_cache = bundle_processor.short_id_cache().clone();
        Self {
            descriptors: Arc::new(Mutex::new(HashMap::new())),
            bundle_processor,
            ctrl_client: None,
            worker_id: String::new(),
            short_id_cache,
            metrics: crate::status::WorkerMetrics::new(),
        }
    }

    pub fn with_client(
        bundle_processor: Arc<BundleProcessor>,
        ctrl_client: BeamFnControlClient<Channel>,
        worker_id: String,
    ) -> Self {
        let short_id_cache = bundle_processor.short_id_cache().clone();
        Self {
            descriptors: Arc::new(Mutex::new(HashMap::new())),
            bundle_processor,
            ctrl_client: Some(ctrl_client),
            worker_id,
            short_id_cache,
            metrics: crate::status::WorkerMetrics::new(),
        }
    }

    pub fn with_metrics(mut self, metrics: crate::status::WorkerMetrics) -> Self {
        self.metrics = metrics;
        self
    }

    pub fn metrics(&self) -> &crate::status::WorkerMetrics {
        &self.metrics
    }

    pub async fn handle_instruction(&self, request: InstructionRequest) -> InstructionResponse {
        let instruction_id = request.instruction_id.clone();
        let span = tracing::debug_span!("instruction", instruction_id = %instruction_id);
        self.handle_instruction_inner(instruction_id, request)
            .instrument(span)
            .await
    }

    async fn handle_instruction_inner(
        &self,
        instruction_id: String,
        request: InstructionRequest,
    ) -> InstructionResponse {
        debug!(
            "Instruction '{}': {}",
            instruction_id,
            RequestDesc(request.request.as_ref())
        );

        match request.request {
            Some(Request::Register(reg)) => {
                let mut map = self.descriptors.lock().await;
                for pbd in reg.process_bundle_descriptor {
                    debug!(
                        "Registered ProcessBundleDescriptor '{}' (transforms: {}, coders: {})",
                        pbd.id,
                        pbd.transforms.len(),
                        pbd.coders.len()
                    );
                    map.insert(pbd.id.clone(), pbd);
                }
                self.metrics.set_descriptors_count(map.len());
                ok_response(instruction_id, Response::Register(RegisterResponse {}))
            }

            Some(Request::ProcessBundle(pb_req)) => {
                self.execute_bundle(
                    instruction_id,
                    &pb_req.process_bundle_descriptor_id,
                    &pb_req.data_stream_id,
                )
                .await
            }

            Some(Request::FinalizeBundle(finalize_req)) => {
                let target_id = if finalize_req.instruction_id.is_empty() {
                    &instruction_id
                } else {
                    &finalize_req.instruction_id
                };
                match self.bundle_processor.finalize_bundle(target_id) {
                    Ok(()) => {
                        debug!("Finalized bundle instruction '{}'", target_id);
                        ok_response(
                            instruction_id,
                            Response::FinalizeBundle(FinalizeBundleResponse {}),
                        )
                    }
                    Err(err) => {
                        warn!(
                            "Finalization error for bundle instruction '{}': {}",
                            target_id, err
                        );
                        error_response(instruction_id, format!("FinalizeBundle failed: {err}"))
                    }
                }
            }

            Some(Request::ProcessBundleProgress(prog_req)) => {
                let progress = self
                    .bundle_processor
                    .get_bundle_progress(&prog_req.instruction_id)
                    .unwrap_or_default();
                ok_response(instruction_id, Response::ProcessBundleProgress(progress))
            }

            Some(Request::ProcessBundleSplit(split_req)) => ok_response(
                instruction_id,
                Response::ProcessBundleSplit(self.bundle_processor.try_split(&split_req)),
            ),

            Some(Request::MonitoringInfos(req)) => {
                let monitoring_info = self.short_id_cache.get_infos(&req.monitoring_info_id);
                debug!(
                    "Resolved {} of {} requested monitoring info(s)",
                    monitoring_info.len(),
                    req.monitoring_info_id.len()
                );
                ok_response(
                    instruction_id,
                    Response::MonitoringInfos(MonitoringInfosMetadataResponse { monitoring_info }),
                )
            }

            Some(Request::HarnessMonitoringInfos(_)) => ok_response(
                instruction_id,
                Response::HarnessMonitoringInfos(HarnessMonitoringInfosResponse::default()),
            ),

            Some(Request::SampleData(_)) => ok_response(
                instruction_id,
                Response::SampleData(SampleDataResponse::default()),
            ),

            other => {
                warn!("Received unhandled instruction request type: {:?}", other);
                error_response(
                    instruction_id,
                    "Unsupported instruction request type".to_string(),
                )
            }
        }
    }

    /// Runs the bundle, reporting failures on the instruction response instead of returning them.
    async fn execute_bundle(
        &self,
        instruction_id: String,
        descriptor_id: &str,
        data_stream_id: &str,
    ) -> InstructionResponse {
        let start = std::time::Instant::now();
        debug!(
            "Starting bundle instruction '{instruction_id}' (descriptor: '{descriptor_id}', data_stream: '{data_stream_id}')"
        );
        self.metrics.record_bundle_start();

        let resp = if let Some(descriptor) = self.resolve_descriptor(descriptor_id).await {
            match self
                .bundle_processor
                .process_bundle(&instruction_id, &descriptor, data_stream_id)
                .await
            {
                Ok(resp) => {
                    debug!(
                        "Completed bundle instruction '{instruction_id}' in {:?}",
                        start.elapsed()
                    );
                    ok_response(instruction_id, Response::ProcessBundle(resp))
                }
                Err(e) => {
                    error!(
                        "Failed bundle instruction '{instruction_id}' after {:?}: {e:?}",
                        start.elapsed()
                    );
                    error_response(instruction_id, format!("Bundle execution failed: {e}"))
                }
            }
        } else {
            let err = format!("Unknown ProcessBundleDescriptor '{descriptor_id}'");
            error!("{err}");
            error_response(instruction_id, err)
        };

        self.metrics.record_bundle_finish();
        resp
    }

    /// The registered descriptor, fetched from the runner and cached if it was never registered.
    async fn resolve_descriptor(&self, descriptor_id: &str) -> Option<ProcessBundleDescriptor> {
        if let Some(cached) = self.descriptors.lock().await.get(descriptor_id).cloned() {
            return Some(cached);
        }

        let mut client = self.ctrl_client.clone()?;
        let mut req = tonic::Request::new(GetProcessBundleDescriptorRequest {
            process_bundle_descriptor_id: descriptor_id.to_string(),
        });
        crate::grpc::attach_worker_id(&mut req, &self.worker_id);

        let desc = client
            .get_process_bundle_descriptor(req)
            .await
            .map_err(|e| warn!("Failed to fetch ProcessBundleDescriptor '{descriptor_id}': {e:?}"))
            .ok()?
            .into_inner();

        debug!("Fetched ProcessBundleDescriptor '{}' on demand", desc.id);
        let mut map = self.descriptors.lock().await;
        map.insert(desc.id.clone(), desc.clone());
        self.metrics.set_descriptors_count(map.len());
        Some(desc)
    }
}

fn ok_response(instruction_id: String, response: Response) -> InstructionResponse {
    InstructionResponse {
        instruction_id,
        error: String::new(),
        response: Some(response),
    }
}

fn error_response(instruction_id: String, error: String) -> InstructionResponse {
    InstructionResponse {
        instruction_id,
        error,
        response: None,
    }
}

/// Zero-allocation Display helper formatting instruction requests for tracing.
struct RequestDesc<'a>(Option<&'a Request>);

impl std::fmt::Display for RequestDesc<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.0 {
            Some(Request::Register(r)) => {
                write!(
                    f,
                    "Register({} descriptors)",
                    r.process_bundle_descriptor.len()
                )
            }
            Some(Request::ProcessBundle(r)) => {
                write!(
                    f,
                    "ProcessBundle(desc='{}')",
                    r.process_bundle_descriptor_id
                )
            }
            Some(Request::ProcessBundleProgress(r)) => {
                write!(f, "ProcessBundleProgress(target='{}')", r.instruction_id)
            }
            Some(Request::ProcessBundleSplit(r)) => {
                write!(f, "ProcessBundleSplit(target='{}')", r.instruction_id)
            }
            Some(Request::FinalizeBundle(r)) => {
                write!(f, "FinalizeBundle(target='{}')", r.instruction_id)
            }
            Some(Request::MonitoringInfos(_)) => write!(f, "MonitoringInfos"),
            Some(Request::HarnessMonitoringInfos(_)) => write!(f, "HarnessMonitoringInfos"),
            Some(Request::SampleData(_)) => write!(f, "SampleData"),
            None => write!(f, "None"),
        }
    }
}
