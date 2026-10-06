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

//! Worker side of `BeamFnWorkerStatus`: answers each `WorkerStatusRequest` with runtime
//! statistics (memory, OS and architecture, uptime, active bundles, build info).

use std::collections::HashMap;
use std::fmt::Write;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Weak};
use std::time::Instant;

use beam::internals::{ShortIdCache, process_wide_container};
use model::fn_execution::WorkerStatusResponse;
use model::fn_execution::beam_fn_worker_status_client::BeamFnWorkerStatusClient;
use tokio::sync::mpsc;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tracing::{debug, info, warn};

use crate::bundle_processor::BundleProcessor;
use crate::grpc;

/// Collects process-wide monitoring infos into short ID payloads with the short ID cache.
pub fn collect_harness_monitoring_data(short_id_cache: &ShortIdCache) -> HashMap<String, Vec<u8>> {
    process_wide_container()
        .to_monitoring_infos()
        .into_iter()
        .map(|info| {
            let short_id = short_id_cache.get_or_create_short_id(&info);
            (short_id, info.payload)
        })
        .collect()
}

/// Live metrics reported in `WorkerStatusResponse`.
#[derive(Debug, Default, Clone)]
pub struct WorkerMetrics {
    active_bundles: Arc<AtomicUsize>,
    total_bundles: Arc<AtomicU64>,
    descriptors_count: Arc<AtomicUsize>,
}

impl WorkerMetrics {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_bundle_start(&self) {
        self.active_bundles.fetch_add(1, Ordering::SeqCst);
    }

    pub fn record_bundle_finish(&self) {
        self.active_bundles.fetch_sub(1, Ordering::SeqCst);
        self.total_bundles.fetch_add(1, Ordering::SeqCst);
    }

    pub fn set_descriptors_count(&self, count: usize) {
        self.descriptors_count.store(count, Ordering::SeqCst);
    }

    pub fn active_bundles(&self) -> usize {
        self.active_bundles.load(Ordering::SeqCst)
    }

    pub fn total_bundles(&self) -> u64 {
        self.total_bundles.load(Ordering::SeqCst)
    }

    pub fn descriptors_count(&self) -> usize {
        self.descriptors_count.load(Ordering::SeqCst)
    }
}

/// Formats the status report. With `bundles`, it lists each active bundle, longest idle first,
/// and the open named data streams.
pub fn format_status_info(
    start_time: Instant,
    metrics: &WorkerMetrics,
    bundles: Option<&BundleProcessor>,
) -> String {
    let mut out = String::with_capacity(1024);

    out.push_str("============Memory Usage============\n");
    format_memory_usage(&mut out);

    out.push_str("\n============Process & Runtime============\n");
    let uptime = start_time.elapsed().as_secs();
    let pid = std::process::id();
    let os = std::env::consts::OS;
    let arch = std::env::consts::ARCH;
    let cpus = std::thread::available_parallelism()
        .map(|p| p.get())
        .unwrap_or(1);
    let _ = writeln!(
        out,
        "PID: {pid}\nUptime: {uptime}s\nOS: {os}\nArchitecture: {arch}\nAvailable Parallelism: {cpus}"
    );

    out.push_str("\n============Active Process Bundle States============\n");
    let _ = writeln!(
        out,
        "Active Bundles: {}\nTotal Bundles Processed: {}\nRegistered Descriptors: {}",
        metrics.active_bundles(),
        metrics.total_bundles(),
        metrics.descriptors_count()
    );

    if let Some(bundles) = bundles {
        let _ = writeln!(
            out,
            "Expired Finalization Callbacks: {}",
            bundles.expired_finalization_count()
        );
    }

    let active_reports = bundles
        .map(BundleProcessor::active_bundle_reports)
        .unwrap_or_default();

    for report in &active_reports {
        let transform = report.current_transform.as_deref().unwrap_or("-");
        let _ = writeln!(
            out,
            "Instruction: {}, Descriptor: {}, Transform: {}, Lull: {:?}, Elements Read: {}, Phase: {}",
            report.instruction_id,
            report.descriptor_id,
            transform,
            report.lull,
            report.elements_read,
            report.phase,
        );
    }

    out.push_str("\n============Data & State Streams============\n");
    let named_streams = bundles
        .map(BundleProcessor::open_named_streams)
        .unwrap_or_default();
    if named_streams.is_empty() {
        out.push_str("Open Named Data Streams: none\n");
    } else {
        let _ = writeln!(out, "Open Named Data Streams: {}", named_streams.join(", "));
    }

    out.push_str("\n============Build Info============\n");
    out.push_str("Package: Apache Beam Rust SDK\n");
    let _ = writeln!(out, "Version: {}", beam::pipeline::BEAM_SDK_VERSION);

    out
}

fn format_memory_usage(out: &mut String) {
    #[cfg(target_os = "linux")]
    {
        if let Ok(content) = std::fs::read_to_string("/proc/self/status") {
            const PREFIXES: &[&str] = &["VmRSS:", "VmSize:", "VmPeak:", "Threads:"];
            let mut found = false;
            for line in content
                .lines()
                .filter(|l| PREFIXES.iter().any(|p| l.starts_with(p)))
            {
                out.push_str(line.trim());
                out.push('\n');
                found = true;
            }
            if found {
                return;
            }
        }
    }
    out.push_str("Memory usage: not available on this platform\n");
}

/// Handle to an active WorkerStatus client loop.
pub struct WorkerStatusHandler {
    shutdown: Arc<AtomicBool>,
    handle: Option<tokio::task::JoinHandle<()>>,
}

impl WorkerStatusHandler {
    /// Connects to the runner's BeamFnWorkerStatus endpoint and handles status requests in the background.
    /// The report reads active bundles from `bundles` while the processor exists.
    pub fn connect(
        endpoint: &str,
        worker_id: &str,
        metrics: WorkerMetrics,
        bundles: Weak<BundleProcessor>,
    ) -> Self {
        let shutdown = Arc::new(AtomicBool::new(false));
        let shutdown_clone = Arc::clone(&shutdown);
        let endpoint_str = endpoint.to_string();
        let worker_id_str = worker_id.to_string();
        let start_time = Instant::now();

        let handle = tokio::spawn(async move {
            let channel = match grpc::channel(&endpoint_str).await {
                Ok(chan) => chan,
                Err(e) => {
                    warn!("Could not connect to WorkerStatus endpoint '{endpoint_str}': {e}");
                    return;
                }
            };

            let mut client = BeamFnWorkerStatusClient::new(channel)
                .max_decoding_message_size(grpc::MAX_MESSAGE_BYTES)
                .max_encoding_message_size(grpc::MAX_MESSAGE_BYTES);

            let (tx, rx) = mpsc::unbounded_channel::<WorkerStatusResponse>();
            let out_stream = UnboundedReceiverStream::new(rx);
            let mut request = tonic::Request::new(out_stream);

            grpc::attach_worker_id(&mut request, &worker_id_str);

            info!(
                "Connected to BeamFnWorkerStatus at '{endpoint_str}' (worker_id: '{worker_id_str}')"
            );
            let mut inbound = match client.worker_status(request).await {
                Ok(resp) => resp.into_inner(),
                Err(e) => {
                    warn!("BeamFnWorkerStatus stream rejected by runner at '{endpoint_str}': {e}");
                    return;
                }
            };

            while !shutdown_clone.load(Ordering::SeqCst) {
                match inbound.message().await {
                    Ok(Some(req)) => {
                        debug!(
                            "Received WorkerStatusRequest id='{}', sending diagnostics response",
                            req.id
                        );
                        let status_info =
                            format_status_info(start_time, &metrics, bundles.upgrade().as_deref());
                        let resp = WorkerStatusResponse {
                            id: req.id,
                            error: String::new(),
                            status_info,
                        };
                        if let Err(e) = tx.send(resp) {
                            warn!("Failed to send WorkerStatusResponse: {e}");
                            break;
                        }
                    }
                    Ok(None) => {
                        debug!("BeamFnWorkerStatus stream closed by runner");
                        break;
                    }
                    Err(e) => {
                        if !shutdown_clone.load(Ordering::SeqCst) {
                            warn!("Error reading from BeamFnWorkerStatus stream: {e}");
                        }
                        break;
                    }
                }
            }
        });

        Self {
            shutdown,
            handle: Some(handle),
        }
    }

    /// Signals the status handler loop to stop and aborts the background task.
    pub fn stop(&self) {
        self.shutdown.store(true, Ordering::SeqCst);
        if let Some(handle) = self.handle.as_ref() {
            handle.abort();
        }
    }
}

impl Drop for WorkerStatusHandler {
    fn drop(&mut self) {
        self.stop();
    }
}
