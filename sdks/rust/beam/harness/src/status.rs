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
use std::time::{Duration, Instant};

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

    out.push_str("\n============Threads============\n");
    format_threads(&mut out);

    out.push_str("\n============Caches============\n");
    format_caches(&mut out, bundles);

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
    let reading = crate::memory::current();
    let _ = writeln!(
        out,
        "Resident: {}, Peak: {}, Limit: {}{}",
        crate::memory::mib(reading.rss_bytes),
        crate::memory::mib(reading.peak_rss_bytes),
        crate::memory::mib(reading.limit_bytes),
        if reading.near_limit() {
            " (near the limit)"
        } else {
            ""
        },
    );
    #[cfg(target_os = "linux")]
    if let Ok(content) = std::fs::read_to_string("/proc/self/status") {
        const PREFIXES: &[&str] = &["VmSize:", "VmPeak:", "VmHWM:"];
        content
            .lines()
            .filter(|l| PREFIXES.iter().any(|p| l.starts_with(p)))
            .for_each(|line| {
                let _ = writeln!(out, "{}", line.trim());
            });
    }
}

/// The threads of the process by name and state, with how many share both.
fn format_threads(out: &mut String) {
    match thread_states() {
        Some(threads) => {
            let total: usize = threads.values().sum();
            let _ = writeln!(out, "Threads: {total}");
            threads.iter().for_each(|((name, state), count)| {
                let _ = writeln!(out, "  {name} ({state}) x{count}");
            });
        }
        None => out.push_str("Thread list: not available on this platform\n"),
    }
    match tokio::runtime::Handle::try_current() {
        Ok(runtime) => {
            let metrics = runtime.metrics();
            let _ = writeln!(
                out,
                "Tokio runtime: {} workers, {} alive tasks, {} tasks in the global queue",
                metrics.num_workers(),
                metrics.num_alive_tasks(),
                metrics.global_queue_depth()
            );
        }
        Err(_) => out.push_str("Tokio runtime: none on this thread\n"),
    }
}

/// The name and the state of each thread from `/proc/self/task`, counted. `None` where the
/// platform has no `/proc`.
fn thread_states() -> Option<std::collections::BTreeMap<(String, String), usize>> {
    let tasks = std::fs::read_dir("/proc/self/task").ok()?;
    Some(
        tasks
            .filter_map(Result::ok)
            .filter_map(|task| {
                let status = std::fs::read_to_string(task.path().join("status")).ok()?;
                let field = |key: &str| {
                    status
                        .lines()
                        .find_map(|line| line.strip_prefix(key))
                        .map(|value| value.trim().to_string())
                };
                Some((field("Name:")?, field("State:").unwrap_or_default()))
            })
            .fold(std::collections::BTreeMap::new(), |mut counts, thread| {
                *counts.entry(thread).or_insert(0) += 1;
                counts
            }),
    )
}

/// The state cache, the idle bundle processors and the short ids.
fn format_caches(out: &mut String, bundles: Option<&BundleProcessor>) {
    let Some(bundles) = bundles else {
        out.push_str("Caches: no bundle processor\n");
        return;
    };
    let cache = bundles.state_cache_stats();
    let _ = writeln!(
        out,
        "State cache: {} entries, {} of {} bytes, {} hits, {} misses, {} evictions",
        cache.entries, cache.bytes, cache.max_bytes, cache.hits, cache.misses, cache.evictions
    );
    let idle = bundles.idle_processors();
    if idle.is_empty() {
        out.push_str("Idle bundle processors: none\n");
    } else {
        let listed = idle
            .iter()
            .map(|(descriptor, count)| format!("{descriptor}={count}"))
            .collect::<Vec<_>>()
            .join(", ");
        let _ = writeln!(out, "Idle bundle processors: {listed}");
    }
    let _ = writeln!(out, "Short ids: {}", bundles.short_id_count());
}

/// The first wait before the status stream reconnects. It doubles up to [`RECONNECT_MAX`].
const RECONNECT_MIN: Duration = Duration::from_secs(1);
const RECONNECT_MAX: Duration = Duration::from_secs(30);

/// What a status stream needs to answer requests, across reconnects.
struct StatusStream {
    endpoint: String,
    worker_id: String,
    start_time: Instant,
    metrics: WorkerMetrics,
    bundles: Weak<BundleProcessor>,
    shutdown: Arc<AtomicBool>,
}

impl StatusStream {
    fn stopped(&self) -> bool {
        self.shutdown.load(Ordering::SeqCst)
    }

    /// Opens one stream and answers its requests until it ends. `Ok` if the stream opened.
    async fn serve(&self) -> Result<(), String> {
        let channel = grpc::channel(&self.endpoint)
            .await
            .map_err(|e| format!("cannot connect: {e}"))?;
        let mut client = BeamFnWorkerStatusClient::new(channel)
            .max_decoding_message_size(grpc::MAX_MESSAGE_BYTES)
            .max_encoding_message_size(grpc::MAX_MESSAGE_BYTES);
        let (tx, rx) = mpsc::unbounded_channel::<WorkerStatusResponse>();
        let mut request = tonic::Request::new(UnboundedReceiverStream::new(rx));
        grpc::attach_worker_id(&mut request, &self.worker_id);
        let mut inbound = client
            .worker_status(request)
            .await
            .map_err(|e| format!("the runner rejected the stream: {e}"))?
            .into_inner();
        info!(
            "Connected to BeamFnWorkerStatus at '{}' (worker_id: '{}')",
            self.endpoint, self.worker_id
        );
        while !self.stopped() {
            match inbound.message().await {
                Ok(Some(req)) => {
                    debug!("Received WorkerStatusRequest id='{}'", req.id);
                    let status_info = format_status_info(
                        self.start_time,
                        &self.metrics,
                        self.bundles.upgrade().as_deref(),
                    );
                    let response = WorkerStatusResponse {
                        id: req.id,
                        error: String::new(),
                        status_info,
                    };
                    if tx.send(response).is_err() {
                        return Ok(());
                    }
                }
                Ok(None) => {
                    debug!("BeamFnWorkerStatus stream closed by runner");
                    return Ok(());
                }
                Err(e) if self.stopped() => {
                    debug!("BeamFnWorkerStatus stream ended at shutdown: {e}");
                    return Ok(());
                }
                Err(e) => return Err(format!("the stream failed: {e}")),
            }
        }
        Ok(())
    }
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
            let stream = StatusStream {
                endpoint: endpoint_str,
                worker_id: worker_id_str,
                start_time,
                metrics,
                bundles,
                shutdown: shutdown_clone,
            };
            let mut backoff = RECONNECT_MIN;
            while !stream.stopped() {
                match stream.serve().await {
                    // The stream was open, so the next one may open quickly.
                    Ok(()) => backoff = RECONNECT_MIN,
                    Err(e) => warn!("BeamFnWorkerStatus at '{}': {e}", stream.endpoint),
                }
                if stream.stopped() {
                    return;
                }
                debug!("Reconnecting to BeamFnWorkerStatus in {backoff:?}");
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(RECONNECT_MAX);
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
