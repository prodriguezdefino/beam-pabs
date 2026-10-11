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

//! Live memory of the worker: its resident size against the memory limit of its container.
//!
//! A task samples the resident size at intervals and keeps the peak. Near the limit it logs a
//! warning with the active bundles, so the logs show what ran before the kernel ends the
//! worker. It does not stop the worker.

use std::sync::Weak;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use beam::internals::process_wide_container;
use tracing::warn;

use crate::bundle_processor::BundleProcessor;

/// How often the monitor samples the memory.
pub const SAMPLE_INTERVAL: Duration = Duration::from_secs(10);
/// The share of the limit, in percent, above which the monitor warns.
pub const WARNING_PERCENT: u64 = 90;
/// The shortest time between two warnings.
const WARNING_INTERVAL: Duration = Duration::from_secs(60);
/// A cgroup v1 limit at or above this value means no limit.
const NO_LIMIT_BYTES: u64 = 1 << 60;

/// Metric namespace of the memory gauges that the worker reports with its harness metrics.
pub const MEMORY_METRIC_NAMESPACE: &str = "beam:worker:memory";

/// The last sample: resident size, peak and limit in bytes. 0 means unknown.
static RSS: AtomicU64 = AtomicU64::new(0);
static PEAK: AtomicU64 = AtomicU64::new(0);
static LIMIT: AtomicU64 = AtomicU64::new(0);

/// One memory sample of the worker.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct MemoryReading {
    /// The resident size, if the platform reports it.
    pub rss_bytes: Option<u64>,
    /// The largest resident size that the monitor saw.
    pub peak_rss_bytes: Option<u64>,
    /// The memory limit of the container, if it has one.
    pub limit_bytes: Option<u64>,
}

impl MemoryReading {
    /// True if the resident size is at least [`WARNING_PERCENT`] of the limit.
    pub fn near_limit(&self) -> bool {
        match (self.rss_bytes, self.limit_bytes) {
            (Some(rss), Some(limit)) => {
                u128::from(rss) * 100 >= u128::from(limit) * u128::from(WARNING_PERCENT)
            }
            (None, _) | (_, None) => false,
        }
    }
}

fn known(value: u64) -> Option<u64> {
    (value > 0).then_some(value)
}

/// The last sample of the monitor. Before the first one, a sample taken now.
pub fn current() -> MemoryReading {
    match known(RSS.load(Ordering::Relaxed)) {
        Some(rss) => MemoryReading {
            rss_bytes: Some(rss),
            peak_rss_bytes: known(PEAK.load(Ordering::Relaxed)),
            limit_bytes: known(LIMIT.load(Ordering::Relaxed)),
        },
        None => sample(),
    }
}

/// Reads the resident size and the limit, and records them as the last sample.
fn sample() -> MemoryReading {
    let rss = read_rss();
    let limit = read_limit();
    RSS.store(rss.unwrap_or(0), Ordering::Relaxed);
    LIMIT.store(limit.unwrap_or(0), Ordering::Relaxed);
    let peak = rss.map(|rss| PEAK.fetch_max(rss, Ordering::Relaxed).max(rss));
    MemoryReading {
        rss_bytes: rss,
        peak_rss_bytes: peak,
        limit_bytes: limit,
    }
}

/// The resident size from the `VmRSS` line of `/proc/<pid>/status` content.
pub fn parse_status_rss(status: &str) -> Option<u64> {
    status
        .lines()
        .find_map(|line| line.strip_prefix("VmRSS:"))
        .and_then(|rest| rest.trim().strip_suffix("kB"))
        .and_then(|kib| kib.trim().parse::<u64>().ok())
        .map(|kib| kib * 1024)
}

/// A cgroup memory limit from the content of `memory.max` (v2) or `memory.limit_in_bytes`
/// (v1). `max` and the v1 value for no limit give `None`.
pub fn parse_cgroup_limit(content: &str) -> Option<u64> {
    content
        .trim()
        .parse::<u64>()
        .ok()
        .filter(|&bytes| bytes > 0 && bytes < NO_LIMIT_BYTES)
}

fn read_rss() -> Option<u64> {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .as_deref()
        .and_then(parse_status_rss)
}

fn read_limit() -> Option<u64> {
    [
        "/sys/fs/cgroup/memory.max",
        "/sys/fs/cgroup/memory/memory.limit_in_bytes",
    ]
    .iter()
    .find_map(|path| std::fs::read_to_string(path).ok())
    .as_deref()
    .and_then(parse_cgroup_limit)
}

/// Starts the monitor. It stops at the first sample after `bundles` is dropped.
pub fn spawn_memory_monitor(bundles: Weak<BundleProcessor>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(SAMPLE_INTERVAL);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut last_warning: Option<Instant> = None;
        loop {
            ticker.tick().await;
            let Some(processor) = bundles.upgrade() else {
                return;
            };
            let reading = sample();
            report(&reading);
            if reading.near_limit()
                && last_warning.is_none_or(|at| at.elapsed() >= WARNING_INTERVAL)
            {
                last_warning = Some(Instant::now());
                warn_near_limit(&reading, &processor);
            }
        }
    })
}

/// Records the sample as process-wide gauges, which the runner reads as harness metrics.
fn report(reading: &MemoryReading) {
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |since| {
            i64::try_from(since.as_millis()).unwrap_or(i64::MAX)
        });
    let gauges = [
        ("rss_bytes", reading.rss_bytes),
        ("limit_bytes", reading.limit_bytes),
    ];
    gauges
        .into_iter()
        .filter_map(|(name, bytes)| Some((name, i64::try_from(bytes?).ok()?)))
        .for_each(|(name, bytes)| {
            process_wide_container().set_gauge("", MEMORY_METRIC_NAMESPACE, name, bytes, now_ms);
        });
}

fn warn_near_limit(reading: &MemoryReading, processor: &BundleProcessor) {
    let bundles = processor
        .active_bundle_reports()
        .iter()
        .map(|report| {
            format!(
                "{} ({})",
                report.instruction_id,
                report.current_transform.as_deref().unwrap_or("-")
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    warn!(
        "Worker memory is near the container limit: {} of {} resident (peak {}). If it keeps \
         growing, the kernel ends the worker. Active bundles (transform): [{bundles}]",
        mib(reading.rss_bytes),
        mib(reading.limit_bytes),
        mib(reading.peak_rss_bytes),
    );
}

/// `bytes` in MiB for logs and the status page, or `unknown`.
pub fn mib(bytes: Option<u64>) -> String {
    bytes.map_or_else(|| "unknown".to_string(), |b| format!("{} MiB", b >> 20))
}
