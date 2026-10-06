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

//! Sampled execution time per transform.
//!
//! A clock read around each operator call costs two `Instant::now()` calls per element per
//! fused transform, truncates sub-millisecond calls to zero, and charges downstream work to
//! the caller, because pushes recurse. Instead, each bundle publishes the transform that it
//! runs, and one worker-wide thread charges each elapsed interval to that transform. This
//! costs a few relaxed atomic accesses per call and gives the exclusive time per transform.
//!
//! The same thread reports a bundle that stays in one transform call (no output, no return)
//! every [`LULL_WARNING`]. With `--element_processing_timeout_minutes`, the worker exits
//! when the timeout passes, so that the runner can retry the work elsewhere.

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::{Duration, Instant};

/// The interval at which the sampler charges elapsed time to the running transform.
const SAMPLING_PERIOD: Duration = Duration::from_millis(10);

/// The time that an element can stay in one transform call between two warnings.
const LULL_WARNING: Duration = Duration::from_secs(5 * 60);

/// Marks a bundle that is not in a transform call (it waits for data or does harness work).
const IDLE: usize = usize::MAX;

/// The configured `--element_processing_timeout_minutes`, if any.
static ELEMENT_PROCESSING_TIMEOUT: OnceLock<Duration> = OnceLock::new();

/// Makes the worker exit when an element stays `timeout` in one transform call with no output
/// and no return. Only the first call to this function has an effect.
pub fn set_element_processing_timeout(timeout: Duration) {
    let _ = ELEMENT_PROCESSING_TIMEOUT.set(timeout);
}

/// Execution state shared between one bundle and the sampler thread.
struct BundleState {
    instruction_id: String,
    operator_ids: Box<[String]>,
    /// The index of the transform that the bundle runs, or [`IDLE`].
    current: AtomicUsize,
    /// The number of transform calls entered. When it does not change, the element is stuck.
    transitions: AtomicU64,
    /// The time that the current call has run without entering another, at the last sample.
    lull_nanos: AtomicU64,
    /// Nanoseconds charged to each transform, by operator index.
    nanos: Box<[AtomicU64]>,
}

/// The view of the sampler thread on one bundle.
struct Watched {
    state: Weak<BundleState>,
    /// The `transitions` value at the last change, its time, and the warnings since then.
    seen: u64,
    since: Instant,
    warned: u32,
}

impl Watched {
    /// Charges `elapsed` to the running transform and checks if its element is stuck.
    /// Returns a message when the element exceeds the processing timeout.
    fn sample(&mut self, state: &BundleState, elapsed: u64, now: Instant) -> Option<String> {
        let current = state.current.load(Ordering::Relaxed);
        if let Some(slot) = state.nanos.get(current) {
            slot.fetch_add(elapsed, Ordering::Relaxed);
        }
        let transitions = state.transitions.load(Ordering::Relaxed);
        if transitions != self.seen || current == IDLE {
            (self.seen, self.since, self.warned) = (transitions, now, 0);
            state.lull_nanos.store(0, Ordering::Relaxed);
            return None;
        }
        let lull = now.duration_since(self.since);
        state.lull_nanos.store(
            u64::try_from(lull.as_nanos()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        let transform = state.operator_ids.get(current).map_or("?", String::as_str);
        let message = |what: &str| {
            format!(
                "{what} in transform '{transform}' of bundle '{}' for at least {}s without \
                 outputting or completing",
                state.instruction_id,
                lull.as_secs()
            )
        };
        if lull >= LULL_WARNING * (self.warned + 1) {
            self.warned += 1;
            tracing::warn!("{}", message("Operation ongoing"));
        }
        ELEMENT_PROCESSING_TIMEOUT
            .get()
            .filter(|&&timeout| lull > timeout)
            .map(|timeout| {
                message(&format!(
                    "Element processing exceeded --element_processing_timeout_minutes ({}m)",
                    timeout.as_secs() / 60
                ))
            })
    }
}

/// The bundles that the sampler watches. The next tick drops dead entries.
fn registry() -> &'static Mutex<Vec<Watched>> {
    static REGISTRY: OnceLock<Mutex<Vec<Watched>>> = OnceLock::new();
    REGISTRY.get_or_init(|| {
        spawn_sampler();
        Mutex::new(Vec::new())
    })
}

fn spawn_sampler() {
    let spawned = std::thread::Builder::new()
        .name("beam-execution-sampler".to_string())
        .spawn(|| {
            let mut last = Instant::now();
            loop {
                std::thread::sleep(SAMPLING_PERIOD);
                let now = Instant::now();
                // Charge the real sleep interval. Under load, it can be longer than the period.
                let elapsed = u64::try_from(now.duration_since(last).as_nanos()).unwrap_or(0);
                last = now;
                let Ok(mut bundles) = registry().lock() else {
                    return;
                };
                let mut timed_out = None;
                bundles.retain_mut(|watched| {
                    let Some(state) = watched.state.upgrade() else {
                        return false;
                    };
                    timed_out = timed_out.take().or(watched.sample(&state, elapsed, now));
                    true
                });
                drop(bundles);
                if let Some(message) = timed_out {
                    exit_worker(&message);
                }
            }
        });
    if let Err(e) = spawned {
        tracing::warn!("Execution-time sampler could not start; transform msecs will read 0: {e}");
    }
}

/// Ends the worker so that the runner retries its work.
fn exit_worker(message: &str) -> ! {
    tracing::error!("{message}. The SDK worker will be terminated.");
    // Give the logging stream time to send the reason.
    std::thread::sleep(Duration::from_secs(2));
    std::process::exit(1)
}

/// The view of one bundle on the sampler. Drop it to stop sampling the bundle.
pub struct ExecutionSampler {
    state: Arc<BundleState>,
}

impl ExecutionSampler {
    /// Starts sampling bundle `instruction_id`, which runs the transforms `operator_ids`.
    pub fn new(instruction_id: &str, operator_ids: &[String]) -> Self {
        let state = Arc::new(BundleState {
            instruction_id: instruction_id.to_string(),
            operator_ids: operator_ids.into(),
            current: AtomicUsize::new(IDLE),
            transitions: AtomicU64::new(0),
            lull_nanos: AtomicU64::new(0),
            nanos: operator_ids.iter().map(|_| AtomicU64::new(0)).collect(),
        });
        if let Ok(mut bundles) = registry().lock() {
            bundles.push(Watched {
                state: Arc::downgrade(&state),
                seen: 0,
                since: Instant::now(),
                warned: 0,
            });
        }
        Self { state }
    }

    /// Returns the time, at the last sample, that the running call has gone without entering
    /// another call, or `None` when the bundle is not in a call.
    pub fn time_in_current_call(&self) -> Option<Duration> {
        (self.state.current.load(Ordering::Relaxed) != IDLE)
            .then(|| Duration::from_nanos(self.state.lull_nanos.load(Ordering::Relaxed)))
    }

    /// Records that the bundle entered transform `index` and returns the state to restore with
    /// [`exit`](Self::exit). Only the bundle thread writes `current`, so a plain load and
    /// store are enough.
    #[inline]
    pub fn enter(&self, index: usize) -> usize {
        let state = &self.state;
        let previous = state.current.load(Ordering::Relaxed);
        state.current.store(index, Ordering::Relaxed);
        let transitions = state.transitions.load(Ordering::Relaxed);
        state.transitions.store(transitions + 1, Ordering::Relaxed);
        previous
    }

    /// Restores the state saved by [`enter`](Self::enter).
    #[inline]
    pub fn exit(&self, previous: usize) {
        self.state.current.store(previous, Ordering::Relaxed);
    }

    /// Returns the milliseconds charged to each transform that has at least one sample.
    pub fn msecs<'a>(
        &'a self,
        operator_ids: &'a [String],
    ) -> impl Iterator<Item = (&'a str, i64)> + 'a {
        operator_ids
            .iter()
            .zip(self.state.nanos.iter())
            .filter_map(|(t_id, nanos)| {
                let nanos = nanos.load(Ordering::Relaxed);
                (nanos > 0).then(|| {
                    (
                        t_id.as_str(),
                        i64::try_from(nanos / 1_000_000).unwrap_or(i64::MAX),
                    )
                })
            })
    }
}
