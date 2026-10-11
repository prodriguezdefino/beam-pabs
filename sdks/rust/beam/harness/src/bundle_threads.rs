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

//! Long-lived threads that run bundles and finalization callbacks.
//!
//! User code can block, so bundles do not run on the worker threads of the runtime. A pool
//! thread stays alive between bundles. The bundle future runs on the pool thread with
//! [`block_on`]. Before a pool thread sleeps, waiting for a job or for its bundle to be woken,
//! it spins for up to its [`SpinBudget`]: when waits are short, as for the data chunks of
//! small bundles, the thread then does not pay a wakeup for each one. When waits are long, as
//! on an idle worker, the budget falls to zero and the thread sleeps at once.

use std::cell::Cell;
use std::collections::VecDeque;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::pin::pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::task::{Context, Poll, Wake, Waker};
use std::thread::{self, Thread};
use std::time::{Duration, Instant};

/// The longest spin. A wait that is longer than this ends with sleep anyway.
const MAX_SPIN: Duration = Duration::from_micros(50);

/// The spin that a budget of zero grows to after a short wait.
const MIN_SPIN: Duration = Duration::from_micros(2);

/// How long a thread spins before it sleeps. It doubles after a wait that a spin of
/// [`MAX_SPIN`] would end, and halves after a longer wait, so spinning stops on a thread
/// whose waits it does not shorten.
#[derive(Clone, Copy)]
struct SpinBudget(Duration);

impl SpinBudget {
    const START: Self = Self(MAX_SPIN);

    fn after(self, waited: Duration) -> Self {
        match waited <= MAX_SPIN {
            true => Self((self.0 * 2).clamp(MIN_SPIN, MAX_SPIN)),
            false => Self(self.0 / 2),
        }
    }
}

thread_local! {
    /// The spin budget of this thread while it waits for a job.
    static JOB_SPIN: Cell<SpinBudget> = const { Cell::new(SpinBudget::START) };
    /// The spin budget of this thread while its bundle waits to be woken.
    static WAKE_SPIN: Cell<SpinBudget> = const { Cell::new(SpinBudget::START) };
}

/// Calls `ready` until it gives a value or `budget` ends; checks at least once.
fn spin<T>(budget: SpinBudget, mut ready: impl FnMut() -> Option<T>) -> Option<T> {
    let until = Instant::now() + budget.0;
    loop {
        if let Some(value) = ready() {
            return Some(value);
        }
        if Instant::now() >= until {
            return None;
        }
        std::hint::spin_loop();
    }
}

/// Waits with `wait` in the budget of `cell`, then updates the budget with the time waited.
fn adaptive<T>(
    cell: &'static thread::LocalKey<Cell<SpinBudget>>,
    wait: impl FnOnce(SpinBudget) -> T,
) -> T {
    let started = Instant::now();
    let budget = cell.get();
    let value = wait(budget);
    cell.set(budget.after(started.elapsed()));
    value
}

/// How long an idle pool thread lives without work.
const KEEP_ALIVE: Duration = Duration::from_secs(60);

/// Most threads in the pool. Beyond them, work waits for a thread to be free.
const MAX_THREADS: usize = 512;

type Job = Box<dyn FnOnce() + Send>;

/// The pool. Threads start when work arrives and none is idle.
#[derive(Clone, Default)]
pub(crate) struct BundleThreads {
    shared: Arc<Shared>,
}

#[derive(Default)]
struct Shared {
    state: Mutex<PoolState>,
    work: Condvar,
    /// The length of `state.jobs`, read without the lock by spinning threads.
    queued: AtomicUsize,
}

#[derive(Default)]
struct PoolState {
    jobs: VecDeque<Job>,
    threads: usize,
    idle: usize,
}

impl BundleThreads {
    /// Runs `work` on a pool thread and returns its result. A panic of `work` continues in
    /// the caller. `work` runs in the current tracing span and subscriber.
    pub(crate) async fn run<T: Send + 'static>(
        &self,
        work: impl FnOnce() -> T + Send + 'static,
    ) -> T {
        let (done, result) = tokio::sync::oneshot::channel();
        let span = tracing::Span::current();
        let dispatch = tracing::dispatcher::get_default(Clone::clone);
        self.submit(Box::new(move || {
            let outcome = std::panic::catch_unwind(AssertUnwindSafe(|| {
                tracing::dispatcher::with_default(&dispatch, || span.in_scope(work))
            }));
            // The caller may have stopped waiting; then nobody needs the result.
            let _ = done.send(outcome);
        }));
        match result
            .await
            .expect("a pool thread sends the outcome of every job it takes")
        {
            Ok(value) => value,
            Err(payload) => std::panic::resume_unwind(payload),
        }
    }

    fn submit(&self, job: Job) {
        let start_thread = {
            let mut state = self.shared.lock();
            state.jobs.push_back(job);
            self.shared.queued.fetch_add(1, Ordering::Release);
            let start = state.idle == 0 && state.threads < MAX_THREADS;
            if start {
                state.threads += 1;
            }
            start
        };
        if start_thread {
            let shared = Arc::clone(&self.shared);
            let started = thread::Builder::new()
                .name("beam-bundle".to_string())
                .spawn(move || shared.run_thread());
            if started.is_err() {
                // The queued job runs when a running thread is free.
                self.shared.lock().threads -= 1;
                tracing::error!("Cannot start a bundle thread; the bundle waits for a free one");
            }
        } else {
            self.shared.work.notify_one();
        }
    }
}

impl Shared {
    fn lock(&self) -> std::sync::MutexGuard<'_, PoolState> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Takes the next job, spinning in the budget of the thread and then sleeping up to
    /// [`KEEP_ALIVE`]. Returns `None` when the thread has been idle for that long and should end.
    fn next_job(&self) -> Option<Job> {
        adaptive(&JOB_SPIN, |budget| {
            spin(budget, || {
                (self.queued.load(Ordering::Acquire) > 0)
                    .then(|| self.take(&mut self.lock()))
                    .flatten()
            })
            .or_else(|| self.sleep_for_job())
        })
    }

    fn sleep_for_job(&self) -> Option<Job> {
        let mut state = self.lock();
        state.idle += 1;
        let (mut state, _) = self
            .work
            .wait_timeout_while(state, KEEP_ALIVE, |s| s.jobs.is_empty())
            .unwrap_or_else(|e| e.into_inner());
        state.idle -= 1;
        let job = self.take(&mut state);
        if job.is_none() {
            state.threads -= 1;
        }
        job
    }

    fn take(&self, state: &mut PoolState) -> Option<Job> {
        let job = state.jobs.pop_front();
        if job.is_some() {
            self.queued.fetch_sub(1, Ordering::Release);
        }
        job
    }

    fn run_thread(&self) {
        while let Some(job) = self.next_job() {
            job();
        }
    }
}

/// Runs `future` to completion on this thread, inside the runtime of `handle`, so Tokio
/// channels, timers and `block_in_place` work in it. While the future waits, the thread
/// spins in its budget before it sleeps.
pub(crate) fn block_on<F: Future>(handle: &tokio::runtime::Handle, future: F) -> F::Output {
    let _runtime = handle.enter();
    let signal = Arc::new(Signal {
        woken: AtomicBool::new(false),
        thread: thread::current(),
    });
    let waker = Waker::from(Arc::clone(&signal));
    let mut cx = Context::from_waker(&waker);
    let mut future = pin!(future);
    loop {
        if let Poll::Ready(output) = future.as_mut().poll(&mut cx) {
            return output;
        }
        signal.wait();
    }
}

/// Wakes the thread of [`block_on`].
struct Signal {
    woken: AtomicBool,
    thread: Thread,
}

impl Signal {
    fn wait(&self) {
        let woken = || self.woken.swap(false, Ordering::Acquire).then_some(());
        adaptive(&WAKE_SPIN, |budget| {
            if spin(budget, woken).is_none() {
                while woken().is_none() {
                    thread::park();
                }
            }
        });
    }
}

impl Wake for Signal {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        if !self.woken.swap(true, Ordering::Release) {
            self.thread.unpark();
        }
    }
}
