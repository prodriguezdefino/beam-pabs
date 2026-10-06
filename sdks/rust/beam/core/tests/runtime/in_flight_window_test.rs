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

//! Order, limits, draining and abort of the `InFlightWindow` of concurrent tasks.

use std::future::pending;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use beam_core::internals::{InFlightLimit, InFlightWindow, io_runtime};
use tokio::sync::oneshot;

/// Increments a counter when dropped so tests can check task cancellation.
struct DropCounter(Arc<AtomicUsize>);

impl Drop for DropCounter {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

fn wait_for_count(counter: &AtomicUsize, expected: usize, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    while counter.load(Ordering::SeqCst) < expected {
        assert!(
            Instant::now() < deadline,
            "timed out waiting for count {expected}, observed {}",
            counter.load(Ordering::SeqCst)
        );
        std::thread::sleep(Duration::from_millis(5));
    }
}

#[test]
fn fifo_order_preserved_when_later_tasks_finish_first() {
    let handle = io_runtime().expect("io runtime starts").handle();
    let mut window = InFlightWindow::new(8, usize::MAX);

    for (index, delay_ms) in [(0usize, 40u64), (1, 20), (2, 5), (3, 0)] {
        window.spawn(handle, index, 0, async move {
            if delay_ms > 0 {
                tokio::time::sleep(Duration::from_millis(delay_ms)).await;
            }
            index + 100
        });
    }

    let mut completed = Vec::new();
    window
        .drain_all(|meta, result| -> Result<(), ()> {
            completed.push((meta, result.expect("task joined")));
            Ok(())
        })
        .expect("drain_all succeeds");

    assert_eq!(completed, vec![(0, 100), (1, 101), (2, 102), (3, 103)]);
    assert!(window.is_empty());
    assert_eq!(window.len(), 0);
    assert!(!window.is_full());
}

#[test]
fn backpressure_enforces_request_and_byte_limits() {
    let handle = io_runtime().expect("io runtime starts").handle();

    // Request cap: fills at 2 tasks and releases back below cap after draining one.
    let mut by_count = InFlightWindow::new(2, usize::MAX);
    let (tx0, rx0) = oneshot::channel();
    let (tx1, rx1) = oneshot::channel();

    by_count.spawn(handle, 0usize, 0, async move { rx0.await.unwrap() });
    assert_eq!(by_count.len(), 1);
    assert!(!by_count.is_empty());
    assert!(!by_count.is_full());

    by_count.spawn(handle, 1usize, 0, async move { rx1.await.unwrap() });
    assert_eq!(by_count.len(), 2);
    assert!(by_count.is_full());

    tx0.send(10usize).expect("send first");
    let mut drained = Vec::new();
    by_count
        .drain_while_full(|meta, result| -> Result<(), ()> {
            drained.push((meta, result.expect("task joined")));
            Ok(())
        })
        .expect("drain_while_full succeeds");
    assert_eq!(drained, vec![(0, 10)]);
    assert_eq!(by_count.len(), 1);
    assert!(!by_count.is_full());

    tx1.send(20usize).expect("send second");
    by_count
        .drain_all(|meta, result| -> Result<(), ()> {
            drained.push((meta, result.expect("task joined")));
            Ok(())
        })
        .expect("drain_all succeeds");
    assert_eq!(drained, vec![(0, 10), (1, 20)]);
    assert!(by_count.is_empty());

    // Byte cap: fills at 100 bytes even when request count is below its limit.
    let mut by_bytes = InFlightWindow::from_limit(InFlightLimit::new(10, 100));
    by_bytes.spawn(handle, "first", 60, async { 1usize });
    assert!(!by_bytes.is_full());
    by_bytes.spawn(handle, "second", 40, async { 2usize });
    assert!(by_bytes.is_full());

    let mut names = Vec::new();
    by_bytes
        .drain_while_full(|meta, result| -> Result<(), ()> {
            names.push((meta, result.expect("task joined")));
            Ok(())
        })
        .expect("drain_while_full succeeds");
    assert_eq!(names, vec![("first", 1)]);
    assert_eq!(by_bytes.len(), 1);
    assert!(!by_bytes.is_full());

    by_bytes
        .drain_all(|meta, result| -> Result<(), ()> {
            names.push((meta, result.expect("task joined")));
            Ok(())
        })
        .expect("drain_all succeeds");
    assert_eq!(names, vec![("first", 1), ("second", 2)]);
    assert!(by_bytes.is_empty());
    assert!(!by_bytes.is_full());
}

#[test]
fn drain_ready_drains_finished_front_entries_without_blocking() {
    let handle = io_runtime().expect("io runtime starts").handle();
    let mut window = InFlightWindow::new(4, usize::MAX);

    let (tx0, rx0) = oneshot::channel();
    let (tx1, rx1) = oneshot::channel();
    let (tx2, rx2) = oneshot::channel();

    let (done1_tx, done1_rx) = std::sync::mpsc::channel();
    window.spawn(handle, 0usize, 0, async move { rx0.await.unwrap() });
    window.spawn(handle, 1usize, 0, async move {
        let value = rx1.await.unwrap();
        done1_tx.send(()).expect("the test waits for task 1");
        value
    });
    window.spawn(handle, 2usize, 0, async move { rx2.await.unwrap() });

    // Task 1 has its value and front task 0 is pending, so drain_ready drains nothing.
    tx1.send(20usize).expect("send second");
    done1_rx.recv().expect("task 1 reaches its end");
    let mut drained = Vec::new();
    window
        .drain_ready(|meta, result| -> Result<(), ()> {
            drained.push((meta, result.expect("task joined")));
            Ok(())
        })
        .expect("drain_ready succeeds");
    assert!(drained.is_empty());
    assert_eq!(window.len(), 3);

    // Completing task 0 allows drain_ready to drain 0 and 1 without blocking on task 2.
    tx0.send(10usize).expect("send first");
    let deadline = Instant::now() + Duration::from_secs(2);
    while drained.len() < 2 {
        assert!(
            Instant::now() < deadline,
            "timed out draining ready entries: {drained:?}"
        );
        window
            .drain_ready(|meta, result| -> Result<(), ()> {
                drained.push((meta, result.expect("task joined")));
                Ok(())
            })
            .expect("drain_ready succeeds");
        if drained.len() < 2 {
            std::thread::sleep(Duration::from_millis(2));
        }
    }
    assert_eq!(drained, vec![(0, 10), (1, 20)]);
    assert_eq!(window.len(), 1);

    tx2.send(30usize).expect("send third");
    window
        .drain_all(|meta, result| -> Result<(), ()> {
            drained.push((meta, result.expect("task joined")));
            Ok(())
        })
        .expect("drain_all succeeds");
    assert_eq!(drained, vec![(0, 10), (1, 20), (2, 30)]);
}

#[test]
fn callback_error_propagates_and_releases_limit() {
    let handle = io_runtime().expect("io runtime starts").handle();
    let mut window = InFlightWindow::new(2, 100);

    window.spawn(handle, 0usize, 50, async { 10usize });
    window.spawn(handle, 1usize, 50, async { 20usize });
    assert!(window.is_full());

    let err = window
        .drain_while_full(|meta, _result| if meta == 0 { Err("boom") } else { Ok(()) })
        .unwrap_err();
    assert_eq!(err, "boom");
    assert_eq!(window.len(), 1);
    assert!(!window.is_full());

    let remaining = window.take_all();
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].0, 1);
    assert_eq!(remaining[0].1.as_ref().expect("task joined"), &20);
    assert!(window.is_empty());
    assert!(!window.is_full());
}

#[test]
fn clear_and_drop_abort_unfinished_tasks() {
    let handle = io_runtime().expect("io runtime starts").handle();

    // `clear` aborts every task and resets active limits.
    let started = Arc::new(AtomicUsize::new(0));
    let dropped = Arc::new(AtomicUsize::new(0));
    let mut window = InFlightWindow::new(3, 300);
    for i in 0..3 {
        let started = Arc::clone(&started);
        let dropped = Arc::clone(&dropped);
        window.spawn(handle, i, 100, async move {
            let _guard = DropCounter(dropped);
            started.fetch_add(1, Ordering::SeqCst);
            pending::<()>().await;
        });
    }
    wait_for_count(&started, 3, Duration::from_secs(2));
    assert!(window.is_full());

    window.clear();
    assert!(window.is_empty());
    assert!(!window.is_full());
    wait_for_count(&dropped, 3, Duration::from_secs(2));

    // Dropping the window aborts every unfinished task automatically.
    let started = Arc::new(AtomicUsize::new(0));
    let dropped = Arc::new(AtomicUsize::new(0));
    {
        let mut scoped = InFlightWindow::new(3, 300);
        for i in 0..3 {
            let started = Arc::clone(&started);
            let dropped = Arc::clone(&dropped);
            scoped.spawn(handle, i, 100, async move {
                let _guard = DropCounter(dropped);
                started.fetch_add(1, Ordering::SeqCst);
                pending::<()>().await;
            });
        }
        wait_for_count(&started, 3, Duration::from_secs(2));
    }
    wait_for_count(&dropped, 3, Duration::from_secs(2));
}

#[test]
fn take_all_returns_remaining_entries_in_fifo_order() {
    let handle = io_runtime().expect("io runtime starts").handle();
    let mut window = InFlightWindow::new(4, usize::MAX);

    for i in 0..3usize {
        window.spawn(handle, i, 10, async move { i * 10 });
    }
    let taken: Vec<(usize, usize)> = window
        .take_all()
        .into_iter()
        .map(|(meta, res)| (meta, res.expect("task joined")))
        .collect();
    assert_eq!(taken, vec![(0, 0), (1, 10), (2, 20)]);
    assert!(window.is_empty());
    assert!(!window.is_full());
}

#[test]
fn panicking_task_surfaces_join_error() {
    let handle = io_runtime().expect("io runtime starts").handle();
    let mut window = InFlightWindow::new(2, usize::MAX);
    window.spawn(handle, "boom", 0, async {
        panic!("task exploded");
    });

    let results = window.take_all();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].0, "boom");
    let err = results[0].1.as_ref().unwrap_err();
    assert!(err.is_panic());
}

#[test]
fn zero_limits_clamp_to_one() {
    let handle = io_runtime().expect("io runtime starts").handle();
    let mut window = InFlightWindow::new(0, 0);
    assert!(!window.is_full());

    window.spawn(handle, 0usize, 0, async { 7usize });
    assert!(window.is_full());
    let results = window.take_all();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].1.as_ref().expect("task joined"), &7);
    assert!(!window.is_full());
}
