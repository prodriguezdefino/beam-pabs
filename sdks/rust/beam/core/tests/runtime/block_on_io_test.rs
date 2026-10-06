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

use std::future::Future;
use std::pin::Pin;
use std::sync::mpsc;
use std::task::{Context, Poll};
use std::time::Duration;

use beam::internals::block_on_io;
use tokio::sync::oneshot;

/// Waits for `rx`. On its first poll it registers its waker, then blocks in `block_on_io`
/// while the value arrives, and returns `Pending` without polling `rx` again.
struct WakeDuringBlockOnIo {
    rx: oneshot::Receiver<u32>,
    blocked: bool,
}

impl Future for WakeDuringBlockOnIo {
    type Output = u32;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<u32> {
        if let Poll::Ready(value) = Pin::new(&mut self.rx).poll(cx) {
            return Poll::Ready(value.unwrap_or_default());
        }
        if !self.blocked {
            self.blocked = true;
            block_on_io(tokio::time::sleep(Duration::from_millis(300)))
                .expect("the async I/O runtime starts");
        }
        Poll::Pending
    }
}

#[test]
fn test_block_on_io_keeps_the_wakeup_of_the_outer_block_on() {
    let (done_tx, done_rx) = mpsc::channel();
    std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("the test runtime starts");
        let (tx, rx) = oneshot::channel();
        // The value arrives while the outer future is inside `block_on_io`.
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(50));
            let _ = tx.send(7);
        });
        let value = runtime.block_on(WakeDuringBlockOnIo { rx, blocked: false });
        let _ = done_tx.send(value);
    });
    let value = done_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("the outer block_on gets its wakeup and ends");
    assert_eq!(value, 7);
}

#[test]
fn test_nested_block_on_io_keeps_the_wakeup_of_the_outer_block_on_io() {
    let (done_tx, done_rx) = mpsc::channel();
    std::thread::spawn(move || {
        let (tx, rx) = oneshot::channel();
        // The value arrives while the outer future is inside the nested `block_on_io`.
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(50));
            let _ = tx.send(7);
        });
        let value = block_on_io(WakeDuringBlockOnIo { rx, blocked: false });
        let _ = done_tx.send(value);
    });
    let value = done_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("the outer block_on_io gets its wakeup and ends")
        .expect("the async I/O runtime starts");
    assert_eq!(value, 7);
}

#[test]
fn test_block_on_io_returns_the_value_outside_and_inside_a_runtime() {
    assert_eq!(block_on_io(async { 1 }).expect("runs outside a runtime"), 1);
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("the test runtime starts");
    let inside = runtime.block_on(async { block_on_io(async { 2 }) });
    assert_eq!(inside.expect("runs inside a multi-thread runtime"), 2);
    let current = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("the test runtime starts");
    let in_current = current.block_on(async { block_on_io(async { 3 }) });
    assert_eq!(in_current.expect("runs inside a current-thread runtime"), 3);
}

/// Sleeps on the I/O runtime timer, then reads `words` through a borrow.
async fn borrowed_len(words: &[&str]) -> usize {
    tokio::time::sleep(Duration::from_millis(5)).await;
    words.len()
}

#[test]
fn test_block_on_io_runs_a_borrowing_future_with_a_timer_from_every_context() {
    let words = ["a", "b", "c"];
    assert_eq!(
        block_on_io(borrowed_len(&words)).expect("runs outside a runtime"),
        3
    );
    let multi = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("the test runtime starts");
    let inside = multi.block_on(async { block_on_io(borrowed_len(&words)) });
    assert_eq!(inside.expect("runs inside a multi-thread runtime"), 3);
    let current = tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("the test runtime starts");
    let in_current = current.block_on(async { block_on_io(borrowed_len(&words)) });
    assert_eq!(in_current.expect("runs inside a current-thread runtime"), 3);
}

#[test]
fn test_send_and_recv_blocking_wait_inside_a_current_thread_runtime() {
    let current = tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("the test runtime starts");
    let (tx, mut rx) = tokio::sync::mpsc::channel(1);
    let received = current.block_on(async {
        beam::internals::send_blocking(&tx, 1).expect("the first send has room");
        let sender = tx.clone();
        // The second send waits for room until the reader below takes the first value.
        std::thread::spawn(move || beam::internals::send_blocking(&sender, 2).is_ok());
        let first = beam::internals::recv_blocking(&mut rx);
        let second = beam::internals::recv_blocking(&mut rx);
        (first, second)
    });
    assert_eq!(received, (Some(Some(1)), Some(Some(2))));
}
