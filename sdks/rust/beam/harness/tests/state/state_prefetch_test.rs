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

//! How a state page stream fetches the next page ahead: the GET of the next page goes out
//! before the caller asks for it, a drop removes the GET that is in flight, and any thread
//! can wait for the reply.

use std::time::{Duration, Instant};

use harness::state::{StateChannel, StateClients, StateGetTotals};
use model::fn_execution::{StateKey, state_key};
use testutils::with_timeout;

use crate::common::WAIT;
use crate::state_mock::{Call, FullKey, Op, StrictStateServer};

fn bag_key() -> StateKey {
    StateKey {
        r#type: Some(state_key::Type::BagUserState(state_key::BagUserState {
            transform_id: "t_a".to_string(),
            user_state_id: "state_accum".to_string(),
            window: Vec::new(),
            key: b"k".to_vec(),
        })),
    }
}

fn bag_cell() -> FullKey {
    FullKey::bag("t_a", "state_accum", b"", b"k")
}

/// A channel with its own stream, so its pending requests are only its own.
fn channel(server: &StrictStateServer, instruction: &str) -> StateChannel {
    StateClients::new(String::new()).channel_to(instruction, server.endpoint())
}

/// Waits until `done` is true. Returns false if it is still false after [`WAIT`].
fn eventually(mut done: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + WAIT;
    while !done() {
        if Instant::now() >= deadline {
            return false;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    true
}

#[test]
fn the_next_page_is_requested_before_the_caller_asks_for_it() {
    with_timeout(
        "the_next_page_is_requested_before_the_caller_asks_for_it",
        || {
            let server = StrictStateServer::start();
            server.backend.preload(bag_cell(), &[1, 2, 3, 4, 5]);
            server.backend.set_page_size(2);
            let channel = channel(&server, "inst_ahead");
            let mut pages = channel.stream_pages(bag_key()).unwrap();

            assert_eq!(pages.next().unwrap().unwrap(), [1, 2]);
            assert!(
                eventually(|| server.backend.calls().len() == 2),
                "the GET of page 2 goes out before the caller asks for it: {:?}",
                server.backend.calls()
            );
            assert_eq!(pages.next().unwrap().unwrap(), [3, 4]);
            assert!(eventually(|| server.backend.calls().len() == 3));
            assert_eq!(pages.next().unwrap().unwrap(), [5]);
            assert!(pages.next().is_none());
            assert!(pages.next().is_none());
            assert_eq!(
                server.backend.calls(),
                [
                    Call::get_page(bag_cell(), b""),
                    Call::get_page(bag_cell(), b"2"),
                    Call::get_page(bag_cell(), b"4"),
                ],
                "no GET after the last page"
            );
            assert_eq!(channel.pending_requests(), 0);
        },
    );
}

#[test]
fn many_pages_come_back_in_order() {
    with_timeout("many_pages_come_back_in_order", || {
        const PAGES: u8 = 64;
        let server = StrictStateServer::start();
        let data: Vec<u8> = (0..PAGES).collect();
        server.backend.preload(bag_cell(), &data);
        server.backend.set_page_size(1);
        let channel = channel(&server, "inst_order");

        let pages: Vec<Vec<u8>> = channel
            .stream_pages(bag_key())
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        assert_eq!(pages, data.iter().map(|b| vec![*b]).collect::<Vec<_>>());
        assert_eq!(channel.get(bag_key()), Ok(data));
        let StateGetTotals { pages, bytes, .. } = channel.state_get_totals();
        assert_eq!(
            (pages, bytes),
            (2 * u64::from(PAGES), 2 * u64::from(PAGES)),
            "each page counts once"
        );
    });
}

#[test]
fn a_drop_with_a_get_in_flight_removes_its_waiter() {
    with_timeout("a_drop_with_a_get_in_flight_removes_its_waiter", || {
        let server = StrictStateServer::start();
        server.backend.preload(bag_cell(), &[1, 2, 3]);
        server.backend.set_page_size(2);
        server.backend.hold_next(Op::Get).release();
        let mut second = server.backend.hold_next(Op::Get);
        let channel = channel(&server, "inst_drop");
        let mut pages = channel.stream_pages(bag_key()).unwrap();

        assert_eq!(pages.next().unwrap().unwrap(), [1, 2]);
        second.wait_held(WAIT);
        assert_eq!(channel.pending_requests(), 1, "the GET of page 2 waits");
        drop(pages);
        assert_eq!(channel.pending_requests(), 0, "the drop removes the waiter");

        // The late reply has no waiter. The stream drops it and serves the next read.
        second.release();
        assert_eq!(channel.get(bag_key()), Ok(vec![1, 2, 3]));
        assert_eq!(channel.pending_requests(), 0);
    });
}

#[test]
fn an_iterator_moved_to_another_thread_wakes_on_that_thread() {
    with_timeout(
        "an_iterator_moved_to_another_thread_wakes_on_that_thread",
        || {
            const HELD: Duration = Duration::from_millis(50);
            let server = StrictStateServer::start();
            server.backend.preload(bag_cell(), &[1, 2, 3]);
            server.backend.set_page_size(2);
            server.backend.hold_next(Op::Get).release();
            let mut second = server.backend.hold_next(Op::Get);
            let channel = channel(&server, "inst_moved");
            let mut pages = channel.stream_pages(bag_key()).unwrap();

            assert_eq!(pages.next().unwrap().unwrap(), [1, 2]);
            second.wait_held(WAIT);
            let wait_before = channel.state_get_totals().wait;
            let reader = std::thread::spawn(move || {
                let page = pages.next().map(|page| page.map_err(|e| e.to_string()));
                (page, pages.next().is_none())
            });
            // The reader parks on its own thread before the reply arrives.
            std::thread::sleep(HELD);
            second.release();

            assert_eq!(reader.join().unwrap(), (Some(Ok(vec![3])), true));
            let waited = channel.state_get_totals().wait - wait_before;
            assert!(waited >= HELD, "the reader blocked for {waited:?}");
        },
    );
}

#[test]
fn the_wait_leaves_out_the_time_that_a_prefetch_was_in_flight() {
    with_timeout(
        "the_wait_leaves_out_the_time_that_a_prefetch_was_in_flight",
        || {
            const DECODE: Duration = Duration::from_millis(200);
            let server = StrictStateServer::start();
            server.backend.preload(bag_cell(), &[1, 2]);
            server.backend.set_page_size(1);
            let channel = channel(&server, "inst_wait");
            let mut pages = channel.stream_pages(bag_key()).unwrap();

            assert_eq!(pages.next().unwrap().unwrap(), [1]);
            assert!(eventually(|| server.backend.calls().len() == 2));
            // The reply to the GET of page 2 arrives while the caller is busy.
            std::thread::sleep(DECODE);
            let wait_before = channel.state_get_totals().wait;
            assert_eq!(pages.next().unwrap().unwrap(), [2]);

            let StateGetTotals { pages, wait, .. } = channel.state_get_totals();
            assert_eq!(pages, 2);
            assert!(
                wait - wait_before < DECODE,
                "page 2 was ready, but the caller waited {:?}",
                wait - wait_before
            );
        },
    );
}

#[test]
fn a_failed_prefetched_get_is_the_error_of_its_page() {
    with_timeout("a_failed_prefetched_get_is_the_error_of_its_page", || {
        let server = StrictStateServer::start();
        server.backend.preload(bag_cell(), &[1, 2, 3, 4, 5]);
        server.backend.set_page_size(2);
        server.backend.fail_next(Op::Get, Some(b"2"), "expired");
        let channel = channel(&server, "inst_failed");

        let pages: Vec<_> = channel
            .stream_pages(bag_key())
            .unwrap()
            .map(|page| page.map_err(|e| e.to_string()))
            .collect();
        assert_eq!(
            pages,
            [
                Ok(vec![1, 2]),
                Err("Runner state error: expired".to_string())
            ],
            "the error comes at page 2, then the stream ends"
        );
        assert_eq!(
            server.backend.calls(),
            [
                Call::get_page(bag_cell(), b""),
                Call::get_page(bag_cell(), b"2"),
            ]
        );
        let StateGetTotals { pages, bytes, .. } = channel.state_get_totals();
        assert_eq!((pages, bytes), (2, 2));
        assert_eq!(channel.pending_requests(), 0);
    });
}
