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

//! How `StateChannel` reads GET pages: a response with no result is empty state on the first
//! page but a protocol error on a continuation page. Also stream sharing, tagging, recovery.

use harness::state::{StateChannel, StateClients};
use model::fn_execution::{StateAppendResponse, StateKey, state_key, state_response};
use testutils::with_timeout;

mod common;
use common::WAIT;
use common::state_mock::{Call, FullKey, Op, StrictStateServer};

/// A channel of `instruction` over the stream of `clients` to `server`.
fn channel(clients: &StateClients, server: &StrictStateServer, instruction: &str) -> StateChannel {
    clients.channel_to(instruction, server.endpoint())
}

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

#[test]
fn an_empty_first_page_reads_as_empty_state() {
    with_timeout("an_empty_first_page_reads_as_empty_state", || {
        let server = StrictStateServer::start();
        let clients = StateClients::new(String::new());
        server.backend.respond_next(Op::Get, Some(b""), None);

        assert_eq!(
            channel(&clients, &server, "inst_empty").get(bag_key()),
            Ok(Vec::new())
        );
        assert_eq!(server.backend.calls(), [Call::get_page(bag_cell(), b"")]);
    });
}

#[test]
fn an_empty_continuation_page_is_a_protocol_error() {
    with_timeout("an_empty_continuation_page_is_a_protocol_error", || {
        let server = StrictStateServer::start();
        let clients = StateClients::new(String::new());
        server.backend.preload(bag_cell(), &[1, 2, 3]);
        server.backend.set_page_size(1);
        server.backend.respond_next(Op::Get, Some(b"1"), None);

        let err = channel(&clients, &server, "inst_truncated")
            .get(bag_key())
            .expect_err("a partial read must not be returned as the full value");
        assert!(err.contains("truncated"), "{err}");
        assert_eq!(
            server.backend.calls(),
            [
                Call::get_page(bag_cell(), b""),
                Call::get_page(bag_cell(), b"1")
            ]
        );
    });
}

#[test]
fn a_non_get_response_to_a_get_is_a_protocol_error() {
    with_timeout("a_non_get_response_to_a_get_is_a_protocol_error", || {
        let server = StrictStateServer::start();
        let clients = StateClients::new(String::new());
        server.backend.respond_next(
            Op::Get,
            None,
            Some(state_response::Response::Append(StateAppendResponse {})),
        );

        assert!(
            channel(&clients, &server, "inst_mismatch")
                .get(bag_key())
                .is_err()
        );
    });
}

#[test]
fn state_requests_carry_their_instruction_id() {
    with_timeout("state_requests_carry_their_instruction_id", || {
        let server = StrictStateServer::start();
        let clients = StateClients::new(String::new());
        server.backend.preload(bag_cell(), &[1, 2]);
        server.backend.set_page_size(1);
        let a = channel(&clients, &server, "inst_a");
        let b = channel(&clients, &server, "inst_b");

        assert_eq!(a.get(bag_key()), Ok(vec![1, 2]));
        b.append(bag_key(), vec![3]).unwrap();
        a.clear(bag_key()).unwrap();

        assert_eq!(
            server.backend.instruction_ids(),
            ["inst_a", "inst_a", "inst_b", "inst_a"]
        );
    });
}

#[test]
fn state_stream_carries_the_worker_id() {
    with_timeout("state_stream_carries_the_worker_id", || {
        for (worker_id, expected) in [("worker_7", Some("worker_7")), ("", None)] {
            let server = StrictStateServer::start();
            StateClients::new(worker_id.to_string())
                .channel_to("inst_worker", server.endpoint())
                .get(bag_key())
                .unwrap();

            assert_eq!(
                server.backend.stream_worker_ids(),
                [expected.map(str::to_string)],
                "worker id {worker_id:?}"
            );
        }
    });
}

/// A pending request fails at once rather than waiting out the response timeout.
#[test]
fn a_closed_state_stream_fails_pending_requests_and_reconnects() {
    with_timeout(
        "a_closed_state_stream_fails_pending_requests_and_reconnects",
        || {
            let server = StrictStateServer::start();
            let clients = StateClients::new(String::new());
            server.backend.preload(bag_cell(), &[5]);
            server.backend.close_stream_on_next(Op::Get);
            let pending = channel(&clients, &server, "inst_closed");
            let stale = pending.clone();

            let (reply_tx, reply_rx) = std::sync::mpsc::channel();
            std::thread::spawn(move || {
                let _ = reply_tx.send(pending.get(bag_key()));
            });
            let err = reply_rx
                .recv_timeout(WAIT)
                .expect("a request pending when the stream closed must fail, not hang")
                .unwrap_err();
            assert!(err.contains("closed by runner"), "{err}");
            assert!(
                stale.get(bag_key()).is_err(),
                "the dead stream is not reused"
            );

            assert_eq!(
                channel(&clients, &server, "inst_after").get(bag_key()),
                Ok(vec![5])
            );
            assert_eq!(server.backend.streams_opened(), 2);
        },
    );
}

#[test]
fn channels_to_one_endpoint_share_one_state_stream() {
    with_timeout("channels_to_one_endpoint_share_one_state_stream", || {
        let server = StrictStateServer::start();
        let clients = StateClients::new(String::new());
        for instruction in ["inst_1", "inst_2", "inst_3"] {
            assert_eq!(
                channel(&clients, &server, instruction).get(bag_key()),
                Ok(Vec::new())
            );
        }
        assert_eq!(server.backend.streams_opened(), 1);
    });
}
