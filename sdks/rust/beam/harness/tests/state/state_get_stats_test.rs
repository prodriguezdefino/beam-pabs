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

//! How a `StateChannel` counts its GET requests: pages, bytes and wait time in its totals,
//! and the `beam-harness` counters of the transform that reads.

use std::sync::Arc;
use std::time::Duration;

use beam::coders::StateStreamReader;
use beam::internals::{MetricsContainer, MetricsScope};
use beam::metrics::MetricKey;
use beam_harness::state::{
    HARNESS_METRICS_NAMESPACE, STATE_GET_BYTES, STATE_GET_PAGES, STATE_GET_WAIT_US, StateClients,
    StateGetTotals,
};
use beam_model::fn_execution::{StateAppendResponse, StateKey, state_key, state_response};
use beam_test_utils::with_timeout;

use crate::common::WAIT;
use crate::state_mock::{FullKey, Op, StrictStateServer};

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

fn harness_counter(container: &MetricsContainer, transform_id: &str, name: &str) -> Option<i64> {
    container
        .counters()
        .get(&MetricKey::new(
            transform_id,
            HARNESS_METRICS_NAMESPACE,
            name,
        ))
        .copied()
}

#[test]
fn a_paged_read_counts_each_page_and_its_bytes() {
    with_timeout("a_paged_read_counts_each_page_and_its_bytes", || {
        let server = StrictStateServer::start();
        let clients = StateClients::new(String::new());
        server.backend.preload(bag_cell(), &[1, 2, 3, 4, 5]);
        server.backend.set_page_size(2);
        let channel = clients.channel_to("inst_pages", server.endpoint());
        assert_eq!(channel.state_get_totals(), StateGetTotals::default());

        assert_eq!(channel.get(bag_key()), Ok(vec![1, 2, 3, 4, 5]));

        let StateGetTotals { pages, bytes, .. } = channel.state_get_totals();
        assert_eq!((pages, bytes), (3, 5));
    });
}

#[test]
fn clones_share_totals_and_other_instructions_do_not() {
    with_timeout("clones_share_totals_and_other_instructions_do_not", || {
        let server = StrictStateServer::start();
        let clients = StateClients::new(String::new());
        server.backend.preload(bag_cell(), &[7, 8]);
        let channel = clients.channel_to("inst_shared", server.endpoint());
        let clone = channel.clone();
        let other = clients.channel_to("inst_other", server.endpoint());

        channel.get(bag_key()).unwrap();
        clone.get(bag_key()).unwrap();

        assert_eq!(channel.state_get_totals(), clone.state_get_totals());
        let StateGetTotals { pages, bytes, .. } = channel.state_get_totals();
        assert_eq!((pages, bytes), (2, 4));
        assert_eq!(other.state_get_totals(), StateGetTotals::default());
    });
}

#[test]
fn a_failed_get_counts_as_a_page_without_bytes() {
    with_timeout("a_failed_get_counts_as_a_page_without_bytes", || {
        let server = StrictStateServer::start();
        let clients = StateClients::new(String::new());
        server.backend.respond_next(
            Op::Get,
            None,
            Some(state_response::Response::Append(StateAppendResponse {})),
        );
        let channel = clients.channel_to("inst_failed", server.endpoint());

        assert!(channel.get(bag_key()).is_err());

        let StateGetTotals { pages, bytes, .. } = channel.state_get_totals();
        assert_eq!((pages, bytes), (1, 0));
    });
}

#[test]
fn the_wait_covers_the_time_until_the_reply() {
    with_timeout("the_wait_covers_the_time_until_the_reply", || {
        const HELD: Duration = Duration::from_millis(50);
        let server = StrictStateServer::start();
        let clients = StateClients::new(String::new());
        server.backend.preload(bag_cell(), &[1]);
        let mut held = server.backend.hold_next(Op::Get);
        let channel = clients.channel_to("inst_wait", server.endpoint());
        let reader = channel.clone();

        let read = std::thread::spawn(move || reader.get(bag_key()));
        held.wait_held(WAIT);
        std::thread::sleep(HELD);
        held.release();

        assert_eq!(read.join().unwrap(), Ok(vec![1]));
        let StateGetTotals { pages, wait, .. } = channel.state_get_totals();
        assert_eq!(pages, 1);
        assert!(wait >= HELD, "waited {wait:?}");
    });
}

#[test]
fn reads_add_to_the_counters_of_the_running_transform() {
    with_timeout("reads_add_to_the_counters_of_the_running_transform", || {
        let server = StrictStateServer::start();
        let clients = StateClients::new(String::new());
        let token = b"gbk_values".to_vec();
        server
            .backend
            .preload(FullKey::Runner { key: token.clone() }, &[10, 11, 12, 13]);
        server.backend.set_page_size(3);
        let channel = clients.channel_to("inst_counters", server.endpoint());
        let container = Arc::new(MetricsContainer::new());

        let pages = {
            let _scope = MetricsScope::enter(Arc::clone(&container), "t_consumer".into());
            channel
                .stream_runner_pages(&token)
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap()
        };

        assert_eq!(pages, [vec![10, 11, 12], vec![13]]);
        let counter = |name| harness_counter(&container, "t_consumer", name);
        assert_eq!(counter(STATE_GET_PAGES), Some(2));
        assert_eq!(counter(STATE_GET_BYTES), Some(4));
        let wait_us = counter(STATE_GET_WAIT_US).expect("the wait counter is recorded");
        let total_us = i64::try_from(channel.state_get_totals().wait.as_micros()).unwrap();
        assert!(
            (0..=total_us).contains(&wait_us),
            "{wait_us} us of {total_us} us"
        );
    });
}
