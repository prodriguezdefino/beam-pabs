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

#![expect(
    clippy::unwrap_used,
    reason = "test fixtures unwrap; a failure is a test failure"
)]

//! The state cache that bundles share: user state and side inputs are read from the runner
//! once while the runner sends the same cache token, and the commit sends only mutations.

use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use prost::Message;

use beam::internals::SideInputReader;
use beam::pipeline::constants::URN_PAR_DO;
use harness::state::{FnApiSideInputReader, StateCache, StateChannel};
use harness::user_state::{BundleUserState, UserStateCache};
use model::fn_execution::ProcessBundleDescriptor;
use model::pipeline::{self as proto, ApiServiceDescriptor, FunctionSpec};
use testutils::with_timeout;

mod common;
use common::state_mock::{Call, FullKey, Op, StrictStateServer};

fn coder(urn: &str) -> proto::Coder {
    proto::Coder {
        spec: Some(FunctionSpec {
            urn: urn.to_string(),
            payload: Vec::new(),
        }),
        component_coder_ids: Vec::new(),
    }
}

fn state_spec(spec: proto::state_spec::Spec) -> proto::StateSpec {
    proto::StateSpec {
        protocol: None,
        spec: Some(spec),
    }
}

/// A transform `t` with a bag `bag` of varints, a map `map` from bytes to varints, and an
/// iterable side input `side` of varints.
fn descriptor(endpoint: &str) -> ProcessBundleDescriptor {
    let payload = proto::ParDoPayload {
        state_specs: HashMap::from([
            (
                "bag".to_string(),
                state_spec(proto::state_spec::Spec::BagSpec(proto::BagStateSpec {
                    element_coder_id: "varint".to_string(),
                })),
            ),
            (
                "map".to_string(),
                state_spec(proto::state_spec::Spec::MapSpec(proto::MapStateSpec {
                    key_coder_id: "bytes".to_string(),
                    value_coder_id: "varint".to_string(),
                })),
            ),
        ]),
        side_inputs: HashMap::from([("side".to_string(), proto::SideInput::default())]),
        ..Default::default()
    };
    ProcessBundleDescriptor {
        id: "desc_cache".to_string(),
        state_api_service_descriptor: Some(ApiServiceDescriptor {
            url: endpoint.to_string(),
            ..Default::default()
        }),
        transforms: HashMap::from([(
            "t".to_string(),
            proto::PTransform {
                spec: Some(FunctionSpec {
                    urn: URN_PAR_DO.to_string(),
                    payload: payload.encode_to_vec(),
                }),
                inputs: HashMap::from([("side".to_string(), "pc_side".to_string())]),
                ..Default::default()
            },
        )]),
        pcollections: HashMap::from([(
            "pc_side".to_string(),
            proto::PCollection {
                coder_id: "varint".to_string(),
                ..Default::default()
            },
        )]),
        coders: HashMap::from([
            ("varint".to_string(), coder("beam:coder:varint:v1")),
            ("bytes".to_string(), coder("beam:coder:bytes:v1")),
        ]),
        ..Default::default()
    }
}

/// The user state of one bundle, with the shared `cache` under `token` when there is one.
fn bundle(
    server: &StrictStateServer,
    instruction: &str,
    cache: &Arc<StateCache>,
    token: Option<&'static [u8]>,
) -> BundleUserState {
    let descriptor = descriptor(server.endpoint());
    let channel = StateChannel::from_descriptor(instruction, &descriptor, "worker").unwrap();
    let cache = token.map(|token| UserStateCache {
        cache: Arc::clone(cache),
        token: Bytes::from_static(token),
    });
    BundleUserState::from_channel_with_cache(&descriptor, channel, cache).unwrap()
}

#[test]
fn user_state_is_read_once_while_the_token_stays() {
    with_timeout("user_state_is_read_once_while_the_token_stays", || {
        let server = StrictStateServer::start();
        let cell = FullKey::bag("t", "bag", b"w", b"k");
        server.backend.preload(cell.clone(), &[1, 2]);
        let cache = Arc::new(StateCache::default());

        let first = bundle(&server, "inst_1", &cache, Some(b"token_a"));
        let state = first.scoped("t");
        assert_eq!(state.get_state("bag", b"w", b"k").unwrap(), [[1], [2]]);
        state.append_state("bag", b"w", b"k", vec![3]).unwrap();
        first.commit().unwrap();
        assert_eq!(
            server.backend.take_calls(),
            [Call::get(cell.clone()), Call::append(cell, &[3])]
        );

        // The second bundle sees the runner content and the write of the first bundle.
        let second = bundle(&server, "inst_2", &cache, Some(b"token_a"));
        assert_eq!(
            second.scoped("t").get_state("bag", b"w", b"k").unwrap(),
            [[1], [2], [3]]
        );
        second.commit().unwrap();
        assert_eq!(server.backend.take_calls(), [], "no read and no write");
    });
}

#[test]
fn a_new_token_reads_user_state_again() {
    with_timeout("a_new_token_reads_user_state_again", || {
        let server = StrictStateServer::start();
        let cell = FullKey::bag("t", "bag", b"w", b"k");
        server.backend.preload(cell.clone(), &[1]);
        let cache = Arc::new(StateCache::default());

        let read = |instruction, token| {
            let state = bundle(&server, instruction, &cache, token);
            let elements = state.scoped("t").get_state("bag", b"w", b"k").unwrap();
            state.commit().unwrap();
            elements
        };
        assert_eq!(read("inst_1", Some(b"token_a")), [[1]]);
        assert_eq!(read("inst_2", Some(b"token_b")), [[1]]);
        assert_eq!(read("inst_3", None), [[1]]);
        assert_eq!(
            server.backend.take_calls(),
            [
                Call::get(cell.clone()),
                Call::get(cell.clone()),
                Call::get(cell)
            ],
            "another token, or no token, reads from the runner"
        );
    });
}

#[test]
fn appends_to_an_unread_bag_do_not_fill_the_cache() {
    with_timeout("appends_to_an_unread_bag_do_not_fill_the_cache", || {
        let server = StrictStateServer::start();
        let cell = FullKey::bag("t", "bag", b"w", b"k");
        server.backend.preload(cell.clone(), &[1]);
        let cache = Arc::new(StateCache::default());

        // The writer does not know the runner content, so it cannot cache only its appends.
        let writer = bundle(&server, "inst_1", &cache, Some(b"token_a"));
        writer
            .scoped("t")
            .append_state("bag", b"w", b"k", vec![2])
            .unwrap();
        writer.commit().unwrap();
        server.backend.take_calls();

        let reader = bundle(&server, "inst_2", &cache, Some(b"token_a"));
        assert_eq!(
            reader.scoped("t").get_state("bag", b"w", b"k").unwrap(),
            [[1], [2]]
        );
        assert_eq!(server.backend.take_calls(), [Call::get(cell)]);
    });
}

#[test]
fn a_failed_commit_leaves_the_cache_unchanged() {
    with_timeout("a_failed_commit_leaves_the_cache_unchanged", || {
        let server = StrictStateServer::start();
        let cell = FullKey::bag("t", "bag", b"w", b"k");
        server.backend.preload(cell, &[1]);
        let cache = Arc::new(StateCache::default());

        let warm = bundle(&server, "inst_1", &cache, Some(b"token_a"));
        warm.scoped("t").get_state("bag", b"w", b"k").unwrap();
        warm.commit().unwrap();

        server.backend.fail_next(Op::Append, None, "quota exceeded");
        let failing = bundle(&server, "inst_2", &cache, Some(b"token_a"));
        failing
            .scoped("t")
            .append_state("bag", b"w", b"k", vec![9])
            .unwrap();
        assert!(failing.commit().is_err());
        server.backend.take_calls();

        let after = bundle(&server, "inst_3", &cache, Some(b"token_a"));
        assert_eq!(
            after.scoped("t").get_state("bag", b"w", b"k").unwrap(),
            [[1]],
            "the cache keeps the content before the failed bundle"
        );
        assert_eq!(server.backend.take_calls(), []);
    });
}

#[test]
fn map_reads_are_not_written_back() {
    with_timeout("map_reads_are_not_written_back", || {
        let server = StrictStateServer::start();
        let entry = FullKey::multimap("t", "map", b"w", b"k", b"a");
        server.backend.preload(entry.clone(), &[7]);
        let cache = Arc::new(StateCache::default());

        let state = bundle(&server, "inst_1", &cache, None);
        assert_eq!(
            state
                .scoped("t")
                .get_map_state("map", b"w", b"k", b"a")
                .unwrap(),
            [[7]]
        );
        state.commit().unwrap();
        assert_eq!(
            server.backend.take_calls(),
            [Call::get(entry)],
            "a read sends no clear and no append"
        );
    });
}

#[test]
fn map_keys_and_entries_are_cached_while_the_token_stays() {
    with_timeout(
        "map_keys_and_entries_are_cached_while_the_token_stays",
        || {
            let server = StrictStateServer::start();
            // Map keys use the bytes coder, which prefixes each key with its length.
            let (key_a, key_b, key_z) = (&[1, b'a'][..], &[1, b'b'][..], &[1, b'z'][..]);
            let keys = FullKey::multimap_keys("t", "map", b"w", b"k");
            server.backend.preload(keys, key_a);
            server
                .backend
                .preload(FullKey::multimap("t", "map", b"w", b"k", key_a), &[7]);
            let cache = Arc::new(StateCache::default());

            let first = bundle(&server, "inst_1", &cache, Some(b"token_a"));
            let state = first.scoped("t");
            assert_eq!(state.get_map_keys("map", b"w", b"k").unwrap(), [key_a]);
            state
                .put_map_state("map", b"w", b"k", key_b.to_vec(), vec![8])
                .unwrap();
            first.commit().unwrap();
            server.backend.take_calls();

            let second = bundle(&server, "inst_2", &cache, Some(b"token_a"));
            let state = second.scoped("t");
            let mut all_keys = state.get_map_keys("map", b"w", b"k").unwrap();
            all_keys.sort();
            assert_eq!(all_keys, [key_a, key_b]);
            assert_eq!(
                state.get_map_state("map", b"w", b"k", key_b).unwrap(),
                [[8]]
            );
            // A key outside the known key set is absent without a read.
            assert_eq!(
                state.get_map_state("map", b"w", b"k", key_z).unwrap().len(),
                0
            );
            assert_eq!(server.backend.take_calls(), []);
        },
    );
}

#[test]
fn large_bag_appends_are_split_into_requests_of_at_most_10_mib() {
    with_timeout(
        "large_bag_appends_are_split_into_requests_of_at_most_10_mib",
        || {
            const ELEMENT_BYTES: usize = 4 << 20;
            let server = StrictStateServer::start();
            let cache = Arc::new(StateCache::default());
            let state = bundle(&server, "inst_1", &cache, None);
            (0..5).for_each(|i| {
                state
                    .scoped("t")
                    .append_state("bag", b"w", b"k", vec![i; ELEMENT_BYTES])
                    .unwrap();
            });
            state.commit().unwrap();

            let sizes: Vec<usize> = server
                .backend
                .take_calls()
                .iter()
                .map(|call| call.data.len())
                .collect();
            assert_eq!(
                sizes,
                [2 * ELEMENT_BYTES, 2 * ELEMENT_BYTES, ELEMENT_BYTES],
                "two 4 MiB elements fit in 10 MiB, three do not"
            );
        },
    );
}

#[test]
fn side_input_pages_are_shared_while_the_token_stays() {
    with_timeout("side_input_pages_are_shared_while_the_token_stays", || {
        let server = StrictStateServer::start();
        let key = FullKey::iterable_side_input("t", "side", b"w");
        server.backend.preload(key.clone(), &[1, 2]);
        let cache = Arc::new(StateCache::default());
        let tokens = |token: &'static [u8]| {
            [(
                ("t".to_string(), "side".to_string()),
                Bytes::from_static(token),
            )]
            .into_iter()
            .collect()
        };

        let read = |instruction, token| {
            let descriptor = descriptor(server.endpoint());
            let channel = StateChannel::from_descriptor(instruction, &descriptor, "worker");
            let reader = FnApiSideInputReader::from_channel(&descriptor, channel)
                .unwrap()
                .with_shared_cache(Arc::clone(&cache), &tokens(token));
            reader
                .get_iterable("side", b"w")
                .unwrap()
                .map(|element| element.unwrap().to_vec())
                .collect::<Vec<_>>()
        };
        assert_eq!(read("inst_1", b"token_a"), [[1], [2]]);
        assert_eq!(read("inst_2", b"token_a"), [[1], [2]]);
        assert_eq!(server.backend.take_calls(), [Call::get(key.clone())]);

        assert_eq!(read("inst_3", b"token_b"), [[1], [2]]);
        assert_eq!(server.backend.take_calls(), [Call::get(key)]);
    });
}
