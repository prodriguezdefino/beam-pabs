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

//! State API protocol tests against a strict runner mock: full state keys, multi-page reads
//! through continuation tokens, runner errors and the side-input cache.

use std::collections::HashMap;

use prost::Message;

use beam::coders::StateStreamReader;
use beam::internals::SideInputReader;
use beam::pipeline::constants::URN_PAR_DO;
use harness::state::{FnApiSideInputReader, StateChannel};
use harness::user_state::BundleUserState;
use model::fn_execution::{ProcessBundleDescriptor, StateKey, state_key};
use model::pipeline::{self as proto, ApiServiceDescriptor, FunctionSpec};
use testutils::with_timeout;

mod common;
use common::state_mock::{Call, FullKey, Op, StrictStateServer};

fn coder(urn: &str, components: &[&str]) -> proto::Coder {
    proto::Coder {
        spec: Some(FunctionSpec {
            urn: urn.to_string(),
            payload: Vec::new(),
        }),
        component_coder_ids: components.iter().map(|c| c.to_string()).collect(),
    }
}

fn bag_spec() -> proto::StateSpec {
    proto::StateSpec {
        protocol: Some(FunctionSpec {
            urn: "beam:user_state:bag:v1".to_string(),
            payload: Vec::new(),
        }),
        spec: Some(proto::state_spec::Spec::BagSpec(proto::BagStateSpec {
            element_coder_id: "varint_coder".to_string(),
        })),
    }
}

fn pardo(payload: proto::ParDoPayload) -> Option<FunctionSpec> {
    Some(FunctionSpec {
        urn: URN_PAR_DO.to_string(),
        payload: payload.encode_to_vec(),
    })
}

/// Two stateful transforms: `t_a` with bags `state_accum` and `state_other`, and `t_b`
/// with its own `state_accum`, so every component of the key can be told apart.
fn stateful_descriptor(endpoint: &str) -> ProcessBundleDescriptor {
    let transform = |state_ids: &[&str]| proto::PTransform {
        spec: pardo(proto::ParDoPayload {
            state_specs: state_ids
                .iter()
                .map(|id| (id.to_string(), bag_spec()))
                .collect(),
            ..Default::default()
        }),
        ..Default::default()
    };
    ProcessBundleDescriptor {
        id: "desc_state_protocol".to_string(),
        state_api_service_descriptor: Some(ApiServiceDescriptor {
            url: endpoint.to_string(),
            ..Default::default()
        }),
        transforms: HashMap::from([
            (
                "t_a".to_string(),
                transform(&["state_accum", "state_other"]),
            ),
            ("t_b".to_string(), transform(&["state_accum"])),
        ]),
        coders: HashMap::from([(
            "varint_coder".to_string(),
            coder("beam:coder:varint:v1", &[]),
        )]),
        ..Default::default()
    }
}

fn user_state(server: &StrictStateServer, instruction: &str) -> BundleUserState {
    let descriptor = stateful_descriptor(server.endpoint());
    let channel = StateChannel::from_descriptor(instruction, &descriptor, "worker").unwrap();
    BundleUserState::from_channel(&descriptor, channel).unwrap()
}

fn bag_proto(transform_id: &str, state_id: &str, window: &[u8], key: &[u8]) -> StateKey {
    StateKey {
        r#type: Some(state_key::Type::BagUserState(state_key::BagUserState {
            transform_id: transform_id.to_string(),
            user_state_id: state_id.to_string(),
            window: window.to_vec(),
            key: key.to_vec(),
        })),
    }
}

#[test]
fn a_multi_page_bag_read_follows_continuation_tokens() {
    with_timeout("a_multi_page_bag_read_follows_continuation_tokens", || {
        let server = StrictStateServer::start();
        let cell = FullKey::bag("t_a", "state_accum", b"w", b"k");
        server.backend.preload(cell.clone(), &[1, 2, 3, 4, 5]);
        server.backend.set_page_size(2);

        let state = user_state(&server, "inst_pages");
        let items = state
            .scoped("t_a")
            .get_state("state_accum", b"w", b"k")
            .unwrap();

        assert_eq!(items, [[1], [2], [3], [4], [5]]);
        assert_eq!(
            server.backend.calls(),
            [
                Call::get_page(cell.clone(), b""),
                Call::get_page(cell.clone(), b"2"),
                Call::get_page(cell, b"4"),
            ]
        );
    });
}

#[test]
fn state_keys_keep_transform_state_id_window_and_key_apart() {
    with_timeout(
        "state_keys_keep_transform_state_id_window_and_key_apart",
        || {
            let server = StrictStateServer::start();
            let cells = [
                (("t_a", "state_accum", b"w1", b"k1"), 1),
                (("t_a", "state_other", b"w1", b"k1"), 2),
                (("t_a", "state_accum", b"w2", b"k1"), 3),
                (("t_b", "state_accum", b"w1", b"k1"), 4),
                (("t_a", "state_accum", b"w1", b"k2"), 5),
            ];
            for ((t, s, w, k), value) in cells {
                server.backend.preload(FullKey::bag(t, s, w, k), &[value]);
            }

            let state = user_state(&server, "inst_isolation");
            for ((t, s, w, k), value) in cells {
                assert_eq!(
                    state.scoped(t).get_state(s, w, k).unwrap(),
                    [[value]],
                    "{t}/{s}/{w:?}/{k:?}"
                );
            }

            // A write reaches exactly its own cell.
            state
                .scoped("t_b")
                .append_state("state_accum", b"w1", b"k1", vec![9])
                .unwrap();
            server.backend.take_calls();
            state.commit().unwrap();
            let written = FullKey::bag("t_b", "state_accum", b"w1", b"k1");
            assert_eq!(
                server.backend.calls(),
                [Call::append(written.clone(), &[9])]
            );
            assert_eq!(server.backend.stored(&written), Some(vec![4, 9]));
            assert_eq!(
                server
                    .backend
                    .stored(&FullKey::bag("t_a", "state_accum", b"w1", b"k1")),
                Some(vec![1])
            );
        },
    );
}

#[test]
fn a_runner_error_on_get_fails_the_whole_read_and_the_stream_recovers() {
    with_timeout("a_runner_error_on_get_fails_the_whole_read", || {
        let server = StrictStateServer::start();
        let cell = FullKey::bag("t_a", "state_accum", b"", b"k");
        server.backend.preload(cell.clone(), &[7]);
        server.backend.fail_next(Op::Get, None, "boom");

        let state = user_state(&server, "inst_err");
        let reader = state.scoped("t_a");
        assert_eq!(
            reader.get_state("state_accum", b"", b"k").unwrap_err(),
            "Runner state error: boom"
        );
        // The failed read cached nothing, so the next read asks again and succeeds.
        assert_eq!(reader.get_state("state_accum", b"", b"k").unwrap(), [[7]]);
        assert_eq!(
            server.backend.calls(),
            [Call::get(cell.clone()), Call::get(cell)]
        );

        let server = StrictStateServer::start();
        let cell = FullKey::bag("t_a", "state_accum", b"", b"k");
        server.backend.preload(cell.clone(), &[1, 2, 3]);
        server.backend.set_page_size(1);
        server.backend.fail_next(Op::Get, Some(b"1"), "page gone");

        let state = user_state(&server, "inst_page_err");
        assert_eq!(
            state
                .scoped("t_a")
                .get_state("state_accum", b"", b"k")
                .unwrap_err(),
            "Runner state error: page gone",
            "a partial read must not be returned as the full value"
        );
        assert_eq!(
            server.backend.calls(),
            [
                Call::get_page(cell.clone(), b""),
                Call::get_page(cell, b"1")
            ]
        );
    });
}

#[test]
fn a_runner_error_on_append_fails_the_commit() {
    with_timeout("a_runner_error_on_append_fails_the_commit", || {
        let server = StrictStateServer::start();
        server.backend.fail_next(Op::Append, None, "quota exceeded");

        let state = user_state(&server, "inst_append_err");
        state
            .scoped("t_a")
            .append_state("state_accum", b"", b"k", vec![1])
            .unwrap();
        assert_eq!(
            state.commit().unwrap_err(),
            "Runner state error: quota exceeded"
        );
        assert_eq!(
            server
                .backend
                .stored(&FullKey::bag("t_a", "state_accum", b"", b"k")),
            None
        );
    });
}

#[test]
fn stream_pages_yields_each_page_lazily() {
    with_timeout("stream_pages_yields_each_page_lazily", || {
        let server = StrictStateServer::start();
        let cell = FullKey::bag("t_a", "state_accum", b"", b"k");
        server.backend.preload(cell.clone(), &[1, 2, 3, 4, 5]);
        server.backend.set_page_size(2);
        let channel = StateChannel::new(
            "inst_stream".to_string(),
            server.endpoint().to_string(),
            String::new(),
        );

        let mut pages = channel
            .stream_pages(bag_proto("t_a", "state_accum", b"", b"k"))
            .unwrap();
        assert_eq!(pages.next(), Some(Ok(vec![1, 2])));
        assert_eq!(
            server.backend.calls(),
            [Call::get_page(cell, b"")],
            "only the first page is fetched until the next one is asked for"
        );
        assert_eq!(pages.next(), Some(Ok(vec![3, 4])));
        assert_eq!(pages.next(), Some(Ok(vec![5])));
        assert_eq!(pages.next(), None);
        assert_eq!(pages.next(), None);
        assert_eq!(server.backend.calls().len(), 3);
    });
}

#[test]
fn stream_pages_ends_after_a_runner_error() {
    with_timeout("stream_pages_ends_after_a_runner_error", || {
        let server = StrictStateServer::start();
        server.backend.preload(
            FullKey::Runner {
                key: b"tok".to_vec(),
            },
            &[1, 2, 3],
        );
        server.backend.set_page_size(2);
        server.backend.fail_next(Op::Get, Some(b"2"), "expired");
        let channel = StateChannel::new(
            "inst_stream_err".to_string(),
            server.endpoint().to_string(),
            String::new(),
        );

        // Runner-issued continuation tokens are read back under a `Runner` key.
        let pages: Vec<_> = channel.stream_runner_pages(b"tok").unwrap().collect();
        assert_eq!(
            pages,
            [
                Ok(vec![1, 2]),
                Err("Runner state error: expired".to_string())
            ]
        );
    });
}

/// A transform `t_side` with an iterable side input of varints (`si_iter`) and a
/// multimap side input of `KV<string, varint>` (`si_map`).
fn side_input_descriptor(endpoint: Option<&str>) -> ProcessBundleDescriptor {
    let pcoll = |coder_id: &str| proto::PCollection {
        coder_id: coder_id.to_string(),
        ..Default::default()
    };
    ProcessBundleDescriptor {
        id: "desc_side".to_string(),
        state_api_service_descriptor: endpoint.map(|url| ApiServiceDescriptor {
            url: url.to_string(),
            ..Default::default()
        }),
        transforms: HashMap::from([(
            "t_side".to_string(),
            proto::PTransform {
                spec: pardo(proto::ParDoPayload {
                    side_inputs: HashMap::from([
                        ("si_iter".to_string(), proto::SideInput::default()),
                        ("si_map".to_string(), proto::SideInput::default()),
                    ]),
                    ..Default::default()
                }),
                inputs: HashMap::from([
                    ("main".to_string(), "pc_main".to_string()),
                    ("si_iter".to_string(), "pc_iter".to_string()),
                    ("si_map".to_string(), "pc_map".to_string()),
                ]),
                ..Default::default()
            },
        )]),
        pcollections: HashMap::from([
            ("pc_iter".to_string(), pcoll("varint_coder")),
            ("pc_map".to_string(), pcoll("kv_coder")),
        ]),
        coders: HashMap::from([
            (
                "varint_coder".to_string(),
                coder("beam:coder:varint:v1", &[]),
            ),
            (
                "string_coder".to_string(),
                coder("beam:coder:string_utf8:v1", &[]),
            ),
            (
                "kv_coder".to_string(),
                coder("beam:coder:kv:v1", &["string_coder", "varint_coder"]),
            ),
        ]),
        ..Default::default()
    }
}

#[test]
fn side_input_reads_are_cached_per_window_and_key() {
    with_timeout("side_input_reads_are_cached_per_window_and_key", || {
        let server = StrictStateServer::start();
        let backend = &server.backend;
        let iter_w1 = FullKey::iterable_side_input("t_side", "si_iter", b"w1");
        let iter_w2 = FullKey::iterable_side_input("t_side", "si_iter", b"w2");
        let map_a = FullKey::multimap_side_input("t_side", "si_map", b"w1", b"\x01a");
        backend.preload(iter_w1.clone(), &[1, 2, 3]);
        backend.preload(iter_w2.clone(), &[4]);
        backend.preload(map_a.clone(), &[7, 8]);
        // Side-input values also arrive paged.
        backend.set_page_size(2);

        let descriptor = side_input_descriptor(Some(server.endpoint()));
        let channel = StateChannel::from_descriptor("inst_side", &descriptor, "worker");
        let reader = FnApiSideInputReader::from_channel(&descriptor, channel).unwrap();

        assert_eq!(
            reader
                .get_iterable_for_transform("t_side", "si_iter", b"w1")
                .unwrap(),
            [[1], [2], [3]]
        );
        let first_read = backend.take_calls();
        assert_eq!(
            first_read,
            [
                Call::get_page(iter_w1.clone(), b""),
                Call::get_page(iter_w1, b"2")
            ]
        );

        // Repeated reads, including by tag alone, are served from the cache.
        assert_eq!(
            reader
                .get_iterable_for_transform("t_side", "si_iter", b"w1")
                .unwrap(),
            [[1], [2], [3]]
        );
        assert_eq!(
            reader.get_iterable("si_iter", b"w1").unwrap(),
            [[1], [2], [3]]
        );
        assert_eq!(backend.take_calls(), [], "cache hits make no RPC");

        // Another window is another cache entry.
        assert_eq!(reader.get_iterable("si_iter", b"w2").unwrap(), [[4]]);
        assert_eq!(backend.take_calls(), [Call::get(iter_w2)]);

        // Multimap side inputs frame values with the KV's value coder.
        assert_eq!(
            reader
                .get_multimap_for_transform("t_side", "si_map", b"w1", b"\x01a")
                .unwrap(),
            [[7], [8]]
        );
        assert_eq!(
            reader.get_multimap("si_map", b"w1", b"\x01a").unwrap(),
            [[7], [8]]
        );
        assert_eq!(
            backend.take_calls(),
            [Call::get_page(map_a, b"")],
            "second multimap read is a cache hit"
        );
        // An absent key reads as empty and is cached too.
        assert!(
            reader
                .get_multimap("si_map", b"w1", b"\x01b")
                .unwrap()
                .is_empty()
        );
        assert!(
            reader
                .get_multimap("si_map", b"w1", b"\x01b")
                .unwrap()
                .is_empty()
        );
        assert_eq!(backend.take_calls().len(), 1);

        assert_eq!(
            reader.get_iterable("nope", b"w1").unwrap_err(),
            "FnApiSideInputReader: unknown side input tag 'nope'"
        );
    });
}

#[test]
fn side_input_reader_needs_a_state_endpoint_to_fetch() {
    let descriptor = side_input_descriptor(None);
    let reader = FnApiSideInputReader::from_channel(&descriptor, None).unwrap();
    assert_eq!(
        reader.get_iterable("si_iter", b"w").unwrap_err(),
        "FnApiSideInputReader: ProcessBundleDescriptor has no state_api_service_descriptor"
    );

    // A bundle declaring no side inputs gets no reader at all.
    let mut plain = side_input_descriptor(None);
    plain.transforms.clear();
    assert!(FnApiSideInputReader::from_channel(&plain, None).is_none());
}

#[test]
fn side_input_reader_from_descriptor_uses_the_advertised_endpoint() {
    with_timeout(
        "side_input_reader_from_descriptor_uses_the_advertised_endpoint",
        || {
            let server = StrictStateServer::start();
            let cell = FullKey::iterable_side_input("t_side", "si_iter", b"w");
            server.backend.preload(cell.clone(), &[1, 2]);

            let descriptor = side_input_descriptor(Some(server.endpoint()));
            let reader =
                FnApiSideInputReader::from_descriptor("inst_si_desc", &descriptor, "worker")
                    .expect("the descriptor declares side inputs");

            assert_eq!(reader.get_iterable("si_iter", b"w").unwrap(), [[1], [2]]);
            assert_eq!(server.backend.calls(), [Call::get(cell)]);
            assert_eq!(server.backend.instruction_ids(), ["inst_si_desc"]);
        },
    );
}

#[test]
fn side_input_tags_resolve_per_transform_when_tags_collide() {
    with_timeout(
        "side_input_tags_resolve_per_transform_when_tags_collide",
        || {
            let server = StrictStateServer::start();
            let mut descriptor = side_input_descriptor(Some(server.endpoint()));
            let twin = descriptor.transforms["t_side"].clone();
            descriptor.transforms.insert("t_twin".to_string(), twin);
            for (transform, value) in [("t_side", 1), ("t_twin", 2)] {
                server.backend.preload(
                    FullKey::iterable_side_input(transform, "si_iter", b"w"),
                    &[value],
                );
                server.backend.preload(
                    FullKey::multimap_side_input(transform, "si_map", b"w", b"\x01a"),
                    &[value + 10],
                );
            }

            let channel = StateChannel::from_descriptor("inst_twins", &descriptor, "worker");
            let reader = FnApiSideInputReader::from_channel(&descriptor, channel).unwrap();
            for (transform, value) in [("t_side", 1), ("t_twin", 2)] {
                assert_eq!(
                    reader
                        .get_iterable_for_transform(transform, "si_iter", b"w")
                        .unwrap(),
                    [[value]],
                    "iterable side input of {transform}"
                );
                assert_eq!(
                    reader
                        .get_multimap_for_transform(transform, "si_map", b"w", b"\x01a")
                        .unwrap(),
                    [[value + 10]],
                    "multimap side input of {transform}"
                );
            }
        },
    );
}

/// The `si_iter` side input of [`side_input_descriptor`], recoded as length-prefixed bytes.
fn length_prefixed_side_input(server: &StrictStateServer, data: &[u8]) -> FnApiSideInputReader {
    let mut descriptor = side_input_descriptor(Some(server.endpoint()));
    descriptor
        .pcollections
        .get_mut("pc_iter")
        .expect("side_input_descriptor defines pc_iter")
        .coder_id = "lp_coder".to_string();
    descriptor.coders.extend([
        (
            "lp_coder".to_string(),
            coder("beam:coder:length_prefix:v1", &["bytes_coder"]),
        ),
        ("bytes_coder".to_string(), coder("beam:coder:bytes:v1", &[])),
    ]);
    server.backend.preload(
        FullKey::iterable_side_input("t_side", "si_iter", b"w"),
        data,
    );
    let channel = StateChannel::from_descriptor("inst_lp", &descriptor, "worker");
    FnApiSideInputReader::from_channel(&descriptor, channel)
        .expect("the descriptor declares side inputs")
}

#[test]
fn length_prefixed_side_input_elements_are_split_on_their_prefixes() {
    with_timeout(
        "length_prefixed_side_input_elements_are_split_on_their_prefixes",
        || {
            let cases: [(&[u8], &[&[u8]]); 3] = [
                (
                    &[2, b'a', b'b', 0, 3, b'c', b'd', b'e'],
                    &[b"ab", b"", b"cde"],
                ),
                (&[1, b'x'], &[b"x"]),
                (&[0], &[b""]),
            ];
            for (data, expected) in cases {
                let server = StrictStateServer::start();
                let reader = length_prefixed_side_input(&server, data);
                assert_eq!(
                    reader.get_iterable("si_iter", b"w").unwrap(),
                    expected,
                    "{data:?}"
                );
            }
        },
    );
}

#[test]
fn a_truncated_length_prefixed_side_input_is_an_error() {
    with_timeout("a_truncated_length_prefixed_side_input_is_an_error", || {
        let server = StrictStateServer::start();
        let reader = length_prefixed_side_input(&server, &[1, b'a', 5, b'b', b'c']);
        assert_eq!(
            reader.get_iterable("si_iter", b"w").unwrap_err(),
            "Length prefix 5 exceeds data length 5"
        );
    });
}

/// The varint encoding of `n` (< 2^14).
fn varint(n: u16) -> Vec<u8> {
    if n < 0x80 {
        vec![n as u8]
    } else {
        vec![(n & 0x7f) as u8 | 0x80, (n >> 7) as u8]
    }
}

/// The cache must compare whole keys, not just hashes, and never serve another window's entry.
#[test]
fn side_input_cache_keeps_many_windows_apart() {
    with_timeout("side_input_cache_keeps_many_windows_apart", || {
        const WINDOWS: u16 = 500;
        let server = StrictStateServer::start();
        let window = |i: u16| format!("w{i}").into_bytes();
        for i in 0..WINDOWS {
            server.backend.preload(
                FullKey::iterable_side_input("t_side", "si_iter", &window(i)),
                &varint(i),
            );
        }
        let descriptor = side_input_descriptor(Some(server.endpoint()));
        let channel = StateChannel::from_descriptor("inst_windows", &descriptor, "worker");
        let reader = FnApiSideInputReader::from_channel(&descriptor, channel).unwrap();

        for pass in ["fetch", "cached"] {
            for i in 0..WINDOWS {
                assert_eq!(
                    reader.get_iterable("si_iter", &window(i)).unwrap(),
                    [varint(i)],
                    "window {i} ({pass})"
                );
            }
        }
        assert_eq!(
            server.backend.calls().len(),
            usize::from(WINDOWS),
            "one fetch per window"
        );
    });
}
