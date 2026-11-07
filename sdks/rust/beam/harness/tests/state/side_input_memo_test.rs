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

//! A side input reader remembers up to eight single-page cells of each side input for the
//! bundle. A repeated read of a remembered cell does not go to the state cache. The tests
//! count the hits and misses of the shared cache to see which reads use it. The kind of read
//! is part of the key of the memo and of the cache.

use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use prost::Message;

use beam::internals::SideInputReader;
use beam::pipeline::URN_PAR_DO;
use beam_harness::state::{CacheStats, FnApiSideInputReader, StateCache, StateClients};
use beam_model::fn_execution::ProcessBundleDescriptor;
use beam_model::pipeline::{self as proto, ApiServiceDescriptor, FunctionSpec};
use beam_test_utils::with_timeout;

use crate::common::side_input_reader;
use crate::state_mock::{Call, FullKey, Op, StrictStateServer};

const TOKEN: &[u8] = b"token_a";

/// A transform `t` with two side inputs of varints, `side` and `other`.
fn descriptor(endpoint: &str) -> ProcessBundleDescriptor {
    descriptor_with_coder(endpoint, "beam:coder:varint:v1")
}

/// A transform `t` with two side inputs, `side` and `other`, of the coder `urn`.
fn descriptor_with_coder(endpoint: &str, urn: &str) -> ProcessBundleDescriptor {
    let varint = proto::Coder {
        spec: Some(FunctionSpec {
            urn: urn.to_string(),
            payload: Vec::new(),
        }),
        component_coder_ids: Vec::new(),
    };
    let payload = proto::ParDoPayload {
        side_inputs: HashMap::from([
            ("side".to_string(), proto::SideInput::default()),
            ("other".to_string(), proto::SideInput::default()),
        ]),
        ..Default::default()
    };
    ProcessBundleDescriptor {
        id: "desc_memo".to_string(),
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
                inputs: HashMap::from([
                    ("side".to_string(), "pc_side".to_string()),
                    ("other".to_string(), "pc_side".to_string()),
                ]),
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
        coders: HashMap::from([("varint".to_string(), varint)]),
        ..Default::default()
    }
}

/// The reader of one bundle. With `cache`, both side inputs use it under [`TOKEN`].
fn reader(
    server: &StrictStateServer,
    instruction: &str,
    cache: Option<&Arc<StateCache>>,
) -> FnApiSideInputReader {
    reader_of(&descriptor(server.endpoint()), instruction, cache)
}

/// The reader of one bundle for `descriptor`. With `cache`, both side inputs use it under
/// [`TOKEN`].
fn reader_of(
    descriptor: &ProcessBundleDescriptor,
    instruction: &str,
    cache: Option<&Arc<StateCache>>,
) -> FnApiSideInputReader {
    let channel = StateClients::new("worker".to_string()).channel(instruction, descriptor);
    let reader = side_input_reader(descriptor, channel).unwrap();
    match cache {
        Some(cache) => {
            let tokens = ["side", "other"]
                .into_iter()
                .map(|tag| {
                    (
                        ("t".to_string(), tag.to_string()),
                        Bytes::from_static(TOKEN),
                    )
                })
                .collect();
            reader.with_shared_cache(Arc::clone(cache), &tokens)
        }
        None => reader,
    }
}

fn iterable(reader: &FnApiSideInputReader, tag: &str, window: &[u8]) -> Vec<Vec<u8>> {
    reader
        .get_iterable(tag, window)
        .unwrap()
        .map(|element| element.unwrap().to_vec())
        .collect()
}

fn hits_and_misses(cache: &StateCache) -> (u64, u64) {
    let CacheStats { hits, misses, .. } = cache.stats();
    (hits, misses)
}

#[test]
fn repeated_reads_of_one_window_do_not_use_the_cache() {
    with_timeout("repeated_reads_of_one_window_do_not_use_the_cache", || {
        let server = StrictStateServer::start();
        let key = FullKey::iterable_side_input("t", "side", b"w1");
        server.backend.preload(key.clone(), &[5]);
        let cache = Arc::new(StateCache::default());

        let bundle = reader(&server, "inst_1", Some(&cache));
        (0..3).for_each(|_| assert_eq!(iterable(&bundle, "side", b"w1"), [[5]]));

        assert_eq!(server.backend.take_calls(), [Call::get(key)]);
        assert_eq!(hits_and_misses(&cache), (0, 1), "one miss, then the memo");
    });
}

#[test]
fn reads_that_alternate_windows_return_the_value_of_each_window() {
    with_timeout(
        "reads_that_alternate_windows_return_the_value_of_each_window",
        || {
            let server = StrictStateServer::start();
            let w1 = FullKey::iterable_side_input("t", "side", b"w1");
            let w2 = FullKey::iterable_side_input("t", "side", b"w2");
            server.backend.preload(w1.clone(), &[1]);
            server.backend.preload(w2.clone(), &[2]);
            let cache = Arc::new(StateCache::default());

            let bundle = reader(&server, "inst_1", Some(&cache));
            let values: Vec<_> = [b"w1", b"w2", b"w1", b"w2", b"w2"]
                .into_iter()
                .map(|window| iterable(&bundle, "side", window))
                .collect();

            assert_eq!(values, [[[1]], [[2]], [[1]], [[2]], [[2]]]);
            assert_eq!(server.backend.take_calls(), [Call::get(w1), Call::get(w2)]);
            assert_eq!(
                hits_and_misses(&cache),
                (0, 2),
                "the memo keeps both windows so alternating reads skip the shared cache"
            );
        },
    );
}

#[test]
fn memo_keeps_eight_windows_and_evicts_the_oldest_on_a_ninth() {
    with_timeout(
        "memo_keeps_eight_windows_and_evicts_the_oldest_on_a_ninth",
        || {
            let server = StrictStateServer::start();
            for i in 0u8..9 {
                let win = [b'w', i];
                server
                    .backend
                    .preload(FullKey::iterable_side_input("t", "side", &win), &[i + 1]);
            }
            let cache = Arc::new(StateCache::default());
            let bundle = reader(&server, "inst_ring", Some(&cache));

            // Read 8 distinct windows twice: the second pass hits the memo with 0 cache hits.
            for _ in 0..2 {
                for i in 0u8..8 {
                    assert_eq!(iterable(&bundle, "side", &[b'w', i]), [[i + 1]]);
                }
            }
            assert_eq!(
                hits_and_misses(&cache),
                (0, 8),
                "all eight windows stay in the memo"
            );

            // A ninth window evicts the oldest slot (window 0), so reading window 0 hits cache.
            assert_eq!(iterable(&bundle, "side", &[b'w', 8]), [[9]]);
            assert_eq!(iterable(&bundle, "side", &[b'w', 0]), [[1]]);
            assert_eq!(
                hits_and_misses(&cache),
                (1, 9),
                "window 0 fell out of the 8-slot memo and served from StateCache"
            );
        },
    );
}

#[test]
fn each_side_input_has_its_own_memo() {
    with_timeout("each_side_input_has_its_own_memo", || {
        let server = StrictStateServer::start();
        server
            .backend
            .preload(FullKey::iterable_side_input("t", "side", b"w"), &[1]);
        server
            .backend
            .preload(FullKey::iterable_side_input("t", "other", b"w"), &[2]);
        let cache = Arc::new(StateCache::default());

        let bundle = reader(&server, "inst_1", Some(&cache));
        let values: Vec<_> = ["side", "other", "side", "other"]
            .into_iter()
            .map(|tag| iterable(&bundle, tag, b"w"))
            .collect();

        assert_eq!(values, [[[1]], [[2]], [[1]], [[2]]]);
        assert_eq!(hits_and_misses(&cache), (0, 2));
    });
}

#[test]
fn a_new_bundle_starts_with_an_empty_memo() {
    with_timeout("a_new_bundle_starts_with_an_empty_memo", || {
        let server = StrictStateServer::start();
        let key = FullKey::iterable_side_input("t", "side", b"w");
        server.backend.preload(key.clone(), &[1]);
        let cache = Arc::new(StateCache::default());

        assert_eq!(
            iterable(&reader(&server, "inst_1", Some(&cache)), "side", b"w"),
            [[1]]
        );
        assert_eq!(
            iterable(&reader(&server, "inst_2", Some(&cache)), "side", b"w"),
            [[1]]
        );

        assert_eq!(server.backend.take_calls(), [Call::get(key)]);
        assert_eq!(
            hits_and_misses(&cache),
            (1, 1),
            "the second bundle reads the shared cache, not the memo of the first bundle"
        );
    });
}

#[test]
fn an_iterable_read_and_a_multimap_read_of_one_window_are_different_cells() {
    with_timeout(
        "an_iterable_read_and_a_multimap_read_of_one_window_are_different_cells",
        || {
            let server = StrictStateServer::start();
            server
                .backend
                .preload(FullKey::iterable_side_input("t", "side", b"w"), &[1]);
            server
                .backend
                .preload(FullKey::multimap_side_input("t", "side", b"w", b"k"), &[7]);
            let cache = Arc::new(StateCache::default());

            let bundle = reader(&server, "inst_1", Some(&cache));
            let multimap = |bundle: &FnApiSideInputReader| -> Vec<Vec<u8>> {
                bundle
                    .get_multimap("side", b"w", b"k")
                    .unwrap()
                    .map(|element| element.unwrap().to_vec())
                    .collect()
            };

            assert_eq!(iterable(&bundle, "side", b"w"), [[1]]);
            assert_eq!(multimap(&bundle), [[7]]);
            assert_eq!(iterable(&bundle, "side", b"w"), [[1]]);
            assert_eq!(
                hits_and_misses(&cache),
                (0, 2),
                "both the iterable cell and the multimap cell stay in the memo"
            );
        },
    );
}

/// The cell of an iterable read and the cell of a multimap read with an empty key have the
/// same transform, side input, window and key bytes. Only the kind of read differs.
#[test]
fn an_iterable_read_and_a_multimap_read_with_an_empty_key_do_not_share_a_cache_entry() {
    with_timeout(
        "an_iterable_read_and_a_multimap_read_with_an_empty_key_do_not_share_a_cache_entry",
        || {
            let server = StrictStateServer::start();
            let iterable_key = FullKey::iterable_side_input("t", "side", b"w");
            let multimap_key = FullKey::multimap_side_input("t", "side", b"w", b"");
            server.backend.preload(iterable_key.clone(), &[1]);
            server.backend.preload(multimap_key.clone(), &[7]);
            let cache = Arc::new(StateCache::default());
            let multimap = |bundle: &FnApiSideInputReader| -> Vec<Vec<u8>> {
                bundle
                    .get_multimap("side", b"w", b"")
                    .unwrap()
                    .map(|element| element.unwrap().to_vec())
                    .collect()
            };

            [("inst_shared", Some(&cache)), ("inst_bundle", None)]
                .into_iter()
                .for_each(|(instruction, cache)| {
                    let bundle = reader(&server, instruction, cache);
                    let values = [
                        iterable(&bundle, "side", b"w"),
                        multimap(&bundle),
                        iterable(&bundle, "side", b"w"),
                        multimap(&bundle),
                    ];

                    assert_eq!(values, [[[1]], [[7]], [[1]], [[7]]], "{instruction}");
                    assert_eq!(
                        server.backend.take_calls(),
                        [
                            Call::get(iterable_key.clone()),
                            Call::get(multimap_key.clone())
                        ],
                        "{instruction}: one runner call for each kind of read"
                    );
                });
        },
    );
}

#[test]
fn a_cell_of_more_pages_is_read_through_the_cache() {
    with_timeout("a_cell_of_more_pages_is_read_through_the_cache", || {
        let server = StrictStateServer::start();
        server.backend.set_page_size(1);
        let key = FullKey::iterable_side_input("t", "side", b"w");
        server.backend.preload(key, &[1, 2, 3]);
        let cache = Arc::new(StateCache::default());

        let bundle = reader(&server, "inst_1", Some(&cache));
        assert_eq!(iterable(&bundle, "side", b"w"), [[1], [2], [3]]);
        let fetched = server.backend.take_calls().len();
        assert_eq!(iterable(&bundle, "side", b"w"), [[1], [2], [3]]);

        assert_eq!(fetched, 3, "one runner call for each page");
        assert_eq!(server.backend.take_calls(), []);
        assert_eq!(
            hits_and_misses(&cache),
            (3, 3),
            "the memo keeps no page of a cell of more pages, so the cache limit holds"
        );
    });
}

#[test]
fn a_failed_read_is_not_remembered() {
    with_timeout("a_failed_read_is_not_remembered", || {
        let server = StrictStateServer::start();
        let key = FullKey::iterable_side_input("t", "side", b"w");
        server.backend.preload(key.clone(), &[1]);
        server.backend.fail_next(Op::Get, None, "unavailable");

        let bundle = reader(&server, "inst_1", None);
        let failed: Vec<_> = bundle.get_iterable("side", b"w").unwrap().collect();
        assert!(matches!(failed.as_slice(), [Err(_)]), "{failed:?}");
        assert_eq!(iterable(&bundle, "side", b"w"), [[1]]);

        assert_eq!(
            server.backend.take_calls(),
            [Call::get(key.clone()), Call::get(key)]
        );
    });
}

/// The memos of a bundle hold at most 64 MiB of pages. A page that would pass the limit goes
/// through the cache only, so a repeated read of it is a cache hit.
#[test]
fn memo_pages_stay_within_the_bundle_byte_limit() {
    with_timeout("memo_pages_stay_within_the_bundle_byte_limit", || {
        const PAGE: usize = 9 << 20;
        let server = StrictStateServer::start();
        let windows: Vec<Vec<u8>> = (0..8u8).map(|w| vec![b'w', w]).collect();
        for (w, window) in windows.iter().enumerate() {
            let element = vec![u8::try_from(w).unwrap(); PAGE];
            let mut page = Vec::new();
            beam::coders::VarIntCoder::encode_varint(PAGE as i64, &mut page).unwrap();
            page.extend_from_slice(&element);
            let key = FullKey::iterable_side_input("t", "side", window);
            server.backend.preload(key, &page);
        }
        let cache = Arc::new(StateCache::default());
        let descriptor = descriptor_with_coder(server.endpoint(), "beam:coder:bytes:v1");
        let bundle = reader_of(&descriptor, "inst_big", Some(&cache));

        for window in &windows {
            assert_eq!(iterable(&bundle, "side", window).len(), 1);
        }
        let (hits, misses) = hits_and_misses(&cache);
        for window in &windows {
            assert_eq!(iterable(&bundle, "side", window).len(), 1);
        }
        // Seven pages fit in the memos. The eighth passes 64 MiB, so it comes from the cache.
        assert_eq!(hits_and_misses(&cache), (hits + 1, misses));
    });
}
