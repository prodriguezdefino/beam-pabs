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

//! Integration tests for the deferred-commit `BundleUserState` and `StateChannel`.

use std::collections::HashMap;

use prost::Message;

use beam::pipeline::constants::URN_PAR_DO;
use harness::state::StateChannel;
use harness::user_state::BundleUserState;
use model::fn_execution::ProcessBundleDescriptor;
use model::pipeline::{self as proto, ApiServiceDescriptor, FunctionSpec};
use testutils::with_timeout;

mod common;
use common::state_mock::{Call, FullKey, Op, StrictStateServer};

fn create_stateful_descriptor(endpoint: &str) -> ProcessBundleDescriptor {
    let mut descriptor = ProcessBundleDescriptor {
        id: "desc_stateful".to_string(),
        state_api_service_descriptor: Some(ApiServiceDescriptor {
            url: endpoint.to_string(),
            ..Default::default()
        }),
        ..Default::default()
    };

    let bag_spec = proto::StateSpec {
        protocol: Some(FunctionSpec {
            urn: "beam:user_state:bag:v1".to_string(),
            payload: Vec::new(),
        }),
        spec: Some(proto::state_spec::Spec::BagSpec(proto::BagStateSpec {
            element_coder_id: "varint_coder".to_string(),
        })),
    };

    let pardo_payload = proto::ParDoPayload {
        state_specs: HashMap::from([("state_accum".to_string(), bag_spec)]),
        ..Default::default()
    };

    descriptor.transforms.insert(
        "t_stateful".to_string(),
        proto::PTransform {
            unique_name: "t_stateful".to_string(),
            spec: Some(FunctionSpec {
                urn: URN_PAR_DO.to_string(),
                payload: pardo_payload.encode_to_vec(),
            }),
            ..Default::default()
        },
    );

    descriptor.coders.insert(
        "varint_coder".to_string(),
        proto::Coder {
            spec: Some(FunctionSpec {
                urn: "beam:coder:varint:v1".to_string(),
                payload: Vec::new(),
            }),
            component_coder_ids: Vec::new(),
        },
    );

    descriptor
}

#[test]
fn test_bundle_user_state_deferred_commit_lifecycle() {
    with_timeout(
        "test_bundle_user_state_deferred_commit_lifecycle",
        bundle_user_state_deferred_commit_lifecycle,
    );
}

fn bundle_user_state_deferred_commit_lifecycle() {
    let server = StrictStateServer::start();
    let backend = &server.backend;

    let descriptor = create_stateful_descriptor(server.endpoint());
    let channel = StateChannel::from_descriptor("inst_1", &descriptor, "worker_1").unwrap();
    let bundle_state = BundleUserState::from_channel(&descriptor, channel).unwrap();
    let reader = bundle_state.scoped("t_stateful");

    let key_a = b"key_a".to_vec();
    let window = Vec::new();
    // The full key the harness must use: transform, state id, window and user key.
    let cell = FullKey::bag("t_stateful", "state_accum", &window, &key_a);

    // The first read fetches the cell from the runner. The mock server returns no items.
    let initial_items = reader.get_state("state_accum", &window, &key_a).unwrap();
    assert!(initial_items.is_empty());

    // A second read in the same bundle uses the in-memory cache and sends no GET.
    let cached_items = reader.get_state("state_accum", &window, &key_a).unwrap();
    assert!(cached_items.is_empty());
    assert_eq!(
        backend.take_calls(),
        [Call::get(cell.clone())],
        "exactly one GET, for the full state key"
    );

    // Appends stay in memory and send no request. varint(10) = [0x0A], varint(20) = [0x14].
    reader
        .append_state("state_accum", &window, &key_a, vec![0x0A])
        .unwrap();
    reader
        .append_state("state_accum", &window, &key_a, vec![0x14])
        .unwrap();
    assert_eq!(
        backend.calls(),
        [],
        "No network RPCs should occur during in-bundle appends"
    );

    // An in-bundle read returns the local appends.
    let read_back = reader.get_state("state_accum", &window, &key_a).unwrap();
    assert_eq!(read_back, vec![vec![0x0A], vec![0x14]]);

    // The commit at the end of the bundle sends all buffered appends in one request.
    bundle_state.commit().unwrap();
    assert_eq!(
        backend.take_calls(),
        [Call::append(cell.clone(), &[0x0A, 0x14])]
    );
    assert_eq!(backend.stored(&cell), Some(vec![0x0A, 0x14]));

    // A new bundle reads the committed items from the runner.
    let channel_2 = StateChannel::from_descriptor("inst_2", &descriptor, "worker_1").unwrap();
    let bundle_state_2 = BundleUserState::from_channel(&descriptor, channel_2).unwrap();
    let reader_2 = bundle_state_2.scoped("t_stateful");

    let bundle2_items = reader_2.get_state("state_accum", &window, &key_a).unwrap();
    assert_eq!(bundle2_items, vec![vec![0x0A], vec![0x14]]);

    // A clear makes the cell read as empty before the commit.
    reader_2
        .clear_state("state_accum", &window, &key_a)
        .unwrap();
    assert!(
        reader_2
            .get_state("state_accum", &window, &key_a)
            .unwrap()
            .is_empty()
    );

    bundle_state_2.commit().unwrap();
    assert_eq!(
        backend.take_calls(),
        [Call::get(cell.clone()), Call::clear(cell.clone())],
        "a cleared cell with no appends commits as one CLEAR"
    );
    assert_eq!(backend.stored(&cell), None);

    // After the committed clear, a new bundle reads the cell as empty.
    let channel_3 = StateChannel::from_descriptor("inst_3", &descriptor, "worker_1").unwrap();
    let bundle_state_3 = BundleUserState::from_channel(&descriptor, channel_3).unwrap();
    let reader_3 = bundle_state_3.scoped("t_stateful");

    let bundle3_items = reader_3.get_state("state_accum", &window, &key_a).unwrap();
    assert!(
        bundle3_items.is_empty(),
        "Expected empty state after CLEAR was committed"
    );
}

fn create_map_stateful_descriptor(endpoint: &str) -> ProcessBundleDescriptor {
    let mut descriptor = ProcessBundleDescriptor {
        id: "desc_map_stateful".to_string(),
        state_api_service_descriptor: Some(ApiServiceDescriptor {
            url: endpoint.to_string(),
            ..Default::default()
        }),
        ..Default::default()
    };

    let map_spec = proto::StateSpec {
        protocol: Some(FunctionSpec {
            urn: "beam:user_state:multimap:v1".to_string(),
            payload: Vec::new(),
        }),
        spec: Some(proto::state_spec::Spec::MapSpec(proto::MapStateSpec {
            key_coder_id: "string_coder".to_string(),
            value_coder_id: "varint_coder".to_string(),
        })),
    };

    let pardo_payload = proto::ParDoPayload {
        state_specs: HashMap::from([("state_map".to_string(), map_spec)]),
        ..Default::default()
    };

    descriptor.transforms.insert(
        "t_map_stateful".to_string(),
        proto::PTransform {
            unique_name: "t_map_stateful".to_string(),
            spec: Some(FunctionSpec {
                urn: URN_PAR_DO.to_string(),
                payload: pardo_payload.encode_to_vec(),
            }),
            ..Default::default()
        },
    );

    descriptor.coders.insert(
        "string_coder".to_string(),
        proto::Coder {
            spec: Some(FunctionSpec {
                urn: "beam:coder:string_utf8:v1".to_string(),
                payload: Vec::new(),
            }),
            component_coder_ids: Vec::new(),
        },
    );

    descriptor.coders.insert(
        "varint_coder".to_string(),
        proto::Coder {
            spec: Some(FunctionSpec {
                urn: "beam:coder:varint:v1".to_string(),
                payload: Vec::new(),
            }),
            component_coder_ids: Vec::new(),
        },
    );

    descriptor
}

#[test]
fn test_bundle_map_state_deferred_commit_lifecycle() {
    with_timeout(
        "test_bundle_map_state_deferred_commit_lifecycle",
        bundle_map_state_deferred_commit_lifecycle,
    );
}

fn bundle_map_state_deferred_commit_lifecycle() {
    let server = StrictStateServer::start();
    let backend = &server.backend;

    let descriptor = create_map_stateful_descriptor(server.endpoint());
    let channel = StateChannel::from_descriptor("inst_map_1", &descriptor, "worker_1").unwrap();
    let bundle_state = BundleUserState::from_channel(&descriptor, channel).unwrap();
    let reader = bundle_state.scoped("t_map_stateful");

    let key_user = b"user_1".to_vec();
    let window = Vec::new();
    // Map keys as the key coder (string_utf8, nested) encodes them.
    let score = b"\x05score".to_vec();
    let level = b"\x05level".to_vec();
    let entry = |map_key: &[u8]| {
        FullKey::multimap("t_map_stateful", "state_map", &window, &key_user, map_key)
    };
    let keys = FullKey::multimap_keys("t_map_stateful", "state_map", &window, &key_user);

    // The first read of an entry returns no items.
    let initial_val = reader
        .get_map_state("state_map", &window, &key_user, &score)
        .unwrap();
    assert!(initial_val.is_empty());
    assert_eq!(backend.take_calls(), [Call::get(entry(&score))]);

    // Puts stay in memory and send no request.
    reader
        .put_map_state(
            "state_map",
            &window,
            &key_user,
            score.clone(),
            vec![0x64], // varint 100
        )
        .unwrap();
    reader
        .put_map_state(
            "state_map",
            &window,
            &key_user,
            level.clone(),
            vec![0x05], // varint 5
        )
        .unwrap();

    // In-bundle reads use the local buffer.
    let read_score = reader
        .get_map_state("state_map", &window, &key_user, &score)
        .unwrap();
    assert_eq!(read_score, vec![vec![0x64]]);

    let mut read_keys = reader
        .get_map_keys("state_map", &window, &key_user)
        .unwrap();
    read_keys.sort();
    assert_eq!(read_keys, [level.clone(), score.clone()]);
    assert_eq!(
        backend.take_calls(),
        [Call::get(keys.clone())],
        "puts are buffered; listing keys fetches the persisted key set once"
    );

    // The commit sends each put as a CLEAR and an APPEND, which replaces the entry.
    bundle_state.commit().unwrap();
    let commit_calls = backend.take_calls();
    for (map_key, value) in [(&score, [0x64]), (&level, [0x05])] {
        let for_entry: Vec<_> = commit_calls
            .iter()
            .filter(|c| c.key == entry(map_key))
            .cloned()
            .collect();
        assert_eq!(
            for_entry,
            [
                Call::clear(entry(map_key)),
                Call::append(entry(map_key), &value)
            ]
        );
    }
    assert_eq!(commit_calls.len(), 4, "{commit_calls:?}");
    assert_eq!(backend.stored(&entry(&score)), Some(vec![0x64]));
    assert_eq!(backend.stored(&entry(&level)), Some(vec![0x05]));

    // A new bundle reads the committed entries from the runner.
    let channel_2 = StateChannel::from_descriptor("inst_map_2", &descriptor, "worker_1").unwrap();
    let bundle_state_2 = BundleUserState::from_channel(&descriptor, channel_2).unwrap();
    let reader_2 = bundle_state_2.scoped("t_map_stateful");

    let b2_score = reader_2
        .get_map_state("state_map", &window, &key_user, &score)
        .unwrap();
    assert_eq!(b2_score, vec![vec![0x64]]);
    let mut b2_keys = reader_2
        .get_map_keys("state_map", &window, &key_user)
        .unwrap();
    b2_keys.sort();
    assert_eq!(b2_keys, [level.clone(), score.clone()], "persisted key set");

    // A removed key reads as empty before the commit.
    reader_2
        .remove_map_key("state_map", &window, &key_user, &score)
        .unwrap();
    assert!(
        reader_2
            .get_map_state("state_map", &window, &key_user, &score)
            .unwrap()
            .is_empty()
    );

    backend.take_calls();
    bundle_state_2.commit().unwrap();
    assert_eq!(backend.take_calls(), [Call::clear(entry(&score))]);

    // After the commit, the removed key is empty and the other key remains.
    let channel_3 = StateChannel::from_descriptor("inst_map_3", &descriptor, "worker_1").unwrap();
    let bundle_state_3 = BundleUserState::from_channel(&descriptor, channel_3).unwrap();
    let reader_3 = bundle_state_3.scoped("t_map_stateful");

    assert!(
        reader_3
            .get_map_state("state_map", &window, &key_user, &score)
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        reader_3
            .get_map_state("state_map", &window, &key_user, &level)
            .unwrap(),
        vec![vec![0x05]]
    );

    // A clear of the whole map commits one CLEAR of the key set.
    reader_3
        .clear_map_state("state_map", &window, &key_user)
        .unwrap();
    backend.take_calls();
    bundle_state_3.commit().unwrap();
    assert_eq!(backend.take_calls(), [Call::clear(keys)]);
    assert_eq!(backend.stored(&entry(&level)), None);

    // After the committed clear, the map has no entries and no keys.
    let channel_4 = StateChannel::from_descriptor("inst_map_4", &descriptor, "worker_1").unwrap();
    let bundle_state_4 = BundleUserState::from_channel(&descriptor, channel_4).unwrap();
    let reader_4 = bundle_state_4.scoped("t_map_stateful");

    assert!(
        reader_4
            .get_map_state("state_map", &window, &key_user, &level)
            .unwrap()
            .is_empty()
    );
    assert!(
        reader_4
            .get_map_keys("state_map", &window, &key_user)
            .unwrap()
            .is_empty()
    );
    assert!(backend.calls().iter().all(|c| c.op == Op::Get));
}

#[test]
fn bundle_user_state_from_descriptor_needs_an_endpoint_and_state_specs() {
    with_timeout(
        "bundle_user_state_from_descriptor_needs_an_endpoint_and_state_specs",
        || {
            let server = StrictStateServer::start();
            let cell = FullKey::bag("t_stateful", "state_accum", b"w", b"k");
            server.backend.preload(cell.clone(), &[0x0A]);

            let state = BundleUserState::from_descriptor(
                "inst_desc",
                &create_stateful_descriptor(server.endpoint()),
                "worker_1",
            )
            .expect("an endpoint and a state spec give user state");
            assert_eq!(
                state
                    .scoped("t_stateful")
                    .get_state("state_accum", b"w", b"k")
                    .unwrap(),
                [[0x0A]]
            );
            assert_eq!(server.backend.calls(), [Call::get(cell)]);

            let mut no_endpoint = create_stateful_descriptor(server.endpoint());
            no_endpoint.state_api_service_descriptor = None;
            let mut no_specs = create_stateful_descriptor(server.endpoint());
            no_specs.transforms.clear();
            for (what, descriptor) in [("no endpoint", no_endpoint), ("no specs", no_specs)] {
                assert!(
                    BundleUserState::from_descriptor("inst_none", &descriptor, "worker_1")
                        .is_none(),
                    "{what}"
                );
            }
        },
    );
}

/// One transform declaring a varint state of every spec kind.
fn every_kind_descriptor(endpoint: &str) -> ProcessBundleDescriptor {
    let spec = |spec| proto::StateSpec {
        protocol: None,
        spec: Some(spec),
    };
    let varint = || "varint_coder".to_string();
    let specs = HashMap::from([
        (
            "bag".to_string(),
            spec(proto::state_spec::Spec::BagSpec(proto::BagStateSpec {
                element_coder_id: varint(),
            })),
        ),
        (
            "rmw".to_string(),
            spec(proto::state_spec::Spec::ReadModifyWriteSpec(
                proto::ReadModifyWriteStateSpec { coder_id: varint() },
            )),
        ),
        (
            "map".to_string(),
            spec(proto::state_spec::Spec::MapSpec(proto::MapStateSpec {
                key_coder_id: varint(),
                value_coder_id: varint(),
            })),
        ),
        (
            "set".to_string(),
            spec(proto::state_spec::Spec::SetSpec(proto::SetStateSpec {
                element_coder_id: varint(),
            })),
        ),
        (
            "multimap".to_string(),
            spec(proto::state_spec::Spec::MultimapSpec(
                proto::MultimapStateSpec {
                    key_coder_id: varint(),
                    value_coder_id: varint(),
                },
            )),
        ),
    ]);
    let mut descriptor = create_stateful_descriptor(endpoint);
    descriptor.transforms.insert(
        "t_kinds".to_string(),
        proto::PTransform {
            spec: Some(FunctionSpec {
                urn: URN_PAR_DO.to_string(),
                payload: proto::ParDoPayload {
                    state_specs: specs,
                    ..Default::default()
                }
                .encode_to_vec(),
            }),
            ..Default::default()
        },
    );
    descriptor
}

#[test]
fn every_state_spec_kind_is_registered() {
    with_timeout("every_state_spec_kind_is_registered", || {
        let server = StrictStateServer::start();
        let descriptor = every_kind_descriptor(server.endpoint());
        let channel = StateChannel::from_descriptor("inst_kinds", &descriptor, "worker_1").unwrap();
        let state = BundleUserState::from_channel(&descriptor, channel).unwrap();
        let reader = state.scoped("t_kinds");

        for state_id in ["bag", "rmw"] {
            server
                .backend
                .preload(FullKey::bag("t_kinds", state_id, b"w", b"k"), &[5]);
            assert_eq!(
                reader.get_state(state_id, b"w", b"k"),
                Ok(vec![vec![5]]),
                "{state_id}"
            );
        }
        for state_id in ["map", "set", "multimap"] {
            server.backend.preload(
                FullKey::multimap("t_kinds", state_id, b"w", b"k", &[1]),
                &[7],
            );
            assert_eq!(
                reader.get_map_keys(state_id, b"w", b"k"),
                Ok(vec![vec![1]]),
                "{state_id}"
            );
            assert_eq!(
                reader.get_map_state(state_id, b"w", b"k", &[1]),
                Ok(vec![vec![7]]),
                "{state_id}"
            );
        }
    });
}

/// Later listings, before or after in-bundle puts, are served from memory.
#[test]
fn map_keys_are_fetched_once_per_cell() {
    with_timeout("map_keys_are_fetched_once_per_cell", || {
        let server = StrictStateServer::start();
        let descriptor = create_map_stateful_descriptor(server.endpoint());
        let channel = StateChannel::from_descriptor("inst_keys", &descriptor, "worker_1").unwrap();
        let state = BundleUserState::from_channel(&descriptor, channel).unwrap();
        let reader = state.scoped("t_map_stateful");
        let score = b"\x05score".to_vec();
        let level = b"\x05level".to_vec();
        server.backend.preload(
            FullKey::multimap("t_map_stateful", "state_map", b"", b"u", &score),
            &[1],
        );

        assert_eq!(
            reader.get_map_keys("state_map", b"", b"u"),
            Ok(vec![score.clone()])
        );
        assert_eq!(
            reader.get_map_keys("state_map", b"", b"u"),
            Ok(vec![score.clone()])
        );
        reader
            .put_map_state("state_map", b"", b"u", level.clone(), vec![2])
            .unwrap();
        let mut keys = reader.get_map_keys("state_map", b"", b"u").unwrap();
        keys.sort();
        assert_eq!(keys, [level, score]);

        assert_eq!(
            server.backend.calls(),
            [Call::get(FullKey::multimap_keys(
                "t_map_stateful",
                "state_map",
                b"",
                b"u"
            ))]
        );
    });
}

/// The removal's CLEAR must not be committed a second time.
#[test]
fn re_putting_a_removed_map_key_commits_a_single_clear() {
    with_timeout(
        "re_putting_a_removed_map_key_commits_a_single_clear",
        || {
            let server = StrictStateServer::start();
            let descriptor = create_map_stateful_descriptor(server.endpoint());
            let channel =
                StateChannel::from_descriptor("inst_reput", &descriptor, "worker_1").unwrap();
            let state = BundleUserState::from_channel(&descriptor, channel).unwrap();
            let reader = state.scoped("t_map_stateful");
            let score = b"\x05score".to_vec();
            let entry = FullKey::multimap("t_map_stateful", "state_map", b"", b"u", &score);
            server.backend.preload(entry.clone(), &[1]);

            reader
                .remove_map_key("state_map", b"", b"u", &score)
                .unwrap();
            reader
                .put_map_state("state_map", b"", b"u", score, vec![2])
                .unwrap();
            state.commit().unwrap();

            assert_eq!(
                server.backend.calls(),
                [
                    Call::clear(entry.clone()),
                    Call::append(entry.clone(), &[2])
                ]
            );
            assert_eq!(server.backend.stored(&entry), Some(vec![2]));
        },
    );
}
