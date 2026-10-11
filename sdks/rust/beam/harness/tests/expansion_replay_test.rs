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

//! Tests for expanded specs: `lookup_handler` replays the expansion of the spec once for
//! each expansion id, then finds the handler key in the replayed handlers.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use prost::Message;

use beam::internals::{ElementSink, HandlerContext, TransformFn};
use beam::pipeline::{URN_PAR_DO, URN_RUST_DOFN};
use harness::bundle_processor::lookup_handler;
use harness::replay::{ExpandedSpec, ReplayEntry, ReplayRegistration, URN_RUST_DOFN_EXPANDED};
use model::pipeline as proto;

/// The expansion id of each replay that this test binary ran.
static REPLAYS: Mutex<Vec<String>> = Mutex::new(Vec::new());

/// Registers the handler `Fn`, which emits the config row, so a test can see which replay
/// built a handler.
fn fake_replay(entry: &ReplayEntry) -> Result<HashMap<String, TransformFn>, String> {
    REPLAYS
        .lock()
        .expect("replay log")
        .push(entry.expansion_id.clone());
    let emit = |config: Vec<u8>| -> TransformFn {
        Arc::new(move |_: &[u8], sink: &mut dyn ElementSink| sink.push(config.clone()))
    };
    Ok(HashMap::from([(
        "Fn".to_string(),
        emit(entry.config_row.clone()),
    )]))
}

inventory::submit! {
    ReplayRegistration { replay: fake_replay }
}

fn replay_count(expansion_id: &str) -> usize {
    REPLAYS
        .lock()
        .expect("replay log")
        .iter()
        .filter(|id| *id == expansion_id)
        .count()
}

/// An expanded spec whose namespace is its expansion id.
fn expanded_spec(urn: &str, key: &str, expansion_id: &str, config: &[u8]) -> proto::FunctionSpec {
    expanded_spec_in(urn, key, expansion_id, expansion_id, config)
}

fn expanded_spec_in(
    urn: &str,
    key: &str,
    expansion_id: &str,
    namespace: &str,
    config: &[u8],
) -> proto::FunctionSpec {
    proto::FunctionSpec {
        urn: urn.to_string(),
        payload: ExpandedSpec {
            handler_key: key.to_string(),
            replay: Some(ReplayEntry {
                provider: "beam:schematransform:test:v1".to_string(),
                config_row: config.to_vec(),
                namespace: namespace.to_string(),
                expansion_id: expansion_id.to_string(),
                ..Default::default()
            }),
        }
        .encode_to_vec(),
    }
}

fn pardo(do_fn: proto::FunctionSpec) -> proto::PTransform {
    proto::PTransform {
        spec: Some(proto::FunctionSpec {
            urn: URN_PAR_DO.to_string(),
            payload: proto::ParDoPayload {
                do_fn: Some(do_fn),
                ..Default::default()
            }
            .encode_to_vec(),
        }),
        ..Default::default()
    }
}

fn run(handler: &TransformFn) -> Vec<Vec<u8>> {
    let mut sink = Vec::<Vec<u8>>::new();
    let mut instance = handler.instantiate();
    instance
        .process(b"x", &mut HandlerContext::new(&mut sink))
        .expect("handler succeeds");
    sink
}

#[test]
fn expanded_do_fn_resolves_in_its_replay_once_per_expansion() {
    let spec = expanded_spec(URN_RUST_DOFN_EXPANDED, "Fn", "replay_once", b"config");
    for _ in 0..3 {
        let handler =
            lookup_handler(&HashMap::new(), &pardo(spec.clone())).expect("expanded do_fn resolves");
        assert_eq!(run(&handler), vec![b"config".to_vec()]);
    }
    assert_eq!(replay_count("replay_once"), 1);
}

#[test]
fn namespaces_with_different_configs_resolve_to_different_handlers() {
    let resolve = |namespace: &str, config: &[u8]| {
        let spec = expanded_spec(URN_RUST_DOFN_EXPANDED, "Fn", namespace, config);
        lookup_handler(&HashMap::new(), &pardo(spec)).expect("expanded do_fn resolves")
    };
    let first = resolve("ns_first", b"first");
    let second = resolve("ns_second", b"second");
    assert_eq!(run(&first), vec![b"first".to_vec()]);
    assert_eq!(run(&second), vec![b"second".to_vec()]);
}

#[test]
fn expansions_that_share_a_namespace_resolve_to_their_own_handlers() {
    // Expansions can have the same namespace, also an empty one. Each gets its own replay.
    for namespace in ["", "External_0"] {
        let resolve = |expansion_id: &str, config: &[u8]| {
            let spec = expanded_spec_in(
                URN_RUST_DOFN_EXPANDED,
                "Fn",
                expansion_id,
                namespace,
                config,
            );
            lookup_handler(&HashMap::new(), &pardo(spec)).expect("expanded do_fn resolves")
        };
        let first_id = format!("shared_first_{namespace}");
        let second_id = format!("shared_second_{namespace}");
        let first = resolve(&first_id, b"first");
        let second = resolve(&second_id, b"second");
        assert_eq!(run(&first), vec![b"first".to_vec()]);
        assert_eq!(run(&second), vec![b"second".to_vec()]);
        assert_eq!(replay_count(&first_id), 1);
        assert_eq!(replay_count(&second_id), 1);
    }
}

#[test]
fn expanded_spec_does_not_resolve_from_pipeline_handlers() {
    // Lookup uses only the replayed handlers, also when the pipeline has the key.
    let noop: TransformFn = Arc::new(|_: &[u8], _: &mut dyn ElementSink| Ok(()));
    let handlers = HashMap::from([("Missing".to_string(), noop)]);
    let spec = expanded_spec(URN_RUST_DOFN_EXPANDED, "Missing", "ns_missing", b"");
    assert!(lookup_handler(&handlers, &pardo(spec)).is_none());
}

#[test]
fn bare_do_fn_resolves_from_pipeline_handlers() {
    let noop: TransformFn = Arc::new(|_: &[u8], _: &mut dyn ElementSink| Ok(()));
    let handlers = HashMap::from([("Fn".to_string(), Arc::clone(&noop))]);
    let bare = proto::FunctionSpec {
        urn: URN_RUST_DOFN.to_string(),
        payload: b"Fn".to_vec(),
    };
    let found = lookup_handler(&handlers, &pardo(bare)).expect("bare key resolves");
    assert!(Arc::ptr_eq(&found, &noop));
}
