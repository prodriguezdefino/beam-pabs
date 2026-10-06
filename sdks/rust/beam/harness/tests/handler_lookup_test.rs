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

//! Tests for `lookup_handler`: how a transform in a runner-supplied descriptor is
//! matched to a registered handler, or to a built-in one (Flatten, WindowInto).
//!
//! Runners (Dataflow, Prism) rename transforms, so the handler is found from the payload
//! alone, never the id or name. Tests check *which* handler comes back, by pointer identity,
//! or run the built-in handler and check its output.

use std::collections::HashMap;
use std::sync::Arc;

use prost::Message;

use beam::coders::{PaneInfo, WindowedHeader};
use beam::internals::HandlerContext;
use beam::internals::{ElementSink, TransformFn};
use beam::pipeline::URN_MAP_WINDOWS;
use beam::values::{URN_WINDOW_MAPPING_GLOBAL, URN_WINDOW_MAPPING_IDENTITY};
use beam::windowing::window_into_handler_key;
use harness::bundle_processor::lookup_handler;
use model::pipeline as proto_pipeline;

fn noop() -> TransformFn {
    Arc::new(|_bytes: &[u8], _sink: &mut dyn ElementSink| Ok(()))
}

fn spec(urn: &str, payload: Vec<u8>) -> Option<proto_pipeline::FunctionSpec> {
    Some(proto_pipeline::FunctionSpec {
        urn: urn.to_string(),
        payload,
    })
}

fn transform(
    unique_name: &str,
    spec: Option<proto_pipeline::FunctionSpec>,
) -> proto_pipeline::PTransform {
    proto_pipeline::PTransform {
        unique_name: unique_name.to_string(),
        spec,
        ..Default::default()
    }
}

fn pardo_payload(do_fn: Option<&[u8]>) -> Vec<u8> {
    proto_pipeline::ParDoPayload {
        do_fn: do_fn.map(|payload| proto_pipeline::FunctionSpec {
            urn: "beam:dofn:rust:v1".to_string(),
            payload: payload.to_vec(),
        }),
        ..Default::default()
    }
    .encode_to_vec()
}

fn combine_payload(combine_fn: &[u8]) -> Vec<u8> {
    proto_pipeline::CombinePayload {
        combine_fn: Some(proto_pipeline::FunctionSpec {
            urn: "beam:combine_fn:rust:v1".to_string(),
            payload: combine_fn.to_vec(),
        }),
        accumulator_coder_id: "accum_coder".to_string(),
    }
    .encode_to_vec()
}

/// Asserts `found` is exactly the handler registered under `key`.
fn assert_resolves_to(
    found: Option<TransformFn>,
    handlers: &HashMap<String, TransformFn>,
    key: &str,
) {
    let found = found.unwrap_or_else(|| panic!("expected the handler registered as '{key}'"));
    let expected = &handlers[key];
    assert!(
        Arc::ptr_eq(&found, expected),
        "resolved a handler other than the one registered as '{key}'"
    );
}

/// Records what a handler emits: `(header bytes if emitted windowed, element bytes)`.
#[derive(Default)]
struct Recorder(Vec<(Option<Vec<u8>>, Vec<u8>)>);

impl ElementSink for Recorder {
    fn push(&mut self, element: Vec<u8>) -> Result<(), String> {
        self.0.push((None, element));
        Ok(())
    }

    fn push_windowed(&mut self, header: &WindowedHeader, element: Vec<u8>) -> Result<(), String> {
        self.0.push((Some(header.as_bytes().to_vec()), element));
        Ok(())
    }
}

fn invoke(
    handler: &TransformFn,
    header: &WindowedHeader,
    element: &[u8],
) -> Vec<(Option<Vec<u8>>, Vec<u8>)> {
    let mut recorder = Recorder::default();
    let mut instance = handler.instantiate();
    {
        let mut ctx = HandlerContext::new(&mut recorder).with_header(header);
        instance
            .process(element, &mut ctx)
            .expect("handler succeeds");
    }
    recorder.0
}

#[test]
fn pardo_payload_lookup_resolves_only_by_the_dofn_key() {
    // Handlers registered under a transform's id or unique_name are never found: the
    // payload is the one key, so a runner renaming the transform cannot change the result.
    let handlers = HashMap::from([
        ("ExtractWords".to_string(), noop()),
        ("CountWords".to_string(), noop()),
        ("step-7".to_string(), noop()),
        ("Named".to_string(), noop()),
        ("FromPayload".to_string(), noop()),
    ]);
    let resolved = [
        // Renamed by the runner, e.g. Dataflow Runner v2 fusion.
        ("ExtractWords-ptransform-40", "ExtractWords"),
        ("CountWords-ptransform-41", "CountWords"),
        ("Named", "FromPayload"),
    ];
    for (unique_name, key) in resolved {
        let t = transform(
            unique_name,
            spec(
                "beam:transform:pardo:v1",
                pardo_payload(Some(key.as_bytes())),
            ),
        );
        assert_resolves_to(lookup_handler(&handlers, &t), &handlers, key);
    }

    let declined: [(&str, Vec<u8>); 5] = [
        ("unregistered DoFn", pardo_payload(Some(b"Missing"))),
        ("empty DoFn key", pardo_payload(Some(b""))),
        ("no DoFn spec", pardo_payload(None)),
        ("non-UTF-8 key", pardo_payload(Some(&[0xff, 0xfe]))),
        ("undecodable payload", vec![0xff; 4]),
    ];
    for (case, payload) in declined {
        let t = transform("Named", spec("beam:transform:pardo:v1", payload));
        assert!(lookup_handler(&handlers, &t).is_none(), "{case}");
    }
}

#[test]
fn test_lookup_handler_via_combine_payload() {
    // A lifted combine registers one handler per stage; each stage must resolve to its own.
    let handlers = HashMap::from([
        ("CountWords/Sum/precombine".to_string(), noop()),
        ("CountWords/Sum/merge".to_string(), noop()),
        ("CountWords/Sum/extract".to_string(), noop()),
    ]);
    for (urn, key) in [
        (
            "beam:transform:combine_per_key_precombine:v1",
            "CountWords/Sum/precombine",
        ),
        (
            "beam:transform:combine_per_key_merge_accumulators:v1",
            "CountWords/Sum/merge",
        ),
        (
            "beam:transform:combine_per_key_extract_outputs:v1",
            "CountWords/Sum/extract",
        ),
    ] {
        // Runner lifted combine with runner-synthesized ID and name (e.g. Prism)
        let t = transform("e1_lift", spec(urn, combine_payload(b"CountWords/Sum")));
        assert_resolves_to(lookup_handler(&handlers, &t), &handlers, key);
    }

    let unknown = transform(
        "e2_lift",
        spec(
            "beam:transform:combine_per_key_precombine:v1",
            combine_payload(b"Other/Sum"),
        ),
    );
    assert!(lookup_handler(&handlers, &unknown).is_none());
    let empty = transform(
        "e3_lift",
        spec(
            "beam:transform:combine_per_key_merge_accumulators:v1",
            combine_payload(b""),
        ),
    );
    assert!(lookup_handler(&handlers, &empty).is_none());
}

#[test]
fn test_lookup_handler_via_flatten_payload() {
    let handlers = HashMap::new();

    // Dataflow Runner v2 synthesized flatten step (e.g. InputIdentity)
    let t = transform(
        "MergeIncomingAndSeed/InputIdentity-ptransform-66",
        spec("beam:transform:flatten:v1", Vec::new()),
    );
    let found =
        lookup_handler(&handlers, &t).expect("Flatten handler should be resolved for URN_FLATTEN");

    // Flatten is the identity: bytes pass through unchanged, header untouched.
    let header = WindowedHeader::global(1_700_000_000_000, PaneInfo::NO_FIRING);
    let element = [0x00, 0x05, b'h', b'e', b'l', b'l', b'o', 0xff];
    assert_eq!(
        invoke(&found, &header, &element),
        [(None, element.to_vec())]
    );
}

fn window_into(window_fn: Option<proto_pipeline::FunctionSpec>) -> proto_pipeline::PTransform {
    transform(
        "Window.Into()",
        spec(
            "beam:transform:window_into:v1",
            proto_pipeline::WindowIntoPayload { window_fn }.encode_to_vec(),
        ),
    )
}

#[test]
fn window_into_resolves_built_in_and_custom_window_fns() {
    let fixed_10s = proto_pipeline::FixedWindowsPayload {
        size: Some(prost_types::Duration {
            seconds: 10,
            nanos: 0,
        }),
        offset: None,
    };
    let t = window_into(spec(
        "beam:window_fn:fixed_windows:v1",
        fixed_10s.encode_to_vec(),
    ));
    let found = lookup_handler(&HashMap::new(), &t).expect("WindowInto is built in");

    let timestamp = 1_700_000_005_123;
    let input = WindowedHeader::global(timestamp, PaneInfo::NO_FIRING);
    let element = b"payload".to_vec();
    let out = invoke(&found, &input, &element);

    // IntervalWindow [1_700_000_000_000, 1_700_000_010_000): the end instant (sign-bit
    // flipped, big-endian) followed by the span as a varint (10_000 = 0x90 0x4E).
    let mut window = (1_700_000_010_000_u64 ^ (1 << 63)).to_be_bytes().to_vec();
    window.extend([0x90, 0x4E]);
    let expected = WindowedHeader::new(timestamp, &[window], PaneInfo::NO_FIRING);
    assert_eq!(out, [(Some(expected.as_bytes().to_vec()), element)]);

    // A custom window fn resolves by its payload key, not the transform name.
    let custom = proto_pipeline::FunctionSpec {
        urn: "beam:window_fn:custom:v1".to_string(),
        payload: vec![1, 2, 3],
    };
    let handlers = HashMap::from([
        (window_into_handler_key(&custom), noop()),
        ("Window.Into()".to_string(), noop()),
    ]);
    let t = window_into(Some(custom.clone()));
    assert_resolves_to(
        lookup_handler(&handlers, &t),
        &handlers,
        &window_into_handler_key(&custom),
    );

    let other = window_into(spec("beam:window_fn:custom:v1", vec![9]));
    assert!(lookup_handler(&handlers, &other).is_none());
}

#[test]
fn window_into_with_an_undecodable_window_fn_is_not_resolved() {
    let handlers = HashMap::new();
    assert!(lookup_handler(&handlers, &window_into(None)).is_none());
    assert!(
        lookup_handler(
            &handlers,
            &window_into(spec("beam:window_fn:not_a_window_fn:v1", Vec::new()))
        )
        .is_none()
    );
    let garbage = transform("w", spec("beam:transform:window_into:v1", vec![0xff; 3]));
    assert!(lookup_handler(&handlers, &garbage).is_none());
}

#[test]
fn sdf_stage_urns_ask_the_registered_handler_for_that_stage() {
    // A plain handler exposes no SDF stages, so an expanded SDF stage finds nothing to run.
    let handlers = HashMap::from([("MySdf".to_string(), noop())]);
    let t = transform(
        "renamed",
        spec(
            "beam:transform:sdf_pair_with_restriction:v1",
            pardo_payload(Some(b"MySdf")),
        ),
    );
    assert!(lookup_handler(&handlers, &t).is_none());
}

/// An SDF handler exposing a distinct handler per expanded stage URN.
#[derive(Clone)]
struct StagedSdf {
    stages: HashMap<String, TransformFn>,
}

impl beam::internals::BundleHandler for StagedSdf {
    fn process(&mut self, _element: &[u8], _ctx: &mut HandlerContext<'_>) -> Result<(), String> {
        Ok(())
    }

    fn stage_handler(&self, stage_urn: &str) -> Option<TransformFn> {
        self.stages.get(stage_urn).cloned()
    }

    fn instantiate(&self) -> beam::internals::HandlerInstance {
        Box::new(self.clone())
    }
}

/// Each stage resolves through the DoFn key in its payload.
#[test]
fn sdf_stage_urns_resolve_to_the_stage_handler() {
    let stage_urns = [
        beam::pipeline::URN_SDF_PAIR_WITH_RESTRICTION,
        beam::pipeline::URN_SDF_SPLIT_AND_SIZE_RESTRICTIONS,
        beam::pipeline::URN_SDF_PROCESS_SIZED_ELEMENT_AND_RESTRICTIONS,
    ];
    let stages: HashMap<String, TransformFn> = stage_urns
        .iter()
        .map(|urn| (urn.to_string(), noop()))
        .collect();
    let sdf: TransformFn = Arc::new(StagedSdf {
        stages: stages.clone(),
    });
    let handlers = HashMap::from([("MySdf".to_string(), sdf)]);

    for urn in stage_urns {
        let t = transform("renamed", spec(urn, pardo_payload(Some(b"MySdf"))));
        assert_resolves_to(lookup_handler(&handlers, &t), &stages, urn);
    }
}

#[test]
fn unknown_urns_and_missing_specs_are_not_resolved() {
    let handlers = HashMap::from([("X".to_string(), noop())]);
    assert!(lookup_handler(&handlers, &transform("b", None)).is_none());
    assert!(
        lookup_handler(
            &handlers,
            &transform("b", spec("beam:transform:group_by_key:v1", b"X".to_vec()))
        )
        .is_none()
    );
}

fn map_windows(mapping_urn: &str) -> proto_pipeline::PTransform {
    let fn_spec = proto_pipeline::FunctionSpec {
        urn: mapping_urn.to_string(),
        payload: Vec::new(),
    };
    transform(
        "map_windows",
        spec(URN_MAP_WINDOWS, fn_spec.encode_to_vec()),
    )
}

/// A mapping the SDK does not implement must not resolve: running it as identity would
/// read side inputs under the wrong window without any error.
#[test]
fn map_windows_with_an_unknown_mapping_is_not_resolved() {
    let handlers = HashMap::new();
    let unknown = map_windows("beam:window_mapping_fn:fixed:v1");
    assert!(lookup_handler(&handlers, &unknown).is_none());

    let undecodable = transform(
        "map_windows",
        spec(URN_MAP_WINDOWS, b"\xffglobal_window".to_vec()),
    );
    assert!(lookup_handler(&handlers, &undecodable).is_none());
}

/// The global mapping keeps only the nested, length-prefixed nonce at the head of the
/// element, whatever window bytes follow it; identity passes the element through.
#[test]
fn map_windows_emits_the_nonce_or_the_unchanged_element() {
    let header = WindowedHeader::global(0, PaneInfo::NO_FIRING);
    let mut nonce_and_window = vec![2u8, 0x01, 0x02];
    nonce_and_window.extend_from_slice(&[0xAA; 16]);
    let cases: [(&str, &[u8], &[u8]); 5] = [
        (
            URN_WINDOW_MAPPING_GLOBAL,
            &nonce_and_window,
            &[2, 0x01, 0x02],
        ),
        (URN_WINDOW_MAPPING_GLOBAL, b"\x03abcWINDOW", b"\x03abc"),
        (URN_WINDOW_MAPPING_GLOBAL, b"\x03abc", b"\x03abc"),
        (URN_WINDOW_MAPPING_GLOBAL, b"\x00rest", b"\x00"),
        (
            URN_WINDOW_MAPPING_IDENTITY,
            &[1, 2, 3, 4, 5],
            &[1, 2, 3, 4, 5],
        ),
    ];
    for (urn, element, expected) in cases {
        let handler =
            lookup_handler(&HashMap::new(), &map_windows(urn)).expect("MapWindows is built in");
        assert_eq!(
            invoke(&handler, &header, element),
            vec![(None, expected.to_vec())],
            "{urn}: {element:?}"
        );
    }

    // An element shorter than its length prefix claims fails the bundle; no partial nonce.
    let handler = lookup_handler(&HashMap::new(), &map_windows(URN_WINDOW_MAPPING_GLOBAL))
        .expect("MapWindows is built in");
    for element in [&b"\x05ab"[..], &b""[..]] {
        let mut recorder = Recorder::default();
        let mut instance = handler.instantiate();
        let mut ctx = HandlerContext::new(&mut recorder).with_header(&header);
        let err = instance
            .process(element, &mut ctx)
            .expect_err("a truncated nonce must fail");
        assert!(
            err.contains("Failed to decode nonce in map_windows"),
            "{err}"
        );
        drop(ctx);
        assert!(recorder.0.is_empty(), "{element:?}");
    }
}

#[test]
fn map_windows_resolves_the_identity_and_global_mappings_only() {
    let handlers = HashMap::new();
    for urn in [URN_WINDOW_MAPPING_GLOBAL, URN_WINDOW_MAPPING_IDENTITY] {
        assert!(
            lookup_handler(&handlers, &map_windows(urn)).is_some(),
            "{urn}"
        );
    }
    assert!(lookup_handler(&handlers, &map_windows("")).is_none());
}
