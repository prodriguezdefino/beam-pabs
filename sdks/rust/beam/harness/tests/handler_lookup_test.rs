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
use beam::internals::{ElementSink, PipelineInternals, TransformFn, window_into_handler_key};
use beam::pipeline::URN_MAP_WINDOWS;
use beam::values::{URN_WINDOW_MAPPING_GLOBAL, URN_WINDOW_MAPPING_IDENTITY};
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

/// The `Process` transform of the `Create` named `name` in `pipeline`.
fn create_process(pipeline: &beam::pipeline::Pipeline, name: &str) -> proto_pipeline::PTransform {
    pipeline
        .to_proto()
        .components
        .expect("components")
        .transforms
        .into_values()
        .find(|t| t.unique_name == format!("{name}/Process"))
        .expect("Create has a Process transform")
}

#[test]
fn create_resolves_from_its_payload_without_registered_handlers() {
    use beam::coders::DefaultCoder;
    use beam::transforms::Create;

    let p = beam::pipeline::Pipeline::new();
    let _ = p.apply(Create::new("Create", vec![1i64, 2, 3]));
    let process = create_process(&p, "Create");
    assert!(
        p.transform_handlers().is_empty(),
        "Create must not register a handler"
    );

    let handler = lookup_handler(&HashMap::new(), &process).expect("Create resolves");
    let emitted: Vec<i64> = invoke(&handler, &WindowedHeader::default(), &[])
        .into_iter()
        .map(|(_, bytes)| <i64 as DefaultCoder>::decode(&bytes).expect("decode"))
        .collect();
    assert_eq!(emitted, [1, 2, 3]);
}

#[test]
fn create_with_an_undecodable_payload_does_not_resolve() {
    let payload = proto_pipeline::ParDoPayload {
        do_fn: spec(beam::pipeline::URN_RUST_DOFN_CREATE, vec![0xff, 0xff]),
        ..Default::default()
    }
    .encode_to_vec();
    let t = transform("Create/Process", spec(beam::pipeline::URN_PAR_DO, payload));
    assert!(lookup_handler(&HashMap::new(), &t).is_none());
}

#[derive(Clone, serde::Serialize, serde::Deserialize, beam::transforms::Register)]
struct ScaleBy {
    factor: i64,
}

impl beam::transforms::DoFn for ScaleBy {
    type In = i64;
    type Out = i64;

    fn process_element(
        &mut self,
        element: i64,
        ctx: &mut beam::transforms::ProcessContext<'_, i64>,
    ) -> beam::Result {
        ctx.emit(element * self.factor)
    }
}

#[test]
fn registered_dofn_resolves_from_its_payload_without_registered_handlers() {
    use beam::coders::DefaultCoder;
    use beam::transforms::{Create, ParDo};

    let p = beam::pipeline::Pipeline::new();
    let _ = p
        .apply(Create::new("Create", vec![1i64, 2, 3]))
        .apply(ParDo::new("Scale", ScaleBy { factor: 10 }));
    let scale_transform = p
        .to_proto()
        .components
        .expect("components")
        .transforms
        .into_values()
        .find(|t| t.unique_name == "Scale")
        .expect("Scale transform");
    assert!(
        p.transform_handlers().is_empty(),
        "registered DoFn must not register a handler"
    );

    let handler = lookup_handler(&HashMap::new(), &scale_transform).expect("Scale resolves");
    let outputs: Vec<i64> = [1i64, 2, 3]
        .into_iter()
        .map(|x| DefaultCoder::encode(&x).expect("encode"))
        .flat_map(|input| invoke(&handler, &WindowedHeader::default(), &input))
        .map(|(_, bytes)| <i64 as DefaultCoder>::decode(&bytes).expect("decode"))
        .collect();
    assert_eq!(outputs, [10, 20, 30]);
}

#[test]
fn registered_dofn_with_unknown_id_does_not_resolve() {
    #[derive(prost::Message)]
    struct RegisteredSpec {
        #[prost(string, tag = "1")]
        id: String,
        #[prost(bytes = "vec", tag = "2")]
        fields: Vec<u8>,
    }

    let payload = proto_pipeline::ParDoPayload {
        do_fn: spec(
            beam::pipeline::URN_RUST_DOFN_REGISTERED,
            RegisteredSpec {
                id: "unknown::DoFn".to_string(),
                fields: Vec::new(),
            }
            .encode_to_vec(),
        ),
        ..Default::default()
    }
    .encode_to_vec();
    let t = transform("Unknown", spec(beam::pipeline::URN_PAR_DO, payload));
    assert!(lookup_handler(&HashMap::new(), &t).is_none());
}

/// Sums the values of a key, each times `factor`.
#[derive(Clone, serde::Serialize, serde::Deserialize, beam::transforms::Register)]
struct WeightedSum {
    factor: i64,
}

impl beam::transforms::CombineFn for WeightedSum {
    type Input = i64;
    type Accum = i64;
    type Output = i64;

    fn create_accumulator(&self) -> beam::Result<i64> {
        Ok(0)
    }

    fn add_input(&self, acc: i64, input: i64) -> beam::Result<i64> {
        Ok(acc + input * self.factor)
    }

    fn merge_accumulators(&self, accs: Vec<i64>) -> beam::Result<i64> {
        Ok(accs.into_iter().sum())
    }

    fn extract_output(&self, acc: i64) -> beam::Result<i64> {
        Ok(acc)
    }
}

#[test]
fn registered_combine_resolves_for_every_combine_stage() {
    use beam::coders::DefaultCoder;
    use beam::pipeline::{
        URN_COMBINE_PER_KEY_CONVERT_TO_ACCUMULATORS, URN_COMBINE_PER_KEY_EXTRACT_OUTPUTS,
        URN_COMBINE_PER_KEY_MERGE_ACCUMULATORS, URN_COMBINE_PER_KEY_PRECOMBINE,
    };
    use beam::transforms::{CombinePerKey, Create};

    let p = beam::pipeline::Pipeline::new();
    let _ = p
        .apply(Create::new("Create", vec![("k".to_string(), 1i64)]))
        .apply(CombinePerKey::new("Sum", WeightedSum { factor: 10 }));
    assert!(
        p.transform_handlers().is_empty(),
        "a registered CombineFn must not register handlers"
    );
    let transforms = p.to_proto().components.expect("components").transforms;
    let named = |name: &str| {
        transforms
            .values()
            .find(|t| t.unique_name == name)
            .cloned()
            .expect("transform exists")
    };
    let combine_payload = named("Sum").spec.expect("spec").payload;

    for urn in [
        URN_COMBINE_PER_KEY_PRECOMBINE,
        URN_COMBINE_PER_KEY_MERGE_ACCUMULATORS,
        URN_COMBINE_PER_KEY_CONVERT_TO_ACCUMULATORS,
    ] {
        let lifted = transform("Lifted", spec(urn, combine_payload.clone()));
        assert!(lookup_handler(&HashMap::new(), &lifted).is_some(), "{urn}");
    }
    assert!(
        lookup_handler(&HashMap::new(), &named("Sum/CombineGroupedValues")).is_some(),
        "the combine_grouped_values step resolves from its payload"
    );
    assert!(
        lookup_handler(&HashMap::new(), &named("Sum")).is_none(),
        "the composite itself has no handler"
    );

    let extract = transform(
        "Extract",
        spec(URN_COMBINE_PER_KEY_EXTRACT_OUTPUTS, combine_payload),
    );
    let handler = lookup_handler(&HashMap::new(), &extract).expect("extract resolves");
    let input = ("k".to_string(), 42i64).encode().expect("encode");
    let outputs: Vec<(String, i64)> = invoke(&handler, &WindowedHeader::default(), &input)
        .into_iter()
        .map(|(_, bytes)| <(String, i64)>::decode(&bytes).expect("decode"))
        .collect();
    assert_eq!(outputs, [("k".to_string(), 42)]);
}

/// The `ParDo` of the reshuffle step `step` in a pipeline that reshuffles one collection.
fn reshuffle_step(step: &str) -> proto_pipeline::PTransform {
    use beam::transforms::{Create, Reshuffle};

    let p = beam::pipeline::Pipeline::new();
    let _ = p
        .apply(Create::new("Create", vec![1i64]))
        .apply(Reshuffle::new("Shuffle"));
    assert!(
        p.transform_handlers().is_empty(),
        "Reshuffle must not register a handler"
    );
    p.to_proto()
        .components
        .expect("components")
        .transforms
        .into_values()
        .find(|t| t.unique_name == format!("Shuffle/{step}"))
        .expect("Reshuffle step")
}

#[test]
fn reshuffle_restores_the_window_timestamp_and_pane_of_each_element() {
    use beam::Instant;
    use beam::coders::{Coder, Context, DefaultCoder, IntervalWindow, IntervalWindowCoder};

    type Shuffled = ((i64, Vec<u8>), Vec<u8>);
    type Emitted = (Option<Vec<u8>>, Vec<u8>);

    let window = IntervalWindow::new(Instant::from_millis(0), Instant::from_millis(10_000));
    let mut window_bytes = Vec::new();
    IntervalWindowCoder
        .encode(&window, &mut window_bytes, Context::Nested)
        .expect("window encodes");
    let header = WindowedHeader::new(4_000, &[window_bytes], PaneInfo::ON_TIME_AND_ONLY_FIRING);
    let element = DefaultCoder::encode(&42i64).expect("element encodes");

    let pair = lookup_handler(&HashMap::new(), &reshuffle_step("PairWithRandomKey"))
        .expect("PairWithRandomKey resolves from its payload");
    let outputs: [Emitted; 1] = invoke(&pair, &header, &element)
        .try_into()
        .expect("one output");
    let [(Some(paired_header), paired)] = outputs else {
        panic!("PairWithRandomKey must emit with a header");
    };
    // The shuffle runs in the global window and keeps the element timestamp.
    let paired_header = WindowedHeader::from_wire(&paired_header, WindowedHeader::WINDOWS_START);
    assert_eq!(paired_header.timestamp_millis(), 4_000);
    assert_eq!(paired_header.window_count(), 1);
    assert!(paired_header.window_bytes().is_empty());
    let (shard, shuffled) = <(i64, Shuffled)>::decode(&paired).expect("paired decodes");
    assert_eq!(
        shuffled.1, element,
        "the element bytes go through unchanged"
    );

    let expand = lookup_handler(&HashMap::new(), &reshuffle_step("Expand"))
        .expect("Expand resolves from its payload");
    let grouped = DefaultCoder::encode(&(shard, vec![shuffled])).expect("group encodes");
    let global = WindowedHeader::global(4_000, PaneInfo::ON_TIME_AND_ONLY_FIRING);
    let outputs: [Emitted; 1] = invoke(&expand, &global, &grouped)
        .try_into()
        .expect("one output");
    let [(Some(restored), expanded)] = outputs else {
        panic!("Expand must emit with the restored header");
    };
    assert_eq!(restored, header.as_bytes());
    assert_eq!(expanded, element);
}
