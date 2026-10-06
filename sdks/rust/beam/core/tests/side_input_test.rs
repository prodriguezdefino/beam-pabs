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

//! Tests for `DoFn`, `ProcessContext`, and `PCollectionView` side inputs.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use beam::coders::{DefaultCoder, PaneInfo, WindowedHeader};
use beam::internals::extract_side_input_tags;
use beam::internals::*;
use beam::pipeline::{PipelineError, URN_PAR_DO};
use beam::prelude::*;
use beam::values::{
    SideInputKind, SideInputWindowing, URN_SIDE_INPUT_ITERABLE, URN_SIDE_INPUT_MULTIMAP,
    URN_WINDOW_MAPPING_GLOBAL, URN_WINDOW_MAPPING_WINDOW_FN,
};
use beam::windowing::{BoundedWindow, WindowFn};

struct MockSideInputReader {
    iterables: HashMap<String, Vec<Vec<u8>>>,
    multimaps: HashMap<(String, Vec<u8>), Vec<Vec<u8>>>,
}

impl SideInputReader for MockSideInputReader {
    fn get_iterable<'a>(
        &'a self,
        tag: &str,
        _window: &[u8],
    ) -> Result<SideInputElements<'a>, SideInputError> {
        let data = self.iterables.get(tag).cloned().unwrap_or_default();
        Ok(Box::new(
            data.into_iter().map(|v| Ok(bytes::Bytes::from(v))),
        ))
    }

    fn get_multimap<'a>(
        &'a self,
        tag: &str,
        _window: &[u8],
        key: &[u8],
    ) -> Result<SideInputElements<'a>, SideInputError> {
        let data = self
            .multimaps
            .get(&(tag.to_string(), key.to_vec()))
            .cloned()
            .unwrap_or_default();
        Ok(Box::new(
            data.into_iter().map(|v| Ok(bytes::Bytes::from(v))),
        ))
    }

    fn window_mapping(&self, tag: &str) -> Result<&SideInputWindowing, SideInputError> {
        Err(SideInputError::MissingReader {
            tag: tag.to_string(),
        })
    }
}

#[test]
fn test_pcollection_view_proto_generation() {
    let p = Pipeline::new();
    let single = p.apply("Single", Create::new(vec![42_i32])).as_singleton();
    let iter = p
        .apply("Iter", Create::new(vec!["a".to_string(), "b".to_string()]))
        .as_iter();
    let mmap = p
        .apply(
            "Mmap",
            Create::new(vec![
                ("k1".to_string(), 100_i32),
                ("k2".to_string(), 200_i32),
            ]),
        )
        .as_multimap();

    assert_eq!(single.kind(), SideInputKind::Singleton);
    assert_eq!(single.access_pattern(), URN_SIDE_INPUT_ITERABLE);
    assert_eq!(
        single.to_proto().window_mapping_fn.unwrap().urn,
        URN_WINDOW_MAPPING_GLOBAL
    );

    assert_eq!(iter.kind(), SideInputKind::Iter);
    assert_eq!(iter.access_pattern(), URN_SIDE_INPUT_ITERABLE);

    assert_eq!(mmap.kind(), SideInputKind::Multimap);
    assert_eq!(mmap.access_pattern(), URN_SIDE_INPUT_MULTIMAP);
}

#[derive(Clone, serde::Serialize, serde::Deserialize, Register)]
struct ScaleByFactorFn {
    factor_view: SingletonView<i32>,
}

impl DoFn for ScaleByFactorFn {
    type In = i32;
    type Out = i32;

    fn side_inputs(&self) -> Vec<&dyn AnySideInput> {
        vec![&self.factor_view]
    }

    fn process_element(&mut self, x: i32, ctx: &mut ProcessContext<'_, i32>) -> Result {
        let factor = ctx.side_input(&self.factor_view)?;
        ctx.emit(x * factor)
    }
}

#[test]
fn test_par_do_with_side_inputs_graph_and_proto() {
    let p = Pipeline::new();
    let main_col = p.apply("Main", Create::new(vec![10_i32, 20_i32]));
    let factor_view = p.apply("Factor", Create::new(vec![5_i32])).as_singleton();

    let scaled = main_col.apply(
        "ScaleByFactor",
        ParDo::new(ScaleByFactorFn {
            factor_view: factor_view.clone(),
        }),
    );
    assert!(!scaled.id().is_empty());

    let proto = p.to_proto();
    let components = proto.components.as_ref().unwrap();
    let transform = components
        .transforms
        .values()
        .find(|t| t.unique_name == "ScaleByFactor")
        .expect("ScaleByFactor transform must exist");

    assert_eq!(transform.spec.as_ref().unwrap().urn, URN_PAR_DO);
    assert_eq!(transform.inputs.len(), 2);
    assert!(transform.inputs.contains_key("in"));
    assert_eq!(
        transform.inputs.get(factor_view.tag()).map(String::as_str),
        Some(factor_view.pcollection_id())
    );

    let side_tags = extract_side_input_tags(transform);
    assert!(side_tags.contains(factor_view.tag()));
}

#[test]
fn views_round_trip_through_serde() {
    let p = Pipeline::new();
    let global_view = p.apply("Global", Create::new(vec![42_i32])).as_singleton();
    let windowed_view = p
        .apply("Windowed", Create::new(vec![("k".to_string(), 10_i32)]))
        .apply(
            "Fixed",
            WindowInto::new(FixedWindows::of(Duration::from_secs(10))),
        )
        .as_multimap();

    let json = serde_json::to_string(&global_view).unwrap();
    let decoded: SingletonView<i32> = serde_json::from_str(&json).unwrap();
    assert_eq!(decoded.to_proto(), global_view.to_proto());

    let json = serde_json::to_string(&windowed_view).unwrap();
    let decoded: MultimapView<String, i32> = serde_json::from_str(&json).unwrap();
    assert_eq!(decoded.to_proto(), windowed_view.to_proto());
}

#[test]
fn a_view_does_not_deserialize_into_another_kind_or_element_type() {
    let p = Pipeline::new();
    let json =
        serde_json::to_string(&p.apply("Ints", Create::new(vec![1_i32])).as_singleton()).unwrap();

    let wrong_kind = serde_json::from_str::<IterView<i32>>(&json).unwrap_err();
    assert!(
        wrong_kind
            .to_string()
            .contains("Singleton view, read as a Iter view"),
        "{wrong_kind}"
    );

    let wrong_type = serde_json::from_str::<SingletonView<String>>(&json).unwrap_err();
    assert!(
        wrong_type.to_string().contains("holds `i32`"),
        "{wrong_type}"
    );
}

#[test]
fn test_unified_dofn_and_process_context_execution() {
    let p = Pipeline::new();
    let single_view = p.apply("Factor", Create::new(vec![3_i32])).as_singleton();
    let iter_view = p
        .apply("Stopwords", Create::new(vec!["bad".to_string()]))
        .as_iter();
    let mmap_view = p
        .apply("Scores", Create::new(vec![("rust".to_string(), 10_i32)]))
        .as_multimap();

    let mut iterables = HashMap::new();
    iterables.insert(single_view.tag().to_string(), vec![3_i32.encode().unwrap()]);
    iterables.insert(
        iter_view.tag().to_string(),
        vec!["bad".to_string().encode().unwrap()],
    );

    let mut multimaps = HashMap::new();
    multimaps.insert(
        (
            mmap_view.tag().to_string(),
            "rust".to_string().encode().unwrap(),
        ),
        vec![10_i32.encode().unwrap(), 20_i32.encode().unwrap()],
    );

    let reader = Arc::new(MockSideInputReader {
        iterables,
        multimaps,
    });

    let mut sink: Vec<Vec<u8>> = Vec::new();
    let header = WindowedHeader::global(12345, PaneInfo::NO_FIRING);
    let mut hctx = HandlerContext::new(&mut sink)
        .with_side_inputs(Some(reader.as_ref()))
        .with_header(&header);
    let mut ctx = hctx.as_process_context::<i32>();

    assert_eq!(ctx.timestamp(), Instant::from_millis(12345));
    assert_eq!(ctx.side_input(&single_view).unwrap(), 3);
    assert_eq!(
        ctx.side_input(&iter_view).unwrap().to_vec().unwrap(),
        vec!["bad"]
    );
    let scores = ctx.side_input(&mmap_view).unwrap();
    assert_eq!(
        scores
            .get_all(&"rust".to_string())
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap(),
        vec![10, 20]
    );
    assert_eq!(scores.get(&"rust".to_string()).unwrap(), Some(10));
    assert_eq!(scores.get(&"go".to_string()).unwrap(), None);

    // A tuple of views reads as a tuple of values.
    let views = (single_view, iter_view);
    let (factor, stop) = ctx.side_input(&views).unwrap();
    assert_eq!(
        (factor, stop.to_vec().unwrap()),
        (3, vec!["bad".to_string()])
    );

    ctx.emit(99).unwrap();
    drop(hctx);
    assert_eq!(sink.len(), 1);
    assert_eq!(i32::decode(&sink[0]).unwrap(), 99);
}

fn window_fn_spec(window_fn: &dyn WindowFn) -> model::pipeline::FunctionSpec {
    model::pipeline::FunctionSpec {
        urn: window_fn.urn().to_string(),
        payload: window_fn.payload(),
    }
}

fn encoded(window: IntervalWindow) -> Vec<u8> {
    let mut bytes = Vec::new();
    BoundedWindow::Interval(window)
        .encode(&mut bytes)
        .expect("encoding into a Vec cannot fail");
    bytes
}

#[test]
fn side_input_windowing_rejects_what_the_other_sdks_reject() {
    let sessions = window_fn_spec(&Sessions::with_gap_duration(Duration::from_secs(5)));
    let custom = model::pipeline::FunctionSpec {
        urn: "beam:window_fn:custom:v1".to_string(),
        payload: Vec::new(),
    };
    for spec in [sessions, custom] {
        let err = SideInputWindowing::of(&spec).expect_err(&spec.urn);
        assert!(err.to_lowercase().contains("side inputs"), "{err}");
    }
    let unknown = model::pipeline::FunctionSpec {
        urn: "beam:window_mapping_fn:unknown:v1".to_string(),
        payload: Vec::new(),
    };
    assert!(SideInputWindowing::from_proto(&unknown).is_err());
}

#[test]
fn side_input_windowing_maps_to_the_earliest_window_holding_the_main_window_end() {
    let main = encoded(IntervalWindow::new(
        Instant::from_millis(5_000),
        Instant::from_millis(6_000),
    ));
    let global = SideInputWindowing::of(&window_fn_spec(&GlobalWindows)).unwrap();
    let fixed = window_fn_spec(&FixedWindows::of(Duration::from_secs(10)));
    let fixed = SideInputWindowing::of(&fixed).unwrap();
    let sliding = SlidingWindows::of(Duration::from_secs(10)).every(Duration::from_secs(5));
    let sliding = SideInputWindowing::of(&window_fn_spec(&sliding)).unwrap();

    let cases = [
        (&global, Vec::new()),
        (&SideInputWindowing::Identity, main.clone()),
        (
            &fixed,
            encoded(IntervalWindow::new(
                Instant::from_millis(0),
                Instant::from_millis(10_000),
            )),
        ),
        // The end 5_999 is in [0, 10s) and [5s, 15s); the earliest wins.
        (
            &sliding,
            encoded(IntervalWindow::new(
                Instant::from_millis(0),
                Instant::from_millis(10_000),
            )),
        ),
    ];
    for (windowing, expected) in cases {
        assert_eq!(
            windowing.map(&main).unwrap().as_ref(),
            expected,
            "{windowing:?}"
        );
        let decoded = SideInputWindowing::from_proto(&windowing.to_proto()).unwrap();
        assert_eq!(decoded.to_proto(), windowing.to_proto(), "{windowing:?}");
    }
    assert_eq!(fixed.to_proto().urn, URN_WINDOW_MAPPING_WINDOW_FN);

    // The global main window and undecodable bytes have no side input window.
    for main in [&b""[..], &b"bad"[..]] {
        assert!(fixed.map(main).is_err(), "{main:?}");
    }
}

#[test]
fn pipeline_validation_rejects_a_sessions_side_input() {
    let p = Pipeline::new();
    let main_col = p.apply("Main", Create::new(vec![1_i32]));
    let view = p
        .apply("Side", Create::new(vec![5_i32]))
        .apply(
            "Sessions",
            WindowInto::new(Sessions::with_gap_duration(Duration::from_secs(5))),
        )
        .as_singleton();
    assert!(view.to_proto().window_mapping_fn.is_none());

    main_col.apply(
        "ReadSide",
        ParDo::new(ScaleByFactorFn { factor_view: view }),
    );
    let err = p
        .validate()
        .expect_err("Sessions side input must be rejected");
    assert!(
        matches!(&err, PipelineError::UnsupportedSideInput { reason, .. } if reason.contains("Sessions")),
        "{err}"
    );
}

#[test]
fn test_view_fn_urns() {
    use beam::values::{
        SideInputKind, URN_VIEW_FN_ITERABLE, URN_VIEW_FN_MULTIMAP, URN_VIEW_FN_SINGLETON,
    };

    assert_eq!(
        SideInputKind::Singleton.view_fn_urn(),
        URN_VIEW_FN_SINGLETON
    );
    assert_eq!(SideInputKind::Iter.view_fn_urn(), URN_VIEW_FN_ITERABLE);
    assert_eq!(SideInputKind::Multimap.view_fn_urn(), URN_VIEW_FN_MULTIMAP);

    let p = Pipeline::new();
    let col = p.apply("Nums", Create::new(vec![1_i32]));
    let singleton = col.as_singleton();
    let iter = col.as_iter();
    assert_eq!(singleton.view_fn_urn(), URN_VIEW_FN_SINGLETON);
    assert_eq!(
        singleton.to_proto().view_fn.unwrap().urn,
        URN_VIEW_FN_SINGLETON
    );
    assert_eq!(iter.view_fn_urn(), URN_VIEW_FN_ITERABLE);
    assert_eq!(iter.to_proto().view_fn.unwrap().urn, URN_VIEW_FN_ITERABLE);
}
