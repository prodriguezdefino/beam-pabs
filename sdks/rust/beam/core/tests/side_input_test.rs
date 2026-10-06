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
    SideInputWindowing, URN_SIDE_INPUT_ITERABLE, URN_SIDE_INPUT_MULTIMAP,
    URN_WINDOW_MAPPING_GLOBAL, URN_WINDOW_MAPPING_WINDOW_FN,
};
use beam::windowing::{BoundedWindow, WindowFn};

struct MockSideInputReader {
    iterables: HashMap<String, Vec<Vec<u8>>>,
    multimaps: HashMap<(String, Vec<u8>), Vec<Vec<u8>>>,
}

impl SideInputReader for MockSideInputReader {
    fn get_iterable(&self, tag: &str, _window: &[u8]) -> Result<Vec<Vec<u8>>, String> {
        Ok(self.iterables.get(tag).cloned().unwrap_or_default())
    }

    fn get_multimap(&self, tag: &str, _window: &[u8], key: &[u8]) -> Result<Vec<Vec<u8>>, String> {
        Ok(self
            .multimaps
            .get(&(tag.to_string(), key.to_vec()))
            .cloned()
            .unwrap_or_default())
    }
}

#[test]
fn test_pcollection_view_proto_generation() {
    let p = Pipeline::new();
    let single = p.apply(Create::new("Single", vec![42_i32])).as_singleton();
    let iter = p
        .apply(Create::new("Iter", vec!["a".to_string(), "b".to_string()]))
        .as_iter();
    let mmap = p
        .apply(Create::new(
            "Mmap",
            vec![("k1".to_string(), 100_i32), ("k2".to_string(), 200_i32)],
        ))
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

#[test]
fn test_par_do_with_side_inputs_graph_and_proto() {
    let p = Pipeline::new();
    let main_col = p.apply(Create::new("Main", vec![10_i32, 20_i32]));
    let factor_view = p.apply(Create::new("Factor", vec![5_i32])).as_singleton();

    let fv = factor_view.clone();
    let scaled = main_col.apply(
        ParDo::from_fn("ScaleByFactor", move |x: i32, ctx| {
            let factor = ctx.side_input(&fv)?;
            ctx.emit(x * factor)
        })
        .with_side_input(&factor_view),
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
fn test_unified_dofn_and_process_context_execution() {
    let p = Pipeline::new();
    let single_view = p.apply(Create::new("Factor", vec![3_i32])).as_singleton();
    let iter_view = p
        .apply(Create::new("Stopwords", vec!["bad".to_string()]))
        .as_iter();
    let mmap_view = p
        .apply(Create::new("Scores", vec![("rust".to_string(), 10_i32)]))
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
    let mut ctx = ProcessContext::<i32>::with_context(&mut sink, Some(reader.as_ref()), &header);

    assert_eq!(ctx.timestamp(), 12345);
    assert_eq!(ctx.side_input(&single_view).unwrap(), 3);
    assert_eq!(ctx.side_input_iter(&iter_view).unwrap(), vec!["bad"]);
    assert_eq!(
        ctx.side_input_map(&mmap_view, &"rust".to_string()).unwrap(),
        vec![10, 20]
    );

    ctx.emit(99).unwrap();
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
    let main = encoded(IntervalWindow::new(5_000, 6_000));
    let global = SideInputWindowing::of(&window_fn_spec(&GlobalWindows)).unwrap();
    let fixed = window_fn_spec(&FixedWindows::of(Duration::from_secs(10)));
    let fixed = SideInputWindowing::of(&fixed).unwrap();
    let sliding = SlidingWindows::of(Duration::from_secs(10)).every(Duration::from_secs(5));
    let sliding = SideInputWindowing::of(&window_fn_spec(&sliding)).unwrap();

    let cases = [
        (&global, Vec::new()),
        (&SideInputWindowing::Identity, main.clone()),
        (&fixed, encoded(IntervalWindow::new(0, 10_000))),
        // The end 5_999 is in [0, 10s) and [5s, 15s); the earliest wins.
        (&sliding, encoded(IntervalWindow::new(0, 10_000))),
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
    let main_col = p.apply(Create::new("Main", vec![1_i32]));
    let view = p
        .apply(Create::new("Side", vec![5_i32]))
        .apply(WindowInto::new(
            "Sessions",
            Sessions::with_gap_duration(Duration::from_secs(5)),
        ))
        .as_singleton();
    assert!(view.to_proto().window_mapping_fn.is_none());

    let v = view.clone();
    main_col.apply(
        ParDo::from_fn("ReadSide", move |x: i32, ctx| {
            let side = ctx.side_input(&v)?;
            ctx.emit(x + side)
        })
        .with_side_input(&view),
    );
    let err = p
        .validate()
        .expect_err("Sessions side input must be rejected");
    assert!(
        matches!(&err, PipelineError::UnsupportedSideInput { reason, .. } if reason.contains("Sessions")),
        "{err}"
    );
}
