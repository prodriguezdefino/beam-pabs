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

//! Unit and integration tests for Reshuffle transform.

use std::collections::{BTreeMap, BTreeSet};
use std::io::{Read, Write};
use std::time::Duration;

use beam::Instant;
use beam::coders::{
    BeamIterable, Coder, CoderError, Context, DefaultCoder, IntervalWindow, PaneInfo, Timing,
    WindowedHeader,
};
use beam::internals::{
    BundleHandler, ElementSink, HandlerContext, registered_handler, reshuffle_bucket,
};
use beam::pipeline::{
    Pipeline, URN_GROUP_BY_KEY, URN_PAR_DO, URN_RESHUFFLE, URN_WINDOW_FN_GLOBAL_WINDOWS,
};
use beam::transforms::{Create, Reshuffle};
use beam::windowing::{
    AccumulationMode, BoundedWindow, FixedWindows, Sessions, Trigger, WindowInto,
};
use beam_model::pipeline as proto;
use beam_testing::{TestPipeline, passert};

#[test]
fn test_reshuffle_graph_expansion() {
    let p = Pipeline::new();
    let words = p.apply(
        "Create",
        Create::new(vec![
            "apple".to_string(),
            "banana".to_string(),
            "cherry".to_string(),
        ]),
    );

    let reshuffled = words.apply("TestReshuffle", Reshuffle::new());
    assert_eq!(reshuffled.id(), "TestReshuffle/Expand_out");

    let proto = p.to_proto();
    let components = proto.components.expect("components present");

    let (_, composite) = components
        .transforms
        .iter()
        .find(|(_, t)| t.unique_name == "TestReshuffle")
        .expect("TestReshuffle composite transform must exist");

    assert_eq!(
        composite.spec.as_ref().map(|s| s.urn.as_str()),
        Some(URN_RESHUFFLE)
    );
    assert_eq!(composite.subtransforms.len(), 3);

    let t1 = &components.transforms[&composite.subtransforms[0]];
    assert!(t1.unique_name.ends_with("/PairWithRandomKey"));
    assert_eq!(t1.spec.as_ref().unwrap().urn, URN_PAR_DO);

    let t2 = &components.transforms[&composite.subtransforms[1]];
    assert!(t2.unique_name.ends_with("/GroupByKey"));
    assert_eq!(t2.spec.as_ref().unwrap().urn, URN_GROUP_BY_KEY);

    let t3 = &components.transforms[&composite.subtransforms[2]];
    assert!(t3.unique_name.ends_with("/Expand"));
    assert_eq!(t3.spec.as_ref().unwrap().urn, URN_PAR_DO);
}

#[derive(Debug, PartialEq, Eq)]
struct NonClone(i64);

struct NonCloneCoder;

impl Coder<NonClone> for NonCloneCoder {
    fn urn(&self) -> &'static str {
        "beam:coder:test:non_clone:v1"
    }

    fn encode(
        &self,
        value: &NonClone,
        writer: &mut Vec<u8>,
        _context: Context,
    ) -> Result<(), CoderError> {
        Ok(writer.write_all(&value.0.to_be_bytes())?)
    }

    fn decode(&self, reader: &mut &[u8], _context: Context) -> Result<NonClone, CoderError> {
        let mut bytes = [0; 8];
        reader.read_exact(&mut bytes)?;
        Ok(NonClone(i64::from_be_bytes(bytes)))
    }
}

impl DefaultCoder for NonClone {
    type Coder = NonCloneCoder;

    fn coder() -> Self::Coder {
        NonCloneCoder
    }

    fn encode_element(&self, writer: &mut Vec<u8>) -> Result<(), CoderError> {
        NonCloneCoder.encode(self, writer, Context::Nested)
    }

    fn decode_element(reader: &mut &[u8]) -> Result<Self, CoderError> {
        NonCloneCoder.decode(reader, Context::Nested)
    }

    fn register_coder<R: beam::coders::CoderRegistry + ?Sized>(registry: &R) -> String {
        registry.register_coder(NonCloneCoder.urn(), Vec::new())
    }
}

#[test]
fn test_reshuffle_non_clone_type() {
    let p = Pipeline::new();
    let items = p.apply("Create", Create::new(vec![NonClone(1), NonClone(2)]));
    let reshuffled = items.apply("TestReshuffleNonClone", Reshuffle::new());
    assert_eq!(reshuffled.id(), "TestReshuffleNonClone/Expand_out");
}

#[test]
fn test_reshuffle_shuffles_in_global_window_and_restores_on_output() {
    let p = Pipeline::new();
    let items = p.apply("Create", Create::new(vec![10i64, 20, 30])).apply(
        "WindowInto",
        WindowInto::new(Sessions::with_gap_duration(Duration::from_secs(10)))
            .with_trigger(Trigger::after_end_of_window().with_late_firings(Trigger::after_count(2)))
            .with_accumulation_mode(AccumulationMode::Accumulating)
            .with_allowed_lateness(Duration::from_secs(42)),
    );

    let input_ws_id = items.windowing_strategy_id();
    let reshuffled = items.apply("ReshuffleCustomTrigger", Reshuffle::new());
    assert_eq!(reshuffled.windowing_strategy_id(), input_ws_id);

    let proto = p.to_proto();
    let components = proto.components.expect("components present");

    let (_, composite) = components
        .transforms
        .iter()
        .find(|(_, t)| t.unique_name == "ReshuffleCustomTrigger")
        .expect("composite transform must exist");

    let pair_transform = &components.transforms[&composite.subtransforms[0]];
    let paired_pcoll_id = pair_transform.outputs.get("out").expect("pair output");
    let paired_pcoll = &components.pcollections[paired_pcoll_id];
    let paired_ws = &components.windowing_strategies[&paired_pcoll.windowing_strategy_id];

    // The shuffle runs in the global window: no input window merges and no element is late.
    let window_fn = paired_ws.window_fn.as_ref().expect("window fn present");
    assert_eq!(window_fn.urn, URN_WINDOW_FN_GLOBAL_WINDOWS);
    assert_eq!(
        paired_ws.merge_status,
        proto::merge_status::Enum::NonMerging as i32
    );
    assert_eq!(
        paired_ws.output_time,
        proto::output_time::Enum::EarliestInPane as i32
    );
    assert_eq!(
        paired_ws.accumulation_mode,
        proto::accumulation_mode::Enum::Discarding as i32
    );
    let decoded_trigger = Trigger::try_from(paired_ws.trigger.as_ref().expect("trigger present"))
        .expect("valid trigger proto");
    assert_eq!(decoded_trigger, Trigger::always());
}

#[tokio::test]
async fn test_reshuffle_with_non_default_trigger_execution() {
    let p = TestPipeline::new();
    let words = p
        .apply(
            "Create",
            Create::new(vec![
                "apple".to_string(),
                "banana".to_string(),
                "cherry".to_string(),
            ]),
        )
        .apply(
            "WindowInto",
            WindowInto::new(FixedWindows::of(Duration::from_secs(10)))
                .with_trigger(
                    Trigger::after_end_of_window().with_late_firings(Trigger::after_count(2)),
                )
                .with_accumulation_mode(AccumulationMode::Discarding)
                .with_allowed_lateness(Duration::from_secs(60)),
        );

    let reshuffled = words.apply("ReshuffleExecution", Reshuffle::new());

    passert::that("AssertReshuffled", &reshuffled)
        .contains_in_any_order(["apple", "banana", "cherry"].map(String::from));

    p.run().await.expect("pipeline execution succeeds");
}

#[test]
fn test_create_reshuffles_only_more_than_one_element() {
    let p = Pipeline::new();
    let many = p.apply("Many", Create::new(vec![1i64, 2, 3]));
    let one = p.apply("One", Create::new(vec![1i64]));
    assert_eq!(many.id(), "Many/Reshuffle/Expand_out");
    assert_eq!(one.id(), "One.out");

    let proto = p.to_proto();
    let components = proto.components.expect("components present");
    let subtransforms = |name: &str| {
        let (_, create) = components
            .transforms
            .iter()
            .find(|(_, t)| t.unique_name == name)
            .expect("Create composite must exist");
        create
            .subtransforms
            .iter()
            .map(|id| components.transforms[id].unique_name.as_str())
            .collect::<Vec<_>>()
    };
    assert_eq!(
        subtransforms("Many"),
        ["Many/Impulse", "Many/Process", "Many/Reshuffle"]
    );
    assert_eq!(subtransforms("One"), ["One/Impulse", "One/Process"]);
}

#[test]
fn test_reshuffle_bucket_matches_java_assign_shard_fn() {
    // (shard, n = 7, n = 20000) from Java's `AssignShardFn` hash with `Integer.remainderUnsigned`.
    let java = [
        (0, 0, 0),
        (1, 6, 16_125),
        (2, 1, 15_482),
        (-1, 4, 2_032),
        (12_345, 1, 16_993),
        (i32::MAX, 4, 6_768),
        (i32::MIN, 4, 12_032),
    ];
    for (shard, mod7, mod20000) in java {
        assert_eq!(reshuffle_bucket(shard, 7), mod7, "shard {shard}");
        assert_eq!(reshuffle_bucket(shard, 20_000), mod20000, "shard {shard}");
        assert_eq!(reshuffle_bucket(shard, 1), 0, "shard {shard}");
    }
    // Zero buckets count as one bucket.
    assert_eq!(reshuffle_bucket(12_345, 0), 0);
}

#[test]
fn test_reshuffle_bucket_keys_stay_below_the_bucket_count() {
    let keys: BTreeSet<i64> = (-5_000..5_000)
        .map(|shard| reshuffle_bucket(shard, 13))
        .collect();
    assert_eq!(keys, (0..13).collect::<BTreeSet<i64>>());
}

#[tokio::test]
async fn test_bucketed_reshuffle_conserves_elements() {
    let p = TestPipeline::new();
    let numbers = p.apply("Create", Create::new((0..1_000_i64).collect::<Vec<_>>()));
    let reshuffled = numbers.apply("BucketedReshuffle", Reshuffle::new().with_num_buckets(7));
    passert::that("AssertBucketedReshuffle", &reshuffled).contains_in_any_order(0..1_000_i64);
    p.run().await.expect("pipeline execution succeeds");
}

/// The element bytes and output header of each element that a handler emits.
#[derive(Default)]
struct HeaderSink(Vec<(WindowedHeader, Vec<u8>)>);

impl ElementSink for HeaderSink {
    fn push(&mut self, _: Vec<u8>) -> Result<(), String> {
        Err("every Reshuffle output must carry a header".to_string())
    }

    fn push_windowed(&mut self, header: &WindowedHeader, element: Vec<u8>) -> Result<(), String> {
        self.0.push((header.clone(), element));
        Ok(())
    }
}

fn run_with_headers(
    handler: &dyn BundleHandler,
    inputs: &[(WindowedHeader, Vec<u8>)],
) -> Vec<(WindowedHeader, Vec<u8>)> {
    let mut stage = handler.instantiate();
    let mut sink = HeaderSink::default();
    stage.setup().expect("setup");
    stage.start_bundle().expect("start_bundle");
    for (header, element) in inputs {
        let mut ctx = HandlerContext::new(&mut sink).with_header(header);
        stage.process(element, &mut ctx).expect("process");
    }
    stage
        .finish_bundle(&mut HandlerContext::new(&mut sink))
        .expect("finish_bundle");
    stage.teardown().expect("teardown");
    sink.0
}

fn registered_do_fn(p: &Pipeline, name: &str) -> beam::internals::TransformFn {
    let components = p.to_proto().components.expect("components");
    let spec = components
        .transforms
        .values()
        .find(|t| t.unique_name == name)
        .and_then(|t| t.spec.clone())
        .unwrap_or_else(|| panic!("transform '{name}' must exist"));
    let payload = <proto::ParDoPayload as prost::Message>::decode(spec.payload.as_slice())
        .expect("ParDoPayload");
    registered_handler(&payload.do_fn.expect("do_fn").payload).expect("registered handler")
}

/// Prism runs `Reshuffle` as a pass-through, so this test runs the SDK expansion itself:
/// pair with a key, group by key, expand. Each element must come out once, with the header
/// it had before the shuffle.
#[test]
fn test_reshuffle_expansion_keeps_each_element_and_its_header() {
    let p = Pipeline::new();
    let _ = p
        .apply("Create", Create::new(vec![1i64, 2, 3]))
        .apply("Shuffle", Reshuffle::new().with_num_buckets(2));
    let pair = registered_do_fn(&p, "Shuffle/PairWithRandomKey");
    let expand = registered_do_fn(&p, "Shuffle/Expand");

    let window = |start: i64| {
        BoundedWindow::Interval(IntervalWindow::new(
            Instant::from_millis(start),
            Instant::from_millis(start + 10_000),
        ))
    };
    let early = PaneInfo::new(true, false, Timing::Early, Some(0), None);
    let late = PaneInfo::new(false, true, Timing::Late, Some(3), Some(1));
    let inputs = vec![
        (
            WindowedHeader::global(1_000, early).with_windows(&[window(0)]),
            1i64.encode().unwrap(),
        ),
        (
            WindowedHeader::global(12_000, PaneInfo::NO_FIRING)
                .with_windows(&[window(10_000), window(5_000)]),
            2i64.encode().unwrap(),
        ),
        (
            WindowedHeader::global(15_000, late).with_windows(&[window(10_000)]),
            3i64.encode().unwrap(),
        ),
    ];

    type Shuffled = ((i64, Vec<u8>), Vec<u8>);
    let global = WindowedHeader::global(0, PaneInfo::NO_FIRING);
    let mut groups: BTreeMap<i64, Vec<Shuffled>> = BTreeMap::new();
    let paired = run_with_headers(pair.as_ref(), &inputs);
    assert_eq!(paired.len(), inputs.len());
    for ((header, bytes), (input_header, _)) in paired.iter().zip(&inputs) {
        // The shuffle runs in the global window, at the element's own timestamp.
        assert_eq!(header.window_bytes(), global.window_bytes());
        assert_eq!(header.timestamp(), input_header.timestamp());
        let (key, shuffled) = <(i64, Shuffled)>::decode(bytes).unwrap();
        assert!((0..2).contains(&key), "bucket key {key}");
        groups.entry(key).or_default().push(shuffled);
    }

    let grouped: Vec<(WindowedHeader, Vec<u8>)> = groups
        .into_iter()
        .map(|(key, values)| {
            let element = (key, BeamIterable::from(values)).encode().unwrap();
            (global.clone(), element)
        })
        .collect();
    let mut expanded: Vec<(Vec<u8>, Vec<u8>)> = run_with_headers(expand.as_ref(), &grouped)
        .into_iter()
        .map(|(header, element)| (element, header.as_bytes().to_vec()))
        .collect();
    expanded.sort();
    let mut expected: Vec<(Vec<u8>, Vec<u8>)> = inputs
        .iter()
        .map(|(header, element)| (element.clone(), header.as_bytes().to_vec()))
        .collect();
    expected.sort();
    assert_eq!(expanded, expected);
}
