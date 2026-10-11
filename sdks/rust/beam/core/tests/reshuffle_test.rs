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

use std::io::{Read, Write};
use std::time::Duration;

use beam::coders::{Coder, CoderError, Context, DefaultCoder};
use beam::pipeline::{
    Pipeline, URN_GROUP_BY_KEY, URN_PAR_DO, URN_RESHUFFLE, URN_WINDOW_FN_GLOBAL_WINDOWS,
};
use beam::transforms::{Create, Reshuffle};
use beam::windowing::{AccumulationMode, FixedWindows, Sessions, Trigger, WindowInto};
use model::pipeline as proto;
use testing::{TestPipeline, passert};

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
