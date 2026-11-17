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

//! Graph and pipeline tests of `RedistributeArbitrarily` and `RedistributeByKey`.

use std::time::Duration;

use beam::coders::IntervalWindow;
use beam::pipeline::{
    Pipeline, URN_GROUP_BY_KEY, URN_PAR_DO, URN_REDISTRIBUTE_ARBITRARILY, URN_REDISTRIBUTE_BY_KEY,
    URN_WINDOW_FN_GLOBAL_WINDOWS,
};
use beam::schema::{FieldType, FieldValue, Row, Schema};
use beam::transforms::display_data::{DisplayDataItem, DisplayDataValue};
use beam::transforms::{Create, RedistributeArbitrarily, RedistributeByKey};
use beam::values::PCollection;
use beam::windowing::{FixedWindows, Sessions, WindowInto};
use beam::{Instant, Result};
use beam_model::pipeline as proto;
use beam_testing::{TestPipeline, passert};
use prost::Message;

use crate::support::graph;

fn pairs(p: &Pipeline) -> PCollection<(String, i64)> {
    let kv = |k: &str, v| (k.to_string(), v);
    p.apply(
        "Pairs",
        Create::new(vec![kv("a", 1), kv("a", 2), kv("b", 3)]),
    )
}

/// The `(key, value)` display data of the transform `name`.
fn display_data(pipeline: &proto::Pipeline, name: &str) -> Vec<(String, DisplayDataValue)> {
    graph::transform(pipeline, name)
        .display_data
        .iter()
        .map(|item| DisplayDataItem::from_proto(item).expect("display data item"))
        .map(|item| (item.key, item.value))
        .collect()
}

/// Checks the spec, the display data and the steps of the composite `name`.
fn assert_composite(
    pipeline: &proto::Pipeline,
    name: &str,
    urn: &str,
    allow_duplicates: bool,
    steps: [&str; 3],
) {
    let composite = graph::transform(pipeline, name);
    let spec = composite.spec.as_ref().expect("composite spec");
    assert_eq!(spec.urn, urn);
    let payload = proto::RedistributePayload::decode(spec.payload.as_slice()).expect("payload");
    assert_eq!(payload.allow_duplicates, allow_duplicates);

    let transforms = &graph::components(pipeline).transforms;
    let subtransforms: Vec<(&str, &str)> = composite
        .subtransforms
        .iter()
        .map(|id| {
            (
                transforms[id].unique_name.as_str(),
                graph::urn(&transforms[id]),
            )
        })
        .collect();
    let [first, second, third] = steps.map(|step| format!("{name}/{step}"));
    assert_eq!(
        subtransforms,
        [
            (first.as_str(), URN_PAR_DO),
            (second.as_str(), URN_GROUP_BY_KEY),
            (third.as_str(), URN_PAR_DO),
        ]
    );

    let transform = name.rsplit('/').next().expect("a transform name");
    let mut expected = vec![(
        "transform".to_string(),
        DisplayDataValue::Text(transform.to_string()),
    )];
    if allow_duplicates {
        expected.push((
            "allow_duplicates".to_string(),
            DisplayDataValue::Boolean(true),
        ));
    }
    assert_eq!(display_data(pipeline, name), expected);
}

#[test]
fn redistribute_arbitrarily_is_a_composite_with_its_urn_and_payload() {
    let p = Pipeline::new();
    let numbers = p.apply("Numbers", Create::new(vec![1i64, 2, 3]));
    let plain = numbers.apply("RedistributeArbitrarily", RedistributeArbitrarily::new());
    let _ = numbers.apply(
        "Duplicates/RedistributeArbitrarily",
        RedistributeArbitrarily::new().with_allow_duplicates(true),
    );
    assert_eq!(plain.id(), "RedistributeArbitrarily/Expand_out");

    let pipeline = p.to_proto();
    let steps = ["PairWithRandomKey", "GroupByKey", "Expand"];
    for (name, allow_duplicates) in [
        ("RedistributeArbitrarily", false),
        ("Duplicates/RedistributeArbitrarily", true),
    ] {
        assert_composite(
            &pipeline,
            name,
            URN_REDISTRIBUTE_ARBITRARILY,
            allow_duplicates,
            steps,
        );
    }
}

#[test]
fn redistribute_by_key_is_a_composite_with_its_urn_and_payload() {
    let p = Pipeline::new();
    let input = pairs(&p);
    let _ = input.apply("RedistributeByKey", RedistributeByKey::new());
    let _ = input.apply(
        "Duplicates/RedistributeByKey",
        RedistributeByKey::new().with_allow_duplicates(true),
    );

    let pipeline = p.to_proto();
    let steps = ["ReifyHeader", "GroupByKey", "RestoreHeader"];
    for (name, allow_duplicates) in [
        ("RedistributeByKey", false),
        ("Duplicates/RedistributeByKey", true),
    ] {
        assert_composite(
            &pipeline,
            name,
            URN_REDISTRIBUTE_BY_KEY,
            allow_duplicates,
            steps,
        );
    }
}

#[test]
fn redistribute_by_key_groups_by_the_element_key_in_the_global_window() {
    let p = Pipeline::new();
    let windowed = pairs(&p).apply(
        "Sessions",
        WindowInto::new(Sessions::with_gap_duration(Duration::from_secs(10))),
    );
    let _ = windowed.apply("ByKey", RedistributeByKey::new());

    let pipeline = p.to_proto();
    // The shuffle key is the key of the element, and the value holds the header.
    assert_eq!(
        graph::output_coder(&pipeline, "ByKey/ReifyHeader"),
        "kv(string_utf8,kv(kv(varint,bytes),varint))"
    );
    assert_eq!(
        graph::output_coder(&pipeline, "ByKey/GroupByKey"),
        "kv(string_utf8,iterable(kv(kv(varint,bytes),varint)))"
    );
    // No session merges in the shuffle.
    let shuffled = graph::single_output(&pipeline, "ByKey/ReifyHeader");
    assert_eq!(
        graph::window_fn_urn(&pipeline, shuffled),
        URN_WINDOW_FN_GLOBAL_WINDOWS
    );
}

#[test]
fn redistribute_keeps_the_coder_and_the_windowing_strategy_of_the_input() {
    let p = Pipeline::new();
    let windowed = pairs(&p).apply(
        "Sessions",
        WindowInto::new(Sessions::with_gap_duration(Duration::from_secs(10))),
    );
    let arbitrarily = windowed.apply("Arbitrarily", RedistributeArbitrarily::new());
    let by_key = windowed.apply("ByKey", RedistributeByKey::new());

    for output in [&arbitrarily, &by_key] {
        assert_eq!(output.coder_id(), windowed.coder_id(), "{}", output.id());
        assert_eq!(
            output.windowing_strategy_id(),
            windowed.windowing_strategy_id(),
            "{}",
            output.id()
        );
    }
    // The graph holds the same coder and strategy as the handles.
    let pipeline = p.to_proto();
    let pcollections = &graph::components(&pipeline).pcollections;
    for output in [&arbitrarily, &by_key] {
        let pcollection = &pcollections[output.id()];
        assert_eq!(pcollection.coder_id, windowed.coder_id());
        assert_eq!(
            pcollection.windowing_strategy_id,
            windowed.windowing_strategy_id()
        );
    }
}

fn schema() -> Schema {
    Schema::builder()
        .field("id", FieldType::int64())
        .nullable_field("name", FieldType::string())
        .build()
}

fn row(id: i64) -> Row {
    Row::new(schema().into(), vec![Some(FieldValue::Int64(id)), None]).unwrap()
}

#[test]
fn redistribute_keeps_the_schema_of_a_row() {
    let p = Pipeline::new();
    let rows = p.apply("Rows", Create::new(vec![row(1), row(2)]));
    let redistributed = rows.apply("Arbitrarily", RedistributeArbitrarily::new());
    assert_eq!(redistributed.coder_id(), rows.coder_id());
    assert_eq!(redistributed.row_schema().as_deref(), Some(&schema()));

    let keyed = p.apply(
        "Keyed",
        Create::new(vec![("a".to_string(), row(1)), ("b".to_string(), row(2))]),
    );
    let by_key = keyed.apply("ByKey", RedistributeByKey::new());
    assert_eq!(by_key.coder_id(), keyed.coder_id());

    // The value coder of the shuffle is the row coder of the input, with its schema.
    let pipeline = p.to_proto();
    let coders = &graph::components(&pipeline).coders;
    let parts = |coder_id: &str| coders[coder_id].component_coder_ids.clone();
    let [key, value] = parts(keyed.coder_id()).try_into().unwrap();
    let shuffled = graph::single_output(&pipeline, "ByKey/ReifyHeader");
    let shuffled_coder = &graph::components(&pipeline).pcollections[shuffled].coder_id;
    let [shuffled_key, reified] = parts(shuffled_coder).try_into().unwrap();
    let [_, shuffled_value] = parts(&reified).try_into().unwrap();
    assert_eq!(shuffled_key, key);
    assert_eq!(shuffled_value, value);
    assert_eq!(value, rows.coder_id());
    p.validate().expect("each row coder has a schema");
}

#[tokio::test]
async fn redistribute_keeps_each_element_in_its_window() {
    let p = TestPipeline::new();
    let kv = |k: &str, v| (k.to_string(), v);
    let windowed = p
        .apply(
            "Create",
            Create::new(vec![(kv("a", 1i64), 1_000i64), (kv("a", 2), 12_000)]),
        )
        .apply(
            "Timestamps",
            beam::transforms::ParDo::from_fn(
                |(element, millis): ((String, i64), i64),
                 ctx: &mut beam::transforms::ProcessContext<'_, (String, i64)>|
                 -> Result {
                    ctx.output(element).at(Instant::from_millis(millis)).emit()
                },
            ),
        )
        .apply(
            "FixedWindows",
            WindowInto::new(FixedWindows::of(Duration::from_secs(10))),
        );
    let arbitrarily = windowed.apply("Arbitrarily", RedistributeArbitrarily::new());
    let by_key = windowed.apply("ByKey", RedistributeByKey::new());

    let window = |start| {
        IntervalWindow::new(
            Instant::from_millis(start),
            Instant::from_millis(start + 10_000),
        )
    };
    for (name, output) in [("Arbitrarily", &arbitrarily), ("ByKey", &by_key)] {
        passert::that_windowed(format!("Assert{name}"), output)
            .contains_in_any_order([(kv("a", 1), window(0)), (kv("a", 2), window(10_000))]);
    }

    p.run().await.expect("pipeline execution succeeds");
}

#[tokio::test]
async fn redistribute_by_key_keeps_rows() {
    let p = TestPipeline::new();
    let keyed = p.apply(
        "Keyed",
        Create::new(vec![
            ("a".to_string(), row(1)),
            ("a".to_string(), row(2)),
            ("b".to_string(), row(3)),
        ]),
    );
    let ids = keyed.apply("ByKey", RedistributeByKey::new()).apply(
        "Ids",
        beam::transforms::Map::new(|(key, row): (String, Row)| {
            (key, row.get_i64("id").expect("an id field"))
        }),
    );
    passert::that("AssertIds", &ids).contains_in_any_order([
        ("a".to_string(), Some(1)),
        ("a".to_string(), Some(2)),
        ("b".to_string(), Some(3)),
    ]);
    p.run().await.expect("pipeline execution succeeds");
}
