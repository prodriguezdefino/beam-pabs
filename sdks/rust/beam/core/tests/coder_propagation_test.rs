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
    reason = "test helpers; a failure is a test failure"
)]

//! Output coders of the core transforms that keep their elements: the elements keep the
//! coder of the input, so a [`Row`] keeps its schema without a `with_row_schema` call.

use beam::coders::{URN_ITERABLE, URN_KV, coder_urn};
use beam::pipeline::Pipeline;
use beam::schema::{FieldType, FieldValue, Row, Schema};
use beam::transforms::{
    Create, Filter, Flatten, GroupByKey, Inspect, Partition, Reshuffle, WithKeys,
};
use beam::values::{PCollection, PCollectionList};

fn schema() -> Schema {
    Schema::builder()
        .field("id", FieldType::int64())
        .nullable_field("name", FieldType::string())
        .build()
}

fn rows(p: &Pipeline) -> PCollection<Row> {
    let schema = schema();
    let rows: Vec<Row> = (0..3)
        .map(|id| {
            Row::new(
                schema.clone().into(),
                vec![Some(FieldValue::Int64(id)), None],
            )
            .unwrap()
        })
        .collect();
    p.apply(Create::new("Rows", rows)).with_row_schema(&schema)
}

/// The component coder ids of `coder_id`, after a check of its URN.
fn components(p: &Pipeline, coder_id: &str, urn: &str) -> Vec<String> {
    let inner = p.lock();
    let coder = &inner.components.coders[coder_id];
    assert_eq!(coder_urn(coder), urn, "coder {coder_id}");
    coder.component_coder_ids.clone()
}

#[test]
fn pass_through_transforms_keep_the_row_coder() {
    let p = Pipeline::new();
    let input = rows(&p);
    let filtered = input.apply(Filter::new("Filter", |_: &Row| true));
    let inspected = filtered.apply(Inspect::new("Inspect", |_: &Row| {}));
    let reshuffled = inspected.apply(Reshuffle::new("Reshuffle"));
    let parts = reshuffled.apply(Partition::new("Partition", 2, |_: &Row| 0));
    let flattened = parts.apply(Flatten::new("Flatten"));

    for output in [&filtered, &inspected, &reshuffled, &flattened] {
        assert_eq!(output.coder_id(), input.coder_id(), "{}", output.id());
        assert_eq!(output.row_schema().as_deref(), Some(&schema()));
    }
}

#[test]
fn with_keys_and_group_by_key_build_their_coders_from_the_input() {
    let p = Pipeline::new();
    let input = rows(&p);
    let keyed = input.apply(WithKeys::new("Key", |_: &Row| 7i32));
    let [key, value] = components(&p, keyed.coder_id(), URN_KV).try_into().unwrap();
    assert_eq!(value, input.coder_id());

    let grouped = keyed.apply(GroupByKey::new("Group"));
    let [grouped_key, values] = components(&p, grouped.coder_id(), URN_KV)
        .try_into()
        .unwrap();
    assert_eq!(grouped_key, key);
    assert_eq!(components(&p, &values, URN_ITERABLE), [input.coder_id()]);
}

#[test]
fn flatten_takes_the_coder_of_the_first_input() {
    let p = Pipeline::new();
    let first = rows(&p);
    let second = p.apply(Create::new("More", Vec::<Row>::new()));
    let flattened =
        PCollectionList::from_vec(p, vec![first.clone(), second]).apply(Flatten::new("Flatten"));
    assert_eq!(flattened.coder_id(), first.coder_id());
}
