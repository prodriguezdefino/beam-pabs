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

//! Pipeline shapes of the native Storage Read connector. The tests build pipelines and do not
//! run them, and give the schema, so no test calls BigQuery.
#![expect(clippy::unwrap_used, reason = "test fixtures panic on setup failure")]

use beam::internals::PipelineInternals;
use std::sync::Arc;

use beam::pipeline::Pipeline;
use beam::schema::{FieldType, Schema};
use gcp::bigquery::storage::read::{ReadOptions, ReadRows, error_schema};

const TABLE: &str = "my-project.my_dataset.my_table";

const DEFAULT_STEPS: [&str; 7] = [
    "CreateSession",
    "Impulse",
    "ReadStreams",
    "Reshuffle",
    "Reshuffle/Expand",
    "Reshuffle/GroupByKey",
    "Reshuffle/PairWithRandomKey",
];

fn read() -> ReadRows {
    let schema = Schema::builder().field("id", FieldType::int64()).build();
    ReadRows::new(ReadOptions::new(TABLE))
        .with_schema(Arc::new(schema))
        .with_encoded_rows()
}

/// The sorted names of the transforms under the read, without the `ReadRows/` prefix. With
/// `nested`, the list also has the steps inside each step.
fn steps(pipeline: &Pipeline, nested: bool) -> Vec<String> {
    let mut names: Vec<String> = pipeline
        .lock()
        .components
        .transforms
        .values()
        .filter_map(|t| t.unique_name.strip_prefix("ReadRows/").map(str::to_string))
        .filter(|name| nested || !name.contains('/'))
        .collect();
    names.sort();
    names
}

/// The unique name of the transform that produces `pcollection`, and the URN of its coder.
fn producer_and_coder(pipeline: &Pipeline, pcollection: &str) -> (String, String) {
    let lock = pipeline.lock();
    let components = &lock.components;
    let producer = components
        .transforms
        .values()
        .filter(|t| t.subtransforms.is_empty())
        .find(|t| t.outputs.values().any(|id| id == pcollection))
        .unwrap()
        .unique_name
        .clone();
    let coder_id = &components.pcollections[pcollection].coder_id;
    let urn = components.coders[coder_id]
        .spec
        .as_ref()
        .unwrap()
        .urn
        .clone();
    (producer, urn)
}

#[test]
fn the_default_read_emits_the_rows_of_the_stream_reader() {
    let pipeline = Pipeline::new();
    let rows = pipeline.try_apply("ReadRows", read()).unwrap();
    // The same steps, the step names inside them included, as a read without an error output.
    assert_eq!(steps(&pipeline, true), DEFAULT_STEPS);
    assert_eq!(
        steps(&pipeline, false),
        ["CreateSession", "Impulse", "ReadStreams", "Reshuffle"]
    );
    let (producer, urn) = producer_and_coder(&pipeline, rows.id());
    assert_eq!(producer, "ReadRows/ReadStreams");
    assert_eq!(urn, "beam:coder:row:v1");
    assert_eq!(rows.row_schema().unwrap().fields[0].name, "id");
}

#[test]
fn the_read_with_failures_emits_the_rows_and_the_error_rows_from_the_reader() {
    let pipeline = Pipeline::new();
    let outputs = pipeline
        .try_apply("ReadRows", read().with_failures())
        .unwrap();
    assert_eq!(
        steps(&pipeline, false),
        ["CreateSession", "Impulse", "ReadStreams", "Reshuffle"]
    );
    for output in [&outputs.output, &outputs.failures] {
        let (producer, urn) = producer_and_coder(&pipeline, output.id());
        assert_eq!(producer, "ReadRows/ReadStreams");
        assert_eq!(urn, "beam:coder:row:v1");
    }
    assert_eq!(outputs.output.row_schema().unwrap().fields[0].name, "id");
    assert_eq!(&outputs.failures.row_schema().unwrap(), error_schema());
}

#[test]
fn the_error_rows_hold_the_row_the_reason_and_the_position() {
    let fields: Vec<_> = error_schema()
        .fields
        .iter()
        .map(|f| (f.name.as_str(), f.field_type.clone()))
        .collect();
    assert_eq!(
        fields,
        [
            ("failed_row", FieldType::string()),
            ("error_message", FieldType::string()),
            ("stream", FieldType::string()),
            ("offset", FieldType::int64()),
        ]
    );
}
