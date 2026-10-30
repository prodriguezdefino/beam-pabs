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

//! Options and pipeline shapes of the native Storage Write connector. The tests build
//! pipelines and do not run them, so no test calls BigQuery.
#![expect(clippy::unwrap_used, reason = "test fixtures panic on setup failure")]

use beam::internals::PipelineInternals;
use std::sync::Arc;
use std::time::Duration;

use beam::pipeline::Pipeline;
use beam::schema::{FieldType, FieldValue, Row, Schema};
use beam::transforms::Create;
use beam::values::PCollection;
use gcp::bigquery::WriteDisposition;
use gcp::bigquery::storage::write::{Semantics, WriteOptions, WriteRows, error_schema};
use model::pipeline::ParDoPayload;
use prost::Message;

const TABLE: &str = "my-project.my_dataset.my_table";

fn rows(pipeline: &Pipeline) -> PCollection<Row> {
    let schema = Schema::builder()
        .field("id", FieldType::int64())
        .nullable_field("name", FieldType::string())
        .build();
    let row = Row::new(
        Arc::new(schema.clone()),
        vec![Some(FieldValue::Int64(1)), None],
    )
    .unwrap();
    pipeline
        .apply("Rows", Create::new([row]))
        .with_row_schema(&schema)
}

/// The names of the transforms under the write, without the `WriteRows/` prefix.
fn steps(options: WriteOptions) -> Vec<String> {
    let pipeline = Pipeline::new();
    let errors = rows(&pipeline).apply("WriteRows", WriteRows::new(options));
    assert_eq!(errors.row_schema().unwrap().fields[1].name, "error_message");
    let mut names: Vec<String> = pipeline
        .lock()
        .components
        .transforms
        .values()
        .filter_map(|t| t.unique_name.strip_prefix("WriteRows/").map(str::to_string))
        .filter(|name| !name.contains('/'))
        .collect();
    names.sort();
    names
}

#[test]
fn at_least_once_appends_with_no_shuffle() {
    let steps = steps(WriteOptions::new(TABLE).with_semantics(Semantics::AtLeastOnce));
    assert_eq!(
        steps,
        [
            "GlobalWindow",
            "Impulse",
            "PrepareTable",
            "WriteDefaultStream"
        ]
    );
}

#[test]
fn exactly_once_on_bounded_input_writes_pending_streams_then_commits() {
    let steps = steps(WriteOptions::new(TABLE));
    assert_eq!(
        steps,
        [
            "CommitStreams",
            "GlobalWindow",
            "GroupStreams",
            "Impulse",
            "PrepareTable",
            "WritePendingStreams",
        ]
    );
}

#[test]
fn exactly_once_with_a_triggering_frequency_writes_buffered_streams_with_state() {
    let pipeline = Pipeline::new();
    rows(&pipeline).apply(
        "WriteRows",
        WriteRows::new(WriteOptions::new(TABLE).with_triggering_frequency(Duration::from_secs(2))),
    );
    let lock = pipeline.lock();
    let writer = lock
        .components
        .transforms
        .values()
        .find(|t| t.unique_name == "WriteRows/WriteBufferedStreams")
        .unwrap();
    let payload = ParDoPayload::decode(writer.spec.as_ref().unwrap().payload.as_slice()).unwrap();
    assert!(payload.state_specs.contains_key("stream"));
    let mut timers: Vec<_> = payload.timer_family_specs.keys().cloned().collect();
    timers.sort();
    assert_eq!(timers, ["expiry", "flush"]);
    assert_eq!(
        payload.side_inputs.len(),
        1,
        "the writer reads the table as a side input"
    );
}

#[test]
fn the_error_rows_hold_the_row_and_the_reason() {
    let schema = Schema::builder().field("id", FieldType::int64()).build();
    let errors = error_schema(&schema);
    let fields: Vec<_> = errors
        .fields
        .iter()
        .map(|f| (f.name.as_str(), f.field_type.clone()))
        .collect();
    assert_eq!(
        fields,
        [
            ("failed_row", FieldType::row(schema)),
            ("error_message", FieldType::string()),
        ]
    );
}

#[test]
fn invalid_options_fail_before_any_call() {
    let cases = [
        (WriteOptions::new("no_dataset"), "Invalid table spec"),
        (
            WriteOptions::new(TABLE).with_write_disposition(WriteDisposition::WriteTruncate),
            "cannot replace",
        ),
        (
            WriteOptions::new(TABLE).with_triggering_frequency(Duration::ZERO),
            "positive",
        ),
    ];
    for (options, expected) in cases {
        let error = options.validate().unwrap_err().to_string();
        assert!(error.contains(expected), "{options:?}: {error}");
    }
}

#[test]
fn a_table_without_a_project_needs_one_from_the_pipeline() {
    let pipeline = Pipeline::new();
    let error = rows(&pipeline)
        .try_apply(
            "WriteRows",
            WriteRows::new(WriteOptions::new("dataset.table")),
        )
        .unwrap_err()
        .to_string();
    assert!(error.contains("has no project"), "{error}");
}
