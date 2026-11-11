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

//! Options and pipeline shapes of the Storage Write connector. The tests build
//! pipelines and do not run them, so no test calls BigQuery.

use beam::internals::PipelineInternals;
use std::sync::Arc;
use std::time::Duration;

use beam::pipeline::Pipeline;
use beam::schema::{BeamRow, FieldType, FieldValue, Row, Schema};
use beam::transforms::{Create, Map, PeriodicImpulse};
use beam::values::{IsBounded, PCollection};
use beam_io_gcp::bigquery::write::{Semantics, WriteOptions, WriteRows, error_schema};
use beam_model::pipeline::ParDoPayload;
use prost::Message;

const TABLE: &str = "my-project.my_dataset.my_table";

fn schema() -> Schema {
    Schema::builder()
        .field("id", FieldType::int64())
        .nullable_field("name", FieldType::string())
        .build()
}

fn row(schema: &Schema) -> Row {
    Row::new(
        Arc::new(schema.clone()),
        vec![Some(FieldValue::Int64(1)), None],
    )
    .unwrap()
}

fn rows(pipeline: &Pipeline) -> PCollection<Row> {
    let schema = schema();
    pipeline
        .apply("Rows", Create::new([row(&schema)]))
        .with_row_schema(&schema)
}

/// An unbounded input: one row for each tick.
fn unbounded_rows(pipeline: &Pipeline) -> PCollection<Row> {
    let schema = schema();
    let row = row(&schema);
    let rows = pipeline
        .apply("Ticks", PeriodicImpulse::new(Duration::from_secs(1)))
        .apply("Rows", Map::new(move |_: i64| row.clone()))
        .with_row_schema(&schema);
    assert_eq!(rows.is_bounded(), IsBounded::Unbounded);
    rows
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
fn exactly_once_on_unbounded_input_writes_buffered_streams_with_state() {
    let pipeline = Pipeline::new();
    unbounded_rows(&pipeline).apply(
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
fn apply_records_an_expansion_error_and_returns_an_errors_output() {
    let pipeline = Pipeline::new();
    let input = rows(&pipeline);
    let schema = input.row_schema().unwrap();
    let errors = input.apply(
        "WriteRows",
        WriteRows::new(WriteOptions::new("dataset.table")),
    );
    errors.apply("Count", Map::new(|row: Row| row.values().len() as i64));
    assert_eq!(errors.row_schema().as_deref(), Some(&error_schema(&schema)));
    let error = pipeline.validate().unwrap_err();
    assert!(
        matches!(
            &error,
            beam::pipeline::PipelineError::InvalidConstruction { transform_id, reason }
                if transform_id == "WriteRows" && reason.contains("has no project")
        ),
        "{error}"
    );
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

#[test]
fn a_bounded_input_with_a_triggering_frequency_does_not_expand() {
    let pipeline = Pipeline::new();
    let error = rows(&pipeline)
        .try_apply(
            "WriteRows",
            WriteRows::new(
                WriteOptions::new(TABLE).with_triggering_frequency(Duration::from_secs(2)),
            ),
        )
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("A triggering frequency applies only to an unbounded input"),
        "{error}"
    );
}

#[test]
fn an_unbounded_input_without_a_triggering_frequency_writes_buffered_streams() {
    let pipeline = Pipeline::new();
    unbounded_rows(&pipeline).apply("WriteRows", WriteRows::new(WriteOptions::new(TABLE)));
    let lock = pipeline.lock();
    let names: Vec<&str> = lock
        .components
        .transforms
        .values()
        .map(|t| t.unique_name.as_str())
        .collect();
    assert!(
        names.contains(&"WriteRows/WriteBufferedStreams"),
        "{names:?}"
    );
    assert!(
        !names.contains(&"WriteRows/WritePendingStreams"),
        "{names:?}"
    );
}

#[test]
fn write_rows_records_display_data_in_pipeline_proto() {
    use std::collections::HashMap;

    use beam::transforms::display_data::DisplayDataItem;
    use beam_io_gcp::bigquery::write::{CreateDisposition, WriteDisposition};

    let pipeline = Pipeline::new();
    unbounded_rows(&pipeline).apply(
        "WriteRows",
        WriteRows::new(
            WriteOptions::new(TABLE)
                .with_create_disposition(CreateDisposition::CreateNever)
                .with_write_disposition(WriteDisposition::WriteEmpty)
                .with_triggering_frequency(Duration::from_secs(7))
                .with_num_streams(4),
        ),
    );

    let components = pipeline.to_proto().components.unwrap();
    let transform = components
        .transforms
        .values()
        .find(|t| t.unique_name == "WriteRows")
        .unwrap();
    let items: HashMap<String, String> = transform
        .display_data
        .iter()
        .map(|d| {
            let item = DisplayDataItem::from_proto(d).unwrap();
            (item.key, item.value.to_string())
        })
        .collect();

    assert_eq!(items.get("table").map(String::as_str), Some(TABLE));
    assert_eq!(
        items.get("semantics").map(String::as_str),
        Some("EXACTLY_ONCE")
    );
    assert_eq!(
        items.get("create_disposition").map(String::as_str),
        Some("CREATE_NEVER")
    );
    assert_eq!(
        items.get("write_disposition").map(String::as_str),
        Some("WRITE_EMPTY")
    );
    assert_eq!(
        items.get("triggering_frequency").map(String::as_str),
        Some("7s")
    );
    assert_eq!(items.get("num_streams").map(String::as_str), Some("4"));
}

#[derive(Clone, Debug, PartialEq, Eq, BeamRow)]
struct TypedItem {
    id: i64,
    name: Option<String>,
}

#[test]
fn write_typed_expands_beam_row_elements_without_to_rows_step() {
    let pipeline = Pipeline::new();
    let failed = pipeline
        .apply(
            "Items",
            Create::new([TypedItem {
                id: 7,
                name: Some("alpha".to_string()),
            }]),
        )
        .apply(
            "WriteTyped",
            WriteRows::new(WriteOptions::new(TABLE)).typed::<TypedItem>(),
        );

    assert_eq!(
        failed.row_schema().as_deref(),
        Some(&error_schema(&TypedItem::beam_schema()))
    );

    let lock = pipeline.lock();
    let names: Vec<&str> = lock
        .components
        .transforms
        .values()
        .map(|t| t.unique_name.as_str())
        .collect();
    assert!(
        names.contains(&"WriteTyped/WritePendingStreams"),
        "{names:?}"
    );
    assert!(!names.contains(&"WriteTyped/ToRows"), "{names:?}");
}
