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

//! Graph tests of the write transforms, with no runner or catalog.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use beam::pipeline::{Pipeline, PipelineError};
use beam::schema::{BeamRow, FieldType, FieldValue, Row, Schema};
use beam::transforms::display_data::DisplayDataItem;
use beam::transforms::{Create, Map, PeriodicImpulse};
use beam::values::{IsBounded, PCollection};
use beam_io_iceberg::{Catalog, SnapshotInfo, Write, WriteOptions, WriteRows};

#[derive(Clone, Debug, BeamRow)]
struct Item {
    id: i64,
}

fn schema() -> Schema {
    Schema::builder().field("id", FieldType::int64()).build()
}

fn row(schema: &Schema) -> Row {
    Row::new(Arc::new(schema.clone()), vec![Some(FieldValue::Int64(1))]).unwrap()
}

fn bounded_rows(pipeline: &Pipeline) -> PCollection<Row> {
    let schema = schema();
    pipeline
        .apply("Create", Create::new([row(&schema)]))
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

fn options() -> WriteOptions {
    WriteOptions::new(Catalog::rest("http://unused"), "ns.t")
}

#[test]
fn a_write_of_rows_without_a_schema_records_an_error_and_returns_outputs() {
    let pipeline = Pipeline::new();
    // An empty `Create` of rows has a row coder with no schema.
    let result = pipeline
        .apply("Create", Create::new(Vec::<Row>::new()))
        .apply(
            "Write",
            WriteRows::new(WriteOptions::new(Catalog::rest("http://unused"), "ns.t")),
        );
    result
        .errors
        .apply("Errors", Map::new(|row: Row| row.values().len() as i64));
    result
        .snapshots
        .apply("Snapshots", Map::new(|_: SnapshotInfo| 1i64));
    assert!(result.errors.row_schema().is_some());
    assert_eq!(
        pipeline.validate(),
        Err(PipelineError::InvalidConstruction {
            transform_id: "Write".to_string(),
            reason: "The input of the write has no row schema".to_string(),
        })
    );
}

#[test]
fn a_bounded_input_with_a_triggering_frequency_records_an_error() {
    let pipeline = Pipeline::new();
    bounded_rows(&pipeline).apply(
        "Write",
        WriteRows::new(options().with_triggering_frequency(Duration::from_secs(1))),
    );
    assert_eq!(
        pipeline.validate(),
        Err(PipelineError::InvalidConstruction {
            transform_id: "Write".to_string(),
            reason: "A triggering frequency applies only to an unbounded input".to_string(),
        })
    );
}

#[test]
fn a_bounded_input_without_a_triggering_frequency_is_valid() {
    let pipeline = Pipeline::new();
    bounded_rows(&pipeline).apply("Write", WriteRows::new(options()));
    assert_eq!(pipeline.validate(), Ok(()));
}

#[test]
fn an_unbounded_input_takes_a_triggering_frequency() {
    for options in [
        options(),
        options().with_triggering_frequency(Duration::from_secs(1)),
    ] {
        let pipeline = Pipeline::new();
        unbounded_rows(&pipeline).apply("Write", WriteRows::new(options));
        assert_eq!(pipeline.validate(), Ok(()));
    }
}

#[test]
fn write_and_write_rows_record_display_data_in_pipeline_proto() {
    let pipeline = Pipeline::new();
    pipeline
        .apply("Items", Create::new([Item { id: 1 }]))
        .apply("WriteTyped", Write::<Item>::new(options()));
    unbounded_rows(&pipeline).apply(
        "WriteDynamic",
        WriteRows::new(
            WriteOptions::new(
                Catalog::rest("https://catalog.example/iceberg")
                    .with_warehouse("bl://projects/p/catalogs/c"),
                "db.events",
            )
            .with_triggering_frequency(Duration::from_secs(30))
            .with_target_file_bytes(1048576)
            .with_num_shards(4),
        ),
    );

    let proto = pipeline.to_proto();
    let components = proto.components.unwrap();
    let display_map = |name: &str| -> HashMap<String, String> {
        let transform = components
            .transforms
            .values()
            .find(|t| t.unique_name == name)
            .unwrap();
        transform
            .display_data
            .iter()
            .map(|d| {
                let item = DisplayDataItem::from_proto(d).unwrap();
                (item.key, item.value.to_string())
            })
            .collect()
    };

    let typed = display_map("WriteTyped");
    assert_eq!(
        typed.get("catalog_uri").map(String::as_str),
        Some("http://unused")
    );
    assert_eq!(typed.get("table").map(String::as_str), Some("ns.t"));
    assert!(!typed.contains_key("warehouse"));
    assert!(!typed.contains_key("triggering_frequency"));
    assert!(!typed.contains_key("target_file_bytes"));
    assert!(!typed.contains_key("num_shards"));

    let dynamic = display_map("WriteDynamic");
    assert_eq!(
        dynamic.get("catalog_uri").map(String::as_str),
        Some("https://catalog.example/iceberg")
    );
    assert_eq!(
        dynamic.get("warehouse").map(String::as_str),
        Some("bl://projects/p/catalogs/c")
    );
    assert_eq!(dynamic.get("table").map(String::as_str), Some("db.events"));
    assert_eq!(
        dynamic.get("triggering_frequency").map(String::as_str),
        Some("30s")
    );
    assert_eq!(
        dynamic.get("target_file_bytes").map(String::as_str),
        Some("1048576")
    );
    assert_eq!(dynamic.get("num_shards").map(String::as_str), Some("4"));
}
