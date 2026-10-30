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

//! The options and the expansion of the write. No test sends a record.

use beam::internals::PipelineInternals;
use std::sync::Arc;

use beam::prelude::*;
use beam::schema::{Field, FieldType, FieldValue, Row, Schema};
use kafka_io::native::codec::managed::Payload;
use kafka_io::native::codec::{Json, write_error_schema};
use kafka_io::native::write::{Write, WriteOptions};

fn bytes_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![Field::new("payload", FieldType::bytes())]))
}

fn rows(pipeline: &Pipeline, schema: &Arc<Schema>) -> PCollection<Row> {
    let row = Row::new(
        Arc::clone(schema),
        vec![Some(FieldValue::Bytes(b"v".to_vec()))],
    )
    .expect("one value for the one field");
    pipeline
        .apply("Create", Create::new(vec![row]))
        .with_row_schema(schema)
}

/// The composite of the write named `name`, and its output.
fn composite_errors(pipeline: &Pipeline, name: &str) -> Option<String> {
    let lock = pipeline.lock();
    let composite = lock
        .components
        .transforms
        .values()
        .find(|t| t.unique_name == name)
        .expect("the composite of the write");
    assert!(!composite.subtransforms.is_empty());
    composite.outputs.get("out").cloned()
}

#[test]
fn options_need_servers_and_a_topic() {
    assert!(
        WriteOptions::new("broker:9092", "events")
            .validate()
            .is_ok()
    );
    assert!(WriteOptions::new("", "events").validate().is_err());
    assert!(WriteOptions::new("broker:9092", " ").validate().is_err());
}

#[test]
fn compression_goes_to_the_connection() {
    use kafka_io::native::write::Compression;

    let options = WriteOptions::new("broker:9092", "events").with_compression(Compression::Zstd);
    assert_eq!(
        options
            .connection
            .properties
            .get("compression.type")
            .map(String::as_str),
        Some("zstd")
    );
    for (codec, expected) in [
        (Compression::None, "none"),
        (Compression::Gzip, "gzip"),
        (Compression::Snappy, "snappy"),
        (Compression::Lz4, "lz4"),
        (Compression::Zstd, "zstd"),
    ] {
        assert_eq!(codec.as_str(), expected);
    }
}

#[test]
fn producer_properties_go_to_the_connection() {
    let options = WriteOptions::new("broker:9092", "events")
        .with_producer_properties([("linger.ms".to_string(), "20".to_string())]);
    assert_eq!(
        options
            .connection
            .properties
            .get("linger.ms")
            .map(String::as_str),
        Some("20")
    );
}

#[test]
fn a_write_of_values_is_one_composite_with_an_errors_output_of_values() {
    let pipeline = Pipeline::new();
    let values = pipeline.apply("Create", Create::new(vec![b"v".to_vec()]));
    let errors = values
        .try_apply(
            "WriteValues",
            Write::new(WriteOptions::new("broker:9092", "events")),
        )
        .unwrap();
    assert_eq!(
        errors.row_schema().as_deref(),
        Some(&write_error_schema(FieldType::bytes()))
    );
    assert_eq!(
        composite_errors(&pipeline, "WriteValues"),
        Some(errors.id().to_string())
    );
}

#[test]
fn a_write_of_rows_rejects_the_input_rows() {
    let pipeline = Pipeline::new();
    let schema = bytes_schema();
    let errors = rows(&pipeline, &schema)
        .try_apply(
            "WriteEvents",
            Write::new(WriteOptions::new("broker:9092", "events")).with_encoder(Json::new()),
        )
        .unwrap();
    assert_eq!(
        errors.row_schema().as_deref(),
        Some(&write_error_schema(FieldType::row((*schema).clone())))
    );
    assert_eq!(
        composite_errors(&pipeline, "WriteEvents"),
        Some(errors.id().to_string())
    );
}

#[test]
fn an_input_that_the_encoder_cannot_write_fails_the_expansion() {
    let pipeline = Pipeline::new();
    let text = Arc::new(Schema::new(vec![Field::new(
        "payload",
        FieldType::string(),
    )]));
    let row = Row::new(
        Arc::clone(&text),
        vec![Some(FieldValue::String("v".into()))],
    )
    .unwrap();
    let input = pipeline
        .apply("Create", Create::new(vec![row]))
        .with_row_schema(&text);
    let error = input
        .try_apply(
            "WriteEvents",
            Write::new(WriteOptions::new("broker:9092", "events")).with_encoder(Payload::raw()),
        )
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("WriteEvents") && error.contains("BYTES"),
        "{error}"
    );
}

#[test]
fn invalid_options_fail_the_expansion_with_the_transform_name() {
    let pipeline = Pipeline::new();
    let values = pipeline.apply("Create", Create::new(vec![b"v".to_vec()]));
    let error = values
        .try_apply(
            "WriteEvents",
            Write::new(WriteOptions::new("broker:9092", "")),
        )
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("WriteEvents") && error.contains("topic"),
        "{error}"
    );
}
