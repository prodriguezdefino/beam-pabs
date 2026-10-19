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

use std::sync::Arc;

use beam::prelude::*;
use beam::schema::{Field, FieldType, FieldValue, Row, Schema};
use kafka_io::native::format::WriteFormat;
use kafka_io::native::write::{WriteOptions, WriteRecords, error_schema};

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
        .apply(Create::new("Create", vec![row]))
        .with_row_schema(schema)
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
fn producer_properties_go_to_the_connection() {
    let options = WriteOptions::new("broker:9092", "events")
        .with_format(WriteFormat::Json)
        .with_producer_properties([("linger.ms".to_string(), "20".to_string())]);
    assert_eq!(options.format, WriteFormat::Json);
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
fn the_error_row_holds_the_input_row_and_the_reason() {
    let schema = error_schema(&bytes_schema());
    let names: Vec<&str> = schema.fields.iter().map(|f| f.name.as_str()).collect();
    assert_eq!(names, ["failed_row", "error_message"]);
}

#[test]
fn the_write_is_one_composite_with_an_errors_output() {
    let pipeline = Pipeline::new();
    let schema = bytes_schema();
    let errors = WriteRecords::new("WriteEvents", WriteOptions::new("broker:9092", "events"))
        .try_expand(&rows(&pipeline, &schema))
        .unwrap();
    assert_eq!(errors.row_schema().as_deref(), Some(&error_schema(&schema)));
    let lock = pipeline.lock();
    let composite = lock
        .components
        .transforms
        .values()
        .find(|t| t.unique_name == "WriteEvents")
        .expect("the composite of the write");
    assert_eq!(
        composite.outputs.get("errors"),
        Some(&errors.id().to_string())
    );
    assert!(!composite.subtransforms.is_empty());
}

#[test]
fn an_input_that_the_format_cannot_write_fails_the_expansion() {
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
        .apply(Create::new("Create", vec![row]))
        .with_row_schema(&text);
    let error = WriteRecords::new("WriteEvents", WriteOptions::new("broker:9092", "events"))
        .try_expand(&input)
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
    let error = WriteRecords::new("WriteEvents", WriteOptions::new("broker:9092", ""))
        .try_expand(&rows(&pipeline, &bytes_schema()))
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("WriteEvents") && error.contains("topic"),
        "{error}"
    );
}
