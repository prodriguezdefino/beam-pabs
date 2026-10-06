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
    reason = "test fixtures unwrap; a failure is a test failure"
)]

use std::sync::Arc;

use beam::io::gcp::bigquery::{URN_BIGQUERY_STORAGE_READ, WriteMethod};
use beam::prelude::*;
use beam::schema::{Field, FieldType};
use bigquery_tornadoes::{
    Args, DEFAULT_INPUT_TABLE, TornadoCount, build_pipeline, extract_tornado_month,
    format_tornado_row, parse_write_method,
};
use testutils::MockExpansionService;

fn weather_row_bool(month: i64, tornado: bool) -> Row {
    let schema = Arc::new(Schema::new(vec![
        Field::new("month", FieldType::int64()),
        Field::new("tornado", FieldType::boolean()),
    ]));
    Row::builder(schema)
        .with_value(month)
        .with_value(tornado)
        .build()
        .unwrap()
}

#[test]
fn test_extract_tornado_month_table() {
    let schema_only_month = Arc::new(Schema::new(vec![Field::new("month", FieldType::int64())]));
    let schema_only_tornado = Arc::new(Schema::new(vec![Field::new(
        "tornado",
        FieldType::boolean(),
    )]));

    let cases = [
        (weather_row_bool(6, true), Some(6)),
        (weather_row_bool(6, false), None),
        (
            Row::builder(schema_only_month)
                .with_value(4i64)
                .build()
                .unwrap(),
            None,
        ),
        (
            Row::builder(schema_only_tornado)
                .with_value(true)
                .build()
                .unwrap(),
            None,
        ),
    ];

    for (row, expected) in cases {
        assert_eq!(extract_tornado_month(&row), expected);
    }
}

#[test]
fn test_tornado_count_schema_and_roundtrip() {
    let schema = TornadoCount::beam_schema();
    assert_eq!(schema.num_fields(), 2);
    assert_eq!(schema.fields[0].name, "month");
    assert_eq!(schema.fields[1].name, "tornado_count");

    let count = TornadoCount {
        month: 11,
        tornado_count: 7,
    };
    let row = count.to_row().expect("to_row must succeed");
    let recovered = TornadoCount::from_row(&row).expect("from_row must succeed");
    assert_eq!(recovered, count);

    let formatted = format_tornado_row(5, 42);
    assert_eq!(formatted.get_i64("month").unwrap(), Some(5));
    assert_eq!(formatted.get_i64("tornado_count").unwrap(), Some(42));
}

#[test]
fn test_parse_write_method() {
    assert_eq!(
        parse_write_method("storage_write_api"),
        WriteMethod::StorageWriteApi
    );
    assert_eq!(
        parse_write_method("STORAGE_API"),
        WriteMethod::StorageWriteApi
    );
    assert_eq!(parse_write_method("file_loads"), WriteMethod::FileLoads);
    assert_eq!(parse_write_method("fileloads"), WriteMethod::FileLoads);
    assert_eq!(
        parse_write_method("at_least_once"),
        WriteMethod::StorageApiAtLeastOnce
    );
    assert_eq!(parse_write_method("auto"), WriteMethod::Auto);
}

#[test]
fn test_bigquery_tornadoes_pipeline_build_and_expansion() {
    let server = MockExpansionService::new()
        .with_source(URN_BIGQUERY_STORAGE_READ, true)
        .start();

    let args = Args {
        input: DEFAULT_INPUT_TABLE.to_string(),
        input_query: None,
        output: Some("my-project:weather_data.tornadoes".to_string()),
        expansion_service: Some(server.endpoint().to_string()),
        write_method: "storage_write_api".to_string(),
    };

    let p = build_pipeline(&PipelineOptions::default(), &args);
    let lock = p.lock();

    let transforms = &lock.components.transforms;
    let bq_read = transforms
        .values()
        .find(|t| t.unique_name == "BigQueryRead")
        .expect("Pipeline must contain expanded BigQueryRead transform");
    let bq_read_out = bq_read
        .outputs
        .get("output")
        .expect("BigQueryRead must produce output PCollection");

    let extract = transforms
        .values()
        .find(|t| t.unique_name == "ExtractTornadoes")
        .expect("Pipeline must contain ExtractTornadoes transform");
    assert!(
        extract
            .inputs
            .values()
            .any(|in_pcoll| in_pcoll == bq_read_out),
        "ExtractTornadoes must consume BigQueryRead output"
    );
    let extract_out = extract
        .outputs
        .values()
        .next()
        .expect("ExtractTornadoes must produce an output PCollection");

    let count = transforms
        .values()
        .find(|t| {
            t.unique_name.starts_with("CountTornadoes")
                && t.inputs.values().any(|in_pcoll| in_pcoll == extract_out)
        })
        .expect("A CountTornadoes stage must consume ExtractTornadoes output");
    assert!(
        count.unique_name.starts_with("CountTornadoes"),
        "stage consuming ExtractTornadoes output must be part of CountTornadoes"
    );

    let format_counts = transforms
        .values()
        .find(|t| t.unique_name == "FormatCounts")
        .expect("Pipeline must contain FormatCounts transform");
    let format_out = format_counts
        .outputs
        .values()
        .next()
        .expect("FormatCounts must produce an output PCollection");

    let bq_write = transforms
        .values()
        .find(|t| t.unique_name == "BigQueryWrite")
        .expect("Pipeline must contain expanded BigQueryWrite transform");
    assert!(
        bq_write
            .inputs
            .values()
            .any(|in_pcoll| in_pcoll == format_out),
        "BigQueryWrite must consume FormatCounts output"
    );
}
