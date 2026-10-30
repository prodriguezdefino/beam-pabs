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

//! Configuration and registration of the GCP providers. No test calls Google Cloud.

use beam::io::gcp::bigquery::storage::read::{ArrowCompression, QueryPriority, ReadOptions};
use beam::schema::BeamRow;
use expansion::{SchemaTransformProvider, SchemaTransformRegistration};
use gcp_expansion::{
    BigQueryStorageReadConfig, BigQueryStorageReadProvider, ErrorHandling,
    URN_BIGQUERY_STORAGE_READ,
};

fn some(value: &str) -> Option<String> {
    Some(value.to_string())
}

fn table(spec: &str) -> BigQueryStorageReadConfig {
    BigQueryStorageReadConfig {
        table_spec: some(spec),
        ..Default::default()
    }
}

fn query(sql: &str) -> BigQueryStorageReadConfig {
    BigQueryStorageReadConfig {
        query: some(sql),
        ..Default::default()
    }
}

#[test]
fn the_provider_is_registered() {
    let found = expansion::inventory::iter::<SchemaTransformRegistration>
        .into_iter()
        .any(|reg| (reg.provider)().identifier() == URN_BIGQUERY_STORAGE_READ);
    assert!(found);
}

#[test]
fn the_config_has_the_java_fields_and_the_rust_options() {
    let schema = BigQueryStorageReadProvider.config_schema();
    let names: Vec<_> = schema.fields.iter().map(|f| f.name.as_str()).collect();
    assert_eq!(
        names,
        [
            "query",
            "table_spec",
            "row_restriction",
            "selected_fields",
            "kms_key",
            "billing_project",
            "max_stream_count",
            "compression",
            "query_priority",
            "query_location",
            "temp_dataset",
            "use_legacy_sql",
            "flatten_results",
            "snapshot_time",
            "preferred_min_stream_count",
            "error_handling",
        ]
    );
    assert!(schema.fields.iter().all(|f| f.field_type.nullable));
}

#[test]
fn a_config_row_decodes_to_the_config() {
    let config = BigQueryStorageReadConfig {
        selected_fields: Some(vec!["title".to_string()]),
        max_stream_count: Some(4),
        use_legacy_sql: Some(true),
        snapshot_time: some("2024-05-01T12:30:00Z"),
        error_handling: Some(ErrorHandling {
            output: "errors".to_string(),
        }),
        ..table("p.d.t")
    };
    let row = config.to_row().unwrap();
    assert_eq!(BigQueryStorageReadConfig::from_row(&row).unwrap(), config);
}

#[test]
fn every_table_option_reaches_the_read() {
    let options = BigQueryStorageReadConfig {
        row_restriction: some("wp_namespace = 0"),
        selected_fields: Some(vec!["title".to_string(), "id".to_string()]),
        billing_project: some("my-project"),
        max_stream_count: Some(16),
        compression: some("lz4_frame"),
        snapshot_time: some("2024-05-01T12:30:00Z"),
        preferred_min_stream_count: Some(8),
        ..table("bigquery-public-data.samples.wikipedia")
    }
    .read_options()
    .unwrap();
    assert_eq!(
        options,
        ReadOptions::new("bigquery-public-data.samples.wikipedia")
            .with_row_restriction("wp_namespace = 0")
            .with_selected_fields(["title", "id"])
            .with_billing_project("my-project")
            .with_max_stream_count(16)
            .with_compression(ArrowCompression::Lz4Frame)
            .with_snapshot_time("2024-05-01T12:30:00Z")
            .with_preferred_min_stream_count(8)
    );
}

#[test]
fn every_query_option_reaches_the_read() {
    let options = BigQueryStorageReadConfig {
        kms_key: some("projects/p/locations/us/keyRings/r/cryptoKeys/k"),
        billing_project: some("my-project"),
        max_stream_count: Some(4),
        compression: some("zstd"),
        query_priority: some("interactive"),
        query_location: some("US"),
        temp_dataset: some("my_temp_dataset"),
        use_legacy_sql: Some(true),
        flatten_results: Some(false),
        ..query("SELECT id, title FROM `p.d.t`")
    }
    .read_options()
    .unwrap();
    assert_eq!(
        options,
        ReadOptions::from_query("SELECT id, title FROM `p.d.t`")
            .with_kms_key("projects/p/locations/us/keyRings/r/cryptoKeys/k")
            .with_billing_project("my-project")
            .with_max_stream_count(4)
            .with_compression(ArrowCompression::Zstd)
            .with_query_priority(QueryPriority::Interactive)
            .with_query_location("US")
            .with_temp_dataset("my_temp_dataset")
            .with_use_legacy_sql(true)
            .with_flatten_results(false)
    );
}

#[test]
fn empty_strings_count_as_unset() {
    let options = BigQueryStorageReadConfig {
        row_restriction: some(""),
        query: some(""),
        selected_fields: Some(Vec::new()),
        ..table("samples.wikipedia")
    }
    .read_options()
    .unwrap();
    assert_eq!(options, ReadOptions::new("samples.wikipedia"));
}

#[test]
fn invalid_configs_fail_before_any_call() {
    let cases = [
        (BigQueryStorageReadConfig::default(), "table_spec"),
        (
            BigQueryStorageReadConfig {
                query: some("SELECT 1"),
                ..table("p.d.t")
            },
            "table_spec",
        ),
        (
            BigQueryStorageReadConfig {
                row_restriction: some("id > 0"),
                ..query("SELECT 1")
            },
            "row_restriction",
        ),
        (
            BigQueryStorageReadConfig {
                selected_fields: Some(vec!["id".to_string()]),
                ..query("SELECT 1")
            },
            "selected_fields",
        ),
        (
            BigQueryStorageReadConfig {
                query_priority: some("urgent"),
                ..query("SELECT 1")
            },
            "urgent",
        ),
        (
            BigQueryStorageReadConfig {
                compression: some("gzip"),
                ..table("p.d.t")
            },
            "gzip",
        ),
        (
            BigQueryStorageReadConfig {
                max_stream_count: Some(i64::MAX),
                ..table("p.d.t")
            },
            "max_stream_count",
        ),
        (
            BigQueryStorageReadConfig {
                preferred_min_stream_count: Some(-1),
                ..table("p.d.t")
            },
            "preferred_min_stream_count",
        ),
        (
            BigQueryStorageReadConfig {
                snapshot_time: some("yesterday"),
                ..table("p.d.t")
            },
            "snapshot_time",
        ),
        (
            BigQueryStorageReadConfig {
                snapshot_time: some("2024-05-01T12:30:00Z"),
                ..query("SELECT 1")
            },
            "snapshot_time",
        ),
    ];
    for (config, expected) in cases {
        let error = config.clone().read_options().unwrap_err();
        assert!(error.contains(expected), "{config:?}: {error}");
    }
}
