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

//! Configuration and registration of the Storage Write provider. No test calls Google Cloud.

use std::time::Duration;

use beam::io::gcp::bigquery::storage::write::{Semantics, WriteOptions};
use beam::io::gcp::bigquery::{CreateDisposition, WriteDisposition};
use beam::schema::{BeamRow, FieldType};
use expansion::{SchemaTransformProvider, SchemaTransformRegistration};
use gcp_expansion::{
    BigQueryStorageWriteConfig, BigQueryStorageWriteProvider, ErrorHandling,
    URN_BIGQUERY_STORAGE_WRITE,
};

fn some(value: &str) -> Option<String> {
    Some(value.to_string())
}

fn table(spec: &str) -> BigQueryStorageWriteConfig {
    BigQueryStorageWriteConfig {
        table: spec.to_string(),
        ..Default::default()
    }
}

#[test]
fn the_provider_is_registered() {
    let found = expansion::inventory::iter::<SchemaTransformRegistration>
        .into_iter()
        .any(|reg| (reg.provider)().identifier() == URN_BIGQUERY_STORAGE_WRITE);
    assert!(found);
}

#[test]
fn the_config_has_the_java_fields() {
    let provider = BigQueryStorageWriteProvider;
    let schema = provider.config_schema();
    let names: Vec<_> = schema.fields.iter().map(|f| f.name.as_str()).collect();
    assert_eq!(
        names,
        [
            "table",
            "create_disposition",
            "write_disposition",
            "triggering_frequency_seconds",
            "use_at_least_once_semantics",
            "auto_sharding",
            "num_streams",
            "kms_key",
            "clustering_fields",
            "keep",
            "drop",
            "only",
            "ignore_unknown_values",
            "error_handling",
        ]
    );
    assert!(schema.fields.iter().skip(1).all(|f| f.field_type.nullable));
    let num_streams = schema.field("num_streams").unwrap();
    assert_eq!(
        num_streams.field_type.type_info,
        FieldType::int32().type_info
    );
    assert_eq!(provider.input_tags(), ["input"]);
}

#[test]
fn a_config_row_decodes_to_the_config() {
    let config = BigQueryStorageWriteConfig {
        num_streams: Some(4),
        clustering_fields: Some(vec!["id".to_string()]),
        error_handling: Some(ErrorHandling {
            output: "errors".to_string(),
        }),
        ..table("p.d.t")
    };
    let row = config.to_row().unwrap();
    assert_eq!(BigQueryStorageWriteConfig::from_row(&row).unwrap(), config);
}

#[test]
fn every_option_reaches_the_write() {
    let options = BigQueryStorageWriteConfig {
        create_disposition: some("create_never"),
        write_disposition: some("WRITE_EMPTY"),
        triggering_frequency_seconds: Some(10),
        num_streams: Some(8),
        kms_key: some("projects/p/locations/us/keyRings/r/cryptoKeys/k"),
        clustering_fields: Some(vec!["id".to_string(), "name".to_string()]),
        ..table("p.d.t")
    }
    .write_options(true)
    .unwrap();
    assert_eq!(
        options,
        WriteOptions::new("p.d.t")
            .with_create_disposition(CreateDisposition::CreateNever)
            .with_write_disposition(WriteDisposition::WriteEmpty)
            .with_triggering_frequency(Duration::from_secs(10))
            .with_num_streams(8)
            .with_kms_key("projects/p/locations/us/keyRings/r/cryptoKeys/k")
            .with_clustering_fields(["id", "name"])
    );
}

#[test]
fn the_field_options_reach_the_write() {
    let keep = BigQueryStorageWriteConfig {
        keep: Some(vec!["id".to_string()]),
        ignore_unknown_values: Some(true),
        ..table("p.d.t")
    };
    assert_eq!(
        keep.write_options(false).unwrap(),
        WriteOptions::new("p.d.t")
            .with_keep(["id"])
            .with_ignore_unknown_values(true)
    );
    let drop = BigQueryStorageWriteConfig {
        drop: Some(vec!["extra".to_string()]),
        only: some(""),
        ..table("p.d.t")
    };
    assert_eq!(
        drop.write_options(false).unwrap(),
        WriteOptions::new("p.d.t").with_drop(["extra"])
    );
    let only = BigQueryStorageWriteConfig {
        only: some("payload"),
        keep: Some(Vec::new()),
        ..table("p.d.t")
    };
    assert_eq!(
        only.write_options(false).unwrap(),
        WriteOptions::new("p.d.t").with_only("payload")
    );
}

#[test]
fn a_bounded_input_ignores_the_triggering_frequency() {
    let config = BigQueryStorageWriteConfig {
        triggering_frequency_seconds: Some(10),
        ..table("p.d.t")
    };
    assert_eq!(
        config.write_options(false).unwrap(),
        WriteOptions::new("p.d.t")
    );
}

#[test]
fn at_least_once_semantics_select_the_default_stream() {
    let options = BigQueryStorageWriteConfig {
        use_at_least_once_semantics: Some(true),
        create_disposition: some(""),
        ..table("p.d.t")
    }
    .write_options(true)
    .unwrap();
    assert_eq!(
        options,
        WriteOptions::new("p.d.t").with_semantics(Semantics::AtLeastOnce)
    );
}

#[test]
fn invalid_configs_fail_before_any_call() {
    let cases = [
        (BigQueryStorageWriteConfig::default(), "table"),
        (table("no_dataset"), "Invalid table spec"),
        (
            BigQueryStorageWriteConfig {
                write_disposition: some("WRITE_TRUNCATE"),
                ..table("p.d.t")
            },
            "cannot replace",
        ),
        (
            BigQueryStorageWriteConfig {
                create_disposition: some("sometimes"),
                ..table("p.d.t")
            },
            "sometimes",
        ),
        (
            BigQueryStorageWriteConfig {
                triggering_frequency_seconds: Some(0),
                ..table("p.d.t")
            },
            "triggering_frequency_seconds",
        ),
        (
            BigQueryStorageWriteConfig {
                num_streams: Some(-1),
                ..table("p.d.t")
            },
            "num_streams",
        ),
        (
            BigQueryStorageWriteConfig {
                auto_sharding: Some(true),
                num_streams: Some(3),
                ..table("p.d.t")
            },
            "auto_sharding",
        ),
        (
            BigQueryStorageWriteConfig {
                auto_sharding: Some(false),
                ..table("p.d.t")
            },
            "auto_sharding false",
        ),
        (
            BigQueryStorageWriteConfig {
                keep: Some(vec!["a".to_string()]),
                only: some("b"),
                ..table("p.d.t")
            },
            "at most one of keep, drop and only",
        ),
    ];
    for (config, expected) in cases {
        let error = config.clone().write_options(true).unwrap_err();
        assert!(error.contains(expected), "{config:?}: {error}");
    }
}
