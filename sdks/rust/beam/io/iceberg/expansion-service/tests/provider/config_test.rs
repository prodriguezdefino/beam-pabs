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

//! Configuration validation and conversion tests for [`IcebergWriteConfig`].

use std::collections::BTreeMap;
use std::time::Duration;

use beam::io::iceberg::{Catalog, PartitionField, PartitionTransform, WriteOptions};
use beam::schema::{BeamRow, FieldType};
use beam_expansion::{SchemaTransformProvider, SchemaTransformRegistration};
use beam_io_iceberg_expansion::{
    ErrorHandling, IcebergWriteConfig, IcebergWriteProvider, URN_ICEBERG_WRITE,
};

use crate::common::{CATALOG_URI, base_config, catalog_props};

#[test]
fn the_provider_is_registered() {
    let found = beam_expansion::inventory::iter::<SchemaTransformRegistration>
        .into_iter()
        .any(|reg| (reg.provider)().identifier() == URN_ICEBERG_WRITE);
    assert!(found);
}

#[test]
fn the_config_has_the_java_fields_and_rust_extensions() {
    let provider = IcebergWriteProvider;
    let schema = provider.config_schema();
    let names: Vec<&str> = schema.fields.iter().map(|f| f.name.as_str()).collect();
    assert_eq!(
        names,
        [
            "table",
            "catalog_name",
            "catalog_properties",
            "config_properties",
            "triggering_frequency_seconds",
            "direct_write_byte_limit",
            "mode",
            "keep",
            "drop",
            "only",
            "partition_fields",
            "table_properties",
            "num_shards",
            "target_file_bytes",
            "error_handling",
        ]
    );
    assert!(!schema.fields[0].field_type.nullable);
    assert!(schema.fields.iter().skip(1).all(|f| f.field_type.nullable));
    assert_eq!(
        schema
            .field("triggering_frequency_seconds")
            .unwrap()
            .field_type
            .type_info,
        FieldType::int32().type_info
    );
    assert_eq!(
        schema.field("num_shards").unwrap().field_type.type_info,
        FieldType::int32().type_info
    );
    assert_eq!(
        schema
            .field("target_file_bytes")
            .unwrap()
            .field_type
            .type_info,
        FieldType::int64().type_info
    );
    assert_eq!(provider.input_tags(), ["input"]);
    assert_eq!(provider.output_tags(), ["snapshots", "errors"]);
}

#[test]
fn a_config_row_round_trips() {
    let config = IcebergWriteConfig {
        catalog_name: Some("biglake".to_string()),
        partition_fields: Some(vec!["hour(timestamp_ms)".to_string()]),
        table_properties: Some(catalog_props(&[(
            "write.parquet.compression-codec",
            "zstd",
        )])),
        triggering_frequency_seconds: Some(30),
        num_shards: Some(4),
        target_file_bytes: Some(64 * 1024 * 1024),
        error_handling: Some(ErrorHandling {
            output: "errors".to_string(),
        }),
        ..base_config("db.metrics")
    };
    let row = config.to_row().unwrap();
    assert_eq!(IcebergWriteConfig::from_row(&row).unwrap(), config);
}

#[test]
fn biglake_catalog_config_maps_to_the_expected_write_options() {
    let options = IcebergWriteConfig {
        table: "iceberg_bench.ingest".to_string(),
        catalog_name: Some("biglake".to_string()),
        catalog_properties: Some(catalog_props(&[
            ("type", "REST"),
            (
                "uri",
                "https://biglake.googleapis.com/iceberg/v1/restcatalog",
            ),
            ("warehouse", "bl://projects/p/catalogs/c"),
            ("gcp_location", "us-central1"),
            ("header.x-goog-user-project", "p"),
            ("rest.auth.type", "google"),
            ("io-impl", "org.apache.iceberg.gcp.gcs.GCSFileIO"),
            ("header.X-Iceberg-Access-Delegation", "vended-credentials"),
            ("rest-metrics-reporting-enabled", "true"),
        ])),
        table_properties: Some(catalog_props(&[
            ("gcp.biglake.table-management.enabled", "true"),
            ("write.parquet.compression-codec", "zstd"),
        ])),
        partition_fields: Some(vec!["hour(timestamp_ms)".to_string()]),
        triggering_frequency_seconds: Some(60),
        mode: Some("APPEND".to_string()),
        num_shards: Some(8),
        target_file_bytes: Some(128 * 1024 * 1024),
        ..Default::default()
    }
    .write_options(true)
    .unwrap();

    let expected = WriteOptions::new(
        Catalog::rest("https://biglake.googleapis.com/iceberg/v1/restcatalog")
            .with_warehouse("bl://projects/p/catalogs/c")
            .with_google_auth()
            .with_vended_credentials()
            .with_metrics_reporting()
            .with_header("x-goog-user-project", "p")
            .with_property("gcp_location", "us-central1"),
        "iceberg_bench.ingest",
    )
    .with_partition_fields([PartitionField::new(
        "timestamp_ms",
        PartitionTransform::Hour,
    )])
    .with_table_property("gcp.biglake.table-management.enabled", "true")
    .with_table_property("write.parquet.compression-codec", "zstd")
    .with_triggering_frequency(Duration::from_secs(60))
    .with_num_shards(8)
    .with_target_file_bytes(128 * 1024 * 1024);

    assert_eq!(options, expected);
}

#[test]
fn auth_modes_map_to_catalog_auth() {
    let convert = |pairs: &[(&str, &str)]| {
        let mut all = vec![("uri", CATALOG_URI)];
        all.extend_from_slice(pairs);
        IcebergWriteConfig {
            catalog_properties: Some(catalog_props(&all)),
            ..base_config("db.t")
        }
        .write_options(false)
        .unwrap()
    };

    assert_eq!(
        convert(&[]),
        WriteOptions::new(Catalog::rest(CATALOG_URI), "db.t")
    );
    assert_eq!(
        convert(&[("rest.auth.type", "none"), ("token", "ignored")]),
        WriteOptions::new(Catalog::rest(CATALOG_URI), "db.t")
    );
    assert_eq!(
        convert(&[("token", "my-token")]),
        WriteOptions::new(Catalog::rest(CATALOG_URI).with_token("my-token"), "db.t")
    );
    assert_eq!(
        convert(&[("rest.auth.type", "OAUTH2"), ("token", "oauth-token")]),
        WriteOptions::new(Catalog::rest(CATALOG_URI).with_token("oauth-token"), "db.t")
    );
    assert_eq!(
        convert(&[(
            "rest.auth.type",
            "org.apache.iceberg.gcp.auth.GoogleAuthManager"
        )]),
        WriteOptions::new(Catalog::rest(CATALOG_URI).with_google_auth(), "db.t")
    );
}

#[test]
fn table_defaults_apply_only_when_table_properties_do_not_override_them() {
    let options = IcebergWriteConfig {
        catalog_properties: Some(catalog_props(&[
            ("uri", CATALOG_URI),
            ("table-default.write.parquet.compression-codec", "gzip"),
            ("table-default.write.object-storage.enabled", "true"),
            ("header.x-iceberg-access-delegation", " VENDED-CREDENTIALS "),
            ("header.X-Custom", "v1"),
            ("rest-metrics-reporting-enabled", "false"),
            ("prefix", "cat1"),
        ])),
        table_properties: Some(catalog_props(&[(
            "write.parquet.compression-codec",
            "zstd",
        )])),
        config_properties: Some(BTreeMap::new()),
        keep: Some(Vec::new()),
        drop: Some(Vec::new()),
        only: Some(String::new()),
        mode: Some(String::new()),
        ..base_config("db.t")
    }
    .write_options(false)
    .unwrap();

    let expected = WriteOptions::new(
        Catalog::rest(CATALOG_URI)
            .with_vended_credentials()
            .with_header("X-Custom", "v1")
            .with_property("prefix", "cat1"),
        "db.t",
    )
    .with_table_property("write.parquet.compression-codec", "zstd")
    .with_table_property("write.object-storage.enabled", "true");

    assert_eq!(options, expected);
}

#[test]
fn bounded_inputs_validate_and_ignore_triggering_frequency() {
    let valid = IcebergWriteConfig {
        triggering_frequency_seconds: Some(15),
        ..base_config("db.t")
    };
    assert_eq!(
        valid.clone().write_options(false).unwrap(),
        WriteOptions::new(Catalog::rest(CATALOG_URI), "db.t")
    );
    assert_eq!(
        valid.write_options(true).unwrap(),
        WriteOptions::new(Catalog::rest(CATALOG_URI), "db.t")
            .with_triggering_frequency(Duration::from_secs(15))
    );

    for bad in [0, -5] {
        let err = IcebergWriteConfig {
            triggering_frequency_seconds: Some(bad),
            ..base_config("db.t")
        }
        .write_options(false)
        .unwrap_err();
        assert!(err.contains("triggering_frequency_seconds"), "{err}");
    }
}

#[test]
fn invalid_configs_fail_with_clear_errors() {
    let cases = [
        (IcebergWriteConfig::default(), "table"),
        (base_config("   "), "table"),
        (base_config("missing_namespace"), "namespace.name"),
        (
            IcebergWriteConfig {
                table: "db.t".to_string(),
                catalog_properties: None,
                ..Default::default()
            },
            "uri",
        ),
        (
            IcebergWriteConfig {
                catalog_properties: Some(catalog_props(&[("uri", "  ")])),
                ..base_config("db.t")
            },
            "uri",
        ),
        (
            IcebergWriteConfig {
                catalog_properties: Some(catalog_props(&[
                    ("type", "hadoop"),
                    ("uri", CATALOG_URI),
                ])),
                ..base_config("db.t")
            },
            "hadoop",
        ),
        (
            IcebergWriteConfig {
                catalog_properties: Some(catalog_props(&[
                    ("uri", CATALOG_URI),
                    ("rest.auth.type", "oauth2"),
                ])),
                ..base_config("db.t")
            },
            "token",
        ),
        (
            IcebergWriteConfig {
                catalog_properties: Some(catalog_props(&[
                    ("uri", CATALOG_URI),
                    ("rest.auth.type", "sigv4"),
                ])),
                ..base_config("db.t")
            },
            "sigv4",
        ),
        (
            IcebergWriteConfig {
                catalog_properties: Some(catalog_props(&[
                    ("uri", CATALOG_URI),
                    ("rest-metrics-reporting-enabled", "sometimes"),
                ])),
                ..base_config("db.t")
            },
            "rest-metrics-reporting-enabled",
        ),
        (
            IcebergWriteConfig {
                catalog_properties: Some(catalog_props(&[
                    ("uri", CATALOG_URI),
                    ("header.X-Iceberg-Access-Delegation", "remote-signing"),
                ])),
                ..base_config("db.t")
            },
            "remote-signing",
        ),
        (
            IcebergWriteConfig {
                catalog_properties: Some(catalog_props(&[
                    ("uri", CATALOG_URI),
                    (
                        "header.X-Iceberg-Access-Delegation",
                        "vended-credentials, remote-signing",
                    ),
                ])),
                ..base_config("db.t")
            },
            "remote-signing",
        ),
        (
            IcebergWriteConfig {
                catalog_properties: Some(catalog_props(&[("uri", CATALOG_URI), ("header.", "x")])),
                ..base_config("db.t")
            },
            "header.",
        ),
        (
            IcebergWriteConfig {
                catalog_properties: Some(catalog_props(&[
                    ("uri", CATALOG_URI),
                    ("table-default.", "x"),
                ])),
                ..base_config("db.t")
            },
            "table-default.",
        ),
        (
            IcebergWriteConfig {
                config_properties: Some(catalog_props(&[("fs.defaultFS", "hdfs://x")])),
                ..base_config("db.t")
            },
            "config_properties",
        ),
        (
            IcebergWriteConfig {
                direct_write_byte_limit: Some(1024),
                ..base_config("db.t")
            },
            "direct_write_byte_limit",
        ),
        (
            IcebergWriteConfig {
                mode: Some("merge-on-read".to_string()),
                ..base_config("db.t")
            },
            "merge-on-read",
        ),
        (
            IcebergWriteConfig {
                keep: Some(vec!["id".to_string()]),
                ..base_config("db.t")
            },
            "keep",
        ),
        (
            IcebergWriteConfig {
                drop: Some(vec!["extra".to_string()]),
                ..base_config("db.t")
            },
            "drop",
        ),
        (
            IcebergWriteConfig {
                only: Some("payload".to_string()),
                ..base_config("db.t")
            },
            "only",
        ),
        (
            IcebergWriteConfig {
                num_shards: Some(0),
                ..base_config("db.t")
            },
            "num_shards",
        ),
        (
            IcebergWriteConfig {
                num_shards: Some(-2),
                ..base_config("db.t")
            },
            "num_shards",
        ),
        (
            IcebergWriteConfig {
                target_file_bytes: Some(0),
                ..base_config("db.t")
            },
            "target_file_bytes",
        ),
        (
            IcebergWriteConfig {
                target_file_bytes: Some(-1),
                ..base_config("db.t")
            },
            "target_file_bytes",
        ),
    ];

    for (config, expected) in cases {
        let error = config.clone().write_options(true).unwrap_err();
        assert!(error.contains(expected), "{config:?}: {error}");
    }
}
