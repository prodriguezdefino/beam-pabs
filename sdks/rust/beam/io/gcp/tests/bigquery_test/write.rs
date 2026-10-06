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

//! Tests for `BigQueryWrite` configuration rows, write methods, and error handling.

use external::{ExpansionError, URN_EXPANSION_SCHEMA_TRANSFORM};
use gcp::bigquery::{
    BigQueryWrite, CreateDisposition, URN_BIGQUERY_FILELOADS, URN_BIGQUERY_STORAGE_WRITE,
    URN_BIGQUERY_WRITE, WriteDisposition, WriteMethod,
};

use crate::common::{decode_payload, describe, error_handling, fixture, values};
use crate::fixtures::{BIGQUERY_WRITE_CONFIG, WriteExpect, managed_target};

#[test]
fn test_bigquery_write_config_row() {
    let row = BigQueryWrite::new("BigQueryWrite", "my-project:analytics.events")
        .with_create_disposition(CreateDisposition::CreateNever)
        .with_write_disposition(WriteDisposition::WriteTruncate)
        .with_method(WriteMethod::StorageWriteApi)
        .with_triggering_frequency_seconds(15)
        .with_auto_sharding(true)
        .with_num_streams(5)
        .with_kms_key("kms-key-id")
        .build_config_row()
        .expect("build write config row");

    assert_eq!(describe(row.schema()), fixture(BIGQUERY_WRITE_CONFIG));
    assert_eq!(
        values(&row),
        WriteExpect {
            auto_sharding: Some(true),
            create_disposition: Some("CREATE_NEVER"),
            kms_key: Some("kms-key-id"),
            num_streams: Some(5),
            table: "my-project:analytics.events",
            triggering_frequency_seconds: Some(15),
            write_disposition: Some("WRITE_TRUNCATE"),
            ..Default::default()
        }
        .values()
    );
}

#[test]
fn test_bigquery_write_disposition_strings() {
    let row = |c, w| {
        BigQueryWrite::new("BigQueryWrite", "p:d.t")
            .with_create_disposition(c)
            .with_write_disposition(w)
            .build_config_row()
            .unwrap()
    };
    let r = row(
        CreateDisposition::CreateIfNeeded,
        WriteDisposition::WriteEmpty,
    );
    assert_eq!(
        r.get_string("create_disposition").unwrap(),
        Some("CREATE_IF_NEEDED")
    );
    assert_eq!(
        r.get_string("write_disposition").unwrap(),
        Some("WRITE_EMPTY")
    );
    let r = row(
        CreateDisposition::CreateNever,
        WriteDisposition::WriteAppend,
    );
    assert_eq!(
        r.get_string("create_disposition").unwrap(),
        Some("CREATE_NEVER")
    );
    assert_eq!(
        r.get_string("write_disposition").unwrap(),
        Some("WRITE_APPEND")
    );
}

/// `use_at_least_once_semantics` is what distinguishes `StorageApiAtLeastOnce` from
/// `Auto` (both expand `bigquery_write:v1`).
#[test]
fn test_bigquery_write_at_least_once_semantics() {
    let config = |w: BigQueryWrite| managed_target(&w.build().unwrap().transform().payload);
    let prefix = r#"{"create_disposition":"CREATE_IF_NEEDED","table":"p:d.t","#;

    let (urn, auto) = config(BigQueryWrite::new("BigQueryWrite", "p:d.t"));
    assert_eq!(urn, URN_BIGQUERY_WRITE);
    assert_eq!(
        auto,
        format!(r#"{prefix}"write_disposition":"WRITE_APPEND"}}"#)
    );

    let (urn, alo) = config(
        BigQueryWrite::new("BigQueryWrite", "p:d.t")
            .with_method(WriteMethod::StorageApiAtLeastOnce),
    );
    assert_eq!(urn, URN_BIGQUERY_WRITE);
    assert_eq!(
        alo,
        format!(
            r#"{prefix}"use_at_least_once_semantics":true,"write_disposition":"WRITE_APPEND"}}"#
        )
    );

    // The method wins over an explicit `false`.
    let row = BigQueryWrite::new("BigQueryWrite", "p:d.t")
        .with_method(WriteMethod::StorageApiAtLeastOnce)
        .with_use_at_least_once_semantics(false)
        .build_config_row()
        .unwrap();
    assert_eq!(
        row.get_bool("use_at_least_once_semantics").unwrap(),
        Some(true)
    );

    // Other methods pass the explicit setting through unchanged.
    let row = BigQueryWrite::new("BigQueryWrite", "p:d.t")
        .with_method(WriteMethod::StorageWriteApi)
        .with_use_at_least_once_semantics(true)
        .with_auto_sharding(false)
        .build_config_row()
        .unwrap();
    assert_eq!(
        row.get_bool("use_at_least_once_semantics").unwrap(),
        Some(true)
    );
    assert_eq!(row.get_bool("auto_sharding").unwrap(), Some(false));
}

#[test]
fn test_bigquery_write_requires_table() {
    let err = BigQueryWrite::new("BigQueryWrite", "")
        .build()
        .expect_err("no table");
    assert_eq!(
        err,
        ExpansionError::InvalidResponse("BigQueryWrite requires destination table".into())
    );
}

#[test]
fn test_bigquery_write_error_handling_is_declared_through_managed() {
    let write = BigQueryWrite::new("BigQueryWrite", "p:d.t").with_error_handling("failed");
    assert_eq!(write.error_output(), Some("failed"));

    let row = write.build_config_row().unwrap();
    assert_eq!(
        row.get_value("error_handling"),
        Some(&error_handling("failed"))
    );

    let managed = write
        .to_managed()
        .unwrap()
        .expect("Auto goes through Managed");
    assert_eq!(managed.declared_outputs(), ["failed"]);
    let sink = write.build().unwrap();
    assert!(sink.transform().output_tags.contains("failed"));
    let (_, config) = managed_target(&sink.transform().payload);
    assert!(
        config.contains(r#""error_handling":{"output":"failed"}"#),
        "{config}"
    );
}

/// The direct transforms cannot surface extra outputs, so error handling there would
/// silently drop failed rows; it is refused instead.
#[test]
fn test_bigquery_write_error_handling_rejects_direct_methods() {
    for (method, name) in [
        (WriteMethod::StorageWriteApi, "StorageWriteApi"),
        (WriteMethod::FileLoads, "FileLoads"),
    ] {
        let err = BigQueryWrite::new("BigQueryWrite", "p:d.t")
            .with_method(method)
            .with_error_handling("failed")
            .build()
            .expect_err("direct methods cannot surface error outputs");
        assert_eq!(
            err,
            ExpansionError::InvalidResponse(format!(
                "BigQueryWrite error handling ('failed') needs WriteMethod::Auto or \
                 WriteMethod::StorageApiAtLeastOnce, not {name}"
            ))
        );
    }
}

#[test]
fn test_bigquery_write_urn_dispatch() {
    let direct = |method| {
        let write = BigQueryWrite::new("BigQueryWrite", "p:d.t").with_method(method);
        assert!(write.to_managed().unwrap().is_none(), "{method:?}");
        let sink = write.build().unwrap();
        assert_eq!(sink.transform().urn, URN_EXPANSION_SCHEMA_TRANSFORM);
        let (identifier, config) = decode_payload(&sink.transform().payload);
        // Direct expansions send the full typed configuration row that the provider decodes.
        assert_eq!(describe(config.schema()), fixture(BIGQUERY_WRITE_CONFIG));
        identifier
    };
    assert_eq!(
        direct(WriteMethod::StorageWriteApi),
        URN_BIGQUERY_STORAGE_WRITE
    );
    assert_eq!(direct(WriteMethod::FileLoads), URN_BIGQUERY_FILELOADS);

    for method in [WriteMethod::Auto, WriteMethod::StorageApiAtLeastOnce] {
        let sink = BigQueryWrite::new("BigQueryWrite", "p:d.t")
            .with_method(method)
            .build()
            .unwrap();
        assert_eq!(
            managed_target(&sink.transform().payload).0,
            URN_BIGQUERY_WRITE,
            "{method:?}"
        );
    }
}
