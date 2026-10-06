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

//! Identifier lookup, config rows/strings and payloads.

use std::collections::BTreeMap;
use std::sync::Arc;

use beam::prelude::*;
use beam::schema::{Field, FieldType, FieldValue};
use external::ExpansionError;
use managed_io::{
    self as managed, GCP_EXPANSION_SERVICE, IO_EXPANSION_SERVICE, ManagedRead, ManagedWrite,
    default_expansion_service, read_urn, row_to_config, urns, write_urn,
};
use serde_json::{Value, json};

use crate::common::config_json;

#[test]
fn test_identifier_lookup_is_case_insensitive() {
    assert_eq!(read_urn("iceberg"), Some(urns::ICEBERG_READ));
    assert_eq!(read_urn("ICEBERG"), Some(urns::ICEBERG_READ));
    assert_eq!(read_urn("Delta"), Some(urns::DELTA_LAKE_READ));
    assert_eq!(write_urn("SqlServer"), Some(urns::SQL_SERVER_WRITE));
    assert_eq!(write_urn(managed::DELTA), None, "Delta Lake is read-only");
    assert_eq!(write_urn(managed::ICEBERG_CDC), None);
    assert_eq!(read_urn("nope"), None);
}

#[test]
fn test_unsupported_names_list_valid_choices() {
    let err = ManagedRead::new("ManagedRead", "nope")
        .build_config_row()
        .unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("unsupported source"), "{msg}");
    assert!(msg.contains("iceberg") && msg.contains("delta"), "{msg}");
    assert!(!msg.contains("iceberg_cdc"), "CDC stays hidden: {msg}");

    let msg = ManagedWrite::new("ManagedWrite", "delta")
        .build()
        .unwrap_err()
        .to_string();
    assert!(msg.contains("unsupported sink"), "{msg}");
    assert!(!msg.contains("\"delta\""), "{msg}");
}

#[test]
fn test_default_expansion_services_match_python() {
    for urn in [
        urns::ICEBERG_READ,
        urns::ICEBERG_WRITE,
        urns::ICEBERG_CDC_READ,
        urns::KAFKA_READ,
        urns::KAFKA_WRITE,
        urns::DELTA_LAKE_READ,
    ] {
        assert_eq!(
            default_expansion_service(urn),
            Some(IO_EXPANSION_SERVICE),
            "{urn}"
        );
    }
    for urn in [
        urns::BIGQUERY_READ,
        urns::BIGQUERY_WRITE,
        urns::POSTGRES_READ,
        urns::POSTGRES_WRITE,
        urns::MYSQL_READ,
        urns::MYSQL_WRITE,
        urns::SQL_SERVER_READ,
        urns::SQL_SERVER_WRITE,
    ] {
        assert_eq!(
            default_expansion_service(urn),
            Some(GCP_EXPANSION_SERVICE),
            "{urn}"
        );
    }
    assert_eq!(default_expansion_service("beam:schematransform:x:v1"), None);

    let read = ManagedRead::new("Managed Read(BIGQUERY)", managed::BIGQUERY);
    assert_eq!(read.expansion_service().unwrap(), GCP_EXPANSION_SERVICE);
    let source = read.build().unwrap();
    assert_eq!(source.transform().endpoint, GCP_EXPANSION_SERVICE);
    assert_eq!(source.main_output_tag(), "output");
    let read = read.with_expansion_service("localhost:8097");
    assert_eq!(read.expansion_service().unwrap(), "localhost:8097");
    assert!(
        ManagedRead::new("ManagedRead", "beam:schematransform:x:v1")
            .expansion_service()
            .is_err()
    );
}

#[test]
fn test_config_row_schema_and_values() {
    #[derive(serde::Serialize)]
    struct Catalog {
        table: String,
        catalog_name: String,
    }

    let read = ManagedRead::new("Managed Read(ICEBERG)", managed::ICEBERG)
        .with_config(Catalog {
            table: "db.events".into(),
            catalog_name: "local".into(),
        })
        .with_config_entry(
            "catalog_properties",
            BTreeMap::from([("type", "hadoop"), ("warehouse", "file:///tmp/w")]),
        )
        .with_config_entry("keep", ["a", "b"]);
    assert_eq!(read.name(), "Managed Read(ICEBERG)");
    assert_eq!(read.transform_identifier(), Some(urns::ICEBERG_READ));

    let row = read.build_config_row().unwrap();
    let names: Vec<_> = row
        .schema()
        .fields
        .iter()
        .map(|f| f.name.as_str())
        .collect();
    assert_eq!(names, ["config", "config_url", "transform_identifier"]);
    assert_eq!(
        row.get_string("transform_identifier").unwrap(),
        Some(urns::ICEBERG_READ)
    );
    assert_eq!(row.get_string("config_url").unwrap(), None);
    assert_eq!(
        config_json(&row),
        json!({
            "table": "db.events",
            "catalog_name": "local",
            "catalog_properties": {"type": "hadoop", "warehouse": "file:///tmp/w"},
            "keep": ["a", "b"],
        })
    );

    // The row must survive the portable row coder the payload uses.
    let bytes = row.to_row_bytes().unwrap();
    assert_eq!(Row::from_row_bytes(row.schema(), &bytes).unwrap(), row);
}

#[test]
fn test_config_is_yaml_with_every_string_quoted() {
    // YAML 1.1 (Java's SnakeYAML) would coerce these if emitted bare.
    let tricky = [
        "off",
        "yes",
        "0123",
        "1e3",
        "null",
        "~",
        "a: b",
        "- x",
        "{\"type\":\"object\"}",
    ];
    let mut write = ManagedWrite::new("Managed Write(KAFKA)", managed::KAFKA);
    for (i, value) in tricky.iter().enumerate() {
        write = write.with_config_entry(format!("k{i}"), *value);
    }
    let row = write.build_config_row().unwrap();
    let config = row.get_string("config").unwrap().unwrap();

    let parsed: Value = serde_saphyr::from_str(config).unwrap();
    for (i, value) in tricky.iter().enumerate() {
        assert_eq!(parsed[format!("k{i}")], json!(value), "k{i} in {config}");
    }
}

#[test]
fn test_config_sources_are_exclusive() {
    let url = ManagedWrite::new("Managed Write(KAFKA)", managed::KAFKA)
        .with_config_url("gs://b/kafka.yaml")
        .build_config_row()
        .unwrap();
    assert_eq!(url.get_string("config").unwrap(), None);
    assert_eq!(
        url.get_string("config_url").unwrap(),
        Some("gs://b/kafka.yaml")
    );

    let yaml = ManagedWrite::new("Managed Write(KAFKA)", managed::KAFKA)
        .with_yaml_config("topic: t\n")
        .build_config_row()
        .unwrap();
    assert_eq!(yaml.get_string("config").unwrap(), Some("topic: t\n"));

    // Java requires a config or a URL, so no config at all is sent as an empty mapping.
    let empty = ManagedRead::new("Managed Read(DELTA)", managed::DELTA)
        .build_config_row()
        .unwrap();
    assert_eq!(empty.get_string("config").unwrap(), Some("{}"));

    let both = |msg: &str| Err(ExpansionError::InvalidResponse(msg.to_string()));
    const CONFIG_AND_URL: &str = "Please specify a config or a config URL, but not both";
    assert_eq!(
        ManagedWrite::new("Managed Write(KAFKA)", managed::KAFKA)
            .with_config_entry("topic", "t")
            .with_config_url("gs://b/kafka.yaml")
            .build_config_row(),
        both(CONFIG_AND_URL)
    );
    assert_eq!(
        ManagedWrite::new("Managed Write(KAFKA)", managed::KAFKA)
            .with_yaml_config("topic: t")
            .with_config_url("gs://b/kafka.yaml")
            .build_config_row(),
        both(CONFIG_AND_URL)
    );
    assert_eq!(
        ManagedWrite::new("Managed Write(KAFKA)", managed::KAFKA)
            .with_yaml_config("topic: t")
            .with_config_entry("topic", "t")
            .build_config_row(),
        both(
            "Managed transform takes either a YAML config string or structured config \
             entries, not both"
        )
    );
}

#[test]
fn test_non_map_config_is_rejected() {
    let err = ManagedRead::new("Managed Read(KAFKA)", managed::KAFKA)
        .with_config(["not", "a", "map"])
        .build_config_row()
        .unwrap_err();
    assert_eq!(
        err,
        ExpansionError::InvalidResponse(
            r#"Managed configuration must serialize to a map, got: ["not","a","map"]"#.into()
        )
    );
}

/// BYTES config values are sent as standard padded base64 (RFC 4648 section 10 vectors).
#[test]
fn base64_matches_rfc4648_vectors() {
    for (input, expected) in [
        ("", ""),
        ("f", "Zg=="),
        ("fo", "Zm8="),
        ("foo", "Zm9v"),
        ("foob", "Zm9vYg=="),
        ("fooba", "Zm9vYmE="),
        ("foobar", "Zm9vYmFy"),
    ] {
        let row = Row::new(
            Arc::new(Schema::new(vec![Field::new("blob", FieldType::bytes())])),
            vec![Some(FieldValue::Bytes(input.as_bytes().to_vec()))],
        )
        .unwrap();
        assert_eq!(
            Value::Object(row_to_config(&row).unwrap()),
            json!({"blob": expected}),
            "{input:?}"
        );
    }
}

#[test]
fn test_row_to_config_omits_nulls_and_converts_types() {
    let nested = Schema::new(vec![Field::new("output", FieldType::string())]);
    let schema = Arc::new(Schema::new(vec![
        Field::nullable("absent", FieldType::string()),
        Field::new("count", FieldType::int32()),
        Field::new("flag", FieldType::boolean()),
        Field::new("fields", FieldType::array(FieldType::string())),
        Field::new(
            "props",
            FieldType::map(FieldType::string(), FieldType::string()),
        ),
        Field::new("blob", FieldType::bytes()),
        Field::new("error_handling", FieldType::row(nested.clone())),
    ]));
    let row = Row::builder(schema)
        .with_named("absent", None::<FieldValue>)
        .with_named("count", Some(7))
        .with_named("flag", Some(true))
        .with_named(
            "fields",
            Some(FieldValue::Array(vec![Some(FieldValue::String(
                "a".into(),
            ))])),
        )
        .with_named(
            "props",
            Some(FieldValue::Map(vec![(
                FieldValue::String("k".into()),
                Some(FieldValue::String("1".into())),
            )])),
        )
        .with_named("blob", Some(FieldValue::Bytes(b"foo".to_vec())))
        .with_named(
            "error_handling",
            Some(FieldValue::Row(
                Row::builder(Arc::new(nested))
                    .with_named("output", Some("errors"))
                    .build()
                    .unwrap(),
            )),
        )
        .build()
        .unwrap();

    assert_eq!(
        Value::Object(row_to_config(&row).unwrap()),
        json!({
            "count": 7,
            "flag": true,
            "fields": ["a"],
            "props": {"k": "1"},
            "blob": "Zm9v",
            "error_handling": {"output": "errors"},
        })
    );
}
