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

use std::sync::Arc;

use beam::io::managed::{self, ManagedWrite};
use beam::prelude::*;
use beam::schema::{Field, FieldType, FieldValue};
use iceberg_wordcount::{
    Args, build_pipeline, catalog_properties, count_row, count_schema, describe_snapshot,
    extract_words, format_count,
};

/// A row shaped like the Java Iceberg `snapshots` output, with only the fields read here.
fn snapshot_row(summary: Option<Vec<(&str, &str)>>) -> Row {
    let schema = Arc::new(Schema::new(vec![
        Field::new("table", FieldType::string()),
        Field::nullable("operation", FieldType::string()),
        Field::new("snapshot_id", FieldType::int64()),
        Field::nullable(
            "summary",
            FieldType::map(FieldType::string(), FieldType::string()),
        ),
    ]));
    let summary = summary.map(|entries| {
        FieldValue::Map(
            entries
                .into_iter()
                .map(|(k, v)| {
                    (
                        FieldValue::String(k.into()),
                        Some(FieldValue::String(v.into())),
                    )
                })
                .collect(),
        )
    });
    Row::builder(schema)
        .with_named("table", Some("rust_sdk.wordcount"))
        .with_named("operation", Some("append"))
        .with_named("snapshot_id", Some(42i64))
        .with_named("summary", summary)
        .build()
        .expect("row matches the snapshot schema")
}

#[test]
fn test_describe_snapshot() {
    let row = snapshot_row(Some(vec![
        ("added-records", "4555"),
        ("total-records", "9110"),
    ]));
    assert_eq!(
        describe_snapshot(&row).as_deref(),
        Some("rust_sdk.wordcount: append snapshot 42 (+4555 records)")
    );
    assert_eq!(
        describe_snapshot(&snapshot_row(None)).as_deref(),
        Some("rust_sdk.wordcount: append snapshot 42")
    );
}

/// Workers rebuild the pipeline without an expansion service. The write must still
/// expose its `snapshots` output there, or building the graph would fail.
#[test]
fn test_write_pipeline_builds_on_a_worker() {
    let (mut options, args) =
        beam::options::parse_from::<Args, _, _>(["iceberg_wordcount", "--warehouse=gs://b/w"]);
    options.harness.worker = true;

    let p = build_pipeline(&options, &args).expect("worker-side build needs no expansion service");
    let lock = p.lock();
    let write = lock
        .components
        .transforms
        .values()
        .find(|t| t.unique_name == "Managed Write(ICEBERG)")
        .expect("Iceberg write in the graph");
    assert!(write.outputs.contains_key(managed::SNAPSHOTS));
}

#[test]
fn test_extract_words() {
    let words: Vec<_> = extract_words("  King Lear, act 1: 'tis!  ").collect();
    assert_eq!(words, ["King", "Lear", "act", "tis"]);
}

#[test]
fn test_count_row_round_trips_through_format() {
    let row = count_row("lear", 42);
    assert_eq!(row.schema(), &count_schema());
    assert_eq!(format_count(&row).as_deref(), Some("lear: 42"));
}

#[test]
fn test_catalog_properties_from_flags() {
    let (_, args) = beam::options::parse_from::<Args, _, _>([
        "iceberg_wordcount",
        "--warehouse=file:///tmp/warehouse",
    ]);
    let props = catalog_properties(&args);
    assert_eq!(props.len(), 2);
    assert_eq!(props["type"], "hadoop");
    assert_eq!(props["warehouse"], "file:///tmp/warehouse");

    let (_, args) = beam::options::parse_from::<Args, _, _>([
        "iceberg_wordcount",
        "--warehouse=s3://bucket/warehouse",
        "--catalog_type=rest",
        "--catalog_property=uri=https://catalog.example.com/iceberg",
        "--catalog_property=rest.auth.type=com.example.AuthManager",
        "--catalog_property=header.X-Iceberg-Access-Delegation=vended-credentials",
        "--catalog_property=io-impl=com.example.FileIO",
    ]);
    let props = catalog_properties(&args);
    assert_eq!(props["type"], "rest");
    assert_eq!(props["warehouse"], "s3://bucket/warehouse");
    assert_eq!(props["uri"], "https://catalog.example.com/iceberg");
    assert_eq!(props["rest.auth.type"], "com.example.AuthManager");
    assert_eq!(
        props["header.X-Iceberg-Access-Delegation"],
        "vended-credentials"
    );
    assert_eq!(props["io-impl"], "com.example.FileIO");

    let (_, args) = beam::options::parse_from::<Args, _, _>([
        "iceberg_wordcount",
        "--warehouse=s3://bucket/warehouse",
        "--catalog_property=type=rest",
        "--catalog_property=warehouse=s3://bucket/other",
    ]);
    let props = catalog_properties(&args);
    assert_eq!(props["type"], "rest");
    assert_eq!(props["warehouse"], "s3://bucket/other");

    let (_, args) = beam::options::parse_from::<Args, _, _>([
        "iceberg_wordcount",
        "--warehouse=s3://bucket/warehouse",
        "--catalog_property=uri=https://catalog.example.com/?a=b",
    ]);
    assert_eq!(
        catalog_properties(&args)["uri"],
        "https://catalog.example.com/?a=b"
    );
}

#[test]
fn test_managed_config_matches_python_shape() {
    let (_, args) =
        beam::options::parse_from::<Args, _, _>(["iceberg_wordcount", "--warehouse=gs://b/w"]);
    let write = ManagedWrite::new("Managed Write(ICEBERG)", managed::ICEBERG)
        .with_config_entry("table", &args.table)
        .with_config_entry("catalog_name", &args.catalog_name)
        .with_config_entry("catalog_properties", catalog_properties(&args));
    let row = write.build_config_row().unwrap();
    assert_eq!(
        row.get_string("transform_identifier").unwrap(),
        Some(managed::urns::ICEBERG_WRITE)
    );
    let config = row.get_string("config").unwrap().unwrap();
    assert!(
        config.contains(r#""table":"rust_sdk.wordcount""#),
        "{config}"
    );
    assert!(config.contains(r#""catalog_name":"rust_sdk""#), "{config}");
    assert!(
        config.contains(r#""catalog_properties":{"type":"hadoop","warehouse":"gs://b/w"}"#),
        "{config}"
    );
}
