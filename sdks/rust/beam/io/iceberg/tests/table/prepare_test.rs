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

//! `prepare` creates a missing table, loads an existing one, and checks the input schema.

use beam::schema::{BeamRow, Field, FieldType, Schema};
use beam_io_iceberg::table::{TableInfo, prepare};
use beam_io_iceberg::{PartitionField, PartitionTransform};
use iceberg::spec::{TableMetadata, Transform as IcebergTransform};
use iceberg::{Catalog as _, NamespaceIdent};
use tempfile::TempDir;

use crate::common::{device_metric, memory_catalog, options};

fn metadata(info: &TableInfo) -> TableMetadata {
    serde_json::from_slice(&info.metadata).expect("the metadata is table metadata JSON")
}

#[tokio::test(flavor = "multi_thread")]
async fn a_missing_table_is_created_with_its_namespace_spec_and_properties() {
    let warehouse = TempDir::new().expect("a temp dir");
    let catalog = memory_catalog(&warehouse).await;
    let options = options("iceberg_poc_db.device_metrics")
        .with_partition_fields([PartitionField::new(
            "timestamp_ms",
            PartitionTransform::Hour,
        )])
        .with_table_property("write.parquet.compression-codec", "zstd");

    let info = prepare(&catalog, &options, &device_metric())
        .await
        .expect("the table is created");

    assert_eq!(info.table, "iceberg_poc_db.device_metrics");
    assert!(
        catalog
            .namespace_exists(&NamespaceIdent::new("iceberg_poc_db".to_string()))
            .await
            .expect("namespace_exists succeeds")
    );
    let metadata = metadata(&info);
    assert_eq!(
        metadata
            .properties()
            .get("write.parquet.compression-codec")
            .map(String::as_str),
        Some("zstd")
    );
    let spec = metadata.default_partition_spec();
    assert_eq!(spec.fields().len(), 1);
    assert_eq!(spec.fields()[0].name, "timestamp_ms_hour");
    assert_eq!(spec.fields()[0].transform, IcebergTransform::Hour);
    assert!(!info.metadata_location.is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn an_existing_table_is_loaded_and_not_changed() {
    let warehouse = TempDir::new().expect("a temp dir");
    let catalog = memory_catalog(&warehouse).await;
    let first = prepare(&catalog, &options("ns.t"), &device_metric())
        .await
        .expect("the table is created");
    let other_options =
        options("ns.t").with_table_property("write.parquet.compression-codec", "zstd");

    let second = prepare(&catalog, &other_options, &device_metric())
        .await
        .expect("the table is loaded");

    assert_eq!(first.metadata_location, second.metadata_location);
    assert!(
        metadata(&second)
            .properties()
            .get("write.parquet.compression-codec")
            .is_none(),
        "the write sets table properties only at create"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn an_input_that_does_not_match_the_table_fails() {
    let warehouse = TempDir::new().expect("a temp dir");
    let catalog = memory_catalog(&warehouse).await;
    prepare(&catalog, &options("ns.t"), &device_metric())
        .await
        .expect("the table is created");
    let other = Schema::new(vec![Field::new("id", FieldType::string())]);

    let error = prepare(&catalog, &options("ns.t"), &other)
        .await
        .expect_err("the schemas do not match");

    assert!(
        error.to_string().contains("'id' has the type STRING"),
        "{error}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn the_table_info_round_trips_through_its_row() {
    let warehouse = TempDir::new().expect("a temp dir");
    let catalog = memory_catalog(&warehouse).await;
    let info = prepare(&catalog, &options("ns.t"), &device_metric())
        .await
        .expect("the table is created");

    let row = info.to_row().expect("a row");
    assert_eq!(TableInfo::from_row(&row).expect("a table info"), info);
}
