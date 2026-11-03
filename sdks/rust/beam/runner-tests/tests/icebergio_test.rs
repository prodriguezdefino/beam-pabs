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

//! Native Iceberg writes on PrismRunner, to an `iceberg` memory catalog on a temp directory.
//! Prism runs the workers in this process, so the steps share the catalog.
#![expect(clippy::unwrap_used, reason = "test fixtures panic on setup failure")]

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use beam::Instant;
use beam::io::iceberg::storage::FileSystemStorageFactory;
use beam::io::iceberg::{
    Catalog, PartitionField, SnapshotInfo, Transform, Write, WriteOptions, WriteRows,
};
use beam::pipeline::Pipeline;
use beam::schema::{BeamRow, Field, FieldType, FieldValue, Row, Schema, URN_MILLIS_INSTANT};
use beam::testing::TestStream;
use beam::transforms::Create;
use fluent::prelude::PCollectionExt;
use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
use iceberg::{CatalogBuilder, TableIdent};
use prism::PrismRunner;
use tests::TempDir;

#[derive(Debug, Clone, PartialEq, BeamRow)]
struct Metric {
    id: i64,
    device: String,
    value: f64,
}

fn metrics(ids: std::ops::Range<i64>) -> Vec<Metric> {
    ids.map(|id| Metric {
        id,
        device: format!("device-{}", id % 4),
        value: id as f64 / 2.0,
    })
    .collect()
}

async fn memory_catalog(warehouse: &TempDir) -> Arc<dyn iceberg::Catalog> {
    let catalog = MemoryCatalogBuilder::default()
        .with_storage_factory(Arc::new(FileSystemStorageFactory))
        .load(
            "test",
            HashMap::from([(
                MEMORY_CATALOG_WAREHOUSE.to_string(),
                format!("file://{}", warehouse.as_path().display()),
            )]),
        )
        .await
        .unwrap();
    Arc::new(catalog)
}

/// Options whose REST catalog is not used: the write gets the memory catalog.
fn options(table: &str) -> WriteOptions {
    WriteOptions::new(Catalog::rest("http://unused"), table)
}

/// The snapshots that a pipeline emits.
fn capture(snapshots: &beam::values::PCollection<SnapshotInfo>) -> Arc<Mutex<Vec<SnapshotInfo>>> {
    let captured = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&captured);
    snapshots.inspect("CaptureSnapshots", move |snapshot: &SnapshotInfo| {
        sink.lock().unwrap().push(snapshot.clone());
    });
    captured
}

/// The record count of the table and its number of snapshots.
async fn table_counts(catalog: &Arc<dyn iceberg::Catalog>, table: &str) -> (u64, usize) {
    let ident = TableIdent::from_strs(table.split('.')).unwrap();
    let table = catalog.load_table(&ident).await.unwrap();
    let metadata = table.metadata();
    let records = metadata
        .current_snapshot()
        .and_then(|snapshot| {
            snapshot
                .summary()
                .additional_properties
                .get("total-records")
                .cloned()
        })
        .map_or(0, |records| records.parse().unwrap());
    (records, metadata.snapshots().count())
}

#[tokio::test]
async fn a_bounded_write_appends_all_rows_in_one_snapshot() {
    let warehouse = TempDir::new("iceberg_bounded");
    let catalog = memory_catalog(&warehouse).await;
    let p = Pipeline::new();
    let written = p.apply("Create", Create::new(metrics(0..200))).apply(
        "Write",
        Write::<Metric>::new(
            options("db.metrics")
                .with_partition_fields([PartitionField::new("device", Transform::Identity)]),
        )
        .with_catalog_client(Arc::clone(&catalog)),
    );
    let snapshots = capture(&written.snapshots);

    p.run_with_runner(&PrismRunner::new()).await.unwrap();

    let snapshots = snapshots.lock().unwrap().clone();
    let [snapshot] = snapshots.as_slice() else {
        panic!("one snapshot: {snapshots:?}");
    };
    assert_eq!(snapshot.operation.as_deref(), Some("append"));
    assert_eq!(
        snapshot.table_identifier_string.as_deref(),
        Some("db.metrics")
    );
    assert_eq!(table_counts(&catalog, "db.metrics").await, (200, 1));
}

#[tokio::test]
async fn write_rows_sends_rows_that_do_not_convert_to_the_errors() {
    let warehouse = TempDir::new("iceberg_rows");
    let catalog = memory_catalog(&warehouse).await;
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", FieldType::int64()),
        Field::new(
            "timestamp_ms",
            FieldType::logical(URN_MILLIS_INSTANT, Vec::new(), FieldType::int64()),
        ),
    ]));
    // `i64::MAX` milliseconds do not fit the microseconds of the Iceberg timestamp.
    let rows: Vec<Row> = [(1, 1_000), (2, i64::MAX), (3, 3_000)]
        .into_iter()
        .map(|(id, millis)| {
            Row::new(
                Arc::clone(&schema),
                vec![Some(FieldValue::Int64(id)), Some(FieldValue::Int64(millis))],
            )
            .unwrap()
        })
        .collect();
    let errors = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&errors);
    let p = Pipeline::new();
    let written = p
        .apply("CreateRows", Create::new(rows))
        .with_row_schema(&schema)
        .apply(
            "Write",
            WriteRows::new(options("db.events")).with_catalog_client(Arc::clone(&catalog)),
        );
    written.errors.inspect("CaptureErrors", move |row: &Row| {
        sink.lock().unwrap().push(row.clone());
    });

    p.run_with_runner(&PrismRunner::new()).await.unwrap();

    let errors = errors.lock().unwrap().clone();
    let [error] = errors.as_slice() else {
        panic!("one error row: {errors:?}");
    };
    let Some(FieldValue::Row(failed)) = &error.values()[0] else {
        panic!("the failed row: {error:?}");
    };
    assert_eq!(failed.values()[0], Some(FieldValue::Int64(2)));
    assert_eq!(table_counts(&catalog, "db.events").await, (2, 1));
}

#[tokio::test]
async fn a_streaming_write_commits_every_row_once() {
    let warehouse = TempDir::new("iceberg_streaming");
    let catalog = memory_catalog(&warehouse).await;
    let at = |metric: Metric| {
        let millis = metric.id * 1_000;
        (metric, Instant::from_millis(millis))
    };
    let p = Pipeline::new();
    let written = p
        .apply(
            "TestStream",
            TestStream::new()
                .add_timestamped_elements(metrics(0..50).into_iter().map(at))
                .advance_processing_time(Duration::from_secs(5))
                .add_timestamped_elements(metrics(50..120).into_iter().map(at))
                .advance_processing_time(Duration::from_secs(5))
                .advance_watermark_to_infinity(),
        )
        .apply(
            "Write",
            Write::<Metric>::new(
                options("db.stream")
                    .with_triggering_frequency(Duration::from_secs(1))
                    .with_num_shards(2),
            )
            .with_catalog_client(Arc::clone(&catalog)),
        );
    let snapshots = capture(&written.snapshots);

    p.run_with_runner(&PrismRunner::new()).await.unwrap();

    let snapshots = snapshots.lock().unwrap().clone();
    let (records, count) = table_counts(&catalog, "db.stream").await;
    assert_eq!(records, 120);
    assert_eq!(
        count,
        snapshots.len(),
        "one emitted snapshot for each commit"
    );
    let ids: HashSet<i64> = snapshots.iter().map(|s| s.snapshot_id).collect();
    assert_eq!(ids.len(), snapshots.len(), "{snapshots:?}");
}

#[tokio::test]
async fn a_bounded_write_with_a_triggering_frequency_does_not_run() {
    let p = Pipeline::new();
    let _ = p.apply("Create", Create::new(metrics(0..3))).apply(
        "Write",
        Write::<Metric>::new(
            options("db.metrics").with_triggering_frequency(Duration::from_secs(1)),
        ),
    );

    let error = p.validate().expect_err("the write is not valid");

    assert!(error.to_string().contains("unbounded input"), "{error}");
}
