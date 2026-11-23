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

//! The commit step on the `iceberg` memory catalog.

mod common;

use std::sync::Arc;
use std::time::Duration;

use beam::internals::{MetricsContainer, MetricsScope};
use beam::metrics::{
    MetricResults, RETRY_WAIT_COUNTER_NAME, THROTTLE_TIME_COUNTER_NAME, THROTTLE_TIME_NAMESPACE,
};
use beam_io_iceberg::write::commit::{
    Added, COMMIT_ID, Commit, SnapshotInfo, commit, commit_id, commit_report, find_commit,
};
use iceberg::spec::TableMetadata;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{Catalog as _, TableIdent};
use tempfile::TempDir;

use common::{Fault, Faulty, data_file, memory_catalog, table};

#[test]
fn commit_id_does_not_depend_on_the_order_of_the_files() {
    let (a, b) = (data_file("a.parquet", 1), data_file("b.parquet", 2));
    let id = commit_id(&[a.clone(), b.clone()]);
    assert_eq!(id, commit_id(&[b, a.clone()]));
    assert_eq!(id.id.len(), 64);
    assert_ne!(id, commit_id(&[a]));
}

#[tokio::test(flavor = "multi_thread")]
async fn commit_adds_the_files_in_one_snapshot_with_the_commit_id() {
    let warehouse = TempDir::new().expect("a temp dir");
    let catalog = memory_catalog(&warehouse).await;
    let ident = table(&catalog).await;
    let files = vec![data_file("a.parquet", 10), data_file("b.parquet", 5)];
    let id = commit_id(&files);

    let Commit::New(committed) = commit(&catalog, &ident, files).await.expect("a commit") else {
        panic!("the first commit makes a snapshot");
    };
    assert_eq!(committed.attempts, 1);
    let snapshot = committed.snapshot;

    let summary = &snapshot.summary().additional_properties;
    assert_eq!(summary.get(COMMIT_ID), Some(&id.id));
    assert_eq!(summary.get("added-records").map(String::as_str), Some("15"));
    assert_eq!(
        summary.get("added-data-files").map(String::as_str),
        Some("2")
    );
    let table = catalog.load_table(&ident).await.expect("the table");
    assert_eq!(
        table.metadata().current_snapshot().map(|s| s.snapshot_id()),
        Some(snapshot.snapshot_id())
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_retried_pane_finds_its_snapshot_and_commits_nothing() {
    let warehouse = TempDir::new().expect("a temp dir");
    let catalog = memory_catalog(&warehouse).await;
    let ident = table(&catalog).await;
    let files = || vec![data_file("a.parquet", 10)];

    let first = commit(&catalog, &ident, files()).await.expect("a commit");
    let again = commit(&catalog, &ident, files()).await.expect("a retry");

    let (Commit::New(made), Commit::Found(found)) = (first, again) else {
        panic!("the retry finds the snapshot of the first commit");
    };
    assert_eq!(made.attempts, 1);
    assert_eq!(made.snapshot, found);
    assert_eq!(snapshot_count(&catalog, &ident).await, 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn the_walk_passes_over_the_snapshots_of_other_writers() {
    let warehouse = TempDir::new().expect("a temp dir");
    let catalog = memory_catalog(&warehouse).await;
    let ident = table(&catalog).await;
    let files = || vec![data_file("a.parquet", 10)];

    let Commit::New(ours) = commit(&catalog, &ident, files()).await.expect("a commit") else {
        panic!("the first commit makes a snapshot");
    };
    let loaded = catalog.load_table(&ident).await.expect("the table");
    let transaction = Transaction::new(&loaded);
    let other = transaction
        .fast_append()
        .add_data_files([data_file("other.parquet", 3)])
        .apply(transaction)
        .expect("an append")
        .commit(&catalog)
        .await
        .expect("a commit of another writer");
    let Commit::New(next) = commit(&catalog, &ident, vec![data_file("b.parquet", 1)])
        .await
        .expect("a commit after the other writer")
    else {
        panic!("new files make a snapshot");
    };

    let metadata = catalog.load_table(&ident).await.expect("the table");
    let metadata = metadata.metadata();
    let other_id = other.metadata().current_snapshot().map(|s| s.snapshot_id());
    assert_eq!(next.snapshot.parent_snapshot_id(), other_id);
    let id = commit_id(&files()).id;
    assert_eq!(find_commit(metadata, &id), Some(&ours.snapshot));
    assert!(matches!(
        commit(&catalog, &ident, files()).await.expect("a retry"),
        Commit::Found(found) if found == ours.snapshot
    ));
    assert_eq!(snapshot_count(&catalog, &ident).await, 3);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_failed_reply_after_the_commit_finds_the_snapshot() {
    let warehouse = TempDir::new().expect("a temp dir");
    let catalog = Faulty::new(memory_catalog(&warehouse).await, Fault::LostReply);
    let ident = table(&catalog.inner).await;
    let files = vec![data_file("a.parquet", 10)];
    let id = commit_id(&files).id;

    let result = commit(&catalog, &ident, files)
        .await
        .expect("a found commit");

    let Commit::Found(snapshot) = result else {
        panic!("the commit happened, so the reload finds it");
    };
    assert_eq!(
        snapshot.summary().additional_properties.get(COMMIT_ID),
        Some(&id)
    );
    assert_eq!(snapshot_count(&catalog.inner, &ident).await, 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_failed_commit_with_no_snapshot_returns_the_error() {
    let warehouse = TempDir::new().expect("a temp dir");
    let catalog = Faulty::new(memory_catalog(&warehouse).await, Fault::Reject);
    let ident = table(&catalog.inner).await;

    let error = commit(&catalog, &ident, vec![data_file("a.parquet", 10)])
        .await
        .expect_err("the commit fails");

    assert!(error.to_string().contains("rejected"), "{error}");
    assert_eq!(snapshot_count(&catalog.inner, &ident).await, 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn the_snapshot_info_and_the_report_describe_the_commit() {
    let warehouse = TempDir::new().expect("a temp dir");
    let catalog = memory_catalog(&warehouse).await;
    let ident = table(&catalog).await;
    let files = vec![data_file("a.parquet", 10), data_file("b.parquet", 5)];
    let added = Added::of(&files);

    let Commit::New(committed) = commit(&catalog, &ident, files).await.expect("a commit") else {
        panic!("the first commit makes a snapshot");
    };
    assert_eq!(committed.attempts, 1);
    let snapshot = committed.snapshot;
    let info = SnapshotInfo::new("db.metrics", &snapshot);
    let report = commit_report(
        "db.metrics",
        &snapshot,
        added,
        committed.attempts,
        Duration::from_millis(3),
    );

    assert_eq!(info.snapshot_id, snapshot.snapshot_id());
    assert_eq!(info.parent_id, None);
    assert_eq!(info.operation.as_deref(), Some("append"));
    assert_eq!(info.table_identifier_string.as_deref(), Some("db.metrics"));
    assert_eq!(
        info.summary
            .as_ref()
            .and_then(|summary| summary.get(COMMIT_ID)),
        snapshot.summary().additional_properties.get(COMMIT_ID)
    );
    assert_eq!(report["report-type"], "commit-report");
    assert_eq!(report["table-name"], "db.metrics");
    assert_eq!(report["snapshot-id"], snapshot.snapshot_id());
    assert_eq!(report["sequence-number"], snapshot.sequence_number());
    assert_eq!(report["operation"], "append");
    assert_eq!(
        report["metrics"]["attempts"],
        serde_json::json!({ "unit": "count", "value": 1 })
    );
    assert_eq!(
        report["metrics"]["added-data-files"],
        serde_json::json!({ "unit": "count", "value": 2 })
    );
    assert_eq!(
        report["metrics"]["added-records"],
        serde_json::json!({ "unit": "count", "value": 15 })
    );
    assert_eq!(
        report["metrics"]["added-files-size-bytes"],
        serde_json::json!({ "unit": "bytes", "value": 1500 })
    );
    assert_eq!(
        report["metrics"]["total-duration"],
        serde_json::json!({
            "time-unit": "nanoseconds",
            "count": 1,
            "total-duration": 3_000_000,
        })
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn commit_report_maps_snapshot_summary_totals_and_skips_missing_keys() {
    let warehouse = TempDir::new().expect("a temp dir");
    let catalog = memory_catalog(&warehouse).await;
    let ident = table(&catalog).await;
    let files = vec![data_file("a.parquet", 10), data_file("b.parquet", 5)];
    let added = Added::of(&files);

    commit(&catalog, &ident, files).await.expect("a commit");
    let loaded = catalog.load_table(&ident).await.expect("the table");
    let base_json = serde_json::to_value(loaded.metadata()).expect("metadata json");

    let mut with_totals = base_json.clone();
    with_totals["snapshots"][0]["summary"] = serde_json::json!({
        "operation": "append",
        "total-data-files": "12",
        "total-delete-files": "3",
        "total-records": "120",
        "total-files-size": "98765",
        "total-position-deletes": "7",
        "total-equality-deletes": "4",
    });
    let full_meta: TableMetadata = serde_json::from_value(with_totals).expect("full metadata");
    let full_snapshot = full_meta.current_snapshot().expect("current snapshot");
    let full = commit_report(
        "db.metrics",
        full_snapshot,
        added,
        3,
        Duration::from_millis(5),
    );
    let full_metrics = &full["metrics"];
    assert_eq!(
        full_metrics["attempts"],
        serde_json::json!({ "unit": "count", "value": 3 })
    );
    assert_eq!(
        full_metrics["total-data-files"],
        serde_json::json!({ "unit": "count", "value": 12 })
    );
    assert_eq!(
        full_metrics["total-delete-files"],
        serde_json::json!({ "unit": "count", "value": 3 })
    );
    assert_eq!(
        full_metrics["total-records"],
        serde_json::json!({ "unit": "count", "value": 120 })
    );
    assert_eq!(
        full_metrics["total-files-size-bytes"],
        serde_json::json!({ "unit": "bytes", "value": 98765 })
    );
    assert_eq!(
        full_metrics["total-positional-deletes"],
        serde_json::json!({ "unit": "count", "value": 7 })
    );
    assert_eq!(
        full_metrics["total-equality-deletes"],
        serde_json::json!({ "unit": "count", "value": 4 })
    );

    let mut without_totals = base_json;
    without_totals["snapshots"][0]["summary"] = serde_json::json!({
        "operation": "append",
        "total-records": "not-a-u64",
    });
    let sparse_meta: TableMetadata =
        serde_json::from_value(without_totals).expect("sparse metadata");
    let sparse_snapshot = sparse_meta.current_snapshot().expect("current snapshot");
    let sparse = commit_report(
        "db.metrics",
        sparse_snapshot,
        added,
        1,
        Duration::from_millis(2),
    );
    let sparse_metrics = sparse["metrics"].as_object().expect("metrics object");
    for key in [
        "total-data-files",
        "total-delete-files",
        "total-records",
        "total-files-size-bytes",
        "total-positional-deletes",
        "total-equality-deletes",
    ] {
        assert!(
            !sparse_metrics.contains_key(key),
            "unexpected {key}: {sparse}"
        );
    }
    assert_eq!(sparse_metrics["attempts"]["value"], 1);
    assert_eq!(sparse_metrics["added-data-files"]["value"], 2);
}

async fn snapshot_count(catalog: &impl iceberg::Catalog, ident: &TableIdent) -> usize {
    let table = catalog.load_table(ident).await.expect("the table");
    table.metadata().snapshots().count()
}

#[test]
fn throttled_commit_retries_and_records_throttling_msecs() {
    let warehouse = TempDir::new().expect("a temp dir");
    let container = Arc::new(MetricsContainer::new());
    let (result, calls, snapshots) = {
        let _scope = MetricsScope::enter(Arc::clone(&container), Arc::from("IcebergCommit"));
        beam::internals::block_on_io(async {
            let catalog = Faulty::new(memory_catalog(&warehouse).await, Fault::ThrottledOnce);
            let ident = table(&catalog.inner).await;
            let result = commit(&catalog, &ident, vec![data_file("a.parquet", 10)])
                .await
                .expect("commit succeeds after 429 retry");
            let snapshots = snapshot_count(&catalog.inner, &ident).await;
            (result, catalog.update_calls(), snapshots)
        })
        .expect("the I/O runtime runs")
    };

    assert!(matches!(result, Commit::New(ref committed) if committed.attempts == 2));
    assert_eq!(calls, 2);
    assert_eq!(snapshots, 1);

    let metrics = MetricResults::from_container(&container);
    assert!(
        metrics
            .counter_for_transform(
                "IcebergCommit",
                THROTTLE_TIME_NAMESPACE,
                THROTTLE_TIME_COUNTER_NAME,
            )
            .unwrap_or(0)
            > 0
    );
    assert_eq!(
        metrics.counter_for_transform("IcebergCommit", "iceberg", RETRY_WAIT_COUNTER_NAME),
        None
    );
}

#[test]
fn throttled_commit_after_server_write_does_not_double_commit() {
    let warehouse = TempDir::new().expect("a temp dir");
    let (result, calls, snapshots) = beam::internals::block_on_io(async {
        let catalog = Faulty::new(
            memory_catalog(&warehouse).await,
            Fault::ThrottledAfterCommit,
        );
        let ident = table(&catalog.inner).await;
        let result = commit(&catalog, &ident, vec![data_file("a.parquet", 10)])
            .await
            .expect("reload finds the committed snapshot");
        let snapshots = snapshot_count(&catalog.inner, &ident).await;
        (result, catalog.update_calls(), snapshots)
    })
    .expect("the I/O runtime runs");

    assert!(matches!(result, Commit::Found(_)));
    assert_eq!(calls, 1);
    assert_eq!(snapshots, 1);
}

#[test]
fn commit_conflict_retries_and_records_retry_wait_not_throttling() {
    let warehouse = TempDir::new().expect("a temp dir");
    let container = Arc::new(MetricsContainer::new());
    let (result, calls, snapshots) = {
        let _scope = MetricsScope::enter(Arc::clone(&container), Arc::from("IcebergCommit"));
        beam::internals::block_on_io(async {
            let catalog = Faulty::new(memory_catalog(&warehouse).await, Fault::ConflictOnce);
            let ident = table(&catalog.inner).await;
            let result = commit(&catalog, &ident, vec![data_file("a.parquet", 10)])
                .await
                .expect("commit succeeds after conflict retry");
            let snapshots = snapshot_count(&catalog.inner, &ident).await;
            (result, catalog.update_calls(), snapshots)
        })
        .expect("the I/O runtime runs")
    };

    assert!(matches!(result, Commit::New(ref committed) if committed.attempts == 2));
    assert_eq!(calls, 2);
    assert_eq!(snapshots, 1);

    let metrics = MetricResults::from_container(&container);
    assert_eq!(
        metrics.counter_for_transform(
            "IcebergCommit",
            THROTTLE_TIME_NAMESPACE,
            THROTTLE_TIME_COUNTER_NAME,
        ),
        None
    );
    assert!(
        metrics
            .counter_for_transform("IcebergCommit", "iceberg", RETRY_WAIT_COUNTER_NAME)
            .unwrap_or(0)
            > 0
    );
}
