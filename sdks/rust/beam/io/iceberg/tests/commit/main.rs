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

use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{Catalog as _, TableIdent};
use iceberg_io::commit::{
    Added, COMMIT_ID, Commit, SnapshotInfo, commit, commit_id, commit_report, find_commit,
};
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

    let Commit::New(snapshot) = commit(&catalog, &ident, files).await.expect("a commit") else {
        panic!("the first commit makes a snapshot");
    };

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
    assert_eq!(made, found);
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
    assert_eq!(next.parent_snapshot_id(), other_id);
    let id = commit_id(&files()).id;
    assert_eq!(find_commit(metadata, &id), Some(&ours));
    assert!(matches!(
        commit(&catalog, &ident, files()).await.expect("a retry"),
        Commit::Found(found) if found == ours
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

    let Commit::New(snapshot) = commit(&catalog, &ident, files).await.expect("a commit") else {
        panic!("the first commit makes a snapshot");
    };
    let info = SnapshotInfo::new("db.metrics", &snapshot);
    let report = commit_report(
        "db.metrics",
        &snapshot,
        added,
        std::time::Duration::from_millis(3),
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
    assert_eq!(report["snapshot-id"], snapshot.snapshot_id());
    assert_eq!(report["metrics"]["added-data-files"]["value"], 2);
    assert_eq!(report["metrics"]["added-records"]["value"], 15);
    assert_eq!(report["metrics"]["added-files-size-bytes"]["value"], 1500);
    assert_eq!(
        report["metrics"]["total-duration"]["total-duration"],
        3_000_000
    );
}

async fn snapshot_count(catalog: &impl iceberg::Catalog, ident: &TableIdent) -> usize {
    let table = catalog.load_table(ident).await.expect("the table");
    table.metadata().snapshots().count()
}
