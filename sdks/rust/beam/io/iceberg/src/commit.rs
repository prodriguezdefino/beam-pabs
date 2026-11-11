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

//! The last step of a write: it groups the data files of each table into panes, and adds the
//! files of each pane to the table in one snapshot.
//!
//! A pane gets a commit id from its file paths. The snapshot summary holds the id, so a retry
//! of a committed pane finds its snapshot and commits nothing.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use beam::coders::BeamIterable;
use beam::metrics::{HistogramBuckets, Lineage, Metrics};
use beam::schema::BeamRow;
use beam::transforms::{DoFn, GroupByKey, PTransform, ProcessContext, WithKeys};
use beam::values::{AnySideInput, IsBounded, PCollection, SingletonView};
use beam::windowing::{AccumulationMode, GlobalWindows, Trigger, WindowInto};
use iceberg::spec::{DataFile, Snapshot, SnapshotRef, TableMetadata};
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{Catalog, TableIdent};
use sha2::{Digest as _, Sha256};
use uuid::Uuid;

use crate::METRICS;
use crate::catalog::CatalogClient;
use crate::data_file::DataFileMeta;
use crate::options::{Catalog as CatalogOptions, WriteOptions};
use crate::table::TableInfo;

/// The snapshot summary property that holds the commit id of a pane.
pub const COMMIT_ID: &str = "beam.commit-id";

/// The id of the commit of a set of data files, and the commit uuid of its snapshot.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommitId {
    pub id: String,
    pub uuid: Uuid,
}

/// The SHA-256 of the sorted paths of `files`. The same files give the same id in any order.
pub fn commit_id(files: &[DataFile]) -> CommitId {
    let mut paths: Vec<&str> = files.iter().map(DataFile::file_path).collect();
    paths.sort_unstable();
    let digest = paths
        .iter()
        .fold(Sha256::new(), |hasher, path| {
            hasher.chain_update(path.as_bytes()).chain_update(b"\n")
        })
        .finalize();
    let mut uuid = [0; 16];
    uuid.copy_from_slice(&digest[..16]);
    CommitId {
        id: digest.iter().map(|byte| format!("{byte:02x}")).collect(),
        uuid: uuid::Builder::from_custom_bytes(uuid).into_uuid(),
    }
}

/// The snapshot with the commit id `id`, from the current snapshot back through its parents.
/// The walk reads only the table metadata. It passes over the snapshots of other writers.
pub fn find_commit<'a>(metadata: &'a TableMetadata, id: &str) -> Option<&'a SnapshotRef> {
    std::iter::successors(metadata.current_snapshot(), |snapshot| {
        snapshot
            .parent_snapshot_id()
            .and_then(|parent| metadata.snapshot_by_id(parent))
    })
    .find(|snapshot| {
        snapshot
            .summary()
            .additional_properties
            .get(COMMIT_ID)
            .is_some_and(|found| found == id)
    })
}

/// The result of [`commit`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Commit {
    /// This call made the snapshot.
    New(SnapshotRef),
    /// The table had the snapshot already: an earlier attempt made it.
    Found(SnapshotRef),
}

/// Adds `files` to the table `ident` in one snapshot, unless the table has a snapshot with
/// their commit id already.
///
/// The append does not scan the manifests for duplicate files: the commit id makes a retry
/// safe. After a failed commit, the table loads again. If the commit id is there, the commit
/// happened and its reply was lost.
///
/// # Errors
///
/// Fails if the table does not load, or if the commit fails and the table has no snapshot
/// with the commit id.
pub async fn commit(
    catalog: &dyn Catalog,
    ident: &TableIdent,
    files: Vec<DataFile>,
) -> beam::Result<Commit> {
    let CommitId { id, uuid } = commit_id(&files);
    let table = catalog.load_table(ident).await?;
    if let Some(snapshot) = find_commit(table.metadata(), &id) {
        return Ok(Commit::Found(Arc::clone(snapshot)));
    }
    let transaction = Transaction::new(&table);
    let append = transaction
        .fast_append()
        .with_check_duplicate(false)
        .set_commit_uuid(uuid)
        .set_snapshot_properties(HashMap::from([(COMMIT_ID.to_string(), id.clone())]))
        .add_data_files(files);
    let committed = match append.apply(transaction)?.commit(catalog).await {
        Ok(table) => table.metadata_ref(),
        Err(error) => {
            let table = catalog.load_table(ident).await.map_err(|reload| {
                beam::Error::msg(format!(
                    "The commit to '{ident}' failed: {error}. The table did not load after it: \
                     {reload}"
                ))
            })?;
            return find_commit(table.metadata(), &id)
                .map(|snapshot| Commit::Found(Arc::clone(snapshot)))
                .ok_or_else(|| error.into());
        }
    };
    find_commit(&committed, &id)
        .map(|snapshot| Commit::New(Arc::clone(snapshot)))
        .ok_or_else(|| {
            beam::Error::msg(format!(
                "The commit to '{ident}' returned a table with no snapshot of commit id {id}"
            ))
        })
}

/// A snapshot that a write committed. The fields match the snapshot row of the Java SDK.
#[derive(Clone, Debug, PartialEq, Eq, BeamRow)]
pub struct SnapshotInfo {
    #[beam(rename = "sequenceNumber")]
    pub sequence_number: i64,
    #[beam(rename = "snapshotId")]
    pub snapshot_id: i64,
    #[beam(rename = "parentId")]
    pub parent_id: Option<i64>,
    #[beam(rename = "timestampMillis")]
    pub timestamp_millis: i64,
    pub operation: Option<String>,
    pub summary: Option<HashMap<String, String>>,
    #[beam(rename = "manifestListLocation")]
    pub manifest_list_location: Option<String>,
    #[beam(rename = "schemaId")]
    pub schema_id: Option<i32>,
    #[beam(rename = "tableIdentifierString")]
    pub table_identifier_string: Option<String>,
}

impl SnapshotInfo {
    /// The info of `snapshot` of the table `table`.
    pub fn new(table: &str, snapshot: &Snapshot) -> Self {
        let summary = snapshot.summary();
        Self {
            sequence_number: snapshot.sequence_number(),
            snapshot_id: snapshot.snapshot_id(),
            parent_id: snapshot.parent_snapshot_id(),
            timestamp_millis: snapshot.timestamp_ms(),
            operation: Some(summary.operation.as_str().to_string()),
            summary: Some(summary.additional_properties.clone()),
            manifest_list_location: Some(snapshot.manifest_list().to_string()),
            schema_id: snapshot.schema_id(),
            table_identifier_string: Some(table.to_string()),
        }
    }
}

/// The counts of the data files of a commit.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Added {
    pub files: usize,
    pub records: u64,
    pub bytes: u64,
}

impl Added {
    pub fn of(files: &[DataFile]) -> Self {
        Self {
            files: files.len(),
            records: files.iter().map(DataFile::record_count).sum(),
            bytes: files.iter().map(DataFile::file_size_in_bytes).sum(),
        }
    }
}

/// The `commit-report` of the REST catalog spec for `snapshot`, which added `added`.
pub fn commit_report(
    table: &str,
    snapshot: &Snapshot,
    added: Added,
    duration: Duration,
) -> serde_json::Value {
    let Added {
        files,
        records,
        bytes,
    } = added;
    serde_json::json!({
        "report-type": "commit-report",
        "table-name": table,
        "snapshot-id": snapshot.snapshot_id(),
        "sequence-number": snapshot.sequence_number(),
        "operation": snapshot.summary().operation.as_str(),
        "metrics": {
            "added-data-files": { "unit": "count", "value": files },
            "added-records": { "unit": "count", "value": records },
            "added-files-size-bytes": { "unit": "bytes", "value": bytes },
            "total-duration": {
                "time-unit": "nanoseconds",
                "count": 1,
                "total-duration": u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX),
            },
        },
    })
}

/// Groups the data files of each table into panes. An unbounded input fires a pane each
/// `frequency` of processing time. A bounded input fires one pane at its end.
pub struct CommitTrigger {
    frequency: Duration,
}

impl CommitTrigger {
    pub fn new(frequency: Duration) -> Self {
        Self { frequency }
    }
}

impl PTransform<PCollection<DataFileMeta>> for CommitTrigger {
    type Output = PCollection<(String, BeamIterable<DataFileMeta>)>;

    fn expand(self, input: &PCollection<DataFileMeta>) -> Self::Output {
        let trigger = match input.is_bounded() {
            IsBounded::Bounded => Trigger::Default,
            IsBounded::Unbounded => {
                Trigger::repeatedly(Trigger::after_processing_time(self.frequency))
            }
        };
        input
            .apply(
                "KeyByTable",
                WithKeys::new(|meta: &DataFileMeta| meta.table.clone()),
            )
            .apply(
                "CommitWindow",
                WindowInto::new(GlobalWindows)
                    .with_trigger(trigger)
                    .with_accumulation_mode(AccumulationMode::Discarding),
            )
            .apply("GroupByTable", GroupByKey::new())
    }
}

/// Commits the data files of each pane to the table in one snapshot, and emits a
/// [`SnapshotInfo`] for it. A retried pane emits the snapshot of its first commit.
#[derive(Clone)]
pub struct CommitFn {
    table: SingletonView<TableInfo>,
    client: CatalogClient,
    catalog: CatalogOptions,
    ident: TableIdent,
    metadata: Option<Arc<TableMetadata>>,
}

impl CommitFn {
    /// # Errors
    ///
    /// Fails if the table name is not valid.
    pub fn new(
        table: SingletonView<TableInfo>,
        client: CatalogClient,
        options: &WriteOptions,
    ) -> beam::Result<Self> {
        Ok(Self {
            table,
            client,
            catalog: options.catalog.clone(),
            ident: options.table_ident()?,
            metadata: None,
        })
    }

    /// The metadata that the writers used. The data files decode with its partition spec.
    fn metadata(
        &mut self,
        ctx: &ProcessContext<'_, SnapshotInfo>,
    ) -> beam::Result<Arc<TableMetadata>> {
        if let Some(metadata) = &self.metadata {
            return Ok(Arc::clone(metadata));
        }
        let info = ctx.side_input(&self.table)?;
        let metadata: Arc<TableMetadata> = Arc::new(serde_json::from_slice(&info.metadata)?);
        self.metadata = Some(Arc::clone(&metadata));
        Ok(metadata)
    }
}

impl DoFn for CommitFn {
    type In = (String, BeamIterable<DataFileMeta>);
    type Out = SnapshotInfo;

    fn side_inputs(&self) -> Vec<&dyn AnySideInput> {
        vec![&self.table]
    }

    fn process_element(
        &mut self,
        (table, metas): (String, BeamIterable<DataFileMeta>),
        ctx: &mut ProcessContext<'_, SnapshotInfo>,
    ) -> beam::Result {
        let metas = metas.into_vec()?;
        if metas.is_empty() {
            return Ok(());
        }
        let metadata = self.metadata(ctx)?;
        let files = metas
            .iter()
            .map(|meta| meta.data_file(&metadata))
            .collect::<beam::Result<Vec<_>>>()?;
        let (client, ident) = (&self.client, &self.ident);
        let started = Instant::now();
        let added = Added::of(&files);
        let committed = beam::internals::block_on_io(async {
            let committed = commit(client.catalog().await?.as_ref(), ident, files).await?;
            if let Commit::New(snapshot) = &committed {
                let report = commit_report(&table, snapshot, added, started.elapsed());
                client.report_metrics(ident, &report).await;
            }
            Ok::<_, beam::Error>(committed)
        })?
        .map_err(|e| e.context(format!("Cannot commit to the Iceberg table '{table}'")))?;
        Lineage::sinks().add(METRICS, self.catalog.lineage_segments(&self.ident));
        let snapshot = match committed {
            Commit::New(snapshot) => {
                Metrics::counter(METRICS, "snapshots_committed").inc();
                let latency = HistogramBuckets::exponential(0, 20)?;
                Metrics::histogram(METRICS, "commit_latency_ms", latency)
                    .update(started.elapsed().as_secs_f64() * 1000.0);
                snapshot
            }
            Commit::Found(snapshot) => {
                Metrics::counter(METRICS, "commits_found").inc();
                snapshot
            }
        };
        ctx.emit(SnapshotInfo::new(&table, &snapshot))
    }
}
