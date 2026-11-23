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

//! The snapshot and the columns of a read, and the planning step that lists its data files.

use std::sync::Arc;

use beam::metrics::{Lineage, Metrics};
use beam::schema::Schema;
use beam::transforms::{DoFn, ProcessContext};
use beam_io_arrow::FieldSelection;
use futures::TryStreamExt;
use iceberg::Catalog;
use iceberg::expr::Bind;
use iceberg::spec::{Schema as TableSchema, SnapshotRef, TableMetadata};

use super::options::{ReadOptions, Snapshot};
use super::task::ScanTask;
use crate::METRICS;
use crate::arrow::table_arrow_schema;
use crate::catalog::CatalogClient;

/// The snapshot of `metadata` that `snapshot` names, or `None` for a table with no snapshot.
fn resolve_snapshot(
    metadata: &TableMetadata,
    snapshot: Snapshot,
) -> beam::Result<Option<&SnapshotRef>> {
    match snapshot {
        Snapshot::Current => Ok(metadata.current_snapshot()),
        Snapshot::Id(id) => metadata
            .snapshot_by_id(id)
            .map(Some)
            .ok_or_else(|| beam::Error::msg(format!("The table has no snapshot with id {id}"))),
        Snapshot::AsOf(millis) => metadata
            .snapshots()
            .filter(|snapshot| snapshot.timestamp_ms() <= millis)
            .max_by_key(|snapshot| (snapshot.timestamp_ms(), snapshot.sequence_number()))
            .map(Some)
            .ok_or_else(|| {
                beam::Error::msg(format!(
                    "The table has no snapshot at or before {millis} ms"
                ))
            }),
    }
}

/// The schema of `snapshot`, or the current schema of the table.
fn snapshot_schema<'a>(
    metadata: &'a TableMetadata,
    snapshot: Option<&SnapshotRef>,
) -> &'a Arc<TableSchema> {
    snapshot
        .and_then(|snapshot| snapshot.schema_id())
        .and_then(|id| metadata.schema_by_id(id))
        .unwrap_or_else(|| metadata.current_schema())
}

/// The schema of the `columns` of `table`, in table order.
fn read_schema(table: &TableSchema, columns: &FieldSelection) -> beam::Result<Schema> {
    if let FieldSelection::Only(_) = columns {
        return Err(beam::Error::msg("only applies to a write, not to a read"));
    }
    let schema = beam_io_arrow::arrow_to_beam_schema(&*table_arrow_schema(table)?)?;
    let (_, read) = columns.projection(&schema)?;
    if read.fields.is_empty() {
        return Err(beam::Error::msg(
            "The column selection leaves no columns to read",
        ));
    }
    Ok(read)
}

/// The Beam schema of the rows that `options` read, with `columns` as the column selection.
///
/// # Errors
///
/// Fails if the table does not load, the snapshot does not exist, a column is not in the
/// table, or the filter does not bind to the table schema.
pub async fn row_schema(
    catalog: &dyn Catalog,
    options: &ReadOptions,
    columns: &FieldSelection,
) -> beam::Result<Schema> {
    let table = catalog.load_table(&options.table_ident()?).await?;
    let snapshot = resolve_snapshot(table.metadata(), options.snapshot)?;
    let schema = snapshot_schema(table.metadata(), snapshot);
    if let Some(filter) = &options.filter {
        filter.bind(Arc::clone(schema), true)?;
    }
    read_schema(schema, columns)
}

/// The encoded scan tasks of the data files that `options` read, with `columns` as the
/// column selection. A table with no snapshot has no tasks.
///
/// # Errors
///
/// Fails as [`row_schema`] fails, or if the manifests do not read.
pub async fn plan_tasks(
    catalog: &dyn Catalog,
    options: &ReadOptions,
    columns: &FieldSelection,
) -> beam::Result<Vec<Vec<u8>>> {
    let table = catalog.load_table(&options.table_ident()?).await?;
    let metadata = table.metadata();
    let Some(snapshot) = resolve_snapshot(metadata, options.snapshot)? else {
        return Ok(Vec::new());
    };
    let names = read_schema(snapshot_schema(metadata, Some(snapshot)), columns)?
        .fields
        .into_iter()
        .map(|field| field.name);
    let scan = table
        .scan()
        .snapshot_id(snapshot.snapshot_id())
        .with_batch_size(Some(options.batch_size))
        .select(names);
    let scan = match &options.filter {
        Some(filter) => scan.with_filter(filter.clone()),
        None => scan,
    };
    let tasks: Vec<_> = scan.build()?.plan_files().await?.try_collect().await?;
    tasks
        .iter()
        .map(|task| {
            Ok(serde_json::to_vec(&ScanTask::new(
                metadata.location(),
                task,
            )?)?)
        })
        .collect()
}

/// Lists the data files of the read after the impulse, and emits one encoded scan task for
/// each file.
#[derive(Clone)]
pub(super) struct PlanTasksFn {
    client: CatalogClient,
    options: ReadOptions,
    columns: FieldSelection,
}

impl PlanTasksFn {
    pub(super) fn new(
        client: CatalogClient,
        options: ReadOptions,
        columns: FieldSelection,
    ) -> Self {
        Self {
            client,
            options,
            columns,
        }
    }
}

impl DoFn for PlanTasksFn {
    type In = Vec<u8>;
    type Out = Vec<u8>;

    fn process_element(
        &mut self,
        _impulse: Vec<u8>,
        ctx: &mut ProcessContext<'_, Vec<u8>>,
    ) -> beam::Result {
        let (client, options, columns) = (&self.client, &self.options, &self.columns);
        let ident = options.table_ident()?;
        Lineage::sources().add(METRICS, options.catalog.lineage_segments(&ident));
        let tasks = beam::internals::block_on_io(async {
            plan_tasks(client.catalog().await?.as_ref(), options, columns).await
        })?
        .map_err(|e| e.context(format!("Cannot plan the read of Iceberg table '{ident}'")))?;
        Metrics::counter(METRICS, "scan_tasks_planned")
            .inc_by(i64::try_from(tasks.len()).unwrap_or(i64::MAX));
        tasks.into_iter().try_for_each(|task| ctx.emit(task))
    }
}
