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

//! A table of the memory catalog with committed data files, for the read tests.

use std::collections::HashMap;
use std::sync::Arc;

use beam::schema::{Field, FieldType, FieldValue, Row, RowElement, Schema};
use beam_io_arrow::arrow_array::RecordBatch;
use beam_io_arrow::{FieldSelection, SchemaRowCodec};
use beam_io_iceberg::catalog::file_io;
use beam_io_iceberg::read::plan::plan_tasks;
use beam_io_iceberg::read::reader::read_task;
use beam_io_iceberg::storage::FileSystemStorageFactory;
use beam_io_iceberg::write::commit::{Commit, commit};
use beam_io_iceberg::write::encode::Encoder;
use beam_io_iceberg::write::files::TableWriter;
use beam_io_iceberg::write::table::{TableInfo, prepare};
use beam_io_iceberg::{Catalog, ReadOptions, WriteOptions};
use iceberg::CatalogBuilder;
use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalog, MemoryCatalogBuilder};
use tempfile::TempDir;

pub(crate) const TABLE: &str = "ns.events";

/// `{id: INT64, name: STRING, score: DOUBLE}`.
pub(crate) fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", FieldType::int64()),
        Field::new("name", FieldType::string()),
        Field::new("score", FieldType::double()),
    ]))
}

/// A table of the memory catalog on a temp warehouse.
pub(crate) struct Fixture {
    _warehouse: TempDir,
    pub(crate) catalog: MemoryCatalog,
    info: TableInfo,
}

impl Fixture {
    pub(crate) async fn new() -> Self {
        let warehouse = TempDir::new().expect("a temp dir");
        let catalog = MemoryCatalogBuilder::default()
            .with_storage_factory(Arc::new(FileSystemStorageFactory))
            .load(
                "test",
                HashMap::from([(
                    MEMORY_CATALOG_WAREHOUSE.to_string(),
                    format!("file://{}", warehouse.path().display()),
                )]),
            )
            .await
            .expect("a memory catalog");
        let options = WriteOptions::new(Catalog::rest("http://unused"), TABLE);
        let info = prepare(&catalog, &options, &schema())
            .await
            .expect("the table is created");
        Self {
            _warehouse: warehouse,
            catalog,
            info,
        }
    }

    pub(crate) fn into_client(self) -> (TempDir, Arc<dyn iceberg::Catalog>) {
        (self._warehouse, Arc::new(self.catalog))
    }

    /// Writes the rows with the ids `ids` to one data file, commits it, and returns the id of
    /// the new snapshot.
    pub(crate) async fn append(&self, ids: std::ops::Range<i64>) -> i64 {
        let prefix = format!("test-{}", ids.start);
        let schema = schema();
        let writer = TableWriter::new(&self.info, None).expect("a writer");
        let encoder = Encoder::new(
            Arc::clone(&schema),
            &FieldSelection::All,
            writer.metadata().current_schema(),
        )
        .expect("an encoder");
        let rows: Vec<RowElement> = ids
            .map(|id| {
                let values = vec![
                    Some(FieldValue::Int64(id)),
                    Some(FieldValue::String(format!("name-{id}"))),
                    Some(FieldValue::Double(id as f64)),
                ];
                RowElement::Row(Row::new(Arc::clone(&schema), values).expect("a row"))
            })
            .collect();
        let (_, batch) = encoder
            .encode(&SchemaRowCodec::new(schema), &rows)
            .expect("the rows encode")
            .batch
            .expect("a batch");
        let files = writer
            .write(file_io([]), prefix, [batch])
            .await
            .expect("the file is written");
        let ident = options().table_ident().expect("a table name");
        match commit(&self.catalog, &ident, files)
            .await
            .expect("a commit")
        {
            Commit::New(committed) => committed.snapshot.snapshot_id(),
            Commit::Found(_) => panic!("a new snapshot"),
        }
    }

    /// The batches that `options` read, with `columns` as the column selection.
    pub(crate) async fn read(
        &self,
        options: &ReadOptions,
        columns: &FieldSelection,
    ) -> beam::Result<Vec<RecordBatch>> {
        let mut batches = Vec::new();
        for task in plan_tasks(&self.catalog, options, columns).await? {
            batches.extend(read_task(&file_io([]), &task, 1024).await?);
        }
        Ok(batches)
    }
}

/// Read options for the table of [`Fixture`].
pub(crate) fn options() -> ReadOptions {
    ReadOptions::new(Catalog::rest("http://unused"), TABLE)
}

/// The number of rows of `batches`.
pub(crate) fn row_count(batches: &[RecordBatch]) -> usize {
    batches.iter().map(RecordBatch::num_rows).sum()
}
