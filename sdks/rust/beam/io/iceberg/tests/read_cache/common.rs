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

//! Shared fixtures for the Iceberg read cache tests: a counting file system and a multi-row-group
//! memory-catalog table helper.

use std::collections::HashMap;
use std::fs;
use std::io;
use std::path::Path;
use std::sync::{Arc, LazyLock, Mutex};

use beam::schema::{Field, FieldType, FieldValue, Row, RowElement, Schema};
use beam_io_arrow::arrow_array::RecordBatch;
use beam_io_arrow::arrow_array::cast::AsArray;
use beam_io_arrow::arrow_array::types::Int64Type;
use beam_io_arrow::{FieldSelection, SchemaRowCodec};
use beam_io_file::{FileSystem, LocalFileSystem};
use beam_io_iceberg::catalog::file_io;
use beam_io_iceberg::storage::FileSystemStorageFactory;
use beam_io_iceberg::write::commit::{Commit, commit};
use beam_io_iceberg::write::encode::Encoder;
use beam_io_iceberg::write::files::TableWriter;
use beam_io_iceberg::write::table::{TableInfo, prepare};
use beam_io_iceberg::{Catalog, ReadOptions, WriteOptions};
use iceberg::CatalogBuilder;
use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalog, MemoryCatalogBuilder};
use iceberg::spec::DataFile;
use tempfile::TempDir;

pub(crate) const TABLE: &str = "ns.events";
pub(crate) const COUNTING_SCHEME: &str = "countfs";

/// A local file system that records each ranged read per URI.
#[derive(Debug, Default)]
pub(crate) struct CountingFileSystem {
    reads: Mutex<HashMap<String, Vec<(u64, u64)>>>,
}

static COUNTING: LazyLock<Arc<CountingFileSystem>> = LazyLock::new(|| {
    let fs = Arc::new(CountingFileSystem::default());
    beam_io_file::register_filesystem(COUNTING_SCHEME, Arc::clone(&fs) as _).unwrap();
    fs
});

impl CountingFileSystem {
    /// Returns the registered counting file system.
    pub(crate) fn get() -> Arc<Self> {
        Arc::clone(&COUNTING)
    }

    /// Returns the `countfs://` URI of `path`, registering the scheme on first use.
    pub(crate) fn uri(path: &Path) -> String {
        LazyLock::force(&COUNTING);
        format!("{COUNTING_SCHEME}://{}", path.display())
    }

    /// Strips the `countfs://` scheme prefix from `uri`.
    pub(crate) fn local(uri: &str) -> &str {
        uri.strip_prefix("countfs://").unwrap_or(uri)
    }

    /// Returns every recorded `(start, length)` ranged read of `uri`.
    pub(crate) fn reads(&self, uri: &str) -> Vec<(u64, u64)> {
        self.reads
            .lock()
            .unwrap()
            .get(uri)
            .cloned()
            .unwrap_or_default()
    }
}

impl FileSystem for CountingFileSystem {
    fn match_files(&self, pattern: &str) -> io::Result<Vec<String>> {
        LocalFileSystem::new().match_files(Self::local(pattern))
    }

    fn open_read(&self, path: &str) -> io::Result<Box<dyn io::Read + Send>> {
        LocalFileSystem::new().open_read(Self::local(path))
    }

    fn open_read_range(
        &self,
        path: &str,
        start: u64,
        length: u64,
    ) -> io::Result<Box<dyn io::Read + Send>> {
        self.reads
            .lock()
            .unwrap()
            .entry(path.to_string())
            .or_default()
            .push((start, length));
        LocalFileSystem::new().open_read_range(Self::local(path), start, length)
    }

    fn open_write(&self, path: &str) -> io::Result<Box<dyn io::Write + Send>> {
        LocalFileSystem::new().open_write(Self::local(path))
    }

    fn open_append(&self, path: &str) -> io::Result<Box<dyn io::Write + Send>> {
        LocalFileSystem::new().open_append(Self::local(path))
    }

    fn size(&self, path: &str) -> io::Result<u64> {
        LocalFileSystem::new().size(Self::local(path))
    }

    fn remove(&self, path: &str) -> io::Result<()> {
        LocalFileSystem::new().remove(Self::local(path))
    }

    fn rename(&self, old_path: &str, new_path: &str) -> io::Result<()> {
        LocalFileSystem::new().rename(Self::local(old_path), Self::local(new_path))
    }

    fn copy(&self, from: &str, to: &str) -> io::Result<()> {
        LocalFileSystem::new().copy(Self::local(from), Self::local(to))
    }

    fn last_modified(&self, path: &str) -> io::Result<std::time::SystemTime> {
        LocalFileSystem::new().last_modified(Self::local(path))
    }
}

/// `{id: INT64, name: STRING, score: DOUBLE}`.
pub(crate) fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", FieldType::int64()),
        Field::new("name", FieldType::string()),
        Field::new("score", FieldType::double()),
    ]))
}

/// A table of the memory catalog backed by [`CountingFileSystem`].
pub(crate) struct Fixture {
    _warehouse: TempDir,
    pub(crate) catalog: MemoryCatalog,
    info: TableInfo,
}

impl Fixture {
    pub(crate) async fn new(row_group_bytes: usize) -> Self {
        let warehouse = TempDir::new().expect("a temp dir");
        let catalog = MemoryCatalogBuilder::default()
            .with_storage_factory(Arc::new(FileSystemStorageFactory))
            .load(
                "test",
                HashMap::from([(
                    MEMORY_CATALOG_WAREHOUSE.to_string(),
                    CountingFileSystem::uri(warehouse.path()),
                )]),
            )
            .await
            .expect("a memory catalog");
        let options = WriteOptions::new(Catalog::rest("http://unused"), TABLE)
            .with_table_property("write.parquet.compression-codec", "uncompressed")
            .with_table_property(
                "write.parquet.row-group-size-bytes",
                row_group_bytes.to_string(),
            );
        let info = prepare(&catalog, &options, &schema())
            .await
            .expect("the table is created");
        Self {
            _warehouse: warehouse,
            catalog,
            info,
        }
    }

    /// Writes `batch_ranges` into one Parquet data file and commits it to the catalog.
    pub(crate) async fn append_batches(
        &self,
        batch_ranges: impl IntoIterator<Item = std::ops::Range<i64>>,
        padding: usize,
    ) -> Vec<DataFile> {
        let schema = schema();
        let writer = TableWriter::new(&self.info, None).expect("a writer");
        let encoder = Encoder::new(
            Arc::clone(&schema),
            &FieldSelection::All,
            writer.metadata().current_schema(),
        )
        .expect("an encoder");
        let codec = SchemaRowCodec::new(Arc::clone(&schema));
        let pad = "a".repeat(padding);
        let batches: Vec<_> = batch_ranges
            .into_iter()
            .map(|ids| {
                let rows: Vec<RowElement> = ids
                    .map(|id| {
                        let values = vec![
                            Some(FieldValue::Int64(id)),
                            Some(FieldValue::String(format!("row-{id:08}-{pad}"))),
                            Some(FieldValue::Double(id as f64)),
                        ];
                        RowElement::Row(Row::new(Arc::clone(&schema), values).expect("a row"))
                    })
                    .collect();
                let (_, batch) = encoder
                    .encode(&codec, &rows)
                    .expect("the rows encode")
                    .batch
                    .expect("a batch");
                batch
            })
            .collect();
        let files = writer
            .write(file_io([]), "multi-rg".to_string(), batches)
            .await
            .expect("the file is written");
        let ident = options().table_ident().expect("a table name");
        match commit(&self.catalog, &ident, files.clone())
            .await
            .expect("a commit")
        {
            Commit::New(_) => files,
            Commit::Found(_) => panic!("expected a new snapshot"),
        }
    }
}

/// Read options for the table of [`Fixture`].
pub(crate) fn options() -> ReadOptions {
    ReadOptions::new(Catalog::rest("http://unused"), TABLE)
}

/// Counts ranged reads that touch the Parquet footer or 8-byte trailer of `local_path`.
pub(crate) fn footer_reads(local_path: &str, reads: &[(u64, u64)]) -> usize {
    let file = fs::read(local_path).unwrap();
    let tail: [u8; 4] = file[file.len() - 8..file.len() - 4].try_into().unwrap();
    let footer_start = (file.len() - 8 - u32::from_le_bytes(tail) as usize) as u64;
    reads
        .iter()
        .filter(|(start, len)| start.saturating_add(*len) > footer_start)
        .count()
}

/// Extracts all `id` values from column 0 of `batch`.
pub(crate) fn batch_ids(batch: &RecordBatch) -> Vec<i64> {
    batch
        .column(0)
        .as_primitive::<Int64Type>()
        .values()
        .iter()
        .copied()
        .collect()
}
