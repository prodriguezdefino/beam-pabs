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

//! The step after the shuffle: it writes each batch of a shard to Parquet data files and emits
//! one [`DataFileMeta`] for each file.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use beam::coders::ShardedKey;
use beam::metrics::{Lineage, Metrics};
use beam::transforms::{DoFn, ProcessContext};
use beam::values::{AnySideInput, SingletonView};
use beam_io_arrow::EncodedBatch;
use beam_io_arrow::arrow_array::RecordBatch;
use iceberg::TableIdent;
use iceberg::arrow::RecordBatchPartitionSplitter;
use iceberg::io::FileIO;
use iceberg::spec::{DataFile, DataFileFormat, TableMetadata};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::DefaultFileNameGenerator;
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::partitioning::PartitioningWriter;
use iceberg::writer::partitioning::fanout_writer::FanoutWriter;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use parquet::file::properties::WriterProperties;

use crate::METRICS;
use crate::arrow::{decode_batch, schema_bytes, table_arrow_schema};
use crate::catalog::{TOKEN_EXPIRES_AT, connect, file_io};
use crate::data_file::DataFileMeta;
use crate::location::TableLocations;
use crate::options::{Catalog, WriteOptions};
use crate::parquet::{target_file_size, writer_properties};
use crate::table::TableInfo;

/// A copy gets new vended credentials when its token expires in less than this.
const REFRESH_MARGIN: Duration = Duration::from_secs(5 * 60);

/// Writes batches of one table version to data files. The parts come from the table
/// metadata once, so each batch only decodes and writes.
pub struct TableWriter {
    table: String,
    metadata: TableMetadata,
    schema_bytes: Vec<u8>,
    splitter: Option<RecordBatchPartitionSplitter>,
    properties: WriterProperties,
    target_file_size: usize,
    locations: TableLocations,
}

impl TableWriter {
    /// A writer for the table of `info`. It rolls a file at `target_file_bytes`, else at the
    /// target size of the table properties.
    ///
    /// # Errors
    ///
    /// Fails if the metadata does not parse, or if a writer property is not valid.
    pub fn new(info: &TableInfo, target_file_bytes: Option<usize>) -> beam::Result<Self> {
        let metadata: TableMetadata = serde_json::from_slice(&info.metadata)?;
        let schema = Arc::clone(metadata.current_schema());
        let spec = Arc::clone(metadata.default_partition_spec());
        let splitter = if spec.is_unpartitioned() {
            None
        } else {
            Some(RecordBatchPartitionSplitter::try_new_with_computed_values(
                Arc::clone(&schema),
                spec,
            )?)
        };
        Ok(Self {
            table: info.table.clone(),
            schema_bytes: schema_bytes(table_arrow_schema(&schema)?.as_ref())?,
            splitter,
            properties: writer_properties(metadata.properties())?,
            target_file_size: match target_file_bytes {
                Some(bytes) => bytes,
                None => target_file_size(metadata.properties())?,
            },
            locations: TableLocations::new(&metadata),
            metadata,
        })
    }

    pub fn metadata(&self) -> &TableMetadata {
        &self.metadata
    }

    /// The record batch of `batch`, an IPC message of the Arrow schema of the table.
    ///
    /// # Errors
    ///
    /// Fails if the bytes do not decode.
    pub fn decode(&self, batch: EncodedBatch) -> beam::Result<RecordBatch> {
        decode_batch(&self.schema_bytes, batch.bytes)
    }

    /// Decodes and writes `batches` one at a time to new data files whose names start with
    /// `prefix`, then closes them. The files of a partitioned table split by partition value.
    ///
    /// # Errors
    ///
    /// Fails if a batch does not decode, or if a file cannot be written or closed.
    pub async fn write(
        &self,
        file_io: FileIO,
        prefix: String,
        batches: impl IntoIterator<Item = EncodedBatch>,
    ) -> beam::Result<Vec<DataFile>> {
        let parquet = ParquetWriterBuilder::new(
            self.properties.clone(),
            Arc::clone(self.metadata.current_schema()),
        );
        let rolling = RollingFileWriterBuilder::new(
            parquet,
            self.target_file_size,
            file_io,
            self.locations.clone(),
            DefaultFileNameGenerator::new(prefix, None, DataFileFormat::Parquet),
        );
        let builder = DataFileWriterBuilder::new(rolling);
        match &self.splitter {
            Some(splitter) => {
                let mut writer = FanoutWriter::new(builder);
                for encoded in batches {
                    let batch = self.decode(encoded)?;
                    for (key, part) in splitter.split(&batch)? {
                        writer.write(key, part).await?;
                    }
                }
                Ok(writer.close().await?)
            }
            None => {
                let mut writer = builder.build(None).await?;
                for encoded in batches {
                    writer.write(self.decode(encoded)?).await?;
                }
                Ok(writer.close().await?)
            }
        }
    }

    /// One meta for each file.
    ///
    /// # Errors
    ///
    /// Fails if a file does not encode.
    pub fn metas(&self, files: Vec<DataFile>) -> beam::Result<Vec<DataFileMeta>> {
        files
            .into_iter()
            .map(|file| DataFileMeta::new(&self.table, file, &self.metadata))
            .collect()
    }
}

/// The file IO of a copy, and when its vended token needs a refresh.
#[derive(Clone)]
struct Storage {
    file_io: FileIO,
    refresh_after: Option<SystemTime>,
}

/// Writes each batch of a shard to Parquet data files, and emits one [`DataFileMeta`] for each
/// file. A retried batch writes new file names, so the files of a failed attempt are orphans
/// that no snapshot lists.
#[derive(Clone)]
pub struct WriteDataFilesFn {
    table: SingletonView<TableInfo>,
    catalog: Catalog,
    ident: TableIdent,
    /// The first part of the names of the files of this write.
    write_id: String,
    target_file_bytes: Option<usize>,
    writer: Option<Arc<TableWriter>>,
    storage: Option<Storage>,
}

impl WriteDataFilesFn {
    /// # Errors
    ///
    /// Fails if the table name is not valid.
    pub fn new(table: SingletonView<TableInfo>, options: &WriteOptions) -> beam::Result<Self> {
        Ok(Self {
            table,
            catalog: options.catalog.clone(),
            ident: options.table_ident()?,
            write_id: uuid::Uuid::new_v4().to_string(),
            target_file_bytes: options.target_file_bytes,
            writer: None,
            storage: None,
        })
    }

    fn writer(&mut self, ctx: &ProcessContext<'_, DataFileMeta>) -> beam::Result<Arc<TableWriter>> {
        if let Some(writer) = &self.writer {
            return Ok(Arc::clone(writer));
        }
        let info = ctx.side_input(&self.table)?;
        let writer = Arc::new(TableWriter::new(&info, self.target_file_bytes)?);
        Lineage::sinks().add(METRICS, self.catalog.lineage_segments(&self.ident));
        self.writer = Some(Arc::clone(&writer));
        Ok(writer)
    }

    /// The file IO, with vended credentials that expire in more than [`REFRESH_MARGIN`].
    fn file_io(&mut self, location: &str) -> beam::Result<FileIO> {
        let fresh = |storage: &Storage| {
            storage
                .refresh_after
                .is_none_or(|refresh_after| SystemTime::now() < refresh_after)
        };
        if let Some(storage) = self.storage.as_ref().filter(|storage| fresh(storage)) {
            return Ok(storage.file_io.clone());
        }
        let storage = if self.catalog.vended_credentials {
            let (catalog, ident) = (&self.catalog, &self.ident);
            let config = beam::internals::block_on_io(async {
                connect(catalog).await?.credentials(ident, location).await
            })??;
            let refresh_after = config
                .get(TOKEN_EXPIRES_AT)
                .and_then(|millis| millis.parse::<u64>().ok())
                .map(|millis| UNIX_EPOCH + Duration::from_millis(millis) - REFRESH_MARGIN);
            Storage {
                file_io: file_io(config),
                refresh_after,
            }
        } else {
            Storage {
                file_io: file_io([]),
                refresh_after: None,
            }
        };
        let file_io = storage.file_io.clone();
        self.storage = Some(storage);
        Ok(file_io)
    }
}

impl DoFn for WriteDataFilesFn {
    type In = (ShardedKey<String>, Vec<EncodedBatch>);
    type Out = DataFileMeta;

    fn side_inputs(&self) -> Vec<&dyn AnySideInput> {
        vec![&self.table]
    }

    fn process_element(
        &mut self,
        (_shard, batches): (ShardedKey<String>, Vec<EncodedBatch>),
        ctx: &mut ProcessContext<'_, DataFileMeta>,
    ) -> beam::Result {
        let writer = self.writer(ctx)?;
        let file_io = self.file_io(writer.metadata().location())?;
        let prefix = format!("{}-{}", self.write_id, uuid::Uuid::new_v4());
        let files = beam::internals::block_on_io(writer.write(file_io, prefix, batches))??;
        let (bytes, rows) = (
            Metrics::distribution(METRICS, "data_file_bytes"),
            Metrics::distribution(METRICS, "data_file_rows"),
        );
        for file in &files {
            bytes.update(i64::try_from(file.file_size_in_bytes()).unwrap_or(i64::MAX));
            rows.update(i64::try_from(file.record_count()).unwrap_or(i64::MAX));
        }
        Metrics::counter(METRICS, "data_files_written")
            .inc_by(i64::try_from(files.len()).unwrap_or(i64::MAX));
        writer
            .metas(files)?
            .into_iter()
            .try_for_each(|meta| ctx.emit(meta))
    }
}
