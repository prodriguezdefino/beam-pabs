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
//! one [`DataFileMeta`] for each file, which the commit step adds to the table.

use std::sync::Arc;

use beam::coders::ShardedKey;
use beam::metrics::{Lineage, Metrics};
use beam::schema::BeamRow;
use beam::transforms::{DoFn, ProcessContext};
use beam::values::{AnySideInput, SingletonView};
use beam_io_arrow::EncodedBatch;
use beam_io_arrow::arrow_array::RecordBatch;
use bytes::Bytes;
use iceberg::TableIdent;
use iceberg::arrow::RecordBatchPartitionSplitter;
use iceberg::io::FileIO;
use iceberg::spec::{
    DataFile, DataFileFormat, TableMetadata, read_data_files_from_avro, write_data_files_to_avro,
};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::DefaultFileNameGenerator;
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::partitioning::PartitioningWriter;
use iceberg::writer::partitioning::fanout_writer::FanoutWriter;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use parquet::file::properties::WriterProperties;

use super::location::TableLocations;
use super::options::WriteOptions;
use super::parquet::{target_file_size, writer_properties};
use super::table::TableInfo;
use crate::METRICS;
use crate::arrow::{decode_batch, schema_bytes, table_arrow_schema};
use crate::catalog::Catalog;
use crate::catalog::TableFileIo;

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
    file_io: TableFileIo,
}

impl WriteDataFilesFn {
    /// # Errors
    ///
    /// Fails if the table name is not valid.
    pub fn new(table: SingletonView<TableInfo>, options: &WriteOptions) -> beam::Result<Self> {
        let ident = options.table_ident()?;
        Ok(Self {
            table,
            catalog: options.catalog.clone(),
            file_io: TableFileIo::new(options.catalog.clone(), ident.clone()),
            ident,
            write_id: uuid::Uuid::new_v4().to_string(),
            target_file_bytes: options.target_file_bytes,
            writer: None,
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
        let file_io = self.file_io.get(writer.metadata().location())?;
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

/// One data file of a table, as the Avro `data_file` struct of an Iceberg manifest entry.
#[derive(Clone, Debug, PartialEq, Eq, BeamRow)]
pub struct DataFileMeta {
    pub table: String,
    pub data_file: Bytes,
}

impl DataFileMeta {
    /// The meta of `file`, a file of the default partition spec of `metadata`.
    ///
    /// # Errors
    ///
    /// Fails if the file does not encode with the partition type of the spec.
    pub fn new(table: &str, file: DataFile, metadata: &TableMetadata) -> beam::Result<Self> {
        let mut bytes = Vec::new();
        write_data_files_to_avro(
            &mut bytes,
            [file],
            metadata.default_partition_type(),
            metadata.format_version(),
        )?;
        Ok(Self {
            table: table.to_string(),
            data_file: Bytes::from(bytes),
        })
    }

    /// The data file, read with the schema and the default partition spec of `metadata`.
    ///
    /// # Errors
    ///
    /// Fails if the bytes do not hold one data file of that spec.
    pub fn data_file(&self, metadata: &TableMetadata) -> beam::Result<DataFile> {
        let files = read_data_files_from_avro(
            &mut self.data_file.as_ref(),
            metadata.current_schema(),
            metadata.default_partition_spec_id(),
            metadata.default_partition_type(),
            metadata.format_version(),
        )?;
        match <[DataFile; 1]>::try_from(files) {
            Ok([file]) => Ok(file),
            Err(files) => Err(beam::Error::msg(format!(
                "A data file meta of '{}' holds {} files, not 1",
                self.table,
                files.len()
            ))),
        }
    }
}
