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

//! Conversion of rows into the Arrow IPC batches of append requests, and the DoFn that
//! converts the rows of an exactly-once write before its shuffle.

use std::ops::Range;
use std::sync::Arc;

use arrow_io::arrow_array::{RecordBatch, RecordBatchOptions};
use arrow_io::arrow_buffer::Buffer;
use arrow_io::arrow_ipc::convert::try_schema_from_ipc_buffer;
use arrow_io::arrow_ipc::reader::StreamDecoder;
use arrow_io::arrow_ipc::writer::{
    DictionaryTracker, EncodedData, IpcDataGenerator, IpcWriteContext, IpcWriteOptions,
    write_message,
};
use arrow_io::arrow_schema::Schema as ArrowSchema;
use beam::coders::RowCoder;
use beam::schema::{FieldValue, Row, RowElement, Schema};
use beam::transforms::{DoFn, FinishBundleContext, ProcessContext};
use beam::values::{AnySideInput, SingletonView};
use bytes::Bytes;
use google_cloud_bigquery::model::ArrowSchema as WriterSchema;

use super::buffer::{Buffered, RowBuffer};
use super::emit_global;
use super::encoded_batch::Request;
use super::projection::{Selection, TableSelection};
use super::table::Destination;
use crate::bigquery::storage::schema::{find_field, write_array, write_batch_to_rows, write_field};

pub use super::buffer::{APPEND_BYTES, MAX_REQUEST_BYTES};
pub use super::encoded_batch::{EncodedBatch, EncodedBatchCoder};

/// The batches of some rows, and an error row for each row that does not convert.
#[derive(Default)]
pub(super) struct Encoded {
    pub(super) batches: Vec<EncodedBatch>,
    pub(super) errors: Vec<Row>,
}

/// Converts rows of one Beam schema into Arrow batches of the column types of one table.
pub struct Encoder {
    input: Arc<Schema>,
    /// The fields of the input that go to the table.
    selection: TableSelection,
    errors: Arc<Schema>,
    schema: Arc<ArrowSchema>,
    schema_bytes: Bytes,
}

impl Encoder {
    /// Maps each selected field of the input to the column with the same name. Fails if the
    /// table has no such column and the selection does not ignore it, or if the write cannot
    /// convert the type.
    pub fn new(
        selection: &Selection,
        errors: Arc<Schema>,
        destination: &Destination,
    ) -> beam::Result<Self> {
        let table = try_schema_from_ipc_buffer(&destination.arrow_schema)
            .map_err(|e| beam::Error::msg(format!("Invalid Arrow schema of the table: {e}")))?;
        let input = Arc::clone(selection.input());
        let selection = selection.for_table(table.fields())?;
        let source =
            arrow_io::beam_to_arrow_schema(&selection.written).map_err(beam::Error::msg)?;
        let fields = source
            .fields()
            .iter()
            .map(|field| write_field(field, find_field(table.fields(), field.name())?))
            .collect::<beam::Result<Vec<_>>>()?;
        let schema = Arc::new(ArrowSchema::new(fields));
        let schema_bytes = Bytes::from(schema_bytes(&schema)?);
        Ok(Self {
            input,
            selection,
            errors,
            schema,
            schema_bytes,
        })
    }

    /// The schema that each append request names.
    pub(super) fn writer_schema(&self) -> WriterSchema {
        WriterSchema::new().set_serialized_schema(self.schema_bytes.clone())
    }

    /// Converts `rows` into batches that each fit in one append request. A row that does not
    /// convert, or that is larger than a request, becomes an error row. The encoded rows
    /// convert from their bytes, with no [`Row`] between.
    pub(super) fn encode(&self, rows: &Buffered) -> beam::Result<Encoded> {
        let mut encoded = Encoded::default();
        if !rows.encoded.is_empty() {
            let converted = match self.convert_encoded(rows.encoded.iter().map(Vec::as_slice)) {
                Ok(batch) => (batch, Vec::new()),
                Err(_) => self.convert_each_encoded(&rows.encoded)?,
            };
            self.split_into(converted, &mut encoded)?;
        }
        if !rows.rows.is_empty() {
            let converted = match self.convert(&rows.rows) {
                Ok(batch) => (batch, Vec::new()),
                Err(_) => self.convert_each(&rows.rows)?,
            };
            self.split_into(converted, &mut encoded)?;
        }
        Ok(encoded)
    }

    /// Adds the batches of `batch` that fit in one request each, and the error rows, to
    /// `encoded`. A row larger than a request becomes an error row.
    fn split_into(
        &self,
        (batch, errors): (RecordBatch, Vec<Row>),
        encoded: &mut Encoded,
    ) -> beam::Result {
        encoded.errors.extend(errors);
        let mut too_large = Vec::new();
        let max_bytes = MAX_REQUEST_BYTES.saturating_sub(self.schema_bytes.len());
        split(batch, max_bytes, &mut encoded.batches, &mut too_large)?;
        too_large.iter().try_for_each(|batch| {
            write_batch_to_rows(batch, &self.selection.written)?
                .into_iter()
                .try_for_each(|row| {
                    let row = self.selection.input_row(row)?;
                    let error = self.error_row(row, "The row is larger than an append request")?;
                    encoded.errors.push(error);
                    Ok::<_, beam::Error>(())
                })
        })
    }

    /// Splits a batch that BigQuery partly rejected. Returns the batches of the rows that it
    /// accepted, and an error row for each row in `rejected`, with its index and reason.
    pub(super) fn split_rejected(
        &self,
        request: &Request,
        rejected: Vec<(usize, String)>,
    ) -> beam::Result<Encoded> {
        let decoded = self.decode(&request.rows.serialized_record_batch)?;
        let rows = decoded.num_rows();
        let errors = rejected
            .iter()
            .filter(|(index, _)| *index < rows)
            .map(|(index, reason)| {
                let row = write_batch_to_rows(&decoded.slice(*index, 1), &self.selection.written)?
                    .into_iter()
                    .next()
                    .ok_or_else(|| beam::Error::msg("A rejected row did not decode"))?;
                self.error_row(self.selection.input_row(row)?, reason)
            })
            .collect::<beam::Result<Vec<_>>>()?;
        let mut indexes: Vec<usize> = rejected.into_iter().map(|(index, _)| index).collect();
        indexes.sort_unstable();
        let batches = accepted(&indexes, rows)
            .map(|range| encode_batch(&decoded.slice(range.start, range.len())))
            .collect::<beam::Result<Vec<_>>>()?;
        Ok(Encoded { batches, errors })
    }

    fn convert(&self, rows: &[Row]) -> beam::Result<RecordBatch> {
        let batch =
            arrow_io::rows_to_projected_batch(&self.input, rows, &self.selection.projection)
                .map_err(beam::Error::msg)?;
        self.to_column_types(&batch)
    }

    fn convert_encoded<'a>(
        &self,
        rows: impl IntoIterator<Item = &'a [u8]>,
    ) -> beam::Result<RecordBatch> {
        let batch = arrow_io::encoded_rows_to_projected_batch(
            &self.input,
            rows,
            &self.selection.projection,
        )
        .map_err(beam::Error::msg)?;
        self.to_column_types(&batch)
    }

    /// Converts each column of `batch` into the type of its table column.
    fn to_column_types(&self, batch: &RecordBatch) -> beam::Result<RecordBatch> {
        let columns = batch
            .columns()
            .iter()
            .zip(self.schema.fields())
            .map(|(column, field)| write_array(column, field.data_type()))
            .collect::<beam::Result<Vec<_>>>()?;
        RecordBatch::try_new_with_options(
            Arc::clone(&self.schema),
            columns,
            &RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
        )
        .map_err(beam::Error::msg)
    }

    /// Converts the encoded rows one at a time, to find the rows that do not convert. Only a
    /// batch with a bad row takes this path. A bad row is decoded for the error output.
    fn convert_each_encoded(&self, rows: &[Vec<u8>]) -> beam::Result<(RecordBatch, Vec<Row>)> {
        let (good, errors) =
            rows.iter()
                .try_fold((Vec::new(), Vec::new()), |(mut good, mut errors), bytes| {
                    match self.convert_encoded([bytes.as_slice()]) {
                        Ok(_) => good.push(bytes.as_slice()),
                        Err(e) => {
                            let row = RowCoder::decode_row(&self.input, &mut bytes.as_slice())
                                .map_err(|decode| {
                                    beam::Error::msg(format!(
                                        "A row does not convert ({e}) and does not decode: {decode}"
                                    ))
                                })?;
                            errors.push(self.error_row(row, &e.to_string())?);
                        }
                    }
                    Ok::<_, beam::Error>((good, errors))
                })?;
        Ok((self.convert_encoded(good)?, errors))
    }

    /// Converts the rows one at a time, to find the rows that do not convert. Only a batch
    /// with a bad row takes this path.
    fn convert_each(&self, rows: &[Row]) -> beam::Result<(RecordBatch, Vec<Row>)> {
        let (good, errors) =
            rows.iter()
                .fold((Vec::new(), Vec::new()), |(mut good, mut errors), row| {
                    match self.convert(std::slice::from_ref(row)) {
                        Ok(_) => good.push(row.clone()),
                        Err(e) => errors.push((row, e.to_string())),
                    }
                    (good, errors)
                });
        let errors = errors
            .into_iter()
            .map(|(row, reason)| self.error_row(row.clone(), &reason))
            .collect::<beam::Result<Vec<_>>>()?;
        Ok((self.convert(&good)?, errors))
    }

    /// Decodes the rows of a request. Only a request that BigQuery partly rejected takes this
    /// path, so the copies into Arrow buffers do not matter.
    fn decode(&self, batch: &[u8]) -> beam::Result<RecordBatch> {
        let mut decoder = StreamDecoder::new();
        let mut schema = Buffer::from(self.schema_bytes.to_vec());
        while !schema.is_empty() {
            decoder.decode(&mut schema).map_err(beam::Error::msg)?;
        }
        let mut bytes = Buffer::from(batch.to_vec());
        let mut decoded = None;
        while !bytes.is_empty() {
            decoded = decoder
                .decode(&mut bytes)
                .map_err(beam::Error::msg)?
                .or(decoded);
        }
        decoded.ok_or_else(|| beam::Error::msg("An Arrow batch of the write did not decode"))
    }

    /// A row of the error output: the rejected row and the reason.
    ///
    /// A row that does not convert is the input row. A row from a batch, too large or rejected
    /// by BigQuery, holds only the written fields, and the other fields are null.
    pub(super) fn error_row(&self, row: Row, reason: &str) -> beam::Result<Row> {
        Row::new(
            Arc::clone(&self.errors),
            vec![
                Some(FieldValue::Row(row)),
                Some(FieldValue::String(reason.to_string())),
            ],
        )
        .map_err(beam::Error::msg)
    }
}

/// The ranges of the rows in `0..rows` that are not in `rejected`, which is sorted.
#[doc(hidden)]
pub fn accepted(rejected: &[usize], rows: usize) -> impl Iterator<Item = Range<usize>> + '_ {
    rejected
        .iter()
        .copied()
        .chain([rows])
        .scan(0, move |start, end| {
            let range = *start..end.min(rows);
            *start = end.saturating_add(1);
            Some(range)
        })
        .filter(|range| !range.is_empty())
}

/// Encodes `batch`, in halves while it is larger than `max_bytes`, the rows that one request
/// holds. A buffer of rows is about [`APPEND_BYTES`], so it is encoded once and not split. A
/// single row larger than `max_bytes` goes to `too_large`.
#[doc(hidden)]
pub fn split(
    batch: RecordBatch,
    max_bytes: usize,
    batches: &mut Vec<EncodedBatch>,
    too_large: &mut Vec<RecordBatch>,
) -> beam::Result {
    let rows = batch.num_rows();
    // The in-memory size is an upper bound of the encoded size, with at most double capacity,
    // so a much larger batch splits before it is encoded.
    let encoded = match (rows, batch.get_array_memory_size() > 2 * max_bytes) {
        (0, _) => return Ok(()),
        (1, _) | (_, false) => Some(encode_batch(&batch)?),
        (_, true) => None,
    };
    match (encoded, rows) {
        (Some(encoded), _) if encoded.bytes.len() <= max_bytes => batches.push(encoded),
        (_, 1) => too_large.push(batch),
        (_, rows) => {
            let half = rows / 2;
            split(batch.slice(0, half), max_bytes, batches, too_large)?;
            split(
                batch.slice(half, rows - half),
                max_bytes,
                batches,
                too_large,
            )?;
        }
    }
    Ok(())
}

/// Returns the IPC message of `schema`, as the `writer_schema` of append requests takes it.
#[doc(hidden)]
pub fn schema_bytes(schema: &ArrowSchema) -> beam::Result<Vec<u8>> {
    let options = IpcWriteOptions::default();
    let encoded = IpcDataGenerator::default().schema_to_bytes_with_dictionary_tracker(
        schema,
        &mut DictionaryTracker::new(false),
        &options,
    );
    message_bytes(encoded, &options)
}

fn encode_batch(batch: &RecordBatch) -> beam::Result<EncodedBatch> {
    let options = IpcWriteOptions::default();
    let (_, encoded) = IpcDataGenerator::default()
        .encode(
            batch,
            &mut DictionaryTracker::new(false),
            &options,
            &mut IpcWriteContext::default(),
        )
        .map_err(beam::Error::msg)?;
    Ok(EncodedBatch {
        bytes: message_bytes(encoded, &options)?,
        rows: i64::try_from(batch.num_rows()).map_err(beam::Error::msg)?,
    })
}

fn message_bytes(encoded: EncodedData, options: &IpcWriteOptions) -> beam::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    write_message(&mut bytes, encoded, options).map_err(beam::Error::msg)?;
    Ok(bytes)
}

/// How an exactly-once write of an unbounded input assigns batches to shards. Each shard
/// writes to its own buffered stream.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Sharding {
    /// One shard for each DoFn copy, so the streams follow the parallelism of the workers. This
    /// is the auto-sharding of the Java SDK on a portable runner: a shard for each worker
    /// thread.
    Auto,
    /// A shard in `0..n` for each bundle.
    Fixed(u32),
}

/// Converts rows into Arrow batches before the shuffle of an exactly-once write, and emits
/// each batch with a shard key. All batches of a bundle take the same shard, so a shard gets
/// few large batches.
pub(super) struct EncodeFn {
    destination: SingletonView<Destination>,
    selection: Arc<Selection>,
    errors: Arc<Schema>,
    sharding: Sharding,
    encoder: Option<Arc<Encoder>>,
    shard: i64,
    rows: RowBuffer,
}

impl EncodeFn {
    pub(super) fn new(
        destination: SingletonView<Destination>,
        selection: Arc<Selection>,
        errors: Arc<Schema>,
        sharding: Sharding,
    ) -> Self {
        Self {
            destination,
            selection,
            errors,
            sharding,
            encoder: None,
            // `random` gives a new value for each copy.
            shard: random() as i64,
            rows: RowBuffer::default(),
        }
    }

    /// Converts the buffered rows: the rows that do not convert, then the batches.
    fn flush(&mut self) -> beam::Result<Vec<EncodeOut>> {
        let Some(encoder) = &self.encoder else {
            return Ok(Vec::new());
        };
        let rows = self.rows.take();
        if rows.is_empty() {
            return Ok(Vec::new());
        }
        let Encoded { batches, errors } = encoder.encode(&rows)?;
        let shard = self.shard;
        Ok(errors
            .into_iter()
            .map(Err)
            .chain(batches.into_iter().map(|batch| Ok((shard, batch))))
            .collect())
    }
}

/// Each copy starts with an empty buffer and no encoder.
impl Clone for EncodeFn {
    fn clone(&self) -> Self {
        Self::new(
            self.destination.clone(),
            Arc::clone(&self.selection),
            Arc::clone(&self.errors),
            self.sharding,
        )
    }
}

/// A batch with its shard key, or a row that does not convert.
type EncodeOut = Result<(i64, EncodedBatch), Row>;

impl DoFn for EncodeFn {
    type In = RowElement;
    type Out = EncodeOut;

    fn side_inputs(&self) -> Vec<&dyn AnySideInput> {
        vec![&self.destination]
    }

    fn start_bundle(&mut self) -> beam::Result {
        match self.sharding {
            Sharding::Auto => {}
            Sharding::Fixed(shards) => {
                self.shard = i64::try_from(random() % u64::from(shards.max(1))).unwrap_or(0);
            }
        }
        Ok(())
    }

    fn process_element(
        &mut self,
        row: RowElement,
        ctx: &mut ProcessContext<'_, Self::Out>,
    ) -> beam::Result {
        if self.encoder.is_none() {
            let destination = ctx.side_input(&self.destination)?;
            self.encoder = Some(Arc::new(Encoder::new(
                &self.selection,
                Arc::clone(&self.errors),
                &destination,
            )?));
        }
        match self.rows.push(row) {
            true => self.flush()?.into_iter().try_for_each(|out| ctx.emit(out)),
            false => Ok(()),
        }
    }

    fn finish_bundle(&mut self, ctx: &mut FinishBundleContext<'_, Self::Out>) -> beam::Result {
        self.flush()?
            .into_iter()
            .try_for_each(|out| emit_global(ctx, out))
    }
}

/// A random number without a random number crate. Shards must be well spread, not
/// unpredictable.
pub(super) fn random() -> u64 {
    use std::hash::{BuildHasher, Hasher};
    std::collections::hash_map::RandomState::new()
        .build_hasher()
        .finish()
}
