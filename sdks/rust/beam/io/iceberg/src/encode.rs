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

//! Conversion of elements into Arrow IPC batches of the table schema, and the DoFn that
//! converts them before the shuffle of the write.

use std::sync::Arc;

use arrow_io::arrow_array::RecordBatch;
use arrow_io::{EncodedBatch, RowBatchBuilder, RowCodec};
use beam::Instant;
use beam::coders::{DefaultCoder, RowCoder};
use beam::metrics::Metrics;
use beam::schema::{Field, FieldType, FieldValue, Row, Schema};
use beam::transforms::{DoFn, FinishBundleContext, ProcessContext};
use beam::values::{AnySideInput, SingletonView};
use beam::windowing::{BoundedWindow, GlobalWindow};
use iceberg::spec::{Schema as TableSchema, TableMetadata};

use crate::METRICS;
use crate::arrow::{Adapter, encode_batch, table_arrow_schema};
use crate::table::TableInfo;

/// The size of the Arrow batch after which [`EncodeFn`] emits its rows, when the write sets
/// none.
pub const DEFAULT_ENCODE_BATCH_BYTES: usize = 4 * 1024 * 1024;

/// The rows of the first batch of an [`EncodeFn`] copy, before it knows the size of a row.
const FIRST_BATCH_ROWS: usize = 1024;

/// The schema of the error output for rows of `schema`: the rejected row and the reason.
pub fn error_schema(schema: &Schema) -> Schema {
    Schema::new(vec![
        Field::new("failed_row", FieldType::row(schema.clone())),
        Field::new("error_message", FieldType::string()),
    ])
}

/// The batch of some elements, and an error row for each element that does not convert.
#[derive(Debug, Default)]
pub struct Encoded {
    pub batch: Option<(RecordBatch, EncodedBatch)>,
    pub errors: Vec<Row>,
}

/// Converts elements of one Beam schema into IPC batches of the Arrow schema of one table.
#[derive(Debug)]
pub struct Encoder {
    input: Arc<Schema>,
    adapter: Adapter,
    errors: Arc<Schema>,
}

impl Encoder {
    /// An encoder from rows of `input` to the table schema `table`.
    ///
    /// # Errors
    ///
    /// Fails if an input type does not change into its table type.
    pub fn new(input: Arc<Schema>, table: &TableSchema) -> beam::Result<Self> {
        let source = arrow_io::beam_to_arrow_schema(&input)?;
        let adapter = Adapter::new(&source, table_arrow_schema(table)?)?;
        let errors = Arc::new(error_schema(&input));
        Ok(Self {
            input,
            adapter,
            errors,
        })
    }

    /// The schema of the error rows.
    pub fn error_schema(&self) -> &Arc<Schema> {
        &self.errors
    }

    /// Converts `elements` into one batch of the table schema.
    ///
    /// If the batch does not convert, converts the elements one at a time. An element that
    /// does not convert becomes an error row, and the batch holds the other elements. Only a
    /// batch with a bad element takes this path.
    ///
    /// # Errors
    ///
    /// Fails if an element that does not convert cannot be read back as a row.
    pub fn encode<T: DefaultCoder + 'static>(
        &self,
        codec: &dyn RowCodec<T>,
        elements: &[T],
    ) -> beam::Result<Encoded> {
        if elements.is_empty() {
            return Ok(Encoded::default());
        }
        let (good, errors) = match self.convert(codec, elements.iter()) {
            Ok(batch) => (batch, Vec::new()),
            Err(_) => {
                let (good, errors) = elements.iter().try_fold(
                    (Vec::new(), Vec::new()),
                    |(mut good, mut errors), element| {
                        match self.convert(codec, std::iter::once(element)) {
                            Ok(_) => good.push(element),
                            Err(e) => errors.push(self.error_row(element, &e.to_string())?),
                        }
                        Ok::<_, beam::Error>((good, errors))
                    },
                )?;
                (self.convert(codec, good.into_iter())?, errors)
            }
        };
        let batch = match good.num_rows() {
            0 => None,
            _ => {
                let encoded = encode_batch(&good)?;
                Some((good, encoded))
            }
        };
        Ok(Encoded { batch, errors })
    }

    fn convert<'a, T: 'static>(
        &self,
        codec: &dyn RowCodec<T>,
        elements: impl Iterator<Item = &'a T>,
    ) -> beam::Result<RecordBatch> {
        let mut builder = RowBatchBuilder::new(&self.input)?;
        elements
            .into_iter()
            .try_for_each(|element| codec.append(element, &mut builder))?;
        self.adapter.adapt(&builder.finish()?)
    }

    /// The error row of `element`: the element as a row of the input schema, and the reason.
    /// The input PCollection has a row coder, so the coder bytes of `element` decode as a row.
    fn error_row<T: DefaultCoder>(&self, element: &T, reason: &str) -> beam::Result<Row> {
        let mut bytes = Vec::new();
        element.encode_element(&mut bytes)?;
        let row = RowCoder::decode_row(&self.input, &mut bytes.as_slice())?;
        Ok(Row::new(
            Arc::clone(&self.errors),
            vec![
                Some(FieldValue::Row(row)),
                Some(FieldValue::String(reason.to_string())),
            ],
        )?)
    }
}

/// A batch of the table with its name, or an element that does not convert.
pub type EncodeOut = Result<(String, EncodedBatch), Row>;

/// Converts elements into Arrow IPC batches of the table before the shuffle of the write.
///
/// A copy buffers the elements of its bundle and converts them when the next batch reaches
/// `batch_bytes`, and at the end of the bundle. It learns the size of a row from its last batch.
pub struct EncodeFn<T> {
    table: SingletonView<TableInfo>,
    codec: Arc<dyn RowCodec<T>>,
    batch_bytes: usize,
    encoder: Option<Arc<(String, Encoder)>>,
    buffer: Vec<T>,
    batch_rows: usize,
    earliest: Option<Instant>,
}

impl<T> EncodeFn<T> {
    pub fn new(
        table: SingletonView<TableInfo>,
        codec: Arc<dyn RowCodec<T>>,
        batch_bytes: usize,
    ) -> Self {
        Self {
            table,
            codec,
            batch_bytes,
            encoder: None,
            buffer: Vec::new(),
            batch_rows: FIRST_BATCH_ROWS,
            earliest: None,
        }
    }
}

/// Each copy starts with an empty buffer and no encoder.
impl<T> Clone for EncodeFn<T> {
    fn clone(&self) -> Self {
        Self::new(
            self.table.clone(),
            Arc::clone(&self.codec),
            self.batch_bytes,
        )
    }
}

impl<T: DefaultCoder + Send + Sync + 'static> EncodeFn<T> {
    fn encoder(
        &mut self,
        ctx: &ProcessContext<'_, EncodeOut>,
    ) -> beam::Result<Arc<(String, Encoder)>> {
        if let Some(encoder) = &self.encoder {
            return Ok(Arc::clone(encoder));
        }
        let TableInfo {
            table, metadata, ..
        } = ctx.side_input(&self.table)?;
        let metadata: TableMetadata = serde_json::from_slice(&metadata)?;
        let encoder = Encoder::new(Arc::clone(self.codec.schema()), metadata.current_schema())?;
        let encoder = Arc::new((table, encoder));
        self.encoder = Some(Arc::clone(&encoder));
        Ok(encoder)
    }

    /// Converts the buffered elements: the error rows, then the batch.
    fn flush(&mut self) -> beam::Result<Vec<EncodeOut>> {
        let Some(encoder) = self.encoder.as_deref() else {
            return Ok(Vec::new());
        };
        let Encoded { batch, errors } = encoder.1.encode(self.codec.as_ref(), &self.buffer)?;
        // Clear, so the buffer keeps its capacity for the next batch.
        self.buffer.clear();
        if let Some((record_batch, _)) = &batch {
            let bytes_per_row = record_batch.get_array_memory_size() / record_batch.num_rows();
            self.batch_rows = (self.batch_bytes / bytes_per_row.max(1)).max(1);
            Metrics::counter(METRICS, "rows_encoded")
                .inc_by(i64::try_from(record_batch.num_rows()).unwrap_or(i64::MAX));
        }
        if !errors.is_empty() {
            Metrics::counter(METRICS, "rows_failed")
                .inc_by(i64::try_from(errors.len()).unwrap_or(i64::MAX));
        }
        Ok(errors
            .into_iter()
            .map(Err)
            .chain(batch.map(|(_, encoded)| Ok((encoder.0.clone(), encoded))))
            .collect())
    }
}

impl<T: DefaultCoder + Send + Sync + 'static> DoFn for EncodeFn<T> {
    type In = T;
    type Out = EncodeOut;

    fn side_inputs(&self) -> Vec<&dyn AnySideInput> {
        vec![&self.table]
    }

    fn process_element(
        &mut self,
        element: T,
        ctx: &mut ProcessContext<'_, Self::Out>,
    ) -> beam::Result {
        self.encoder(ctx)?;
        let timestamp = ctx.timestamp();
        self.earliest = Some(self.earliest.map_or(timestamp, |t| t.min(timestamp)));
        self.buffer.push(element);
        match self.buffer.len() >= self.batch_rows {
            true => self.flush()?.into_iter().try_for_each(|out| ctx.emit(out)),
            false => Ok(()),
        }
    }

    /// Emits the rest of the bundle in the global window, at the earliest timestamp of the
    /// bundle. The write puts its input in the global window first.
    fn finish_bundle(&mut self, ctx: &mut FinishBundleContext<'_, Self::Out>) -> beam::Result {
        let timestamp = self
            .earliest
            .take()
            .unwrap_or(Instant::END_OF_GLOBAL_WINDOW);
        self.flush()?
            .into_iter()
            .try_for_each(|out| ctx.emit(out, BoundedWindow::Global(GlobalWindow), timestamp))
    }
}
