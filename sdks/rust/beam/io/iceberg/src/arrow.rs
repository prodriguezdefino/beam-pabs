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

//! Arrow batches of the input schema as batches of the Arrow schema of the table, and their
//! Arrow IPC form.
//!
//! The Arrow schema of the table comes from `iceberg`: every field has its `PARQUET:field_id`,
//! `timestamptz` is `Timestamp(µs, "+00:00")`, and map entries are named `key_value`. The
//! adapter changes the batches of `arrow_io` to that schema. It shares the value and offset
//! buffers; only `millis_instant` values are scaled to microseconds.

use std::sync::Arc;

use arrow_io::EncodedBatch;
use arrow_io::arrow_array::cast::AsArray;
use arrow_io::arrow_array::types::{TimestampMicrosecondType, TimestampMillisecondType};
use arrow_io::arrow_array::{
    Array, ArrayRef, ListArray, MapArray, RecordBatch, RecordBatchOptions, StructArray,
    new_null_array,
};
use arrow_io::arrow_buffer::Buffer;
use arrow_io::arrow_ipc::reader::StreamDecoder;
use arrow_io::arrow_ipc::writer::{
    CompressionContext, DictionaryTracker, EncodedData, IpcDataGenerator, IpcWriteOptions,
    write_message,
};
use arrow_io::arrow_schema::{
    ArrowError, DataType, Fields, Schema as ArrowSchema, SchemaRef, TimeUnit,
};
use iceberg::spec::Schema as TableSchema;

/// The Arrow schema of a table schema, with the field ids as `PARQUET:field_id` metadata.
///
/// # Errors
///
/// Fails if the table has a type with no Arrow form.
pub fn table_arrow_schema(table: &TableSchema) -> beam::Result<SchemaRef> {
    Ok(Arc::new(iceberg::arrow::schema_to_arrow_schema(table)?))
}

/// Changes batches of one input Arrow schema into batches of the Arrow schema of a table.
#[derive(Debug)]
pub struct Adapter {
    target: SchemaRef,
    /// For each table column, the input column that fills it. `None` fills it with nulls.
    sources: Vec<Option<usize>>,
}

impl Adapter {
    /// Matches the columns by name and checks that each input type changes into its table
    /// type.
    ///
    /// # Errors
    ///
    /// Fails if a type cannot change, or if a required table column has no input column.
    pub fn new(input: &ArrowSchema, target: SchemaRef) -> beam::Result<Self> {
        let sources = target
            .fields()
            .iter()
            .map(|field| match input.index_of(field.name()) {
                Ok(index) => {
                    check(input.field(index).data_type(), field.data_type())
                        .map_err(|e| beam::Error::msg(format!("Column '{}': {e}", field.name())))?;
                    Ok(Some(index))
                }
                Err(_) if field.is_nullable() => Ok(None),
                Err(_) => Err(beam::Error::msg(format!(
                    "The required column '{}' is not in the input",
                    field.name()
                ))),
            })
            .collect::<beam::Result<Vec<_>>>()?;
        Ok(Self { target, sources })
    }

    /// The Arrow schema of the table.
    pub fn target(&self) -> &SchemaRef {
        &self.target
    }

    /// `batch` with the columns, names, field ids and types of the table.
    ///
    /// # Errors
    ///
    /// Fails if a value does not fit its table type, for example a null in a required field
    /// or a timestamp out of range.
    pub fn adapt(&self, batch: &RecordBatch) -> beam::Result<RecordBatch> {
        let rows = batch.num_rows();
        let columns = self
            .target
            .fields()
            .iter()
            .zip(&self.sources)
            .map(|(field, source)| match source {
                Some(index) => adapt(batch.column(*index), field.data_type()),
                None => Ok(new_null_array(field.data_type(), rows)),
            })
            .collect::<Result<Vec<_>, ArrowError>>()?;
        Ok(RecordBatch::try_new_with_options(
            Arc::clone(&self.target),
            columns,
            &RecordBatchOptions::new().with_row_count(Some(rows)),
        )?)
    }
}

/// Checks that values of `from` change into `to` with [`adapt`].
fn check(from: &DataType, to: &DataType) -> Result<(), ArrowError> {
    match (from, to) {
        (from, to) if from == to => Ok(()),
        (
            DataType::Timestamp(TimeUnit::Millisecond | TimeUnit::Microsecond, _),
            DataType::Timestamp(TimeUnit::Microsecond, _),
        ) => Ok(()),
        (DataType::Struct(from), DataType::Struct(to)) => struct_sources(from, to).map(|_| ()),
        (DataType::List(from), DataType::List(to)) => check(from.data_type(), to.data_type()),
        (DataType::Map(from, _), DataType::Map(to, _)) => check(from.data_type(), to.data_type()),
        // Types that differ only in field names, field metadata or nullability.
        (from, to) if from.equals_datatype(to) => Ok(()),
        (from, to) => Err(ArrowError::SchemaError(format!(
            "the input type {from} does not change into the table type {to}"
        ))),
    }
}

/// For each field of `to`, the index of the field of `from` with its name. A field that is
/// only in `to` must be nullable.
fn struct_sources(from: &Fields, to: &Fields) -> Result<Vec<Option<usize>>, ArrowError> {
    to.iter()
        .map(|field| match from.find(field.name()) {
            Some((index, source)) => {
                check(source.data_type(), field.data_type())?;
                Ok(Some(index))
            }
            None if field.is_nullable() => Ok(None),
            None => Err(ArrowError::SchemaError(format!(
                "the required field '{}' is not in the input",
                field.name()
            ))),
        })
        .collect()
}

/// `array` with the type `to`. The types passed [`check`].
fn adapt(array: &ArrayRef, to: &DataType) -> Result<ArrayRef, ArrowError> {
    match (array.data_type(), to) {
        (from, to) if from == to => Ok(Arc::clone(array)),
        (
            DataType::Timestamp(TimeUnit::Millisecond, _),
            DataType::Timestamp(TimeUnit::Microsecond, zone),
        ) => {
            let millis = array
                .as_primitive_opt::<TimestampMillisecondType>()
                .ok_or_else(|| mismatch(array, to))?;
            let micros = millis.try_unary::<_, TimestampMicrosecondType, _>(|value| {
                value.checked_mul(1000).ok_or_else(|| {
                    ArrowError::ComputeError(format!(
                        "the timestamp {value} ms is out of the range of microseconds"
                    ))
                })
            })?;
            Ok(Arc::new(micros.with_timezone_opt(zone.clone())))
        }
        (
            DataType::Timestamp(TimeUnit::Microsecond, _),
            DataType::Timestamp(TimeUnit::Microsecond, zone),
        ) => {
            let micros = array
                .as_primitive_opt::<TimestampMicrosecondType>()
                .ok_or_else(|| mismatch(array, to))?;
            Ok(Arc::new(micros.clone().with_timezone_opt(zone.clone())))
        }
        (DataType::Struct(_), DataType::Struct(fields)) => {
            let input = array.as_struct_opt().ok_or_else(|| mismatch(array, to))?;
            Ok(Arc::new(adapt_struct(input, fields)?))
        }
        (DataType::List(_), DataType::List(field)) => {
            let list = array
                .as_list_opt::<i32>()
                .ok_or_else(|| mismatch(array, to))?;
            Ok(Arc::new(ListArray::try_new(
                Arc::clone(field),
                list.offsets().clone(),
                adapt(list.values(), field.data_type())?,
                list.nulls().cloned(),
            )?))
        }
        (DataType::Map(_, ordered), DataType::Map(entries, _)) => {
            let map = array.as_map_opt().ok_or_else(|| mismatch(array, to))?;
            let DataType::Struct(fields) = entries.data_type() else {
                return Err(mismatch(array, to));
            };
            Ok(Arc::new(MapArray::try_new(
                Arc::clone(entries),
                map.offsets().clone(),
                adapt_struct(map.entries(), fields)?,
                map.nulls().cloned(),
                *ordered,
            )?))
        }
        (_, to) => {
            let data = array
                .to_data()
                .into_builder()
                .data_type(to.clone())
                .build()?;
            Ok(arrow_io::arrow_array::make_array(data))
        }
    }
}

fn adapt_struct(input: &StructArray, fields: &Fields) -> Result<StructArray, ArrowError> {
    let sources = struct_sources(input.fields(), fields)?;
    let columns = fields
        .iter()
        .zip(sources)
        .map(|(field, source)| match source {
            Some(index) => adapt(input.column(index), field.data_type()),
            None => Ok(new_null_array(field.data_type(), input.len())),
        })
        .collect::<Result<Vec<_>, _>>()?;
    StructArray::try_new_with_length(fields.clone(), columns, input.nulls().cloned(), input.len())
}

fn mismatch(array: &ArrayRef, to: &DataType) -> ArrowError {
    ArrowError::SchemaError(format!(
        "an array of {} does not change into {to}",
        array.data_type()
    ))
}

/// The IPC record-batch message of `batch`, with no schema message.
///
/// # Errors
///
/// Fails if the batch does not encode.
pub fn encode_batch(batch: &RecordBatch) -> beam::Result<EncodedBatch> {
    let options = IpcWriteOptions::default();
    let (_, encoded) = IpcDataGenerator::default().encode(
        batch,
        &mut DictionaryTracker::new(false),
        &options,
        &mut CompressionContext::default(),
    )?;
    Ok(EncodedBatch {
        bytes: message_bytes(encoded, &options)?,
        rows: i64::try_from(batch.num_rows())?,
    })
}

/// The IPC schema message of `schema`, which a decoder reads before the batches.
///
/// # Errors
///
/// Fails if the message does not write.
pub fn schema_bytes(schema: &ArrowSchema) -> beam::Result<Vec<u8>> {
    let options = IpcWriteOptions::default();
    let encoded = IpcDataGenerator::default().schema_to_bytes_with_dictionary_tracker(
        schema,
        &mut DictionaryTracker::new(false),
        &options,
    );
    message_bytes(encoded, &options)
}

fn message_bytes(encoded: EncodedData, options: &IpcWriteOptions) -> beam::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    write_message(&mut bytes, encoded, options)?;
    Ok(bytes)
}

/// Decodes the batch of `batch`, an IPC record-batch message of the schema of `schema`, an
/// IPC schema message.
///
/// # Errors
///
/// Fails if the bytes do not decode to one batch.
pub fn decode_batch(schema: &[u8], batch: Vec<u8>) -> beam::Result<RecordBatch> {
    let mut decoder = StreamDecoder::new();
    let mut schema = Buffer::from(schema.to_vec());
    while !schema.is_empty() {
        decoder.decode(&mut schema)?;
    }
    let mut bytes = Buffer::from(batch);
    let mut decoded = None;
    while !bytes.is_empty() {
        decoded = decoder.decode(&mut bytes)?.or(decoded);
    }
    decoded.ok_or_else(|| beam::Error::msg("The IPC bytes hold no record batch"))
}
