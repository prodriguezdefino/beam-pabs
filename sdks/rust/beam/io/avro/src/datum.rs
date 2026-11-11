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

//! Avro binary datums and Beam [`Row`]s, built on `arrow-avro` and `arrow_io`.
//!
//! Connectors that read or write record-level Avro datums use this module, for example the
//! Kafka connector with the `AVRO` format. [`DatumDecoder`] decodes batches of [`Frame`]s
//! into rows, and [`DatumEncoder`] encodes rows into plain datums or Confluent frames.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_avro::reader::{Decoder as BatchDecoder, ReaderBuilder};
use arrow_avro::schema::{
    AVRO_ENUM_SYMBOLS_METADATA_KEY, AvroSchema, Fingerprint, FingerprintAlgorithm,
    FingerprintStrategy, SCHEMA_METADATA_KEY, SchemaStore,
};
use arrow_avro::writer::format::{AvroBinaryFormat, AvroSoeFormat};
use arrow_avro::writer::{Encoder as BatchEncoder, WriterBuilder};
use arrow_schema::{DataType, Field, Fields, Schema as ArrowSchema, SchemaRef};
use beam::schema::{BeamRow, Row, RowElement, Schema};
use beam_io_arrow::RowBatchBuilder;
use beam_io_arrow::arrow_array::cast::AsArray;
use beam_io_arrow::arrow_array::types::Int32Type;
use beam_io_arrow::arrow_array::{
    Array, ArrayRef, DictionaryArray, Int32Array, ListArray, MapArray, RecordBatch, StringArray,
    StructArray,
};

/// First byte of a frame in the Confluent wire format, before the 4-byte schema ID.
pub const CONFLUENT_MAGIC: u8 = 0;

/// Schema ID assigned to plain datums when framed for `arrow-avro`.
pub const PLAIN_ID: u32 = 0;

/// Maximum rows decoded per Arrow batch.
pub const BATCH_ROWS: usize = 1024;

/// Why a frame that does not hold one datum fails.
const NOT_ONE_DATUM: &str = "The value is not one Avro datum: it ends early or has bytes after it";

/// Wire framing of an encoded Avro datum.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WireFormat {
    /// The Avro binary datum alone.
    Plain,
    /// Byte `0`, the big-endian 4-byte schema ID, then the Avro binary datum.
    Confluent(u32),
}

/// Returns the Beam [`Schema`] for the Avro record schema JSON `avro_schema`.
///
/// # Errors
///
/// Fails if `avro_schema` is not a valid Avro record schema or does not map to a Beam schema.
pub fn beam_schema(avro_schema: &str) -> Result<Arc<Schema>, String> {
    let schema: Arc<str> = avro_schema.into();
    let decoder = build_decoder(&schema, &HashMap::from([(PLAIN_ID, Arc::clone(&schema))]))?;
    beam_io_arrow::arrow_to_beam_schema(&decoder.schema())
        .map(Arc::new)
        .map_err(|e| format!("The Avro schema has no Beam schema: {e}"))
}

/// Returns the Avro record schema JSON derived from `row_schema`.
///
/// # Errors
///
/// Fails if `row_schema` cannot map to an Arrow and Avro schema.
pub fn avro_schema(row_schema: &Schema) -> Result<String, String> {
    let arrow_schema =
        beam_io_arrow::beam_to_arrow_schema(row_schema).map_err(|e| e.to_string())?;
    AvroSchema::try_from(&arrow_schema)
        .map(|schema| schema.json_string)
        .map_err(|e| format!("The rows have no Avro schema: {e}"))
}

/// Checks that `schema` has the field names of `expected`, in order.
///
/// # Errors
///
/// Fails when the field names or order differ.
pub fn check_fields(expected: &Schema, schema: &Schema) -> Result<(), String> {
    let names = schema.fields.iter().map(|f| f.name.as_str());
    if names.eq(expected.fields.iter().map(|f| f.name.as_str())) {
        Ok(())
    } else {
        Err(format!(
            "The rows have the fields {:?}, but the Avro schema has {:?}",
            schema.fields.iter().map(|f| &f.name).collect::<Vec<_>>(),
            expected.fields.iter().map(|f| &f.name).collect::<Vec<_>>(),
        ))
    }
}

/// Checks that `writer_schema` with ID `id` resolves to `read_schema`.
///
/// # Errors
///
/// Fails when `writer_schema` is invalid or does not resolve to `read_schema`.
pub fn check_writer_schema(
    read_schema: &Arc<str>,
    id: u32,
    writer_schema: &Arc<str>,
) -> Result<(), String> {
    build_decoder(
        read_schema,
        &HashMap::from([(id, Arc::clone(writer_schema))]),
    )
    .map(|_| ())
    .map_err(|e| format!("The writer schema {id}: {e}"))
}

/// Checks that an encoder can be built for `avro_schema` and `format`.
///
/// # Errors
///
/// Fails when `avro_schema` cannot build an encoder.
pub fn check_encoder(avro_schema: &Arc<str>, format: WireFormat) -> Result<(), String> {
    build_encoder(avro_schema, format).map(|_| ())
}

/// One Avro datum and the ID of its writer schema.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Frame<'a> {
    pub schema_id: u32,
    /// The Avro binary datum, with no header.
    pub datum: &'a [u8],
}

/// Stateful batch decoder of Avro datums for one reader worker.
///
/// It keeps the writer schema of each schema ID that it got, and one `arrow-avro` decoder for
/// all of them.
#[derive(Default)]
pub struct DatumDecoder(Option<Reader>);

struct Reader {
    decoder: BatchDecoder,
    writers: HashMap<u32, Arc<str>>,
}

impl DatumDecoder {
    /// Decodes `frames` into one [`Row`] result per frame, in their order.
    ///
    /// `writer` gives the writer schema of a schema ID that the decoder does not have yet. A
    /// frame whose writer schema fails gets that error. A frame that does not decode gets its
    /// own error, and the other frames still decode.
    pub fn decode_frames(
        &mut self,
        read_schema: &Arc<str>,
        row_schema: &Arc<Schema>,
        frames: &[Frame<'_>],
        writer: impl FnMut(u32) -> Result<Arc<str>, String>,
    ) -> Vec<Result<Row, String>> {
        let NewWriters { added, failed } = self.new_writers(frames, writer);
        if failed.is_empty() {
            return decode_known(&mut self.0, read_schema, row_schema, added, frames);
        }
        let failure = |id: u32| failed.iter().find(|(failed, _)| *failed == id);
        let usable: Vec<Frame<'_>> = frames
            .iter()
            .filter(|frame| failure(frame.schema_id).is_none())
            .copied()
            .collect();
        let mut rows =
            decode_known(&mut self.0, read_schema, row_schema, added, &usable).into_iter();
        frames
            .iter()
            .map(|frame| match failure(frame.schema_id) {
                Some((_, e)) => Err(e.clone()),
                None => rows
                    .next()
                    .unwrap_or_else(|| Err("The batch gave fewer rows than frames".to_string())),
            })
            .collect()
    }

    /// The writer schemas of the IDs in `frames` that the decoder does not have, and the IDs
    /// whose writer schema fails.
    fn new_writers(
        &self,
        frames: &[Frame<'_>],
        mut writer: impl FnMut(u32) -> Result<Arc<str>, String>,
    ) -> NewWriters {
        let known = |id: &u32| self.0.as_ref().is_some_and(|r| r.writers.contains_key(id));
        let mut added: Vec<(u32, Arc<str>)> = Vec::new();
        let mut failed: Vec<(u32, String)> = Vec::new();
        let mut previous = None;
        for &Frame { schema_id, .. } in frames {
            // Most batches have one schema ID, so compare with the previous ID first.
            let seen = previous == Some(schema_id)
                || known(&schema_id)
                || added.iter().any(|(id, _)| *id == schema_id)
                || failed.iter().any(|(id, _)| *id == schema_id);
            previous = Some(schema_id);
            if !seen {
                match writer(schema_id) {
                    Ok(schema) => added.push((schema_id, schema)),
                    Err(e) => failed.push((schema_id, e)),
                }
            }
        }
        NewWriters { added, failed }
    }
}

/// The writer schemas of the new IDs of a batch, and the new IDs whose writer schema fails.
struct NewWriters {
    added: Vec<(u32, Arc<str>)>,
    failed: Vec<(u32, String)>,
}

/// Stateful datum encoder for one writer worker.
///
/// It keeps the `arrow-avro` encoder and the builder of the one-row batches. `arrow-avro`
/// encodes only record batches, so each row becomes a batch of one row.
#[derive(Default)]
pub struct DatumEncoder(Option<Writer>);

struct Writer {
    encoder: BatchEncoder,
    rows: RowBatchBuilder,
    conform: Conform,
}

/// How a batch of the row schema becomes a batch of the encoder schema.
enum Conform {
    /// The fields are equal, so the encoder takes the batch as it is.
    Same,
    /// The column types are equal, so the columns get the encoder schema with no cast.
    Relabel(SchemaRef),
    /// The column types differ, for example strings for an Avro enum, so each column is cast.
    Cast(SchemaRef),
}

impl DatumEncoder {
    /// Encodes `row`, a row of `row_schema`, into `buffer` using `avro_schema` and `format`.
    ///
    /// The first call builds the encoder. Later calls use the same encoder, so give each call
    /// the same `avro_schema`, `format` and `row_schema`.
    ///
    /// # Errors
    ///
    /// Fails if `row` does not match `avro_schema` or cannot encode.
    pub fn encode_row(
        &mut self,
        avro_schema: &Arc<str>,
        format: WireFormat,
        row_schema: &Schema,
        row: &RowElement,
        buffer: &mut Vec<u8>,
    ) -> Result<(), String> {
        self.encode_with(avro_schema, format, row_schema, buffer, |rows| {
            rows.append(row)
        })
    }

    /// Encodes `row` as [`encode_row`](Self::encode_row) does.
    ///
    /// # Errors
    ///
    /// Fails if `row` does not match `avro_schema` or cannot encode.
    pub fn encode_dynamic_row(
        &mut self,
        avro_schema: &Arc<str>,
        format: WireFormat,
        row_schema: &Schema,
        row: &Row,
        buffer: &mut Vec<u8>,
    ) -> Result<(), String> {
        self.encode_with(avro_schema, format, row_schema, buffer, |rows| {
            rows.append_row(row)
        })
    }

    /// Encodes `value`, a value of a `#[derive(BeamRow)]` type, with no [`Row`] between them.
    /// `row_schema` is the schema of `T`. See [`encode_row`](Self::encode_row).
    ///
    /// # Errors
    ///
    /// Fails if `value` does not match `avro_schema` or cannot encode.
    pub fn encode_beam_row<T: BeamRow>(
        &mut self,
        avro_schema: &Arc<str>,
        format: WireFormat,
        row_schema: &Schema,
        value: &T,
        buffer: &mut Vec<u8>,
    ) -> Result<(), String> {
        self.encode_with(avro_schema, format, row_schema, buffer, |rows| {
            rows.append_beam_row(value)
        })
    }

    fn encode_with(
        &mut self,
        avro_schema: &Arc<str>,
        format: WireFormat,
        row_schema: &Schema,
        buffer: &mut Vec<u8>,
        append: impl FnOnce(&mut RowBatchBuilder) -> beam_io_arrow::Result<()>,
    ) -> Result<(), String> {
        let writer = match &mut self.0 {
            Some(writer) => writer,
            slot @ None => slot.insert(Writer::new(avro_schema, format, row_schema)?),
        };
        let encoded = writer.encode(append, buffer);
        if encoded.is_err() {
            // The builder or the encoder can hold part of a row after a failure; drop them.
            self.0 = None;
        }
        encoded
    }
}

impl Writer {
    fn new(
        avro_schema: &Arc<str>,
        format: WireFormat,
        row_schema: &Schema,
    ) -> Result<Self, String> {
        let encoder = build_encoder(avro_schema, format)?;
        let source = beam_io_arrow::beam_to_arrow_schema(row_schema).map_err(|e| e.to_string())?;
        let target = encoder.schema();
        let same_types = source.fields().len() == target.fields().len()
            && source
                .fields()
                .iter()
                .zip(target.fields())
                .all(|(source, target)| source.data_type() == target.data_type());
        let conform = match (source.fields() == target.fields(), same_types) {
            (true, _) => Conform::Same,
            (false, true) => Conform::Relabel(target),
            (false, false) => Conform::Cast(target),
        };
        Ok(Self {
            encoder,
            rows: RowBatchBuilder::new(row_schema).map_err(|e| e.to_string())?,
            conform,
        })
    }

    fn encode(
        &mut self,
        append: impl FnOnce(&mut RowBatchBuilder) -> beam_io_arrow::Result<()>,
        buffer: &mut Vec<u8>,
    ) -> Result<(), String> {
        append(&mut self.rows).map_err(|e| e.to_string())?;
        let batch = self.rows.finish().map_err(|e| e.to_string())?;
        let batch = match &self.conform {
            Conform::Same => batch,
            Conform::Relabel(schema) => {
                let (_, columns, _) = batch.into_parts();
                RecordBatch::try_new(Arc::clone(schema), columns).map_err(avro_error)?
            }
            Conform::Cast(schema) => cast_batch(&batch, Arc::clone(schema))?,
        };
        self.encoder.encode(&batch).map_err(avro_error)?;
        // The batch has one row, so the encoded bytes are its datum.
        let encoded = self.encoder.flush();
        buffer.clear();
        buffer.extend_from_slice(encoded.bytes());
        Ok(())
    }
}

/// Decodes `frames`, whose writer schemas the reader has or `added` gives.
fn decode_known(
    slot: &mut Option<Reader>,
    read_schema: &Arc<str>,
    row_schema: &Arc<Schema>,
    added: Vec<(u32, Arc<str>)>,
    frames: &[Frame<'_>],
) -> Vec<Result<Row, String>> {
    if frames.is_empty() {
        return Vec::new();
    }
    match reader_for(slot, read_schema, added) {
        Ok(reader) => decode_isolating(reader, read_schema, row_schema, frames),
        Err(e) => frames.iter().map(|_| Err(e.clone())).collect(),
    }
}

/// The reader in `slot`, built again when `added` has writer schemas.
fn reader_for<'a>(
    slot: &'a mut Option<Reader>,
    read_schema: &Arc<str>,
    added: Vec<(u32, Arc<str>)>,
) -> Result<&'a mut Reader, String> {
    let reader = match (slot.take(), added.is_empty()) {
        (Some(reader), true) => reader,
        (old, _) => {
            let mut writers = old.map(|old| old.writers).unwrap_or_default();
            writers.extend(added);
            Reader {
                decoder: build_decoder(read_schema, &writers)?,
                writers,
            }
        }
    };
    Ok(slot.insert(reader))
}

/// Decodes `frames` as one batch. After a frame fails, it builds the decoder again, decodes
/// the frames before the failed one again, and goes on after it.
fn decode_isolating(
    reader: &mut Reader,
    read_schema: &Arc<str>,
    row_schema: &Arc<Schema>,
    frames: &[Frame<'_>],
) -> Vec<Result<Row, String>> {
    let mut out = Vec::with_capacity(frames.len());
    let mut rest = frames;
    while !rest.is_empty() {
        let (index, error) = match decode_batch(&mut reader.decoder, rest, row_schema) {
            Ok(rows) => {
                out.extend(rows);
                break;
            }
            Err(failure) => failure,
        };
        // The decoder can hold part of a datum after a failure.
        match build_decoder(read_schema, &reader.writers) {
            Ok(decoder) => reader.decoder = decoder,
            Err(e) => {
                out.extend(rest.iter().map(|_| Err(e.clone())));
                break;
            }
        }
        out.extend(decode_isolating(
            reader,
            read_schema,
            row_schema,
            &rest[..index],
        ));
        out.push(Err(error));
        rest = &rest[index + 1..];
    }
    out
}

fn build_decoder(
    read_schema: &Arc<str>,
    writers: &HashMap<u32, Arc<str>>,
) -> Result<BatchDecoder, String> {
    let mut store = SchemaStore::new_with_type(FingerprintAlgorithm::Id);
    for (id, writer) in writers {
        store
            .set(Fingerprint::Id(*id), AvroSchema::new(writer.to_string()))
            .map_err(|e| format!("The writer schema {id}: {e}"))?;
    }
    ReaderBuilder::new()
        .with_batch_size(BATCH_ROWS)
        .with_reader_schema(AvroSchema::new(read_schema.to_string()))
        .with_writer_schema_store(store)
        .build_decoder()
        .map_err(|e| format!("The Avro schemas do not resolve: {e}"))
}

/// Decodes `frames` into rows, or fails with the index of the first frame that fails.
fn decode_batch(
    decoder: &mut BatchDecoder,
    frames: &[Frame<'_>],
    row_schema: &Arc<Schema>,
) -> Result<Vec<Result<Row, String>>, (usize, String)> {
    let mut batches = Vec::new();
    let mut flushed = 0;
    for (index, &Frame { schema_id, datum }) in frames.iter().enumerate() {
        // The decoder takes the header and the datum in two calls, so the datum is not copied.
        let [a, b, c, d] = schema_id.to_be_bytes();
        let header = [CONFLUENT_MAGIC, a, b, c, d];
        feed(decoder, &header, &mut batches, &mut flushed)
            .and_then(|()| feed(decoder, datum, &mut batches, &mut flushed))
            .and_then(|()| {
                if flushed + decoder.batch_size() - decoder.capacity() == index + 1 {
                    Ok(())
                } else {
                    Err(NOT_ONE_DATUM.to_string())
                }
            })
            .map_err(|e| (index, e))?;
    }
    let last = frames.len().saturating_sub(1);
    batches.extend(decoder.flush().map_err(|e| (last, avro_error(e)))?);
    Ok(batches
        .iter()
        .flat_map(
            |batch| match beam_io_arrow::record_batch_to_rows(batch, row_schema) {
                Ok(rows) => rows.into_iter().map(Ok).collect::<Vec<_>>(),
                Err(e) => (0..batch.num_rows()).map(|_| Err(e.to_string())).collect(),
            },
        )
        .collect())
}

/// Gives all of `bytes` to `decoder`, and flushes a full batch into `batches`.
fn feed(
    decoder: &mut BatchDecoder,
    bytes: &[u8],
    batches: &mut Vec<RecordBatch>,
    flushed: &mut usize,
) -> Result<(), String> {
    let mut rest = bytes;
    while !rest.is_empty() {
        let consumed = decoder.decode(rest).map_err(avro_error)?;
        rest = &rest[consumed..];
        match (rest.is_empty(), decoder.batch_is_empty()) {
            (true, _) => {}
            (false, false) => {
                let batch = decoder.flush().map_err(avro_error)?;
                *flushed += batch.as_ref().map_or(0, RecordBatch::num_rows);
                batches.extend(batch);
            }
            (false, true) => return Err(NOT_ONE_DATUM.to_string()),
        }
    }
    Ok(())
}

fn build_encoder(avro_schema: &Arc<str>, format: WireFormat) -> Result<BatchEncoder, String> {
    let arrow_schema = build_decoder(
        avro_schema,
        &HashMap::from([(PLAIN_ID, Arc::clone(avro_schema))]),
    )?
    .schema();
    let metadata = HashMap::from([(SCHEMA_METADATA_KEY.to_string(), avro_schema.to_string())]);
    let builder = WriterBuilder::new(ArrowSchema::new_with_metadata(
        arrow_schema.fields().clone(),
        metadata,
    ));
    match format {
        WireFormat::Plain => builder.build_encoder::<AvroBinaryFormat>(),
        WireFormat::Confluent(id) => builder
            .with_fingerprint_strategy(FingerprintStrategy::Id(id))
            .build_encoder::<AvroSoeFormat>(),
    }
    .map_err(|e| format!("Avro: {e}"))
}

fn cast_batch(batch: &RecordBatch, schema: SchemaRef) -> Result<RecordBatch, String> {
    let columns = cast_columns(batch.columns(), schema.fields())?;
    RecordBatch::try_new(schema, columns).map_err(avro_error)
}

fn cast_columns(columns: &[ArrayRef], fields: &Fields) -> Result<Vec<ArrayRef>, String> {
    columns
        .iter()
        .zip(fields)
        .map(|(column, field)| cast_column(column, field))
        .collect()
}

/// Casts `column` to the Arrow type of `field`, building full symbol dictionaries for enums
/// at top level and inside records, arrays and maps.
fn cast_column(column: &ArrayRef, field: &Field) -> Result<ArrayRef, String> {
    if column.data_type() == field.data_type() {
        // Shares the `Arc` buffer without copying.
        return Ok(Arc::clone(column));
    }
    let array: ArrayRef = match (
        field.metadata().get(AVRO_ENUM_SYMBOLS_METADATA_KEY),
        field.data_type(),
    ) {
        (Some(symbols), _) => return enum_column(column, symbols),
        (None, DataType::Struct(fields)) => match column.as_struct_opt() {
            Some(array) => Arc::new(cast_struct(array, fields)?),
            None => return cast_other(column, field),
        },
        (None, DataType::List(item)) => match column.as_list_opt::<i32>() {
            Some(array) => Arc::new(
                ListArray::try_new(
                    Arc::clone(item),
                    array.offsets().clone(),
                    cast_column(array.values(), item)?,
                    array.nulls().cloned(),
                )
                .map_err(avro_error)?,
            ),
            None => return cast_other(column, field),
        },
        (None, DataType::Map(entries, sorted)) => {
            match (column.as_map_opt(), entries.data_type()) {
                (Some(array), DataType::Struct(fields)) => Arc::new(
                    MapArray::try_new(
                        Arc::clone(entries),
                        array.offsets().clone(),
                        cast_struct(array.entries(), fields)?,
                        array.nulls().cloned(),
                        *sorted,
                    )
                    .map_err(avro_error)?,
                ),
                _ => return cast_other(column, field),
            }
        }
        (None, _) => return cast_other(column, field),
    };
    Ok(array)
}

fn cast_struct(array: &StructArray, fields: &Fields) -> Result<StructArray, String> {
    let children = cast_columns(array.columns(), fields)?;
    StructArray::try_new(fields.clone(), children, array.nulls().cloned()).map_err(avro_error)
}

fn cast_other(column: &ArrayRef, field: &Field) -> Result<ArrayRef, String> {
    arrow_cast::cast(column, field.data_type()).map_err(avro_error)
}

fn avro_error(e: impl std::fmt::Display) -> String {
    format!("Avro: {e}")
}

fn enum_column(column: &ArrayRef, symbols: &str) -> Result<ArrayRef, String> {
    let symbols: Vec<String> =
        serde_json::from_str(symbols).map_err(|e| format!("The enum symbols are not JSON: {e}"))?;
    let strings = arrow_cast::cast(column, &DataType::Utf8).map_err(avro_error)?;
    let strings = strings
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| "An enum column is not strings".to_string())?;
    let keys = strings
        .iter()
        .map(|value| {
            value
                .map(|value| {
                    symbols
                        .iter()
                        .position(|symbol| symbol == value)
                        .and_then(|index| i32::try_from(index).ok())
                        .ok_or_else(|| format!("'{value}' is not a symbol of the enum"))
                })
                .transpose()
        })
        .collect::<Result<Int32Array, _>>()?;
    DictionaryArray::<Int32Type>::try_new(keys, Arc::new(StringArray::from(symbols)))
        .map(|array| Arc::new(array) as ArrayRef)
        .map_err(avro_error)
}
