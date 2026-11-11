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

//! Record values that are Avro binary datums ([`Avro`]), on [`beam_io_avro::datum`].
//!
//! The read decodes the records of one poll as one batch, and the write encodes each row into
//! a plain datum or a Confluent frame. See [`beam_io_avro::datum`] for the Avro-to-Beam schema
//! mapping.
//!
//! # Schema registry
//!
//! With a [`SchemaRegistry`], the record values are in the Confluent wire format: byte `0`,
//! the schema ID (4 bytes, big-endian), then the datum. The read fetches the writer schema of
//! each new ID from the registry once and resolves it to the read schema of the codec. After a
//! failed call, the read uses the same error for a short time, and the time doubles with each
//! failure (at most 30 seconds). The write puts the ID of its schema in front of each datum: the
//! ID of [`Avro::latest`], or the ID returned when the write registers its schema
//! ([`Avro::with_subject`]).

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError, RwLock};
use std::time::{Duration, Instant};

use beam::schema::{BeamRow, FieldType, FieldValue, Row, RowElement, Schema};
use beam::values::PCollection;
use beam_io_avro::datum::{
    BATCH_ROWS, CONFLUENT_MAGIC, DatumDecoder, DatumEncoder, Frame, PLAIN_ID, WireFormat,
    avro_schema, beam_schema, check_encoder, check_fields, check_writer_schema,
};

use super::row_input::{RowInput, bind_rows};
use super::{
    CodecError, Decoder, Encoder, Outgoing, RecordRef, RegisteredSchema, RowEncoder, SchemaRegistry,
};

/// Why a read of this codec with no schema fails.
const NO_SCHEMA: &str = "An Avro read needs an Avro schema: use Avro::schema";

/// How the datum sits in the record value.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Framing {
    /// The datum alone.
    #[default]
    Plain,
    /// The Confluent wire format: byte `0`, the schema ID (4 bytes, big-endian), the datum.
    /// The write puts the header in front of the datum. Without a registry, the read checks
    /// the ID if the framing has one, and reads every datum with the schema of the codec. With
    /// a registry, see the module documentation.
    Confluent { schema_id: Option<u32> },
}

/// Record values that are Avro binary datums, read into and written from `Row`s.
///
/// The read needs the Avro schema of the datums ([`Avro::schema`] or [`Avro::latest`]). The
/// write takes rows of any schema ([`Avro::new`]) and writes the datum of the Avro schema of
/// their Beam schema, or of a given Avro schema whose fields match the fields of the rows.
#[derive(Clone, Debug, Default)]
pub struct Avro {
    /// The Avro schema in JSON, from the caller, the registry or the input of the write.
    schema: Option<Arc<str>>,
    /// The Beam schema of the rows.
    row_schema: Option<Arc<Schema>>,
    framing: Framing,
    input: RowInput,
    registry: Option<Arc<Registry>>,
    /// The subject under which the write registers its schema.
    subject: Option<String>,
}

/// A schema registry, and the writer schemas that the read got from it.
#[derive(Debug)]
struct Registry {
    client: SchemaRegistry,
    /// The lookup of each writer schema ID. Every reader thread shares them.
    schemas: RwLock<HashMap<u32, Arc<Mutex<Lookup>>>>,
}

/// What the read knows about one writer schema ID.
#[derive(Debug, Default)]
enum Lookup {
    #[default]
    Unknown,
    /// The writer schema, or why it does not resolve to the read schema. It does not change.
    Resolved(Result<Arc<str>, CodecError>),
    /// The last call to the registry failed. No call happens before `retry_at`.
    Failed {
        error: CodecError,
        retry_at: Instant,
        backoff: Duration,
    },
}

/// The wait after the first failed call to the registry for an ID.
const FIRST_BACKOFF: Duration = Duration::from_secs(1);

/// The longest wait between two failed calls to the registry for an ID.
const MAX_BACKOFF: Duration = Duration::from_secs(30);

impl Registry {
    /// The writer schema with ID `id`, if it resolves to `read_schema`.
    ///
    /// Only one thread calls the registry for an ID. The other threads that need the ID wait
    /// for that call, and then use its result.
    fn schema(&self, id: u32, read_schema: &Arc<str>) -> Result<Arc<str>, CodecError> {
        let lookup = self.lookup(id);
        let mut lookup = lookup.lock().unwrap_or_else(PoisonError::into_inner);
        let backoff = match &*lookup {
            Lookup::Resolved(schema) => return schema.clone(),
            Lookup::Failed {
                error, retry_at, ..
            } if Instant::now() < *retry_at => return Err(error.clone()),
            Lookup::Failed { backoff, .. } => backoff.saturating_mul(2).min(MAX_BACKOFF),
            Lookup::Unknown => FIRST_BACKOFF,
        };
        let (next, schema) = match self.client.schema(id) {
            Ok(schema) => {
                let schema: Arc<str> = schema.into();
                let resolved = check_writer_schema(read_schema, id, &schema)
                    .map(|()| schema)
                    .map_err(CodecError::new);
                (Lookup::Resolved(resolved.clone()), resolved)
            }
            Err(e) => {
                let error = CodecError::new(e.to_string());
                let failed = Lookup::Failed {
                    error: error.clone(),
                    retry_at: Instant::now() + backoff,
                    backoff,
                };
                (failed, Err(error))
            }
        };
        *lookup = next;
        schema
    }

    /// The lookup of `id`, added when the map does not have it.
    fn lookup(&self, id: u32) -> Arc<Mutex<Lookup>> {
        let found = self
            .schemas
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&id)
            .cloned();
        found.unwrap_or_else(|| {
            let mut schemas = self.schemas.write().unwrap_or_else(PoisonError::into_inner);
            Arc::clone(schemas.entry(id).or_default())
        })
    }
}

impl Avro {
    /// The codec of a write: rows of any schema, as datums of the Avro schema of their Beam
    /// schema (a nullable field is a union with `null`).
    pub fn new() -> Self {
        Self::default()
    }

    /// The codec of the datums of the Avro schema `avro_schema`, a record schema in JSON.
    ///
    /// # Errors
    ///
    /// Fails if `avro_schema` is not a record schema that maps to a Beam schema.
    pub fn schema(avro_schema: &str) -> beam::Result<Self> {
        let row_schema = beam_schema(avro_schema).map_err(beam::Error::msg)?;
        Ok(Self {
            schema: Some(avro_schema.into()),
            row_schema: Some(row_schema),
            ..Self::default()
        })
    }

    /// The codec of the latest schema of `subject` in `registry`, in the Confluent wire
    /// format. The read gives rows of that schema. The write writes rows of that schema with
    /// its ID. The codec calls the registry here, so the pipeline needs the registry when it
    /// is built.
    ///
    /// # Errors
    ///
    /// Fails if the registry call fails, or the schema is not a record schema that maps to a
    /// Beam schema.
    pub fn latest(registry: SchemaRegistry, subject: &str) -> beam::Result<Self> {
        let RegisteredSchema { id, schema } = registry.latest(subject)?;
        Ok(Self::schema(&schema)?
            .with_framing(Framing::Confluent {
                schema_id: Some(id),
            })
            .with_registry(registry))
    }

    /// The same codec, in the Confluent wire format with the schemas of `registry`. The read
    /// decodes each datum with the schema of its ID. The write needs [`Avro::latest`] or
    /// [`Avro::with_subject`] for the ID that it writes.
    pub fn with_registry(mut self, registry: SchemaRegistry) -> Self {
        self.framing = match self.framing {
            Framing::Plain => Framing::Confluent { schema_id: None },
            confluent @ Framing::Confluent { .. } => confluent,
        };
        self.registry = Some(Arc::new(Registry {
            client: registry,
            schemas: RwLock::default(),
        }));
        self
    }

    /// The same codec, where the write registers its schema under `subject`, for example
    /// `<topic>-value`, and writes the ID that the registry gives. The write registers the
    /// schema when it expands, so the pipeline needs the registry when it is built. Set a
    /// registry with [`Avro::with_registry`].
    pub fn with_subject(mut self, subject: impl Into<String>) -> Self {
        self.subject = Some(subject.into());
        self
    }

    /// The same codec, with the datums in the record values as `framing` says.
    pub fn with_framing(mut self, framing: Framing) -> Self {
        self.framing = framing;
        self
    }

    /// The Beam schema of the rows, if the codec has an Avro schema.
    pub fn row_schema(&self) -> Option<Arc<Schema>> {
        self.row_schema.clone()
    }

    /// The writer schema of the datums with the ID `id`.
    fn writer_schema(&self, id: u32, read_schema: &Arc<str>) -> Result<Arc<str>, CodecError> {
        match (&self.registry, self.framing) {
            (Some(registry), Framing::Confluent { schema_id }) if schema_id != Some(id) => {
                registry.schema(id, read_schema)
            }
            (Some(_), Framing::Confluent { .. })
            | (None, Framing::Confluent { .. })
            | (Some(_) | None, Framing::Plain) => Ok(Arc::clone(read_schema)),
        }
    }

    /// The datum in `value` and its schema ID. A plain datum gets [`PLAIN_ID`].
    fn frame<'a>(&self, value: &'a [u8]) -> Result<Frame<'a>, CodecError> {
        let schema_id = match self.framing {
            Framing::Plain => {
                return Ok(Frame {
                    schema_id: PLAIN_ID,
                    datum: value,
                });
            }
            Framing::Confluent { schema_id } => schema_id,
        };
        let [CONFLUENT_MAGIC, a, b, c, d, datum @ ..] = value else {
            return Err(CodecError::new(
                "The value is not in the Confluent wire format (magic byte 0 and a schema ID)",
            ));
        };
        let found = u32::from_be_bytes([*a, *b, *c, *d]);
        match (schema_id, &self.registry) {
            (Some(expected), None) if expected != found => Err(CodecError::new(format!(
                "The value has the schema ID {found}, expected {expected}"
            ))),
            (Some(_), None) | (None, None) | (Some(_) | None, Some(_)) => Ok(Frame {
                schema_id: found,
                datum,
            }),
        }
    }

    /// The rows of `records`, one result for each record.
    fn read_rows(
        &self,
        decoder: &mut DatumDecoder,
        records: &[RecordRef<'_>],
    ) -> Vec<Result<Row, CodecError>> {
        let (Some(read_schema), Some(row_schema)) = (&self.schema, &self.row_schema) else {
            return records
                .iter()
                .map(|_| Err(CodecError::new(NO_SCHEMA)))
                .collect();
        };
        let slots: Vec<Result<Frame<'_>, CodecError>> = records
            .iter()
            .map(|record| self.frame(record.value_or_err()?))
            .collect();
        let frames: Vec<Frame<'_>> = slots
            .iter()
            .filter_map(|slot| slot.as_ref().ok().copied())
            .collect();
        let writer = |id| {
            self.writer_schema(id, read_schema)
                .map_err(|e| e.message().to_string())
        };
        let mut rows = decoder
            .decode_frames(read_schema, row_schema, &frames, writer)
            .into_iter();
        slots
            .into_iter()
            .map(|slot| match slot {
                Ok(_) => rows
                    .next()
                    .unwrap_or_else(|| Err("The batch gave fewer rows than records".to_string()))
                    .map_err(CodecError::new),
                Err(e) => Err(e),
            })
            .collect()
    }

    /// Registers the schema of the write under its subject, if it has one, and keeps the ID.
    fn register(&mut self, schema: &str) -> beam::Result {
        let Some(subject) = &self.subject else {
            return Ok(());
        };
        let registry = self.registry.as_ref().ok_or_else(|| {
            beam::Error::msg("Set a schema registry to register the schema of the subject")
        })?;
        let id = registry.client.register(subject, schema)?;
        self.framing = Framing::Confluent {
            schema_id: Some(id),
        };
        Ok(())
    }

    fn wire_format(&self) -> Result<WireFormat, CodecError> {
        match self.framing {
            Framing::Plain => Ok(WireFormat::Plain),
            Framing::Confluent { schema_id } => {
                schema_id.map(WireFormat::Confluent).ok_or_else(|| {
                    CodecError::new("A write in the Confluent wire format needs the schema ID")
                })
            }
        }
    }

    /// The Avro schema and the wire format of the write.
    fn writer(&self) -> Result<(&Arc<str>, WireFormat), CodecError> {
        let avro_schema = self
            .schema
            .as_ref()
            .ok_or_else(|| CodecError::new("The encoder has no Avro schema yet"))?;
        Ok((avro_schema, self.wire_format()?))
    }
}

impl Decoder for Avro {
    type Element = RowElement;
    type Output = Row;
    type State = DatumDecoder;

    fn validate(&self) -> beam::Result {
        match self.schema {
            Some(_) => Ok(()),
            None => Err(beam::Error::msg(NO_SCHEMA)),
        }
    }

    fn decode(
        &self,
        state: &mut DatumDecoder,
        record: &RecordRef<'_>,
    ) -> Result<RowElement, CodecError> {
        self.read_rows(state, std::slice::from_ref(record))
            .pop()
            .unwrap_or_else(|| Err(CodecError::new("The record gave no row")))
            .map(RowElement::Row)
    }

    fn decode_batch(
        &self,
        state: &mut DatumDecoder,
        records: &[RecordRef<'_>],
        out: &mut Vec<Result<RowElement, CodecError>>,
    ) {
        out.extend(
            records
                .chunks(BATCH_ROWS)
                .flat_map(|chunk| self.read_rows(state, chunk))
                .map(|row| row.map(RowElement::Row)),
        );
    }

    fn decodes_batches(&self) -> bool {
        true
    }

    fn output(&self, elements: PCollection<RowElement>) -> PCollection<Row> {
        let rows = elements.as_rows();
        match &self.row_schema {
            Some(schema) => rows.with_row_schema(schema),
            None => rows,
        }
    }
}

impl Encoder for Avro {
    type Input = Row;
    type Element = RowElement;
    type State = DatumEncoder;

    fn elements(&mut self, input: &PCollection<Row>) -> beam::Result<PCollection<RowElement>> {
        bind_rows(self, input)
    }

    fn encode<'a>(
        &self,
        state: &mut DatumEncoder,
        element: &'a RowElement,
        buffer: &'a mut Vec<u8>,
    ) -> Result<Outgoing<'a>, CodecError> {
        let (avro_schema, format) = self.writer()?;
        state
            .encode_row(avro_schema, format, self.input.schema()?, element, buffer)
            .map_err(CodecError::new)?;
        Ok(Outgoing::value(buffer))
    }

    fn failed_type(&self) -> FieldType {
        self.input.failed_type()
    }

    fn failed_value(&self, element: RowElement) -> Result<FieldValue, CodecError> {
        self.input.failed_value(element)
    }
}

impl RowEncoder for Avro {
    fn bind_schema(&mut self, schema: Arc<Schema>) -> beam::Result {
        let avro_schema = match (&self.schema, &self.row_schema) {
            (Some(existing), Some(row_schema)) => {
                check_fields(row_schema, &schema).map_err(beam::Error::msg)?;
                Arc::clone(existing)
            }
            _ => {
                let derived: Arc<str> = avro_schema(&schema).map_err(beam::Error::msg)?.into();
                self.schema = Some(Arc::clone(&derived));
                self.row_schema = Some(Arc::clone(&schema));
                derived
            }
        };
        self.input.bind(schema);
        self.register(&avro_schema)?;
        match (&self.registry, self.framing) {
            (Some(_), Framing::Confluent { schema_id: None }) => Err(beam::Error::msg(
                "A write with a schema registry needs a schema ID: use Avro::latest or \
                 Avro::with_subject",
            )),
            _ => {
                let format = self.wire_format().map_err(beam::Error::msg)?;
                check_encoder(&avro_schema, format).map_err(beam::Error::msg)
            }
        }
    }

    fn encode_row<'a>(
        &self,
        state: &mut DatumEncoder,
        row: &Row,
        buffer: &'a mut Vec<u8>,
    ) -> Result<Outgoing<'a>, CodecError> {
        let (avro_schema, format) = self.writer()?;
        state
            .encode_dynamic_row(avro_schema, format, self.input.schema()?, row, buffer)
            .map_err(CodecError::new)?;
        Ok(Outgoing::value(buffer))
    }

    /// Fills the Arrow columns from the row coder bytes of `element`, with no `Row` between them.
    fn encode_typed<'a, T: BeamRow>(
        &self,
        state: &mut DatumEncoder,
        element: &T,
        buffer: &'a mut Vec<u8>,
    ) -> Result<Outgoing<'a>, CodecError> {
        let (avro_schema, format) = self.writer()?;
        state
            .encode_beam_row(avro_schema, format, self.input.schema()?, element, buffer)
            .map_err(CodecError::new)?;
        Ok(Outgoing::value(buffer))
    }
}
