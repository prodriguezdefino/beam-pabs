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

//! Adapts a `Row` codec (`Json`, `Avro`, `Proto`, `BeamRow`, `Payload`) to a
//! `#[derive(BeamRow)]` struct `T`.

use std::fmt;
use std::marker::PhantomData;
use std::sync::Arc;

use beam::coders::{DefaultCoder, FieldMapCache};
use beam::schema::{BeamRow, FieldType, FieldValue, Row, RowElement, Schema};
use beam::values::PCollection;

use super::{CodecError, Decoder, Encoder, Outgoing, RecordRef, RowEncoder};

/// Wraps a row value codec `C` to read and write values of `T: BeamRow`.
///
/// The read takes any [`Decoder`] of `RowElement`s. It decodes encoded rows with the
/// [`encoded_row_schema`](Decoder::encoded_row_schema) of `C`. The write takes any
/// [`RowEncoder`]. Combine with [`WithMetadata`](super::WithMetadata)
/// (`WithMetadata::new(Typed::new(codec))`) to read or write keys, headers and timestamps.
///
/// ```
/// use beam::prelude::*;
/// use beam_io_kafka::codec::{Json, Typed};
///
/// #[derive(Clone, Debug, BeamRow)]
/// struct Event {
///     id: i64,
/// }
///
/// let codec: Typed<Json, Event> = Typed::new(Json::new());
/// ```
pub struct Typed<C, T> {
    inner: C,
    _type: PhantomData<fn(T) -> T>,
}

impl<C, T> Typed<C, T> {
    /// Adapts `inner` to decode into `T` and encode from `T`.
    pub fn new(inner: C) -> Self {
        Self {
            inner,
            _type: PhantomData,
        }
    }
}

impl<C: Clone, T> Clone for Typed<C, T> {
    fn clone(&self) -> Self {
        Self::new(self.inner.clone())
    }
}

impl<C: fmt::Debug, T> fmt::Debug for Typed<C, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Typed").field("inner", &self.inner).finish()
    }
}

/// Why an encoded row with no schema does not convert.
const NO_ENCODED_SCHEMA: &str = "The decoder gives encoded rows without a schema";

fn codec_error(error: impl fmt::Display) -> CodecError {
    CodecError::new(error.to_string())
}

fn convert_row_element<T: BeamRow + DefaultCoder>(
    element: RowElement,
    writer_schema: Option<&Arc<Schema>>,
    cache: &mut FieldMapCache,
) -> Result<T, CodecError> {
    if matches!(element, RowElement::Encoded(_)) && writer_schema.is_none() {
        return Err(CodecError::new(NO_ENCODED_SCHEMA));
    }
    cache
        .from_row_element::<T>(element, writer_schema)
        .map_err(codec_error)
}

impl<C, T> Decoder for Typed<C, T>
where
    C: Decoder<Element = RowElement, Output = Row>,
    T: BeamRow + DefaultCoder,
{
    type Element = T;
    type Output = T;
    /// The state of `inner`, the field map of the last row schema, and the elements of `inner`
    /// for a batch.
    type State = (C::State, FieldMapCache, Vec<Result<RowElement, CodecError>>);

    fn validate(&self) -> beam::Result {
        self.inner.validate()
    }

    fn decode(
        &self,
        (inner_state, cache, _): &mut Self::State,
        record: &RecordRef<'_>,
    ) -> Result<T, CodecError> {
        let element = self.inner.decode(inner_state, record)?;
        convert_row_element::<T>(element, self.inner.encoded_row_schema(), cache)
    }

    fn decode_batch(
        &self,
        (inner_state, cache, raw): &mut Self::State,
        records: &[RecordRef<'_>],
        out: &mut Vec<Result<T, CodecError>>,
    ) {
        raw.clear();
        self.inner.decode_batch(inner_state, records, raw);
        out.extend(raw.drain(..).map(|res| {
            res.and_then(|el| convert_row_element::<T>(el, self.inner.encoded_row_schema(), cache))
        }));
    }

    fn reads_headers(&self) -> bool {
        self.inner.reads_headers()
    }

    fn decodes_batches(&self) -> bool {
        self.inner.decodes_batches()
    }

    fn output(&self, elements: PCollection<T>) -> PCollection<T> {
        elements
    }
}

impl<C, T> Encoder for Typed<C, T>
where
    C: RowEncoder,
    T: BeamRow + DefaultCoder,
{
    type Input = T;
    type Element = T;
    type State = C::State;

    fn elements(&mut self, input: &PCollection<T>) -> beam::Result<PCollection<T>> {
        self.prepare()?;
        Ok(input.clone())
    }

    fn prepare(&mut self) -> beam::Result {
        self.inner.bind_schema(T::beam_schema())
    }

    fn encode<'a>(
        &self,
        state: &mut C::State,
        element: &'a T,
        buffer: &'a mut Vec<u8>,
    ) -> Result<Outgoing<'a>, CodecError> {
        self.inner.encode_typed(state, element, buffer)
    }

    fn failed_type(&self) -> FieldType {
        FieldType::row(Arc::unwrap_or_clone(T::beam_schema()))
    }

    fn failed_value(&self, element: T) -> Result<FieldValue, CodecError> {
        element
            .to_row()
            .map(FieldValue::Row)
            .map_err(|e| CodecError::new(format!("Cannot convert the rejected value: {e}")))
    }
}
