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

//! Core traits and context definitions for Beam coders.

use std::io::{Read, Write};
use std::sync::Arc;
use thiserror::Error;

/// Error encountered during element encoding or decoding.
#[derive(Error, Debug)]
pub enum CoderError {
    #[error("IO error during encoding/decoding: {0}")]
    Io(#[from] std::io::Error),
    #[error("UTF-8 decoding error: {0}")]
    Utf8(#[from] std::string::FromUtf8Error),
    #[error("Schema error during encoding/decoding: {0}")]
    Schema(#[from] crate::schema::SchemaError),
    #[error("Decoding format error: {0}")]
    Format(String),
}

/// Position of an element in the byte stream, which sets how it is delimited. In `Nested`
/// context (inside a tuple, a KV or a stream), a variable-length element must have a length
/// prefix. In `WholeStream` context, the element extends to the end of the stream.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum Context {
    WholeStream,
    Nested,
}

/// Encodes and decodes values of type `T` in a Beam wire format.
pub trait Coder<T>: Send + Sync + 'static {
    /// Returns the Runner API coder URN, standard or custom.
    fn urn(&self) -> &'static str;

    fn encode(&self, value: &T, writer: &mut dyn Write, context: Context)
    -> Result<(), CoderError>;

    fn decode(&self, reader: &mut dyn Read, context: Context) -> Result<T, CoderError>;
}

/// Records coders in the Runner API pipeline components.
pub trait CoderRegistry {
    fn register_coder(&self, urn: &str, component_coder_ids: Vec<String>) -> String;

    fn register_coder_with_payload(
        &self,
        urn: &str,
        component_coder_ids: Vec<String>,
        payload: Vec<u8>,
    ) -> String {
        let _ = payload;
        self.register_coder(urn, component_coder_ids)
    }
}

impl<T: ?Sized + CoderRegistry> CoderRegistry for &T {
    fn register_coder(&self, urn: &str, component_coder_ids: Vec<String>) -> String {
        (**self).register_coder(urn, component_coder_ids)
    }

    fn register_coder_with_payload(
        &self,
        urn: &str,
        component_coder_ids: Vec<String>,
        payload: Vec<u8>,
    ) -> String {
        (**self).register_coder_with_payload(urn, component_coder_ids, payload)
    }
}

/// Associates a pipeline type with its default coder and gives byte-level encode and decode.
pub trait DefaultCoder: Send + Sync + Sized + 'static {
    type Coder: Coder<Self>;
    fn coder() -> Self::Coder;

    /// Encodes this value to element bytes.
    fn encode(&self) -> Result<Vec<u8>, CoderError> {
        let mut buf = Vec::new();
        self.encode_element(&mut buf)?;
        Ok(buf)
    }

    /// Decodes an element from bytes.
    fn decode(mut bytes: &[u8]) -> Result<Self, CoderError> {
        Self::decode_element(&mut bytes)
    }

    /// Encodes this value to a byte writer in the element context of the type.
    fn encode_element(&self, writer: &mut dyn Write) -> Result<(), CoderError>;

    /// Decodes an element from a byte reader.
    fn decode_element(reader: &mut dyn Read) -> Result<Self, CoderError>;

    /// Decodes with an optional schema. The default calls [`Self::decode_element`]. Override it
    /// for a type that needs a schema, such as [`Row`](crate::schema::Row), or wraps one.
    fn decode_element_with_schema(
        reader: &mut dyn Read,
        _schema: Option<&Arc<crate::schema::Schema>>,
    ) -> Result<Self, CoderError> {
        Self::decode_element(reader)
    }

    /// Decodes with an optional schema and state stream reader. The default calls
    /// [`Self::decode_element_with_schema`]. Override it for a type that reads state-backed
    /// iterables, such as [`BeamIterable`](crate::coders::BeamIterable) and [`Vec`], or wraps
    /// one, and pass the reader on so that continuation tokens can fetch the remaining pages.
    fn decode_element_with_context(
        reader: &mut dyn Read,
        schema: Option<&Arc<crate::schema::Schema>>,
        _state_reader: Option<&Arc<dyn super::iterable::StateStreamReader>>,
    ) -> Result<Self, CoderError> {
        Self::decode_element_with_schema(reader, schema)
    }

    /// Decodes an element from bytes with an optional schema context.
    fn decode_with_schema(
        mut bytes: &[u8],
        schema: Option<&Arc<crate::schema::Schema>>,
    ) -> Result<Self, CoderError> {
        Self::decode_element_with_schema(&mut bytes, schema)
    }

    /// Decodes an element from bytes with an optional schema and state stream reader.
    fn decode_with_context(
        mut bytes: &[u8],
        schema: Option<&Arc<crate::schema::Schema>>,
        state_reader: Option<&Arc<dyn super::iterable::StateStreamReader>>,
    ) -> Result<Self, CoderError> {
        Self::decode_element_with_context(&mut bytes, schema, state_reader)
    }

    /// Registers the coder for this type in `registry` and returns its coder ID.
    fn register_coder<R: CoderRegistry + ?Sized>(registry: &R) -> String;
}
