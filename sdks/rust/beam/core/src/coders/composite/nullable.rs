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

//! Coder for `Option<T>` (`beam:coder:nullable:v1`).

use std::io::Write;
use std::sync::Arc;

use crate::coders::URN_NULLABLE;
use crate::coders::standard::read_array;
use crate::coders::traits::{Coder, CoderError, CoderRegistry, Context, DefaultCoder};

fn decode_nullable<T>(
    reader: &mut &[u8],
    decode_some: impl FnOnce(&mut &[u8]) -> Result<T, CoderError>,
) -> Result<Option<T>, CoderError> {
    let [tag] = read_array(reader)?;
    match tag {
        0x00 => Ok(None),
        0x01 => decode_some(reader).map(Some),
        other => Err(CoderError::Format(format!("Invalid nullable tag: {other}"))),
    }
}

/// Coder for `Option<T>`: `0x00` for `None`, or `0x01` and then the value for `Some`.
#[derive(Clone, Debug)]
pub struct NullableCoder<T, C: Coder<T>> {
    inner: C,
    _marker: std::marker::PhantomData<T>,
}

impl<T, C: Coder<T>> NullableCoder<T, C> {
    pub fn new(inner: C) -> Self {
        Self {
            inner,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<T: Send + Sync + 'static, C: Coder<T>> Coder<Option<T>> for NullableCoder<T, C> {
    fn urn(&self) -> &'static str {
        URN_NULLABLE
    }

    fn encode(
        &self,
        value: &Option<T>,
        writer: &mut Vec<u8>,
        context: Context,
    ) -> Result<(), CoderError> {
        match value {
            None => Ok(writer.write_all(&[0x00])?),
            Some(inner) => {
                writer.write_all(&[0x01])?;
                self.inner.encode(inner, writer, context)
            }
        }
    }

    fn decode(&self, reader: &mut &[u8], context: Context) -> Result<Option<T>, CoderError> {
        decode_nullable(reader, |r| self.inner.decode(r, context))
    }
}

impl<T: DefaultCoder> DefaultCoder for Option<T> {
    type Coder = NullableCoder<T, T::Coder>;
    fn coder() -> Self::Coder {
        NullableCoder::new(T::coder())
    }

    fn encode_element(&self, writer: &mut Vec<u8>) -> Result<(), CoderError> {
        match self {
            None => Ok(writer.write_all(&[0x00])?),
            Some(inner) => {
                writer.write_all(&[0x01])?;
                inner.encode_element(writer)
            }
        }
    }

    fn decode_element(reader: &mut &[u8]) -> Result<Self, CoderError> {
        decode_nullable(reader, T::decode_element)
    }

    fn decode_element_with_schema(
        reader: &mut &[u8],
        schema: Option<&Arc<crate::schema::Schema>>,
    ) -> Result<Self, CoderError> {
        decode_nullable(reader, |r| T::decode_element_with_schema(r, schema))
    }

    fn decode_element_with_context(
        reader: &mut &[u8],
        schema: Option<&Arc<crate::schema::Schema>>,
        state_reader: Option<&Arc<dyn crate::coders::iterable::StateStreamReader>>,
    ) -> Result<Self, CoderError> {
        decode_nullable(reader, |r| {
            T::decode_element_with_context(r, schema, state_reader)
        })
    }

    fn register_coder<R: CoderRegistry + ?Sized>(registry: &R) -> String {
        let elem_id = T::register_coder(registry);
        registry.register_coder(URN_NULLABLE, vec![elem_id])
    }

    fn register_value_coder<R: CoderRegistry + ?Sized>(&self, registry: &R) -> String {
        let elem_id = match self {
            Some(inner) => inner.register_value_coder(registry),
            None => T::register_coder(registry),
        };
        registry.register_coder(URN_NULLABLE, vec![elem_id])
    }
}
