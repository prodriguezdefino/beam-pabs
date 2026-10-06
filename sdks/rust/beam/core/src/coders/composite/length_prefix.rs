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

//! Coders that write a varint byte length before the value.

use std::io::Write;
use std::sync::Arc;

use crate::coders::URN_LENGTH_PREFIX;
use crate::coders::standard::{VarIntCoder, read_exact_vec};
use crate::coders::traits::{Coder, CoderError, CoderRegistry, Context, DefaultCoder};

/// Coder that writes a varint byte length before the encoded value.
#[derive(Clone, Debug)]
pub struct LengthPrefixCoder<T, C: Coder<T>> {
    inner: C,
    _marker: std::marker::PhantomData<T>,
}

impl<T, C: Coder<T>> LengthPrefixCoder<T, C> {
    pub fn new(inner: C) -> Self {
        Self {
            inner,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<T: Send + Sync + 'static, C: Coder<T>> Coder<T> for LengthPrefixCoder<T, C> {
    fn urn(&self) -> &'static str {
        URN_LENGTH_PREFIX
    }

    fn encode(&self, value: &T, writer: &mut Vec<u8>, _context: Context) -> Result<(), CoderError> {
        // Because writer is not seekable, buf stages the payload to compute its varint length.
        let mut buf = Vec::new();
        self.inner.encode(value, &mut buf, Context::WholeStream)?;
        VarIntCoder::encode_varint(buf.len() as i64, writer)?;
        writer.write_all(&buf)?;
        Ok(())
    }

    fn decode(&self, reader: &mut &[u8], _context: Context) -> Result<T, CoderError> {
        let buf = read_length_prefixed_frame(reader)?;
        self.inner.decode(&mut &buf[..], Context::WholeStream)
    }
}

/// Wrapper that marks the inner value for encoding with a length prefix.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct LengthPrefixed<T>(pub T);

impl<T> LengthPrefixed<T> {
    pub fn new(inner: T) -> Self {
        Self(inner)
    }

    pub fn into_inner(self) -> T {
        self.0
    }
}

impl<T> std::ops::Deref for LengthPrefixed<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T> std::ops::DerefMut for LengthPrefixed<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<T> From<T> for LengthPrefixed<T> {
    fn from(val: T) -> Self {
        Self(val)
    }
}

/// Coder for [`LengthPrefixed<T>`].
#[derive(Clone, Debug)]
pub struct LengthPrefixedCoder<T, C: Coder<T>>(pub LengthPrefixCoder<T, C>);

impl<T, C: Coder<T>> LengthPrefixedCoder<T, C> {
    pub fn new(inner: C) -> Self {
        Self(LengthPrefixCoder::new(inner))
    }
}

impl<T: Send + Sync + 'static, C: Coder<T>> Coder<LengthPrefixed<T>> for LengthPrefixedCoder<T, C> {
    fn urn(&self) -> &'static str {
        URN_LENGTH_PREFIX
    }

    fn encode(
        &self,
        value: &LengthPrefixed<T>,
        writer: &mut Vec<u8>,
        context: Context,
    ) -> Result<(), CoderError> {
        self.0.encode(&value.0, writer, context)
    }

    fn decode(
        &self,
        reader: &mut &[u8],
        context: Context,
    ) -> Result<LengthPrefixed<T>, CoderError> {
        let val = self.0.decode(reader, context)?;
        Ok(LengthPrefixed(val))
    }
}

/// Reads a length-prefixed payload. A negative length is an error, not a huge `usize`.
fn read_length_prefixed_frame(reader: &mut &[u8]) -> Result<Vec<u8>, CoderError> {
    let len = usize::try_from(VarIntCoder::decode_varint(reader)?)
        .map_err(|_| CoderError::Format("Negative length prefix".to_string()))?;
    Ok(read_exact_vec(reader, len)?)
}

impl<T: DefaultCoder> DefaultCoder for LengthPrefixed<T> {
    type Coder = LengthPrefixedCoder<T, T::Coder>;

    fn coder() -> Self::Coder {
        LengthPrefixedCoder::new(T::coder())
    }

    fn encode_element(&self, writer: &mut Vec<u8>) -> Result<(), CoderError> {
        Self::coder().encode(self, writer, Context::Nested)
    }

    fn decode_element(reader: &mut &[u8]) -> Result<Self, CoderError> {
        Self::coder().decode(reader, Context::Nested)
    }

    fn decode_element_with_schema(
        reader: &mut &[u8],
        schema: Option<&Arc<crate::schema::Schema>>,
    ) -> Result<Self, CoderError> {
        let buf = read_length_prefixed_frame(reader)?;
        T::decode_element_with_schema(&mut &buf[..], schema).map(LengthPrefixed)
    }

    fn decode_element_with_context(
        reader: &mut &[u8],
        schema: Option<&Arc<crate::schema::Schema>>,
        state_reader: Option<&Arc<dyn crate::coders::iterable::StateStreamReader>>,
    ) -> Result<Self, CoderError> {
        let buf = read_length_prefixed_frame(reader)?;
        T::decode_element_with_context(&mut &buf[..], schema, state_reader).map(LengthPrefixed)
    }

    fn register_coder<R: CoderRegistry + ?Sized>(registry: &R) -> String {
        let inner_id = T::register_coder(registry);
        registry.register_coder(URN_LENGTH_PREFIX, vec![inner_id])
    }
}
