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

//! Composite Beam coders (KV, LengthPrefix, Iterable, Nullable).

use std::io::{Read, Write};
use std::sync::Arc;

use super::standard::{VarIntCoder, decode_iterable, read_array, read_exact_vec};
use super::traits::{Coder, CoderError, CoderRegistry, Context, DefaultCoder};
use super::{URN_ITERABLE, URN_KV, URN_LENGTH_PREFIX, URN_NULLABLE, URN_STATE_BACKED_ITERABLE};

/// Coder for `KV<K, V>` pairs, represented as `(K, V)`.
#[derive(Clone, Debug)]
pub struct KvCoder<K, V, KC: Coder<K>, VC: Coder<V>> {
    key_coder: KC,
    value_coder: VC,
    _marker: std::marker::PhantomData<(K, V)>,
}

impl<K, V, KC: Coder<K>, VC: Coder<V>> KvCoder<K, V, KC, VC> {
    pub fn new(key_coder: KC, value_coder: VC) -> Self {
        Self {
            key_coder,
            value_coder,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<K: Send + Sync + 'static, V: Send + Sync + 'static, KC: Coder<K>, VC: Coder<V>> Coder<(K, V)>
    for KvCoder<K, V, KC, VC>
{
    fn urn(&self) -> &'static str {
        URN_KV
    }

    fn encode(
        &self,
        value: &(K, V),
        writer: &mut dyn Write,
        context: Context,
    ) -> Result<(), CoderError> {
        // The key is always nested. The value takes the enclosing context.
        self.key_coder.encode(&value.0, writer, Context::Nested)?;
        self.value_coder.encode(&value.1, writer, context)?;
        Ok(())
    }

    fn decode(&self, reader: &mut dyn Read, context: Context) -> Result<(K, V), CoderError> {
        let k = self.key_coder.decode(reader, Context::Nested)?;
        let v = self.value_coder.decode(reader, context)?;
        Ok((k, v))
    }
}

impl<K: DefaultCoder, V: DefaultCoder> DefaultCoder for (K, V) {
    type Coder = KvCoder<K, V, K::Coder, V::Coder>;
    fn coder() -> Self::Coder {
        KvCoder::new(K::coder(), V::coder())
    }

    fn encode_element(&self, writer: &mut dyn Write) -> Result<(), CoderError> {
        self.0.encode_element(writer)?;
        self.1.encode_element(writer)?;
        Ok(())
    }

    fn decode_element(reader: &mut dyn Read) -> Result<Self, CoderError> {
        let k = K::decode_element(reader)?;
        let v = V::decode_element(reader)?;
        Ok((k, v))
    }

    fn decode_element_with_schema(
        reader: &mut dyn Read,
        schema: Option<&Arc<crate::schema::Schema>>,
    ) -> Result<Self, CoderError> {
        let k = K::decode_element_with_schema(reader, schema)?;
        let v = V::decode_element_with_schema(reader, schema)?;
        Ok((k, v))
    }

    fn decode_element_with_context(
        reader: &mut dyn Read,
        schema: Option<&Arc<crate::schema::Schema>>,
        state_reader: Option<&Arc<dyn super::iterable::StateStreamReader>>,
    ) -> Result<Self, CoderError> {
        let k = K::decode_element_with_context(reader, schema, state_reader)?;
        let v = V::decode_element_with_context(reader, schema, state_reader)?;
        Ok((k, v))
    }

    fn register_coder<R: CoderRegistry + ?Sized>(registry: &R) -> String {
        let k_id = K::register_coder(registry);
        let v_id = V::register_coder(registry);
        registry.register_coder(URN_KV, vec![k_id, v_id])
    }
}

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

    fn encode(
        &self,
        value: &T,
        writer: &mut dyn Write,
        _context: Context,
    ) -> Result<(), CoderError> {
        let mut buf = Vec::new();
        self.inner.encode(value, &mut buf, Context::WholeStream)?;
        VarIntCoder::encode_varint(buf.len() as i64, writer)?;
        writer.write_all(&buf)?;
        Ok(())
    }

    fn decode(&self, reader: &mut dyn Read, _context: Context) -> Result<T, CoderError> {
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
        writer: &mut dyn Write,
        context: Context,
    ) -> Result<(), CoderError> {
        self.0.encode(&value.0, writer, context)
    }

    fn decode(
        &self,
        reader: &mut dyn Read,
        context: Context,
    ) -> Result<LengthPrefixed<T>, CoderError> {
        let val = self.0.decode(reader, context)?;
        Ok(LengthPrefixed(val))
    }
}

/// Reads a length-prefixed payload. A negative length is an error, not a huge `usize`.
fn read_length_prefixed_frame(reader: &mut dyn Read) -> Result<Vec<u8>, CoderError> {
    let len = usize::try_from(VarIntCoder::decode_varint(reader)?)
        .map_err(|_| CoderError::Format("Negative length prefix".to_string()))?;
    Ok(read_exact_vec(reader, len)?)
}

impl<T: DefaultCoder> DefaultCoder for LengthPrefixed<T> {
    type Coder = LengthPrefixedCoder<T, T::Coder>;

    fn coder() -> Self::Coder {
        LengthPrefixedCoder::new(T::coder())
    }

    fn encode_element(&self, writer: &mut dyn Write) -> Result<(), CoderError> {
        Self::coder().encode(self, writer, Context::Nested)
    }

    fn decode_element(reader: &mut dyn Read) -> Result<Self, CoderError> {
        Self::coder().decode(reader, Context::Nested)
    }

    fn decode_element_with_schema(
        reader: &mut dyn Read,
        schema: Option<&Arc<crate::schema::Schema>>,
    ) -> Result<Self, CoderError> {
        let buf = read_length_prefixed_frame(reader)?;
        T::decode_element_with_schema(&mut &buf[..], schema).map(LengthPrefixed)
    }

    fn decode_element_with_context(
        reader: &mut dyn Read,
        schema: Option<&Arc<crate::schema::Schema>>,
        state_reader: Option<&Arc<dyn super::iterable::StateStreamReader>>,
    ) -> Result<Self, CoderError> {
        let buf = read_length_prefixed_frame(reader)?;
        T::decode_element_with_context(&mut &buf[..], schema, state_reader).map(LengthPrefixed)
    }

    fn register_coder<R: CoderRegistry + ?Sized>(registry: &R) -> String {
        let inner_id = T::register_coder(registry);
        registry.register_coder(URN_LENGTH_PREFIX, vec![inner_id])
    }
}

/// Coder for iterables as `Vec<T>`: a 32-bit big-endian count, then each element nested.
#[derive(Clone, Debug)]
pub struct IterableCoder<T, C: Coder<T>> {
    element_coder: Arc<C>,
    _marker: std::marker::PhantomData<T>,
}

impl<T, C: Coder<T>> IterableCoder<T, C> {
    pub fn new(element_coder: C) -> Self {
        Self {
            element_coder: Arc::new(element_coder),
            _marker: std::marker::PhantomData,
        }
    }
}

impl<T: Send + Sync + 'static, C: Coder<T>> Coder<Vec<T>> for IterableCoder<T, C> {
    fn urn(&self) -> &'static str {
        URN_ITERABLE
    }

    fn encode(
        &self,
        value: &Vec<T>,
        writer: &mut dyn Write,
        _context: Context,
    ) -> Result<(), CoderError> {
        let count = value.len() as i32;
        writer.write_all(&count.to_be_bytes())?;
        value
            .iter()
            .try_for_each(|item| self.element_coder.encode(item, writer, Context::Nested))
    }

    fn decode(&self, reader: &mut dyn Read, _context: Context) -> Result<Vec<T>, CoderError> {
        decode_iterable(reader, |r| self.element_coder.decode(r, Context::Nested))
    }
}

impl<T: DefaultCoder> DefaultCoder for Vec<T> {
    type Coder = IterableCoder<T, T::Coder>;
    fn coder() -> Self::Coder {
        IterableCoder::new(T::coder())
    }

    fn encode_element(&self, writer: &mut dyn Write) -> Result<(), CoderError> {
        let count = self.len() as i32;
        writer.write_all(&count.to_be_bytes())?;
        self.iter().try_for_each(|item| item.encode_element(writer))
    }

    fn decode_element(reader: &mut dyn Read) -> Result<Self, CoderError> {
        decode_iterable(reader, T::decode_element)
    }

    fn decode_element_with_schema(
        reader: &mut dyn Read,
        schema: Option<&Arc<crate::schema::Schema>>,
    ) -> Result<Self, CoderError> {
        decode_iterable(reader, |r| T::decode_element_with_schema(r, schema))
    }

    fn decode_element_with_context(
        reader: &mut dyn Read,
        schema: Option<&Arc<crate::schema::Schema>>,
        state_reader: Option<&Arc<dyn super::iterable::StateStreamReader>>,
    ) -> Result<Self, CoderError> {
        super::iterable::decode_iterable_with_state(
            reader,
            |r| T::decode_element_with_context(r, schema, state_reader),
            state_reader.map(|r| r.as_ref()),
        )
    }

    fn register_coder<R: CoderRegistry + ?Sized>(registry: &R) -> String {
        let elem_id = T::register_coder(registry);
        registry.register_coder(URN_ITERABLE, vec![elem_id])
    }
}

impl<T: Send + Sync + 'static, C: Coder<T> + 'static> Coder<super::iterable::BeamIterable<T>>
    for IterableCoder<T, C>
{
    fn urn(&self) -> &'static str {
        URN_STATE_BACKED_ITERABLE
    }

    fn encode(
        &self,
        value: &super::iterable::BeamIterable<T>,
        writer: &mut dyn Write,
        _context: Context,
    ) -> Result<(), CoderError> {
        let inlined = value.inlined_prefix();
        let count = inlined.len() as i32;
        writer.write_all(&count.to_be_bytes())?;
        inlined
            .iter()
            .try_for_each(|item| self.element_coder.encode(item, writer, Context::Nested))
    }

    fn decode(
        &self,
        reader: &mut dyn Read,
        _context: Context,
    ) -> Result<super::iterable::BeamIterable<T>, CoderError> {
        let element_coder = Arc::clone(&self.element_coder);
        super::iterable::decode_beam_iterable(
            reader,
            Arc::new(move |r| element_coder.decode(r, Context::Nested)),
            None,
        )
    }
}

fn decode_nullable<T>(
    reader: &mut dyn Read,
    decode_some: impl FnOnce(&mut dyn Read) -> Result<T, CoderError>,
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
        writer: &mut dyn Write,
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

    fn decode(&self, reader: &mut dyn Read, context: Context) -> Result<Option<T>, CoderError> {
        decode_nullable(reader, |r| self.inner.decode(r, context))
    }
}

impl<T: DefaultCoder> DefaultCoder for Option<T> {
    type Coder = NullableCoder<T, T::Coder>;
    fn coder() -> Self::Coder {
        NullableCoder::new(T::coder())
    }

    fn encode_element(&self, writer: &mut dyn Write) -> Result<(), CoderError> {
        match self {
            None => Ok(writer.write_all(&[0x00])?),
            Some(inner) => {
                writer.write_all(&[0x01])?;
                inner.encode_element(writer)
            }
        }
    }

    fn decode_element(reader: &mut dyn Read) -> Result<Self, CoderError> {
        decode_nullable(reader, T::decode_element)
    }

    fn decode_element_with_schema(
        reader: &mut dyn Read,
        schema: Option<&Arc<crate::schema::Schema>>,
    ) -> Result<Self, CoderError> {
        decode_nullable(reader, |r| T::decode_element_with_schema(r, schema))
    }

    fn decode_element_with_context(
        reader: &mut dyn Read,
        schema: Option<&Arc<crate::schema::Schema>>,
        state_reader: Option<&Arc<dyn super::iterable::StateStreamReader>>,
    ) -> Result<Self, CoderError> {
        decode_nullable(reader, |r| {
            T::decode_element_with_context(r, schema, state_reader)
        })
    }

    fn register_coder<R: CoderRegistry + ?Sized>(registry: &R) -> String {
        let elem_id = T::register_coder(registry);
        registry.register_coder(URN_NULLABLE, vec![elem_id])
    }
}
