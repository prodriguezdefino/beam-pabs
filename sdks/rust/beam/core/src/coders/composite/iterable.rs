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

//! Coder for in-memory iterables (`beam:coder:iterable:v1`).

use std::sync::Arc;

use crate::coders::URN_ITERABLE;
use crate::coders::standard::{decode_iterable, write_count};
use crate::coders::traits::{Coder, CoderError, CoderRegistry, Context, DefaultCoder};

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
        writer: &mut Vec<u8>,
        _context: Context,
    ) -> Result<(), CoderError> {
        write_count(value.len(), writer)?;
        value
            .iter()
            .try_for_each(|item| self.element_coder.encode(item, writer, Context::Nested))
    }

    fn decode(&self, reader: &mut &[u8], _context: Context) -> Result<Vec<T>, CoderError> {
        decode_iterable(reader, |r| self.element_coder.decode(r, Context::Nested))
    }
}

impl<T: DefaultCoder> DefaultCoder for Vec<T> {
    type Coder = IterableCoder<T, T::Coder>;
    fn coder() -> Self::Coder {
        IterableCoder::new(T::coder())
    }

    fn encode_element(&self, writer: &mut Vec<u8>) -> Result<(), CoderError> {
        write_count(self.len(), writer)?;
        self.iter().try_for_each(|item| item.encode_element(writer))
    }

    fn decode_element(reader: &mut &[u8]) -> Result<Self, CoderError> {
        decode_iterable(reader, T::decode_element)
    }

    fn decode_element_with_schema(
        reader: &mut &[u8],
        schema: Option<&Arc<crate::schema::Schema>>,
    ) -> Result<Self, CoderError> {
        decode_iterable(reader, |r| T::decode_element_with_schema(r, schema))
    }

    fn decode_element_with_context(
        reader: &mut &[u8],
        schema: Option<&Arc<crate::schema::Schema>>,
        state_reader: Option<&Arc<dyn crate::coders::iterable::StateStreamReader>>,
    ) -> Result<Self, CoderError> {
        crate::coders::iterable::decode_iterable_with_state(
            reader,
            |r| T::decode_element_with_context(r, schema, state_reader),
            state_reader.map(|r| r.as_ref()),
        )
    }

    fn register_coder<R: CoderRegistry + ?Sized>(registry: &R) -> String {
        let elem_id = T::register_coder(registry);
        registry.register_coder(URN_ITERABLE, vec![elem_id])
    }

    /// The first element gives the element coder. All elements share one coder.
    fn register_value_coder<R: CoderRegistry + ?Sized>(&self, registry: &R) -> String {
        let elem_id = self.first().map_or_else(
            || T::register_coder(registry),
            |first| first.register_value_coder(registry),
        );
        registry.register_coder(URN_ITERABLE, vec![elem_id])
    }
}

impl<T: Send + Sync + 'static, C: Coder<T> + 'static>
    Coder<crate::coders::iterable::BeamIterable<T>> for IterableCoder<T, C>
{
    /// The in-memory encoding is `beam:coder:iterable:v1`, the URN that `register_coder` also
    /// writes. Only a runner sends `beam:coder:state_backed_iterable:v1`, for its state streams.
    fn urn(&self) -> &'static str {
        URN_ITERABLE
    }

    fn encode(
        &self,
        value: &crate::coders::iterable::BeamIterable<T>,
        writer: &mut Vec<u8>,
        _context: Context,
    ) -> Result<(), CoderError> {
        // Encoding only the prefix would write a wrong count and lose the suffix elements.
        if value.has_state_suffix() {
            return Err(CoderError::Format(
                "cannot encode a state-backed BeamIterable: drain it with `into_vec` first"
                    .to_string(),
            ));
        }
        let inlined = value.inlined_prefix();
        write_count(inlined.len(), writer)?;
        inlined
            .iter()
            .try_for_each(|item| self.element_coder.encode(item, writer, Context::Nested))
    }

    fn decode(
        &self,
        reader: &mut &[u8],
        _context: Context,
    ) -> Result<crate::coders::iterable::BeamIterable<T>, CoderError> {
        // Cheap: Arc clone on element_coder for potential state-backed suffix.
        let element_coder = Arc::clone(&self.element_coder);
        crate::coders::iterable::decode_beam_iterable(
            reader,
            move |r| element_coder.decode(r, Context::Nested),
            None,
        )
    }
}
