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

//! Coder for `KV<K, V>` pairs.

use std::sync::Arc;

use crate::coders::URN_KV;
use crate::coders::traits::{Coder, CoderError, CoderRegistry, Context, DefaultCoder};

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
        writer: &mut Vec<u8>,
        context: Context,
    ) -> Result<(), CoderError> {
        // The key is always nested. The value takes the enclosing context.
        self.key_coder.encode(&value.0, writer, Context::Nested)?;
        self.value_coder.encode(&value.1, writer, context)?;
        Ok(())
    }

    fn decode(&self, reader: &mut &[u8], context: Context) -> Result<(K, V), CoderError> {
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

    fn encode_element(&self, writer: &mut Vec<u8>) -> Result<(), CoderError> {
        self.0.encode_element(writer)?;
        self.1.encode_element(writer)?;
        Ok(())
    }

    fn decode_element(reader: &mut &[u8]) -> Result<Self, CoderError> {
        let k = K::decode_element(reader)?;
        let v = V::decode_element(reader)?;
        Ok((k, v))
    }

    fn decode_element_with_schema(
        reader: &mut &[u8],
        schema: Option<&Arc<crate::schema::Schema>>,
    ) -> Result<Self, CoderError> {
        let k = K::decode_element_with_schema(reader, schema)?;
        let v = V::decode_element_with_schema(reader, schema)?;
        Ok((k, v))
    }

    fn decode_element_with_context(
        reader: &mut &[u8],
        schema: Option<&Arc<crate::schema::Schema>>,
        state_reader: Option<&Arc<dyn crate::coders::iterable::StateStreamReader>>,
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

    fn register_value_coder<R: CoderRegistry + ?Sized>(&self, registry: &R) -> String {
        let k_id = self.0.register_value_coder(registry);
        let v_id = self.1.register_value_coder(registry);
        registry.register_coder(URN_KV, vec![k_id, v_id])
    }
}
