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

//! Coder for [`ShardedKey<K>`] (`beam:coder:sharded_key:v1`).

use std::sync::Arc;

use bytes::Bytes;

use crate::coders::URN_SHARDED_KEY;
use crate::coders::standard::{read_bytes, write_bytes};
use crate::coders::traits::{Coder, CoderError, CoderRegistry, Context, DefaultCoder};

/// A user key with a shard id, so that the runner spreads one key over several workers.
///
/// Two values with the same `key` and different `shard_id`s are different keys for
/// grouping, state and timers.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ShardedKey<K> {
    pub key: K,
    /// Opaque bytes. `Bytes` lets each element share the shard id of its producer.
    pub shard_id: Bytes,
}

impl<K> ShardedKey<K> {
    pub fn new(key: K, shard_id: impl Into<Bytes>) -> Self {
        Self {
            key,
            shard_id: shard_id.into(),
        }
    }
}

/// Coder for [`ShardedKey<K>`]: the shard id as nested bytes, then the nested key.
///
/// Both parts are nested in every context, as the standard coder specification requires.
#[derive(Clone, Debug)]
pub struct ShardedKeyCoder<K, C: Coder<K>> {
    key_coder: C,
    _marker: std::marker::PhantomData<K>,
}

impl<K, C: Coder<K>> ShardedKeyCoder<K, C> {
    pub fn new(key_coder: C) -> Self {
        Self {
            key_coder,
            _marker: std::marker::PhantomData,
        }
    }
}

fn decode_sharded_key<K>(
    reader: &mut &[u8],
    decode_key: impl FnOnce(&mut &[u8]) -> Result<K, CoderError>,
) -> Result<ShardedKey<K>, CoderError> {
    let shard_id = Bytes::from(read_bytes(reader, Context::Nested)?);
    let key = decode_key(reader)?;
    Ok(ShardedKey { key, shard_id })
}

impl<K: Send + Sync + 'static, C: Coder<K>> Coder<ShardedKey<K>> for ShardedKeyCoder<K, C> {
    fn urn(&self) -> &'static str {
        URN_SHARDED_KEY
    }

    fn encode(
        &self,
        value: &ShardedKey<K>,
        writer: &mut Vec<u8>,
        _context: Context,
    ) -> Result<(), CoderError> {
        write_bytes(&value.shard_id, writer, Context::Nested)?;
        self.key_coder.encode(&value.key, writer, Context::Nested)
    }

    fn decode(&self, reader: &mut &[u8], _context: Context) -> Result<ShardedKey<K>, CoderError> {
        decode_sharded_key(reader, |r| self.key_coder.decode(r, Context::Nested))
    }
}

impl<K: DefaultCoder> DefaultCoder for ShardedKey<K> {
    type Coder = ShardedKeyCoder<K, K::Coder>;
    fn coder() -> Self::Coder {
        ShardedKeyCoder::new(K::coder())
    }

    fn encode_element(&self, writer: &mut Vec<u8>) -> Result<(), CoderError> {
        write_bytes(&self.shard_id, writer, Context::Nested)?;
        self.key.encode_element(writer)
    }

    fn decode_element(reader: &mut &[u8]) -> Result<Self, CoderError> {
        decode_sharded_key(reader, K::decode_element)
    }

    fn decode_element_with_schema(
        reader: &mut &[u8],
        schema: Option<&Arc<crate::schema::Schema>>,
    ) -> Result<Self, CoderError> {
        decode_sharded_key(reader, |r| K::decode_element_with_schema(r, schema))
    }

    fn decode_element_with_context(
        reader: &mut &[u8],
        schema: Option<&Arc<crate::schema::Schema>>,
        state_reader: Option<&Arc<dyn crate::coders::iterable::StateStreamReader>>,
    ) -> Result<Self, CoderError> {
        decode_sharded_key(reader, |r| {
            K::decode_element_with_context(r, schema, state_reader)
        })
    }

    fn register_coder<R: CoderRegistry + ?Sized>(registry: &R) -> String {
        let key_id = K::register_coder(registry);
        registry.register_coder(URN_SHARDED_KEY, vec![key_id])
    }

    fn register_value_coder<R: CoderRegistry + ?Sized>(&self, registry: &R) -> String {
        let key_id = self.key.register_value_coder(registry);
        registry.register_coder(URN_SHARDED_KEY, vec![key_id])
    }
}
