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

//! Multimap user state: a list of values per key.

use std::marker::PhantomData;
use std::sync::Arc;

use model::pipeline as proto;

use super::backend::UserStateBackend;
use super::{AnyStateSpec, state_spec};
use crate::coders::DefaultCoder;
use crate::pipeline::Pipeline;
use crate::pipeline::constants::URN_USER_STATE_MULTIMAP;

/// Specification for a multimap user state cell: each key holds a list of values.
#[derive(Clone, Debug)]
pub struct MultimapStateSpec<K, V> {
    name: String,
    _marker: PhantomData<(K, V)>,
}

impl<K: DefaultCoder, V: DefaultCoder> MultimapStateSpec<K, V> {
    /// Creates a multimap state specification with the given unique state ID.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            _marker: PhantomData,
        }
    }
}

impl<K: DefaultCoder, V: DefaultCoder> AnyStateSpec for MultimapStateSpec<K, V> {
    fn name(&self) -> &str {
        &self.name
    }

    fn register_and_encode(&self, pipeline: &Pipeline) -> proto::StateSpec {
        state_spec(
            proto::state_spec::Spec::MultimapSpec(proto::MultimapStateSpec {
                key_coder_id: K::register_coder(pipeline),
                value_coder_id: V::register_coder(pipeline),
            }),
            URN_USER_STATE_MULTIMAP,
        )
    }
}

/// A typed handle to a multimap user state cell for a specific key and window. Each map key
/// holds the values that [`put`](Self::put) added, in order.
///
/// On Dataflow Streaming Engine, `get` in a later bundle returns no values for a key that
/// earlier bundles only `put` to, and [`keys`](Self::keys) still lists it. To read the values
/// back, call `remove` or `clear` before `put` in the same bundle, or use [`MapState`] with a
/// `Vec` value.
///
/// [`MapState`]: super::MapState
pub struct MultimapState<K, V> {
    backend: Arc<dyn UserStateBackend>,
    state_id: Arc<str>,
    window: Arc<[u8]>,
    key: Arc<[u8]>,
    _marker: PhantomData<(K, V)>,
}

impl<K: DefaultCoder, V: DefaultCoder> MultimapState<K, V> {
    /// Creates a handle to the cell `state_id` of `key` in `window`.
    pub fn new(
        backend: Arc<dyn UserStateBackend>,
        state_id: impl Into<Arc<str>>,
        window: impl Into<Arc<[u8]>>,
        key: impl Into<Arc<[u8]>>,
    ) -> Self {
        Self {
            backend,
            state_id: state_id.into(),
            window: window.into(),
            key: key.into(),
            _marker: PhantomData,
        }
    }

    /// Reads the values of `map_key`, in the order that they were added. Empty if absent.
    pub fn get(&self, map_key: &K) -> crate::Result<Vec<V>> {
        self.get_encoded(&self.encode_key(map_key)?)?
            .into_iter()
            .map(|bytes| {
                V::decode(&bytes).map_err(|e| {
                    crate::Error::from(e).context(format!(
                        "Failed to decode multimap state value for '{}'",
                        self.state_id
                    ))
                })
            })
            .collect()
    }

    /// Adds `value` to the values of `map_key`. Earlier values stay.
    pub fn put(&mut self, map_key: K, value: V) -> crate::Result {
        let key_bytes = self.encode_key(&map_key)?;
        let val_bytes = value.encode().map_err(|e| {
            crate::Error::from(e).context(format!(
                "Failed to encode multimap value for '{}'",
                self.state_id
            ))
        })?;
        Ok(self.backend.append_map_state(
            &self.state_id,
            &self.window,
            &self.key,
            key_bytes,
            val_bytes,
        )?)
    }

    /// Removes `map_key` and all its values.
    pub fn remove(&mut self, map_key: &K) -> crate::Result {
        let key_bytes = self.encode_key(map_key)?;
        Ok(self
            .backend
            .remove_map_key(&self.state_id, &self.window, &self.key, &key_bytes)?)
    }

    /// Returns `true` if `map_key` has at least one value. Does not decode the values.
    pub fn contains_key(&self, map_key: &K) -> crate::Result<bool> {
        Ok(!self.get_encoded(&self.encode_key(map_key)?)?.is_empty())
    }

    /// Removes all keys and values.
    pub fn clear(&mut self) -> crate::Result {
        Ok(self
            .backend
            .clear_map_state(&self.state_id, &self.window, &self.key)?)
    }

    /// Returns all keys in the cell.
    pub fn keys(&self) -> crate::Result<Vec<K>> {
        self.backend
            .get_map_keys(&self.state_id, &self.window, &self.key)?
            .into_iter()
            .map(|bytes| {
                K::decode(&bytes).map_err(|e| {
                    crate::Error::from(e).context(format!(
                        "Failed to decode multimap state key for '{}'",
                        self.state_id
                    ))
                })
            })
            .collect()
    }

    /// Returns each key that has values, with its values. Reads one key after the other.
    pub fn entries(&self) -> crate::Result<Vec<(K, Vec<V>)>> {
        self.keys()?
            .into_iter()
            .filter_map(|k| match self.get(&k) {
                Ok(values) if values.is_empty() => None,
                Ok(values) => Some(Ok((k, values))),
                Err(e) => Some(Err(e)),
            })
            .collect()
    }

    fn encode_key(&self, map_key: &K) -> crate::Result<Vec<u8>> {
        map_key.encode().map_err(|e| {
            crate::Error::from(e).context(format!(
                "Failed to encode multimap key for '{}'",
                self.state_id
            ))
        })
    }

    fn get_encoded(&self, key_bytes: &[u8]) -> crate::Result<Vec<Vec<u8>>> {
        Ok(self
            .backend
            .get_map_state(&self.state_id, &self.window, &self.key, key_bytes)?)
    }
}
