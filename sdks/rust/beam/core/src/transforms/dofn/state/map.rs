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

//! Map user state: one value per key.

use std::marker::PhantomData;
use std::sync::Arc;

use model::pipeline as proto;

use super::backend::UserStateBackend;
use super::{AnyStateSpec, state_spec};
use crate::coders::DefaultCoder;
use crate::pipeline::Pipeline;
use crate::pipeline::constants::URN_USER_STATE_MULTIMAP;

/// Specification for a key-value map user state cell.
#[derive(Clone, Debug)]
pub struct MapStateSpec<K, V> {
    name: String,
    _marker: PhantomData<(K, V)>,
}

impl<K: DefaultCoder, V: DefaultCoder> MapStateSpec<K, V> {
    /// Creates a map state specification with the given unique state ID.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            _marker: PhantomData,
        }
    }
}

impl<K: DefaultCoder, V: DefaultCoder> AnyStateSpec for MapStateSpec<K, V> {
    fn name(&self) -> &str {
        &self.name
    }

    fn register_and_encode(&self, pipeline: &Pipeline) -> proto::StateSpec {
        state_spec(
            proto::state_spec::Spec::MapSpec(proto::MapStateSpec {
                key_coder_id: K::register_coder(pipeline),
                value_coder_id: V::register_coder(pipeline),
            }),
            URN_USER_STATE_MULTIMAP,
        )
    }
}

/// A typed handle to a persistent map user state cell for a specific key and window.
pub struct MapState<K, V> {
    backend: Arc<dyn UserStateBackend>,
    state_id: Arc<str>,
    window: Arc<[u8]>,
    key: Arc<[u8]>,
    _marker: PhantomData<(K, V)>,
}

impl<K: DefaultCoder, V: DefaultCoder> MapState<K, V> {
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

    /// Reads the value for `map_key`, or `None` if absent. If the storage returns more than
    /// one value for the key, the last value is used.
    pub fn get(&self, map_key: &K) -> crate::Result<Option<V>> {
        let key_bytes = map_key.encode().map_err(|e| {
            crate::Error::from(e)
                .context(format!("Failed to encode map key for '{}'", self.state_id))
        })?;
        let values =
            self.backend
                .get_map_state(&self.state_id, &self.window, &self.key, &key_bytes)?;
        values
            .into_iter()
            .last()
            .map(|bytes| {
                V::decode(&bytes).map_err(|e| {
                    crate::Error::from(e).context(format!(
                        "Failed to decode map state value for '{}'",
                        self.state_id
                    ))
                })
            })
            .transpose()
    }

    /// Sets `value` for `map_key` in this map state cell.
    pub fn put(&mut self, map_key: K, value: V) -> crate::Result {
        let key_bytes = map_key.encode().map_err(|e| {
            crate::Error::from(e)
                .context(format!("Failed to encode map key for '{}'", self.state_id))
        })?;
        let val_bytes = value.encode().map_err(|e| {
            crate::Error::from(e).context(format!(
                "Failed to encode map value for '{}'",
                self.state_id
            ))
        })?;
        Ok(self.backend.put_map_state(
            &self.state_id,
            &self.window,
            &self.key,
            key_bytes,
            val_bytes,
        )?)
    }

    /// Removes the entry for `map_key` from this map state cell.
    pub fn remove(&mut self, map_key: &K) -> crate::Result {
        let key_bytes = map_key.encode().map_err(|e| {
            crate::Error::from(e)
                .context(format!("Failed to encode map key for '{}'", self.state_id))
        })?;
        Ok(self
            .backend
            .remove_map_key(&self.state_id, &self.window, &self.key, &key_bytes)?)
    }

    pub fn clear(&mut self) -> crate::Result {
        Ok(self
            .backend
            .clear_map_state(&self.state_id, &self.window, &self.key)?)
    }

    /// Returns all keys in this map state cell.
    pub fn keys(&self) -> crate::Result<Vec<K>> {
        let raw_keys = self
            .backend
            .get_map_keys(&self.state_id, &self.window, &self.key)?;
        raw_keys
            .into_iter()
            .map(|bytes| {
                K::decode(&bytes).map_err(|e| {
                    crate::Error::from(e).context(format!(
                        "Failed to decode map state key for '{}'",
                        self.state_id
                    ))
                })
            })
            .collect()
    }

    /// Returns all key-value entries in this map state cell.
    ///
    /// Fetches all keys from the state reader and queries the value for each key sequentially.
    pub fn entries(&self) -> crate::Result<Vec<(K, V)>> {
        self.keys()?
            .into_iter()
            .filter_map(|k| match self.get(&k) {
                Ok(Some(v)) => Some(Ok((k, v))),
                Ok(None) => None,
                Err(e) => Some(Err(e)),
            })
            .collect()
    }
}
