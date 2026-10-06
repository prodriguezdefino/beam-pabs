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

//! User state specifications and handles for stateful DoFns.
//!
//! Following the Beam portability model, state is scoped to one key and one window. Writes
//! and clears are buffered during the bundle and committed when the bundle is finalized.

use std::marker::PhantomData;
use std::sync::Arc;

use model::pipeline as proto;

use crate::coders::DefaultCoder;
use crate::pipeline::Pipeline;
use crate::pipeline::constants::{URN_USER_STATE_BAG, URN_USER_STATE_MULTIMAP};

/// Specification for a bag user state cell.
#[derive(Clone, Debug)]
pub struct BagStateSpec<V> {
    name: String,
    _marker: PhantomData<V>,
}

impl<V: DefaultCoder> BagStateSpec<V> {
    /// Creates a bag state specification with the given unique state ID.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            _marker: PhantomData,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

/// Specification for a single-value user state cell.
#[derive(Clone, Debug)]
pub struct ValueStateSpec<V> {
    name: String,
    _marker: PhantomData<V>,
}

impl<V: DefaultCoder> ValueStateSpec<V> {
    /// Creates a value state specification with the given unique state ID.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            _marker: PhantomData,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

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

    pub fn name(&self) -> &str {
        &self.name
    }
}

/// Specification for a unique-element set user state cell.
#[derive(Clone, Debug)]
pub struct SetStateSpec<T> {
    name: String,
    _marker: PhantomData<T>,
}

impl<T: DefaultCoder> SetStateSpec<T> {
    /// Creates a set state specification with the given unique state ID.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            _marker: PhantomData,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

/// Type-erased state specification for pipeline graph construction.
pub trait AnyStateSpec: Send + Sync {
    /// Returns the state ID, which is unique within the transform.
    fn name(&self) -> &str;

    /// Registers the state value coder in the pipeline and encodes the Runner API `StateSpec`.
    fn register_and_encode(&self, pipeline: &Pipeline) -> proto::StateSpec;
}

fn state_spec(spec: proto::state_spec::Spec, protocol_urn: &str) -> proto::StateSpec {
    proto::StateSpec {
        spec: Some(spec),
        protocol: Some(proto::FunctionSpec {
            urn: protocol_urn.to_string(),
            payload: Vec::new(),
        }),
    }
}

impl<V: DefaultCoder> AnyStateSpec for BagStateSpec<V> {
    fn name(&self) -> &str {
        &self.name
    }

    fn register_and_encode(&self, pipeline: &Pipeline) -> proto::StateSpec {
        state_spec(
            proto::state_spec::Spec::BagSpec(proto::BagStateSpec {
                element_coder_id: V::register_coder(pipeline),
            }),
            URN_USER_STATE_BAG,
        )
    }
}

impl<V: DefaultCoder> AnyStateSpec for ValueStateSpec<V> {
    fn name(&self) -> &str {
        &self.name
    }

    fn register_and_encode(&self, pipeline: &Pipeline) -> proto::StateSpec {
        state_spec(
            proto::state_spec::Spec::BagSpec(proto::BagStateSpec {
                element_coder_id: V::register_coder(pipeline),
            }),
            URN_USER_STATE_BAG,
        )
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

impl<T: DefaultCoder> AnyStateSpec for SetStateSpec<T> {
    fn name(&self) -> &str {
        &self.name
    }

    fn register_and_encode(&self, pipeline: &Pipeline) -> proto::StateSpec {
        state_spec(
            proto::state_spec::Spec::SetSpec(proto::SetStateSpec {
                element_coder_id: T::register_coder(pipeline),
            }),
            URN_USER_STATE_MULTIMAP,
        )
    }
}

/// Interface that reads and changes user state storage.
pub trait UserStateReader: Send + Sync {
    /// Reads all elements in the state cell for `(state_id, window, key)`.
    fn get_state(&self, state_id: &str, window: &[u8], key: &[u8]) -> Result<Vec<Vec<u8>>, String>;

    /// Appends an encoded element to the state cell for `(state_id, window, key)`.
    fn append_state(
        &self,
        state_id: &str,
        window: &[u8],
        key: &[u8],
        element: Vec<u8>,
    ) -> Result<(), String>;

    /// Clears the state cell for `(state_id, window, key)`.
    fn clear_state(&self, state_id: &str, window: &[u8], key: &[u8]) -> Result<(), String>;

    /// Reads all values associated with `map_key` in the multimap state cell.
    fn get_map_state(
        &self,
        state_id: &str,
        window: &[u8],
        key: &[u8],
        map_key: &[u8],
    ) -> Result<Vec<Vec<u8>>, String>;

    /// Reads all distinct keys in the multimap state cell.
    fn get_map_keys(
        &self,
        state_id: &str,
        window: &[u8],
        key: &[u8],
    ) -> Result<Vec<Vec<u8>>, String>;

    /// Puts a key-value entry in the map state cell and replaces any earlier entry.
    fn put_map_state(
        &self,
        state_id: &str,
        window: &[u8],
        key: &[u8],
        map_key: Vec<u8>,
        element: Vec<u8>,
    ) -> Result<(), String>;

    /// Removes a key and its value from the map state cell.
    fn remove_map_key(
        &self,
        state_id: &str,
        window: &[u8],
        key: &[u8],
        map_key: &[u8],
    ) -> Result<(), String>;

    /// Clears all entries in the map state cell.
    fn clear_map_state(&self, state_id: &str, window: &[u8], key: &[u8]) -> Result<(), String>;
}

/// A typed handle to a persistent bag state cell for a specific key and window.
pub struct BagState<V> {
    reader: Arc<dyn UserStateReader>,
    state_id: String,
    window: Vec<u8>,
    key: Vec<u8>,
    _marker: PhantomData<V>,
}

impl<V: DefaultCoder> BagState<V> {
    pub fn new(
        reader: Arc<dyn UserStateReader>,
        state_id: String,
        window: Vec<u8>,
        key: Vec<u8>,
    ) -> Self {
        Self {
            reader,
            state_id,
            window,
            key,
            _marker: PhantomData,
        }
    }

    /// Reads all elements in this bag state cell.
    pub fn read(&self) -> crate::Result<Vec<V>> {
        self.reader
            .get_state(&self.state_id, &self.window, &self.key)?
            .into_iter()
            .map(|bytes| {
                V::decode(&bytes).map_err(|e| {
                    crate::Error::from(e).context(format!(
                        "Failed to decode bag state element for '{}'",
                        self.state_id
                    ))
                })
            })
            .collect()
    }

    /// Appends an element to this bag state cell.
    pub fn append(&mut self, value: V) -> crate::Result {
        let bytes = value.encode().map_err(|e| {
            crate::Error::from(e).context(format!(
                "Failed to encode bag state element for '{}'",
                self.state_id
            ))
        })?;
        Ok(self
            .reader
            .append_state(&self.state_id, &self.window, &self.key, bytes)?)
    }

    pub fn clear(&mut self) -> crate::Result {
        Ok(self
            .reader
            .clear_state(&self.state_id, &self.window, &self.key)?)
    }
}

/// A typed handle to a single-value user state cell for a specific key and window.
///
/// It is a thin layer on [`BagState`]. A write clears the bag, then appends the value.
pub struct ValueState<V> {
    bag: BagState<V>,
}

impl<V: DefaultCoder> ValueState<V> {
    /// Creates a value state handle on top of `bag`.
    pub fn new(bag: BagState<V>) -> Self {
        Self { bag }
    }

    /// Reads the current value of this state cell. Returns `None` if the value is not set.
    pub fn read(&self) -> crate::Result<Option<V>> {
        self.bag.read().map(|items| items.into_iter().next())
    }

    /// Writes the value of this state cell and replaces any earlier value.
    pub fn write(&mut self, value: V) -> crate::Result {
        self.bag.clear().and_then(|()| self.bag.append(value))
    }

    /// Clears this state cell. The value is then not set.
    pub fn clear(&mut self) -> crate::Result {
        self.bag.clear()
    }
}

/// A typed handle to a persistent map user state cell for a specific key and window.
pub struct MapState<K, V> {
    reader: Arc<dyn UserStateReader>,
    state_id: String,
    window: Vec<u8>,
    key: Vec<u8>,
    _marker: PhantomData<(K, V)>,
}

impl<K: DefaultCoder, V: DefaultCoder> MapState<K, V> {
    pub fn new(
        reader: Arc<dyn UserStateReader>,
        state_id: String,
        window: Vec<u8>,
        key: Vec<u8>,
    ) -> Self {
        Self {
            reader,
            state_id,
            window,
            key,
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
            self.reader
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
        Ok(self.reader.put_map_state(
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
            .reader
            .remove_map_key(&self.state_id, &self.window, &self.key, &key_bytes)?)
    }

    pub fn clear(&mut self) -> crate::Result {
        Ok(self
            .reader
            .clear_map_state(&self.state_id, &self.window, &self.key)?)
    }

    /// Returns all keys in this map state cell.
    pub fn keys(&self) -> crate::Result<Vec<K>> {
        let raw_keys = self
            .reader
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

/// A typed handle to a persistent set user state cell for a specific key and window.
pub struct SetState<T> {
    map: MapState<T, ()>,
}

impl<T: DefaultCoder> SetState<T> {
    /// Creates a set state handle on top of a map state handle with unit values.
    pub fn new(map: MapState<T, ()>) -> Self {
        Self { map }
    }

    /// Returns `true` if the set contains `value`.
    pub fn contains(&self, value: &T) -> crate::Result<bool> {
        self.map.get(value).map(|opt| opt.is_some())
    }

    pub fn insert(&mut self, value: T) -> crate::Result {
        self.map.put(value, ())
    }

    pub fn remove(&mut self, value: &T) -> crate::Result {
        self.map.remove(value)
    }

    pub fn clear(&mut self) -> crate::Result {
        self.map.clear()
    }

    /// Returns all elements in this set.
    pub fn read(&self) -> crate::Result<Vec<T>> {
        self.map.keys()
    }
}
