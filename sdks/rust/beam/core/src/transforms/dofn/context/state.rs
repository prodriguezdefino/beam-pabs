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

//! Persistent user state accessors on [`ProcessContext`]. Each accessor binds a state spec to
//! a key and the current window. The handle buffers reads and mutations until the bundle
//! completes, then commits them.

use std::sync::Arc;

use super::ProcessContext;
use crate::coders::DefaultCoder;
use crate::transforms::dofn::state::{
    BagState, BagStateSpec, MapState, MapStateSpec, SetState, SetStateSpec, ValueState,
    ValueStateSpec,
};

impl<T> ProcessContext<'_, T> {
    /// Returns the bag state cell for `key` in the current window.
    pub fn bag_state<V: DefaultCoder, K: DefaultCoder>(
        &self,
        spec: &BagStateSpec<V>,
        key: &K,
    ) -> crate::Result<BagState<V>> {
        let reader = self
            .state_reader
            .map(Arc::clone)
            .ok_or_else(|| format!("No active UserStateReader for state '{}'", spec.name()))?;
        let key_bytes = key
            .encode()
            .map_err(|e| crate::Error::from(e).context("Failed to encode state key"))?;
        Ok(BagState::new(
            reader,
            spec.name().to_string(),
            self.window().to_vec(),
            key_bytes,
        ))
    }

    /// Returns the single-value state cell for `key` in the current window.
    pub fn value_state<V: DefaultCoder, K: DefaultCoder>(
        &self,
        spec: &ValueStateSpec<V>,
        key: &K,
    ) -> crate::Result<ValueState<V>> {
        let bag_spec = BagStateSpec::<V>::new(spec.name());
        let bag = self.bag_state(&bag_spec, key)?;
        Ok(ValueState::new(bag))
    }

    /// Returns the map state cell for `key` in the current window.
    pub fn map_state<KState: DefaultCoder, V: DefaultCoder, K: DefaultCoder>(
        &self,
        spec: &MapStateSpec<KState, V>,
        key: &K,
    ) -> crate::Result<MapState<KState, V>> {
        let reader = self
            .state_reader
            .map(Arc::clone)
            .ok_or_else(|| format!("No active UserStateReader for state '{}'", spec.name()))?;
        let key_bytes = key
            .encode()
            .map_err(|e| crate::Error::from(e).context("Failed to encode state key"))?;
        Ok(MapState::new(
            reader,
            spec.name().to_string(),
            self.window().to_vec(),
            key_bytes,
        ))
    }

    /// Returns the set state cell for `key` in the current window.
    pub fn set_state<Elem: DefaultCoder, K: DefaultCoder>(
        &self,
        spec: &SetStateSpec<Elem>,
        key: &K,
    ) -> crate::Result<SetState<Elem>> {
        let map_spec = MapStateSpec::<Elem, ()>::new(spec.name());
        let map = self.map_state(&map_spec, key)?;
        Ok(SetState::new(map))
    }
}
