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

//! Bag user state: a list of values.

use std::marker::PhantomData;
use std::sync::Arc;

use beam_model::pipeline as proto;

use super::backend::UserStateBackend;
use super::{AnyStateSpec, StateId, check_row_schema, state_spec};
use crate::coders::DefaultCoder;
use crate::pipeline::Pipeline;
use crate::pipeline::constants::URN_USER_STATE_BAG;

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

/// A typed handle to a persistent bag state cell for a specific key and window.
///
/// State handles own their state identifier, window bytes, and key bytes for independent lifetime.
pub struct BagState<V> {
    backend: Arc<dyn UserStateBackend>,
    state_id: StateId,
    window: Arc<[u8]>,
    key: Arc<[u8]>,
    _marker: PhantomData<V>,
}

impl<V: DefaultCoder> BagState<V> {
    pub fn new(
        backend: Arc<dyn UserStateBackend>,
        state_id: impl Into<Arc<str>>,
        window: impl Into<Arc<[u8]>>,
        key: impl Into<Arc<[u8]>>,
    ) -> Self {
        Self::from_parts(
            backend,
            StateId::from(state_id.into()),
            window.into(),
            key.into(),
        )
    }

    pub(crate) fn from_parts(
        backend: Arc<dyn UserStateBackend>,
        state_id: StateId,
        window: Arc<[u8]>,
        key: Arc<[u8]>,
    ) -> Self {
        Self {
            backend,
            state_id,
            window,
            key,
            _marker: PhantomData,
        }
    }

    /// Reads all elements in this bag state cell.
    pub fn read(&self) -> crate::Result<Vec<V>> {
        let bag = self
            .backend
            .get_state(&self.state_id, &self.window, &self.key)?;
        if bag.is_empty() {
            return Ok(Vec::new());
        }
        let schema = self.backend.state_schema(&self.state_id);
        bag.iter()
            .map(|bytes| {
                V::decode_with_schema(bytes, schema).map_err(|e| {
                    crate::Error::from(e).context(format!(
                        "Failed to decode bag state element for '{}'",
                        self.state_id
                    ))
                })
            })
            .collect()
    }

    /// Reads the first element in this bag state cell, or `None` when empty.
    pub(crate) fn read_first(&self) -> crate::Result<Option<V>> {
        let Some(bytes) = self
            .backend
            .get_first_state(&self.state_id, &self.window, &self.key)?
        else {
            return Ok(None);
        };
        let schema = self.backend.state_schema(&self.state_id);
        V::decode_with_schema(&bytes, schema)
            .map(Some)
            .map_err(|e| {
                crate::Error::from(e).context(format!(
                    "Failed to decode bag state element for '{}'",
                    self.state_id
                ))
            })
    }

    /// Returns `true` when this bag state cell holds no elements.
    pub(crate) fn is_empty(&self) -> crate::Result<bool> {
        Ok(self
            .backend
            .get_state(&self.state_id, &self.window, &self.key)?
            .is_empty())
    }

    /// Appends an element to this bag state cell.
    pub fn append(&mut self, value: &V) -> crate::Result {
        let bytes = self.encode_value(value)?;
        Ok(self
            .backend
            .append_state(&self.state_id, &self.window, &self.key, bytes)?)
    }

    /// Replaces this bag state cell with `value`.
    pub(crate) fn set(&mut self, value: &V) -> crate::Result {
        let bytes = self.encode_value(value)?;
        Ok(self
            .backend
            .set_state(&self.state_id, &self.window, &self.key, bytes)?)
    }

    pub fn clear(&mut self) -> crate::Result {
        Ok(self
            .backend
            .clear_state(&self.state_id, &self.window, &self.key)?)
    }

    fn encode_value(&self, value: &V) -> crate::Result<Vec<u8>> {
        check_row_schema(
            value,
            || self.backend.state_schema(&self.state_id),
            &self.state_id,
        )?;
        value.encode().map_err(|e| {
            crate::Error::from(e).context(format!(
                "Failed to encode bag state element for '{}'",
                self.state_id
            ))
        })
    }
}
