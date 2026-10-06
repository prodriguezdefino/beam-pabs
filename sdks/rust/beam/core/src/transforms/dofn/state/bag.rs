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

use model::pipeline as proto;

use super::backend::UserStateBackend;
use super::{AnyStateSpec, state_spec};
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
    state_id: Arc<str>,
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
        Self {
            backend,
            state_id: state_id.into(),
            window: window.into(),
            key: key.into(),
            _marker: PhantomData,
        }
    }

    /// Reads all elements in this bag state cell.
    pub fn read(&self) -> crate::Result<Vec<V>> {
        self.backend
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
            .backend
            .append_state(&self.state_id, &self.window, &self.key, bytes)?)
    }

    pub fn clear(&mut self) -> crate::Result {
        Ok(self
            .backend
            .clear_state(&self.state_id, &self.window, &self.key)?)
    }
}
