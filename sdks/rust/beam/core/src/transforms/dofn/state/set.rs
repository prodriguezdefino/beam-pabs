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

//! Set user state: unique values, stored as map keys.

use std::marker::PhantomData;

use beam_model::pipeline as proto;

use super::map::MapState;
use super::{AnyStateSpec, state_spec};
use crate::coders::DefaultCoder;
use crate::pipeline::Pipeline;
use crate::pipeline::constants::URN_USER_STATE_MULTIMAP;

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

    pub fn insert(&mut self, value: &T) -> crate::Result {
        self.map.put(value, &())
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
