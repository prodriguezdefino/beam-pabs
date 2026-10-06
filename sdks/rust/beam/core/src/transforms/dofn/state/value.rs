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

//! Value user state: one value, stored with the bag protocol.

use std::marker::PhantomData;

use model::pipeline as proto;

use super::bag::BagState;
use super::{AnyStateSpec, state_spec};
use crate::coders::DefaultCoder;
use crate::pipeline::Pipeline;
use crate::pipeline::constants::URN_USER_STATE_BAG;

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
}

impl<V: DefaultCoder> AnyStateSpec for ValueStateSpec<V> {
    fn name(&self) -> &str {
        &self.name
    }

    fn register_and_encode(&self, pipeline: &Pipeline) -> proto::StateSpec {
        // The spec tells the runner that the cell holds one value. The state API reads and
        // writes it with the bag protocol.
        state_spec(
            proto::state_spec::Spec::ReadModifyWriteSpec(proto::ReadModifyWriteStateSpec {
                coder_id: V::register_coder(pipeline),
            }),
            URN_USER_STATE_BAG,
        )
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
