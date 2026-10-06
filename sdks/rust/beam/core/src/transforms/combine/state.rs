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

//! `CombiningState`: a user state cell that folds its inputs with a [`CombineFn`].

use std::marker::PhantomData;
use std::sync::Arc;

use model::pipeline as proto;

use super::CombineFn;
use crate::coders::DefaultCoder;
use crate::pipeline::Pipeline;
use crate::pipeline::constants::{URN_RUST_COMBINE_FN_STATE, URN_USER_STATE_BAG};
use crate::transforms::dofn::state::AnyStateSpec;
use crate::transforms::dofn::{BagState, StateBinding, StateCell};

/// Specification of a [`CombiningState`] cell. The runner stores the accumulators with the
/// bag protocol and the accumulator coder.
#[derive(Clone, Debug)]
pub(crate) struct CombiningStateSpec<CF> {
    name: String,
    _marker: PhantomData<fn() -> CF>,
}

impl<CF: CombineFn> CombiningStateSpec<CF> {
    /// Creates the specification of the cell with state id `name`.
    pub(crate) fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            _marker: PhantomData,
        }
    }
}

impl<CF: CombineFn> AnyStateSpec for CombiningStateSpec<CF> {
    fn name(&self) -> &str {
        &self.name
    }

    fn register_and_encode(&self, pipeline: &Pipeline) -> proto::StateSpec {
        proto::StateSpec {
            spec: Some(proto::state_spec::Spec::CombiningSpec(
                proto::CombiningStateSpec {
                    accumulator_coder_id: CF::Accum::register_coder(pipeline),
                    combine_fn: Some(proto::FunctionSpec {
                        urn: URN_RUST_COMBINE_FN_STATE.to_string(),
                        payload: std::any::type_name::<CF>().as_bytes().to_vec(),
                    }),
                },
            )),
            protocol: Some(proto::FunctionSpec {
                urn: URN_USER_STATE_BAG.to_string(),
                payload: Vec::new(),
            }),
        }
    }
}

/// A user state cell that combines its inputs with `CF`, for one key and one window.
///
/// The cell stores one accumulator. [`add`](Self::add) reads it, adds the input and writes
/// it back. The harness keeps the cell in memory for the bundle and writes the final
/// accumulator when the bundle commits, so the runner sees one accumulator per key and
/// window, not one write per input.
///
/// A `#[derive(State)]` struct builds the cell from its type, so `CF` implements `Default`.
/// The built-in [`Sum`](super::Sum), [`Max`](super::Max), [`Min`](super::Min) and
/// [`Mean`](super::Mean) do. For a combine function with settings, such as
/// [`Top`](super::Top), use a type that implements `Default` with those settings.
///
/// ```
/// use beam::prelude::*;
///
/// #[derive(State)]
/// struct ScoreState {
///     total: CombiningState<Sum<i64>>,
///     best: CombiningState<Max<i64>>,
///     average: CombiningState<Mean<i64>>,
/// }
/// ```
pub struct CombiningState<CF: CombineFn> {
    bag: BagState<CF::Accum>,
    combine_fn: CF,
}

impl<CF: CombineFn> CombiningState<CF> {
    /// Creates a cell that stores the accumulators of `combine_fn` in `bag`.
    pub fn new(bag: BagState<CF::Accum>, combine_fn: CF) -> Self {
        Self { bag, combine_fn }
    }

    /// Reads the combined accumulator. An empty cell gives a new accumulator.
    pub fn read_accumulator(&self) -> crate::Result<CF::Accum> {
        let mut stored = self.bag.read()?;
        match stored.len() {
            0 => self.combine_fn.create_accumulator(),
            1 => Ok(stored.remove(0)),
            _ => self.combine_fn.merge_accumulators(stored),
        }
    }

    /// Reads the output for the inputs so far. An empty cell gives the output of a new
    /// accumulator: for example 0 for `Sum`, and NaN for `Mean`.
    pub fn read(&self) -> crate::Result<CF::Output> {
        self.combine_fn.extract_output(self.read_accumulator()?)
    }

    /// Returns true if the cell holds no accumulator.
    pub fn is_empty(&self) -> crate::Result<bool> {
        Ok(self.bag.read()?.is_empty())
    }

    /// Adds `input` to the accumulator.
    pub fn add(&mut self, input: CF::Input) -> crate::Result {
        let accumulator = self.combine_fn.add_input(self.read_accumulator()?, input)?;
        self.write(accumulator)
    }

    /// Merges `accumulator`, for example a partial result from another cell, into the
    /// stored accumulator.
    pub fn add_accumulator(&mut self, accumulator: CF::Accum) -> crate::Result {
        let mut stored = self.bag.read()?;
        let merged = match stored.is_empty() {
            true => accumulator,
            false => {
                stored.push(accumulator);
                self.combine_fn.merge_accumulators(stored)?
            }
        };
        self.write(merged)
    }

    /// Clears the cell.
    pub fn clear(&mut self) -> crate::Result {
        self.bag.clear()
    }

    /// Replaces the stored accumulator with `accumulator`.
    fn write(&mut self, accumulator: CF::Accum) -> crate::Result {
        self.bag.clear()?;
        self.bag.append(accumulator)
    }
}

impl<CF: CombineFn + Default> StateCell for CombiningState<CF> {
    fn spec(id: &str) -> Arc<dyn AnyStateSpec> {
        Arc::new(CombiningStateSpec::<CF>::new(id))
    }

    fn bind(id: &str, binding: &StateBinding<'_>) -> crate::Result<Self> {
        BagState::bind(id, binding).map(|bag| Self::new(bag, CF::default()))
    }
}
