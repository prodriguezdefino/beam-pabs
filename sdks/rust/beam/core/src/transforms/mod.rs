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

//! Transforms: the operations that build a pipeline's graph.
//!
//! A [`PTransform`] expands a [`PInput`] into a [`POutput`]; [`ParDo`] lifts a [`DoFn`]
//! into one. The byte-level handlers that a runner executes are in [`crate::internals`].
//!
//! This crate is `apply`-style: `pcoll.apply("Name", Map::new(f))`. Method syntax
//! (`.map(..)`) comes from extension traits in `apache-beam-fluent`; `beam::prelude`
//! brings both.

use crate::pipeline::ResourceHints;
use crate::values::{PInput, POutput};

mod batch_elements;
pub(crate) mod co_group_by_key;
pub(crate) mod combine;
mod composite;
mod count;
pub(crate) mod create;
pub mod display_data;
mod distinct;
pub(crate) mod dofn;
pub(crate) mod failure;
mod filter;
mod flat_map;
mod flatten;
mod generate_sequence;
mod group_by_key;
pub(crate) mod handler;
mod impulse;
mod inspect;
mod join;
mod map;
mod partition;
mod periodic_impulse;
mod reshuffle;
mod sets;
mod to_rows;
mod try_map;
mod with_keys;

pub use batch_elements::{BatchElements, ExplodeBatch};
pub use co_group_by_key::{CoGbkResult, CoGroupByKey, KeyedPCollectionTuple};
pub use combine::{
    AnySample, ApproximateQuantiles, ApproximateUnique, CombineFn, CombineGlobally, CombinePerKey,
    CombiningState, Compare, Compose, ComposePart, MapOutput, Max, Mean, Min, Natural, Numeric, On,
    Reversed, Sample, Sum, Top,
};
pub use count::{CountGlobally, CountPerElement};
pub use create::Create;
pub use distinct::{Distinct, DistinctBy};
// The derive macros: `Register` for `DoFn`, `StatefulDoFn`, `SplittableDoFn` and `CombineFn`
// types, `State` for the state cells and `Timers` for the timer families of a `StatefulDoFn`.
#[cfg(feature = "derive")]
pub use derive::{Outputs, Register, State, Timers};
use dofn::ClosureFn;
pub use dofn::sdf;
pub use dofn::{
    BagState, BatchConverter, BatchedDoFn, BatchedDoFnAdapter, DEFAULT_FINALIZER_EXPIRY, DoFn,
    DoFnInput, Finalizer, FinishBundleContext, FinishOutput, MapState, NoTimers, OrderedListState,
    OutputBuilder, Outputs, ParDo, ProcessContext, SetState, StatefulContext, StatefulDoFn, Timer,
    ValueState, VecBatchConverter, Windowed, WithFinalizer,
};
pub use failure::{ExceptionElement, Failure, FailureCoder, WithFailures};
pub use filter::Filter;
pub use flat_map::FlatMap;
pub use flatten::Flatten;
pub use generate_sequence::{DEFAULT_SEQUENCE_SPLIT_SIZE, GenerateSequence};
pub use group_by_key::GroupByKey;
pub(crate) use handler::{BundleHandler, ElementSink, HandlerInstance, TransformFn, TypedElement};
pub use impulse::Impulse;
pub use inspect::Inspect;
pub use join::{
    BroadcastInnerJoin, BroadcastLeftJoin, FullOuterJoin, InnerJoin, LeftJoin, RightJoin,
};
pub use map::Map;
pub use partition::Partition;
pub use periodic_impulse::PeriodicImpulse;
pub use reshuffle::Reshuffle;
pub use sets::Sets;
pub use to_rows::ToRows;
pub use try_map::TryMap;
pub use with_keys::WithKeys;

pub(crate) use display_data::{DisplayDataBuilder, HasDisplayData};

/// A composite or primitive transform converting a [`PInput`] into a [`POutput`].
pub trait PTransform<Input: PInput> {
    /// The value this transform produces.
    type Output: POutput;

    /// Builds the transform's sub-graph and returns its output.
    fn expand(&self, input: &Input) -> Self::Output;
}

/// A [`PTransform`] whose expansion can fail for a reason outside the program, for example
/// a cross-language expansion service that cannot be reached.
///
/// Apply it with `try_apply` (on [`Pipeline`](crate::pipeline::Pipeline),
/// [`PBegin`](crate::values::PBegin), [`PCollection`](crate::values::PCollection), ...) to
/// get the error as a value. `apply` calls [`PTransform::expand`], which panics on the
/// error; each implementation documents this in a `# Panics` section.
///
/// Configuration errors that the program can fix are not reported here: `expand` records
/// them with [`Pipeline::record_construction_error`](crate::pipeline::Pipeline::record_construction_error),
/// and [`Pipeline::validate`](crate::pipeline::Pipeline::validate) returns them.
pub trait TryPTransform<Input: PInput>: PTransform<Input> {
    /// The expansion error.
    type Error;

    /// Builds the transform's sub-graph, or returns why it cannot.
    fn try_expand(&self, input: &Input) -> Result<Self::Output, Self::Error>;
}

/// A transform wrapper that scopes execution environment resource hints during expansion.
pub struct WithResourceHints<T> {
    transform: T,
    hints: ResourceHints,
}

impl<T> WithResourceHints<T> {
    pub fn new(transform: T, hints: ResourceHints) -> Self {
        Self { transform, hints }
    }

    /// Merges `hints` over the hints already set.
    pub fn with_resource_hints(mut self, hints: ResourceHints) -> Self {
        self.hints = hints.merge_with_outer(&self.hints);
        self
    }
}

impl<In: PInput, T: PTransform<In>> PTransform<In> for WithResourceHints<T> {
    type Output = T::Output;

    fn expand(&self, input: &In) -> Self::Output {
        let _guard = input
            .pipeline()
            .enter_resource_hints_scope(self.hints.clone());
        self.transform.expand(input)
    }
}

/// Extension trait providing `.with_resource_hints()` on all [`PTransform`] implementors.
pub trait WithResourceHintsExt<In: PInput>: PTransform<In> + Sized {
    /// Scopes the specified resource hints during the expansion of this transform.
    fn with_resource_hints(self, hints: ResourceHints) -> WithResourceHints<Self> {
        WithResourceHints::new(self, hints)
    }

    /// Scopes a single resource hint by URN and serialized payload during expansion.
    fn with_resource_hint(
        self,
        urn: impl Into<String>,
        payload: impl Into<Vec<u8>>,
    ) -> WithResourceHints<Self> {
        let hints = ResourceHints::new().with_hint(urn, payload);
        self.with_resource_hints(hints)
    }
}

impl<In: PInput, T: PTransform<In>> WithResourceHintsExt<In> for T {}
