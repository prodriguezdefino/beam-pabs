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

//! Combines that read side inputs: [`CombineFnWithContext`] and the expansions of
//! [`CombinePerKey`] and [`CombineGlobally`] with side inputs.
//!
//! A combine with side inputs expands to a `GroupByKey` and a `ParDo` that reads the side
//! inputs. The runner does not lift it, because the lifted stages have no side inputs.

use std::hash::Hash;
use std::marker::PhantomData;
use std::sync::Arc;

use super::globally::default_error;
use super::{CombineGlobally, CombinePerKey};
use crate::coders::{BeamIterable, DefaultCoder};
use crate::transforms::{
    Create, DisplayDataBuilder, DoFn, Flatten, GroupByKey, Map, PTransform, ParDo, ProcessContext,
};
use crate::values::{AnySideInput, IterView, PCollection, SideInputs};

/// The side inputs of a combine without side inputs. Only the default of the `S` parameter of
/// [`CombinePerKey`] and [`CombineGlobally`] uses it.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoSideInputs;

/// What a [`CombineFnWithContext`] reads: the values of its side inputs `S` in the window of
/// the combine.
pub struct CombineContext<'c, S: SideInputs> {
    side_inputs: S::Values<'c>,
}

impl<'c, S: SideInputs> CombineContext<'c, S> {
    /// The values of the side inputs, in the shape of `S`.
    pub fn side_inputs(&self) -> &S::Values<'c> {
        &self.side_inputs
    }
}

/// A [`CombineFn`](super::CombineFn) that also reads side inputs `S` through a
/// [`CombineContext`].
///
/// Apply it with [`CombinePerKey::with_side_inputs`] or [`CombineGlobally::with_side_inputs`].
/// The runner does not lift the combine, so all values of a key and window go through one
/// worker.
///
/// ```
/// # use beam_core as beam;
/// use beam::prelude::*;
/// use beam::transforms::{CombineContext, CombineFnWithContext};
///
/// /// Sums the values above a threshold that a side input gives.
/// struct SumAbove;
///
/// impl CombineFnWithContext<SingletonView<i64>> for SumAbove {
///     type Input = i64;
///     type Accum = i64;
///     type Output = i64;
///
///     fn create_accumulator(&self, _: &CombineContext<'_, SingletonView<i64>>) -> beam::Result<i64> {
///         Ok(0)
///     }
///
///     fn add_input(
///         &self,
///         sum: i64,
///         value: i64,
///         ctx: &CombineContext<'_, SingletonView<i64>>,
///     ) -> beam::Result<i64> {
///         Ok(if value > *ctx.side_inputs() { sum + value } else { sum })
///     }
///
///     fn merge_accumulators(
///         &self,
///         sums: Vec<i64>,
///         _: &CombineContext<'_, SingletonView<i64>>,
///     ) -> beam::Result<i64> {
///         Ok(sums.into_iter().sum())
///     }
///
///     fn extract_output(&self, sum: i64, _: &CombineContext<'_, SingletonView<i64>>) -> beam::Result<i64> {
///         Ok(sum)
///     }
/// }
///
/// # let p = Pipeline::default();
/// let threshold = p.apply("Threshold", Create::new(vec![2_i64])).as_singleton();
/// let sums = p
///     .apply("Values", Create::new(vec![("a".to_string(), 1_i64), ("a".to_string(), 3)]))
///     .apply("SumAbove", CombinePerKey::new(SumAbove).with_side_inputs(threshold));
/// # let _ = sums;
/// ```
pub trait CombineFnWithContext<S: SideInputs>: Send + Sync + 'static {
    /// The type of the values to aggregate.
    type Input: DefaultCoder;
    /// The intermediate state between elements.
    type Accum: DefaultCoder;
    /// The final result for a key.
    type Output: DefaultCoder;

    /// Produces the identity accumulator.
    fn create_accumulator(&self, ctx: &CombineContext<'_, S>) -> crate::Result<Self::Accum>;

    /// Folds one input value into an accumulator.
    fn add_input(
        &self,
        accumulator: Self::Accum,
        input: Self::Input,
        ctx: &CombineContext<'_, S>,
    ) -> crate::Result<Self::Accum>;

    /// Merges accumulators built independently.
    fn merge_accumulators(
        &self,
        accumulators: Vec<Self::Accum>,
        ctx: &CombineContext<'_, S>,
    ) -> crate::Result<Self::Accum>;

    /// Converts the fully merged accumulator into the output value for the key.
    fn extract_output(
        &self,
        accumulator: Self::Accum,
        ctx: &CombineContext<'_, S>,
    ) -> crate::Result<Self::Output>;
}

/// The context of `views` in the window of `ctx`.
fn context<'s, 'a: 's, S: SideInputs, T>(
    views: &'s S,
    ctx: &ProcessContext<'a, T>,
) -> crate::Result<CombineContext<'s, S>> {
    Ok(CombineContext {
        side_inputs: ctx.side_input(views)?,
    })
}

/// Combines the grouped values of each key with the side inputs of the window.
struct CombineGroupedWithContextFn<K, CF, S> {
    combine_fn: Arc<CF>,
    side_inputs: S,
    _marker: PhantomData<fn(K)>,
}

impl<K, CF, S: Clone> Clone for CombineGroupedWithContextFn<K, CF, S> {
    fn clone(&self) -> Self {
        Self {
            combine_fn: Arc::clone(&self.combine_fn),
            side_inputs: self.side_inputs.clone(),
            _marker: PhantomData,
        }
    }
}

impl<K, CF, S> DoFn for CombineGroupedWithContextFn<K, CF, S>
where
    K: DefaultCoder,
    S: SideInputs,
    CF: CombineFnWithContext<S>,
{
    type In = (K, BeamIterable<CF::Input>);
    type Out = (K, CF::Output);

    fn side_inputs(&self) -> Vec<&dyn AnySideInput> {
        self.side_inputs.views()
    }

    fn process_element(
        &mut self,
        (key, values): Self::In,
        ctx: &mut ProcessContext<'_, Self::Out>,
    ) -> crate::Result {
        let combine_fn = &self.combine_fn;
        let output = {
            let context = context(&self.side_inputs, ctx)?;
            let accumulator = values.try_into_iter().try_fold(
                combine_fn.create_accumulator(&context)?,
                |accumulator, value| {
                    let value =
                        value.map_err(|e| format!("Failed to read a grouped value: {e}"))?;
                    combine_fn.add_input(accumulator, value, &context)
                },
            )?;
            combine_fn.extract_output(accumulator, &context)?
        };
        ctx.emit((key, output))
    }
}

impl<K, CF, S> PTransform<PCollection<(K, CF::Input)>> for CombinePerKey<K, CF, S>
where
    K: DefaultCoder + Eq + Hash,
    S: SideInputs,
    CF: CombineFnWithContext<S>,
{
    type Output = PCollection<(K, CF::Output)>;

    fn expand(self, input: &PCollection<(K, CF::Input)>) -> PCollection<(K, CF::Output)> {
        let pipeline = input.pipeline();
        let mut builder =
            DisplayDataBuilder::with_namespace(pipeline.scope_name().unwrap_or_default());
        builder.add_text("transform", "CombinePerKey");
        builder.add_text("combine_fn", std::any::type_name::<CF>());
        pipeline.set_scope_display_data(builder.into_proto());
        input.apply("GroupByKey", GroupByKey::new()).apply(
            "CombineGroupedValues",
            ParDo::new(CombineGroupedWithContextFn {
                combine_fn: self.combine_fn,
                side_inputs: self.side_inputs,
                _marker: PhantomData,
            }),
        )
    }
}

/// Emits the identity value of the combine when the input was empty. It reads the result of
/// the combine and the side inputs of the combine.
struct InjectDefaultWithContextFn<CF, S, O> {
    combine_fn: Arc<CF>,
    side_inputs: (IterView<O>, S),
}

impl<CF, S: Clone, O> Clone for InjectDefaultWithContextFn<CF, S, O> {
    fn clone(&self) -> Self {
        Self {
            combine_fn: Arc::clone(&self.combine_fn),
            side_inputs: self.side_inputs.clone(),
        }
    }
}

impl<CF, S> DoFn for InjectDefaultWithContextFn<CF, S, CF::Output>
where
    S: SideInputs,
    CF: CombineFnWithContext<S>,
{
    type In = ();
    type Out = CF::Output;

    fn side_inputs(&self) -> Vec<&dyn AnySideInput> {
        self.side_inputs.views()
    }

    fn process_element(
        &mut self,
        _seed: (),
        ctx: &mut ProcessContext<'_, CF::Output>,
    ) -> crate::Result {
        let (result, side_inputs) = &self.side_inputs;
        if let Some(first) = ctx.side_input(result)?.iter()?.next() {
            return first.map(drop);
        }
        let default = {
            let context = context(side_inputs, ctx)?;
            self.combine_fn
                .extract_output(self.combine_fn.create_accumulator(&context)?, &context)?
        };
        ctx.emit(default)
    }
}

impl<CF, S> PTransform<PCollection<CF::Input>> for CombineGlobally<CF, S>
where
    S: SideInputs,
    CF: CombineFnWithContext<S>,
{
    type Output = PCollection<CF::Output>;

    fn expand(self, input: &PCollection<CF::Input>) -> PCollection<CF::Output> {
        let pipeline = input.pipeline();
        let mut builder =
            DisplayDataBuilder::with_namespace(pipeline.scope_name().unwrap_or_default());
        builder.add_text("transform", "CombineGlobally");
        builder.add_text("combine_fn", std::any::type_name::<CF>());
        pipeline.set_scope_display_data(builder.into_proto());

        let combined = input
            .apply("KeyWithVoid", Map::new(|element: CF::Input| ((), element)))
            .apply(
                "Combine",
                CombinePerKey::<(), CF, S>::with_context(
                    Arc::clone(&self.combine_fn),
                    self.side_inputs.clone(),
                ),
            )
            .apply(
                "ExtractValues",
                Map::new(|(_key, val): ((), CF::Output)| val),
            );
        if !self.insert_default {
            return combined;
        }
        if let Some(reason) = default_error(input) {
            pipeline.record_invalid_construction(reason);
            return combined;
        }
        let default = pipeline.apply("DefaultSeed", Create::new(vec![()])).apply(
            "InjectDefault",
            ParDo::new(InjectDefaultWithContextFn {
                combine_fn: self.combine_fn,
                side_inputs: (combined.as_iter(), self.side_inputs),
            }),
        );
        Flatten::pcollections("WithDefault", &[&combined, &default])
    }
}
