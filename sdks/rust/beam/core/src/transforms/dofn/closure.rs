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

//! Closure-backed [`DoFn`]s, built with [`ParDo::from_fn`] and
//! [`ParDo::from_fn_with_side_inputs`].

use std::sync::Arc;

use super::context::ProcessContext;
use super::input::DoFnInput;
use super::outputs::Outputs;
use super::pardo::{DoFn, ParDo};
use crate::transforms::DisplayDataBuilder;
use crate::values::{AnySideInput, IntoSideInputs, SideInputs};

type ProcessElementFn<In, Out, S> = dyn for<'s> Fn(In, <S as SideInputs>::Values<'s>, &mut ProcessContext<'_, Out>) -> crate::Result
    + Send
    + Sync;

/// Adapts a closure into a stateless [`DoFn`]. `pub` in a private module: it appears in the
/// type of [`ParDo::from_fn`], but code outside the crate cannot name it.
pub struct ClosureFn<In, Out, S: SideInputs = ()> {
    kind: &'static str,
    side_inputs: S,
    func: Arc<ProcessElementFn<In, Out, S>>,
}

/// Copies share the closure. An `Fn` holds no per-bundle state.
impl<In, Out, S: SideInputs> Clone for ClosureFn<In, Out, S> {
    fn clone(&self) -> Self {
        Self {
            kind: self.kind,
            side_inputs: self.side_inputs.clone(),
            func: Arc::clone(&self.func),
        }
    }
}

impl<In, Out> ClosureFn<In, Out, ()> {
    pub(crate) fn new<F>(kind: &'static str, func: F) -> Self
    where
        F: Fn(In, &mut ProcessContext<'_, Out>) -> crate::Result + Send + Sync + 'static,
    {
        Self {
            kind,
            side_inputs: (),
            func: Arc::new(move |input, (), ctx| func(input, ctx)),
        }
    }
}

impl<In, Out, S: SideInputs> ClosureFn<In, Out, S> {
    pub(crate) fn with_side_inputs<F>(kind: &'static str, side_inputs: S, func: F) -> Self
    where
        F: for<'s> Fn(In, S::Values<'s>, &mut ProcessContext<'_, Out>) -> crate::Result
            + Send
            + Sync
            + 'static,
    {
        Self {
            kind,
            side_inputs,
            func: Arc::new(func),
        }
    }
}

impl<In: DoFnInput, Out: Outputs, S: SideInputs> DoFn for ClosureFn<In, Out, S> {
    type In = In;
    type Out = Out;

    fn process_element(&mut self, input: In, ctx: &mut ProcessContext<'_, Out>) -> crate::Result {
        let Self {
            side_inputs, func, ..
        } = self;
        let values = side_inputs.read(ctx.side_input_scope())?;
        func(input, values, ctx)
    }

    fn side_inputs(&self) -> Vec<&dyn AnySideInput> {
        self.side_inputs.views()
    }

    fn populate_display_data(&self, builder: &mut DisplayDataBuilder) {
        builder.add_text("transform", self.kind);
    }
}

impl<In: DoFnInput, Out: Outputs> ParDo<ClosureFn<In, Out>> {
    /// A `ParDo` over a stateless closure. Take [`Windowed<T>`](crate::transforms::Windowed)
    /// instead of `T` to also receive the window.
    ///
    /// ```
    /// use beam::transforms::ParDo;
    ///
    /// let split = ParDo::from_fn("SplitWords", |line: String, ctx| {
    ///     line.split_whitespace().try_for_each(|w| ctx.emit(w.to_string()))
    /// });
    /// # let _ = split;
    /// ```
    pub fn from_fn<F>(name: impl Into<String>, func: F) -> Self
    where
        F: Fn(In, &mut ProcessContext<'_, Out>) -> crate::Result + Send + Sync + 'static,
    {
        Self::new(name, ClosureFn::new("ParDoFn", func))
    }
}

impl<In: DoFnInput, Out: Outputs, S: SideInputs> ParDo<ClosureFn<In, Out, S>> {
    /// A `ParDo` over a stateless closure that also receives the values of `side_inputs`
    /// (one view, or a tuple, array or `Vec` of views) in the same shape. See [`SideInputs`].
    ///
    /// ```
    /// # use beam::prelude::*;
    /// # let p = Pipeline::default();
    /// # let words = p.apply(Create::new("Words", vec!["keep".to_string()]));
    /// let min_len = p.apply(Create::new("MinLen", vec![3_i64])).as_singleton();
    /// let long = words.apply(ParDo::from_fn_with_side_inputs("Long", &min_len, |w: String, min, ctx| {
    ///     if w.len() >= min as usize { ctx.emit(w) } else { Ok(()) }
    /// }));
    /// # let _ = long;
    /// ```
    pub fn from_fn_with_side_inputs<V, F>(name: impl Into<String>, side_inputs: V, func: F) -> Self
    where
        V: IntoSideInputs<Owned = S>,
        F: for<'s> Fn(In, S::Values<'s>, &mut ProcessContext<'_, Out>) -> crate::Result
            + Send
            + Sync
            + 'static,
    {
        Self::new(
            name,
            ClosureFn::with_side_inputs("ParDoFn", side_inputs.into_side_inputs(), func),
        )
    }
}
