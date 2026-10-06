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

//! Closure-backed [`DoFn`]s, built with [`ParDo::from_fn`].

use std::sync::Arc;

use super::context::ProcessContext;
use super::pardo::{DoFn, ParDo};
use crate::coders::DefaultCoder;
use crate::transforms::DisplayDataBuilder;

type ProcessElementFn<In, Out> =
    dyn Fn(In, &mut ProcessContext<'_, Out>) -> crate::Result + Send + Sync;

/// Adapts a closure into a stateless [`DoFn`]. It is `pub` in a private module, so it can
/// appear in the type of [`ParDo::from_fn`], but code outside the crate cannot name it.
pub struct ClosureFn<In, Out> {
    kind: &'static str,
    func: Arc<ProcessElementFn<In, Out>>,
}

/// Copies share the closure. An `Fn` holds no per-bundle state.
impl<In, Out> Clone for ClosureFn<In, Out> {
    fn clone(&self) -> Self {
        Self {
            kind: self.kind,
            func: Arc::clone(&self.func),
        }
    }
}

impl<In, Out> ClosureFn<In, Out> {
    pub(crate) fn new<F>(kind: &'static str, func: F) -> Self
    where
        F: Fn(In, &mut ProcessContext<'_, Out>) -> crate::Result + Send + Sync + 'static,
    {
        Self {
            kind,
            func: Arc::new(func),
        }
    }
}

impl<In: DefaultCoder, Out: DefaultCoder> DoFn for ClosureFn<In, Out> {
    type In = In;
    type Out = Out;

    fn process_element(&mut self, element: In, ctx: &mut ProcessContext<'_, Out>) -> crate::Result {
        (self.func)(element, ctx)
    }

    fn populate_display_data(&self, builder: &mut DisplayDataBuilder) {
        builder.add_text("transform", self.kind);
    }
}

impl<In: DefaultCoder, Out: DefaultCoder> ParDo<ClosureFn<In, Out>> {
    /// Creates a `ParDo` over a stateless closure that receives the [`ProcessContext`], for
    /// side inputs ([`ParDo::with_side_input`]), multiple outputs, timestamps and windows. For
    /// state across elements, use a [`DoFn`].
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
