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

//! Fallible one-to-one element mapping with dead-letter routing.

use std::fmt::Display;
use std::sync::Arc;

use super::failure::{FAILURES_TAG, OUTPUT_TAG};
use super::{ClosureFn, ExceptionElement, Failure, PTransform, TryParDo, WithFailures};
use crate::coders::DefaultCoder;
use crate::values::PCollection;

/// Applies a fallible function to every element and sends failures to a dead-letter output.
///
/// `Ok` values go to [`output`](WithFailures::output). The exception handler converts each `Err`
/// (an [`ExceptionElement`]) into a failure of type `F` on [`failures`](WithFailures::failures);
/// the bundle does not fail. The default handler from [`new`](Self::new) produces
/// [`Failure<In>`] with the `Display` text of the error. To replace it, use
/// [`exceptions_via`](Self::exceptions_via):
///
/// ```ignore
/// let parsed = lines.apply(
///     TryMap::new("Parse", |s: &String| s.parse::<i64>())
///         .exceptions_via(|e| format!("{}: {}", e.element, e.exception)),
/// );
/// ```
pub struct TryMap<In, Out, E, F = Failure<In>> {
    name: String,
    func: Arc<MapFn<In, Out, E>>,
    handler: Arc<HandlerFn<In, E, F>>,
}

type MapFn<In, Out, E> = dyn Fn(&In) -> Result<Out, E> + Send + Sync;
type HandlerFn<In, E, F> = dyn Fn(ExceptionElement<In, E>) -> F + Send + Sync;

impl<In, Out, E> TryMap<In, Out, E>
where
    In: 'static,
    E: Display + 'static,
{
    /// Maps each element with `func`. Each `Err` becomes a [`Failure<In>`] with the input element
    /// and the `Display` text of the error.
    pub fn new<Func>(name: impl Into<String>, func: Func) -> Self
    where
        Func: Fn(&In) -> Result<Out, E> + Send + Sync + 'static,
    {
        Self {
            name: name.into(),
            func: Arc::new(func),
            handler: Arc::new(|e: ExceptionElement<In, E>| {
                Failure::new(e.element, e.exception.to_string())
            }),
        }
    }
}

impl<In, Out, E, F> TryMap<In, Out, E, F> {
    /// Replaces the exception handler that converts each failed element and its error into the
    /// failure element on [`failures`](WithFailures::failures).
    pub fn exceptions_via<G, H>(self, handler: H) -> TryMap<In, Out, E, G>
    where
        H: Fn(ExceptionElement<In, E>) -> G + Send + Sync + 'static,
    {
        TryMap {
            name: self.name,
            func: self.func,
            handler: Arc::new(handler),
        }
    }
}

impl<In, Out, E, F> PTransform<PCollection<In>> for TryMap<In, Out, E, F>
where
    In: DefaultCoder,
    Out: DefaultCoder,
    E: 'static,
    F: DefaultCoder,
{
    type Output = WithFailures<Out, F>;

    fn expand(&self, input: &PCollection<In>) -> Self::Output {
        let func = Arc::clone(&self.func);
        let handler = Arc::clone(&self.handler);
        input.apply(TryParDo::<_, F>::new(
            self.name.clone(),
            ClosureFn::new("TryMap", move |element: In, ctx| match func(&element) {
                Ok(value) => ctx.output(value).to(OUTPUT_TAG).emit(),
                Err(exception) => ctx
                    .output_to(
                        FAILURES_TAG,
                        handler(ExceptionElement { element, exception }),
                    )
                    .emit(),
            }),
        ))
    }
}
