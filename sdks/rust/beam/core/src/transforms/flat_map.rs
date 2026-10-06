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

//! One-to-many element mapping with flattening.

use std::sync::Arc;

use super::{ClosureFn, PTransform, ParDo};
use crate::coders::DefaultCoder;
use crate::transforms::dofn::context::ProcessContext;
use crate::values::PCollection;

/// Expands one element, emitting each result straight to the receiver.
type ExpandFn<In, Out> = dyn Fn(In, &mut ProcessContext<Out>) -> crate::Result + Send + Sync;

/// Applies a function producing an iterable for each element, flattening the results.
pub struct FlatMap<In, Out> {
    name: String,
    func: Arc<ExpandFn<In, Out>>,
}

impl<In, Out> FlatMap<In, Out> {
    pub fn new<Iter, F>(name: impl Into<String>, func: F) -> Self
    where
        Out: DefaultCoder,
        Iter: IntoIterator<Item = Out>,
        F: Fn(In) -> Iter + Send + Sync + 'static,
    {
        Self {
            name: name.into(),
            func: Arc::new(move |x, out| func(x).into_iter().try_for_each(|item| out.emit(item))),
        }
    }
}

impl<In, Out> PTransform<PCollection<In>> for FlatMap<In, Out>
where
    In: DefaultCoder,
    Out: DefaultCoder,
{
    type Output = PCollection<Out>;

    fn expand(&self, input: &PCollection<In>) -> PCollection<Out> {
        let func = Arc::clone(&self.func);
        input.apply(ParDo::new(
            self.name.clone(),
            ClosureFn::new("FlatMap", move |element, out| (func)(element, out)),
        ))
    }
}
