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

//! [`FoldCombineFn`]: a `CombineFn` from closures.

use std::marker::PhantomData;

use beam::coders::DefaultCoder;
use beam::transforms::CombineFn;

/// A [`CombineFn`] built from an initial accumulator value, fold closure, and merge closure.
#[derive(Clone)]
pub struct FoldCombineFn<Accum, Input, F, M> {
    zero: Accum,
    fold_fn: F,
    merge_fn: M,
    _marker: PhantomData<Input>,
}

impl<Accum, Input, F, M> FoldCombineFn<Accum, Input, F, M>
where
    Accum: DefaultCoder + Clone + Send + Sync + 'static,
    Input: DefaultCoder + Send + Sync + 'static,
    F: Fn(Accum, Input) -> Accum + Send + Sync + 'static,
    M: Fn(Accum, Accum) -> Accum + Send + Sync + 'static,
{
    /// Creates a `FoldCombineFn` from a zero accumulator, fold closure, and merge closure.
    pub fn new(zero: Accum, fold_fn: F, merge_fn: M) -> Self {
        Self {
            zero,
            fold_fn,
            merge_fn,
            _marker: PhantomData,
        }
    }
}

impl<Accum, Input, F, M> CombineFn for FoldCombineFn<Accum, Input, F, M>
where
    Accum: DefaultCoder + Clone + Send + Sync + 'static,
    Input: DefaultCoder + Send + Sync + 'static,
    F: Fn(Accum, Input) -> Accum + Send + Sync + 'static,
    M: Fn(Accum, Accum) -> Accum + Send + Sync + 'static,
{
    type Input = Input;
    type Accum = Accum;
    type Output = Accum;

    // The fold closures are infallible (D3): each hook wraps the result in `Ok`.
    fn create_accumulator(&self) -> beam::Result<Self::Accum> {
        Ok(self.zero.clone())
    }

    fn add_input(&self, accumulator: Self::Accum, input: Self::Input) -> beam::Result<Self::Accum> {
        Ok((self.fold_fn)(accumulator, input))
    }

    fn merge_accumulators(&self, accumulators: Vec<Self::Accum>) -> beam::Result<Self::Accum> {
        Ok(accumulators
            .into_iter()
            .reduce(&self.merge_fn)
            .unwrap_or_else(|| self.zero.clone()))
    }

    fn extract_output(&self, accumulator: Self::Accum) -> beam::Result<Self::Output> {
        Ok(accumulator)
    }
}
