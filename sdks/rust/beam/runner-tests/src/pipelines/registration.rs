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

//! Registered function types: the worker builds them from the pipeline graph.
//!
//! On a runner that does not lift combines, such as Dataflow, the combine runs as the
//! partial-combine and merge `ParDo`s; on Prism, as the lifted stages.

use beam::prelude::*;
use beam::testing::{TestPipeline, passert};
use beam::transforms::sdf::{
    OffsetRange, OffsetRangeTracker, ProcessContinuation, RestrictionTracker, SplittableDoFn,
    SplittableParDo,
};
use serde::{Deserialize, Serialize};

/// Multiplies by `factor` and adds `offset`. `offset` is not serialized: `setup` sets it
/// on the worker.
#[derive(Clone, Serialize, Deserialize, Register)]
pub struct ScaleAndShift {
    factor: i64,
    #[serde(skip)]
    offset: i64,
}

impl DoFn for ScaleAndShift {
    type In = i64;
    type Out = i64;

    fn setup(&mut self) -> Result {
        self.offset = 1;
        Ok(())
    }

    fn process_element(&mut self, element: i64, ctx: &mut ProcessContext<'_, i64>) -> Result {
        ctx.emit(element * self.factor + self.offset)
    }
}

/// Emits `base + i` for every `i` in `0..n`, split into ranges of 2.
#[derive(Clone, Serialize, Deserialize, Register)]
pub struct CountUpFrom {
    base: i64,
}

impl SplittableDoFn for CountUpFrom {
    type In = i64;
    type Out = i64;
    type Restriction = OffsetRange;
    type Tracker = OffsetRangeTracker;

    fn initial_restriction(&self, n: &i64) -> Result<OffsetRange> {
        Ok(OffsetRange::new(0, *n))
    }

    fn split_restriction(&self, _n: &i64, restriction: &OffsetRange) -> Vec<OffsetRange> {
        restriction.sized_splits(2)
    }

    fn create_tracker(&self, restriction: &OffsetRange) -> OffsetRangeTracker {
        OffsetRangeTracker::new(*restriction)
    }

    fn process_element(
        &mut self,
        _n: i64,
        tracker: &OffsetRangeTracker,
        ctx: &mut ProcessContext<'_, i64>,
    ) -> Result<ProcessContinuation> {
        let start = tracker.current_restriction().start;
        (start..)
            .take_while(|i| tracker.try_claim(i))
            .try_for_each(|i| ctx.emit(self.base + i))?;
        Ok(ProcessContinuation::Stop)
    }
}

/// Sums the values of a key, each times `factor`.
#[derive(Clone, Serialize, Deserialize, Register)]
pub struct WeightedSum {
    factor: i64,
}

impl CombineFn for WeightedSum {
    type Input = i64;
    type Accum = i64;
    type Output = i64;

    fn create_accumulator(&self) -> Result<i64> {
        Ok(0)
    }

    fn add_input(&self, acc: i64, input: i64) -> Result<i64> {
        Ok(acc + input * self.factor)
    }

    fn merge_accumulators(&self, accs: Vec<i64>) -> Result<i64> {
        Ok(accs.into_iter().sum())
    }

    fn extract_output(&self, acc: i64) -> Result<i64> {
        Ok(acc)
    }
}

/// Validates a registered `DoFn`, with a field that `setup` builds on the worker.
pub fn build_registered_dofn(p: &TestPipeline) {
    let out = p.apply("Create", Create::new(vec![1i64, 2, 3])).apply(
        "ScaleAndShift",
        ParDo::new(ScaleAndShift {
            factor: 10,
            offset: 0,
        }),
    );
    passert::that("AssertScaled", &out).contains_in_any_order([11i64, 21, 31]);
}

/// Validates a registered splittable `DoFn` through the expanded SDF stages.
pub fn build_registered_splittable_dofn(p: &TestPipeline) {
    let out = p
        .apply("Create", Create::new(vec![3i64, 2]))
        .apply("CountUp", SplittableParDo::new(CountUpFrom { base: 100 }));
    passert::that("AssertCounted", &out).contains_in_any_order([100i64, 101, 102, 100, 101]);
}

/// Validates a registered `CombineFn` per key, over keys kept as encoded bytes.
pub fn build_registered_combine_per_key(p: &TestPipeline) {
    let elements = [("a", 1i64), ("b", 10), ("a", 2), ("b", 20), ("a", 3)]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v))
        .collect::<Vec<_>>();
    let sums = p
        .apply("Create", Create::new(elements))
        .apply("WeightedSum", CombinePerKey::new(WeightedSum { factor: 2 }));
    passert::that("AssertSums", &sums)
        .contains_in_any_order([("a".to_string(), 12i64), ("b".to_string(), 60)]);
}

/// Validates a registered `CombineFn` over a whole collection.
pub fn build_registered_combine_globally(p: &TestPipeline) {
    let total = p.apply("Create", Create::new(vec![1i64, 2, 3, 4])).apply(
        "WeightedTotal",
        CombineGlobally::new(WeightedSum { factor: 3 }),
    );
    passert::that("AssertTotal", &total).contains_in_any_order([30i64]);
}
