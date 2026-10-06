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

//! Tests for the built-in `CombineFn`s and the lifted pre-combine stage.

use beam::coders::DefaultCoder;
use beam::internals::{HandlerContext, PipelineInternals};
use beam::pipeline::{CombineStage, Pipeline, stage_key};
use beam::prelude::*;
use beam::transforms::{CombineFn, CombinePerKey, Max, Min, Sum};

/// Folds each half of `inputs` into a partial, merges the partials and extracts.
fn combine<T, CF>(combine_fn: &CF, inputs: &[T]) -> Result<T>
where
    T: Copy,
    CF: CombineFn<Input = T, Accum = T, Output = T>,
{
    let partial = |part: &[T]| {
        part.iter()
            .try_fold(combine_fn.create_accumulator()?, |acc, &x| {
                combine_fn.add_input(acc, x)
            })
    };
    let (left, right) = inputs.split_at(inputs.len() / 2);
    combine_fn.extract_output(combine_fn.merge_accumulators(vec![partial(left)?, partial(right)?])?)
}

#[test]
fn built_in_combine_fns_fold_merge_and_extract() -> Result {
    let mixed = [-7, 12, 5, -3];
    for (case, combined, expected) in [
        ("sum", combine(&Sum::new(), &mixed), 7),
        ("max", combine(&Max::new(), &mixed), 12),
        ("min", combine(&Min::new(), &mixed), -7),
        // All inputs have the same sign, so a wrong identity value changes the result.
        ("max of negatives", combine(&Max::new(), &[-9, -4, -6]), -4),
        ("min of positives", combine(&Min::new(), &[9, 4, 6]), 4),
        ("sum of none", combine(&Sum::new(), &[]), 0),
        ("max of none", combine(&Max::new(), &[]), i64::MIN),
        ("min of none", combine(&Min::new(), &[]), i64::MAX),
    ] {
        assert_eq!(combined?, expected, "{case}");
    }
    Ok(())
}

#[test]
fn built_in_combine_fns_accept_i32() -> Result {
    let values = [-7i32, 12, 5, -3];
    assert_eq!(combine(&Sum::new(), &values)?, 7);
    assert_eq!(combine(&Max::new(), &values)?, 12);
    assert_eq!(combine(&Min::new(), &values)?, -7);
    assert_eq!(combine(&Max::new(), &[] as &[i32])?, i32::MIN);
    Ok(())
}

/// A float NaN loses to any number, and the result for no values is an infinity.
#[test]
fn built_in_combine_fns_accept_f64() -> Result {
    let values = [-7.5, 12.25, f64::NAN, 5.0];
    assert_eq!(combine(&Max::new(), &values)?, 12.25);
    assert_eq!(combine(&Min::new(), &values)?, -7.5);
    assert_eq!(combine(&Sum::new(), &[1.5, 2.25, -0.75])?, 3.0);
    assert_eq!(combine(&Sum::new(), &[] as &[f64])?, 0.0);
    assert_eq!(combine(&Max::new(), &[] as &[f64])?, f64::NEG_INFINITY);
    assert_eq!(combine(&Min::new(), &[] as &[f64])?, f64::INFINITY);
    Ok(())
}

/// `Sum` returns an error on integer overflow, in `add_input` and in `merge_accumulators`,
/// so debug and release builds give the same result.
#[test]
fn sum_overflow_is_an_error() {
    for (case, inputs) in [
        ("add_input", [i64::MAX, 1, 0, 0]),
        ("merge_accumulators", [i64::MAX, 0, 1, 0]),
    ] {
        let err = combine(&Sum::new(), &inputs).expect_err(case);
        assert!(err.to_string().contains("overflowed i64"), "{case}: {err}");
    }
    let err = combine(&Sum::new(), &[i32::MAX, 1]).expect_err("i32");
    assert!(err.to_string().contains("overflowed i32"), "{err}");
}

/// A sum that is not registered, so its stages are handlers of the rebuild path, which the
/// test below reads. `Sum<i64>` is registered, so its stages travel in the graph.
struct UnregisteredSum;

impl CombineFn for UnregisteredSum {
    type Input = i64;
    type Accum = i64;
    type Output = i64;

    fn create_accumulator(&self) -> Result<i64> {
        Ok(0)
    }

    fn add_input(&self, accumulator: i64, input: i64) -> Result<i64> {
        Ok(accumulator + input)
    }

    fn merge_accumulators(&self, accumulators: Vec<i64>) -> Result<i64> {
        Ok(accumulators.into_iter().sum())
    }

    fn extract_output(&self, accumulator: i64) -> Result<i64> {
        Ok(accumulator)
    }
}

/// Under its memory limit, pre-combine keeps all accumulators until the bundle ends, so
/// each key leaves the bundle as one partial.
#[test]
fn precombine_emits_one_partial_per_key_at_bundle_end() {
    let p = Pipeline::new();
    let _ = p
        .apply("Create", Create::new(vec![("a".to_string(), 0i64)]))
        .apply("Sum", CombinePerKey::new(UnregisteredSum));
    let handler = p
        .transform_handlers()
        .remove(&stage_key("Sum", CombineStage::Precombine))
        .expect("pre-combine stage handler must be registered");

    let mut stage = handler.instantiate();
    let mut sink = Vec::<Vec<u8>>::new();
    let mut ctx = HandlerContext::new(&mut sink);
    stage.start_bundle().expect("start_bundle");
    for (key, value) in [("a", 1i64), ("b", 10), ("a", 2), ("a", 3)] {
        let element = (key.to_string(), value).encode().expect("encode");
        stage.process(&element, &mut ctx).expect("process");
    }
    stage.finish_bundle(&mut ctx).expect("finish_bundle");
    drop(ctx);

    let mut partials = sink
        .iter()
        .map(|bytes| <(String, i64)>::decode(bytes).expect("decode"))
        .collect::<Vec<_>>();
    partials.sort();
    assert_eq!(partials, vec![("a".to_string(), 6), ("b".to_string(), 10)]);
}
