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
use beam::internals::HandlerContext;
use beam::pipeline::Pipeline;
use beam::pipeline::constants::{COMBINE_STAGE_PRECOMBINE, combine_stage_key};
use beam::prelude::*;
use beam::transforms::{CombineFn, CombinePerKey, Max, Min, Sum};

/// Folds each half of `inputs` into a partial, merges the partials and extracts.
fn combine<CF>(combine_fn: &CF, inputs: &[i64]) -> i64
where
    CF: CombineFn<Input = i64, Accum = i64, Output = i64>,
{
    let partial = |part: &[i64]| {
        part.iter()
            .fold(combine_fn.create_accumulator(), |acc, &x| {
                combine_fn.add_input(acc, x)
            })
    };
    let (left, right) = inputs.split_at(inputs.len() / 2);
    combine_fn.extract_output(combine_fn.merge_accumulators(vec![partial(left), partial(right)]))
}

#[test]
fn built_in_combine_fns_fold_merge_and_extract() {
    let mixed = [-7, 12, 5, -3];
    for (case, combined, expected) in [
        ("sum", combine(&Sum, &mixed), 7),
        ("max", combine(&Max, &mixed), 12),
        ("min", combine(&Min, &mixed), -7),
        // All inputs have the same sign, so a wrong identity value changes the result.
        ("max of negatives", combine(&Max, &[-9, -4, -6]), -4),
        ("min of positives", combine(&Min, &[9, 4, 6]), 4),
        ("sum of none", combine(&Sum, &[]), 0),
        ("max of none", combine(&Max, &[]), i64::MIN),
        ("min of none", combine(&Min, &[]), i64::MAX),
    ] {
        assert_eq!(combined, expected, "{case}");
    }
}

/// Under its memory budget, pre-combine keeps all accumulators until the bundle ends, so
/// each key leaves the bundle as one partial.
#[test]
fn precombine_emits_one_partial_per_key_at_bundle_end() {
    let p = Pipeline::new();
    let _ = p
        .apply(Create::new("Create", vec![("a".to_string(), 0i64)]))
        .apply(CombinePerKey::new("Sum", Sum));
    let handler = p
        .transform_handlers()
        .remove(&combine_stage_key("Sum", COMBINE_STAGE_PRECOMBINE))
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
