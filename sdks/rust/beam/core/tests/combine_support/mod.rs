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

//! Helpers for the `CombineFn` tests in `core/tests`.

#![allow(dead_code, reason = "shared test helper")]

use beam::transforms::CombineFn;

/// Runs `combine_fn` as a runner does: it deals `inputs` to `parts` partial accumulators in
/// turn, merges the partials and extracts the output. More than one part makes the merge of
/// the function run.
pub fn combine_in_parts<CF: CombineFn>(
    combine_fn: &CF,
    inputs: Vec<CF::Input>,
    parts: usize,
) -> beam::Result<CF::Output> {
    let groups = inputs.into_iter().enumerate().fold(
        (0..parts).map(|_| Vec::new()).collect::<Vec<_>>(),
        |mut groups, (index, input)| {
            groups[index % parts].push(input);
            groups
        },
    );
    let partials = groups
        .into_iter()
        .map(|group| {
            group
                .into_iter()
                .try_fold(combine_fn.create_accumulator()?, |acc, input| {
                    combine_fn.add_input(acc, input)
                })
        })
        .collect::<beam::Result<Vec<_>>>()?;
    combine_fn.extract_output(combine_fn.merge_accumulators(partials)?)
}
