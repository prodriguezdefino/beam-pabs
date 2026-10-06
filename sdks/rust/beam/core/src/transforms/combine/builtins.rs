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

//! Built-in `i64` combine functions.

use super::CombineFn;

/// Adds `i64` values.
pub struct Sum;

impl CombineFn for Sum {
    type Input = i64;
    type Accum = i64;
    type Output = i64;

    fn create_accumulator(&self) -> i64 {
        0
    }

    fn add_input(&self, accumulator: i64, input: i64) -> i64 {
        accumulator + input
    }

    fn merge_accumulators(&self, accumulators: Vec<i64>) -> i64 {
        accumulators.into_iter().sum()
    }

    fn extract_output(&self, accumulator: i64) -> i64 {
        accumulator
    }
}

/// Computes the maximum over `i64` values.
pub struct Max;

impl CombineFn for Max {
    type Input = i64;
    type Accum = i64;
    type Output = i64;

    fn create_accumulator(&self) -> i64 {
        i64::MIN
    }

    fn add_input(&self, accumulator: i64, input: i64) -> i64 {
        accumulator.max(input)
    }

    fn merge_accumulators(&self, accumulators: Vec<i64>) -> i64 {
        accumulators.into_iter().fold(i64::MIN, i64::max)
    }

    fn extract_output(&self, accumulator: i64) -> i64 {
        accumulator
    }
}

/// Computes the minimum over `i64` values.
pub struct Min;

impl CombineFn for Min {
    type Input = i64;
    type Accum = i64;
    type Output = i64;

    fn create_accumulator(&self) -> i64 {
        i64::MAX
    }

    fn add_input(&self, accumulator: i64, input: i64) -> i64 {
        accumulator.min(input)
    }

    fn merge_accumulators(&self, accumulators: Vec<i64>) -> i64 {
        accumulators.into_iter().fold(i64::MAX, i64::min)
    }

    fn extract_output(&self, accumulator: i64) -> i64 {
        accumulator
    }
}
