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

//! Fixtures shared by the fluent test suites.

#![allow(
    dead_code,
    reason = "each test binary uses a different subset of these fixtures"
)]

use fluent::prelude::*;

/// Returns an owned `String`.
pub fn s(v: &str) -> String {
    v.to_string()
}

/// Creates a `(String, i64)` pair.
pub fn kv(k: &str, v: i64) -> (String, i64) {
    (k.to_string(), v)
}

/// Collects grouped values into a sorted `Vec`. Grouping does not preserve order.
pub fn sorted_groups(
    grouped: &PCollection<(String, BeamIterable<i64>)>,
    name: &str,
) -> PCollection<(String, Vec<i64>)> {
    grouped.map(name, |(k, values): (String, BeamIterable<i64>)| {
        let mut values: Vec<i64> = values.into_iter().collect();
        values.sort();
        (k, values)
    })
}

/// `[("a", 10), ("a", 20), ("b", 30)]`.
pub fn pairs(p: &Pipeline) -> PCollection<(String, i64)> {
    p.apply(Create::new(
        "Pairs",
        vec![kv("a", 10), kv("a", 20), kv("b", 30)],
    ))
}
