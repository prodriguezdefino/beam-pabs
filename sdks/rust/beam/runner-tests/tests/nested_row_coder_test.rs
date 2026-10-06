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

//! Schema'd (`#[derive(BeamRow)]`) values nested inside composites on PrismRunner.
//!
//! Prism cannot interpret `beam:coder:row:v1`, so it length-prefixes every Row coder at
//! the bundle's data ports, including Rows nested in a `KV` or a grouped iterable. These
//! pipelines fail unless the harness honours those nested prefixes in both directions.

use std::sync::{Arc, Mutex};

use beam::pipeline::Pipeline;
use beam::schema::BeamRow;
use beam::transforms::{Create, Map};
use fluent::prelude::PCollectionExt;
use prism::PrismRunner;

#[derive(Debug, Clone, PartialEq, BeamRow)]
struct Reading {
    id: i64,
    sensor: String,
    value: Option<f64>,
}

fn readings(n: i64) -> Vec<Reading> {
    (0..n)
        .map(|id| Reading {
            id,
            sensor: format!("sensor-{}", id % 4),
            value: (id % 5 != 0).then_some(id as f64 / 2.0),
        })
        .collect()
}

#[tokio::test]
async fn row_keyed_values_cross_a_fused_stage_boundary() {
    // KV<i32, Row> written to a data sink and read back by the next stage, without a
    // grouping in between that would hide a mis-framed element.
    let captured = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&captured);
    let p = Pipeline::new();
    p.apply(Create::new("Create", readings(20)))
        .apply(Map::new("Key", |r: Reading| (r.id as i32, r)))
        .apply(beam::transforms::Reshuffle::new("Reshuffle"))
        .inspect("Capture", move |kv: &(i32, Reading)| {
            sink.lock().unwrap().push(kv.clone());
        });
    let res = p.run_with_runner(&PrismRunner::new()).await;
    assert!(res.is_ok(), "pipeline failed: {res:?}");

    let mut actual = captured.lock().unwrap().clone();
    actual.sort_by_key(|kv| kv.0);
    let expected: Vec<(i32, Reading)> =
        readings(20).into_iter().map(|r| (r.id as i32, r)).collect();
    assert_eq!(actual, expected);
}
