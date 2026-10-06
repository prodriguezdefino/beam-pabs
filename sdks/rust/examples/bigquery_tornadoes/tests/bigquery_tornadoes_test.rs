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

use beam::prelude::*;
use beam::testing::{TestPipeline, passert};
use bigquery_tornadoes::{CountTornadoes, TornadoCount};

/// The columns of a weather station row that the example reads.
#[derive(BeamRow)]
struct Observation {
    month: i64,
    tornado: bool,
}

#[tokio::test]
async fn test_count_tornadoes() {
    let p = TestPipeline::new();

    let observations = [(1, true), (1, false), (1, true), (5, true), (7, false)]
        .map(|(month, tornado)| Observation { month, tornado }.to_row().expect("valid row"));
    let counts = p
        .apply("Create", Create::new(observations.to_vec()))
        .with_row_schema(Observation::beam_schema())
        .apply("CountTornadoes", CountTornadoes)
        .par_do_fn("FromRow", |row: Row, out| {
            out.emit(TornadoCount::from_row(&row)?)
        });

    passert::that("AssertCounts", &counts).contains_in_any_order([
        TornadoCount {
            month: 1,
            tornado_count: 2,
        },
        TornadoCount {
            month: 5,
            tornado_count: 1,
        },
    ]);

    p.run().await.expect("CountTornadoes assertions must pass");
}
