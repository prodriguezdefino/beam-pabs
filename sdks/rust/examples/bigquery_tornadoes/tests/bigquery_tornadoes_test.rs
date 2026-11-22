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
use bigquery_tornadoes::{CountTornadoes, TornadoCount, WeatherStation};

#[tokio::test]
async fn test_count_tornadoes() {
    let p = TestPipeline::new();

    let observations = [
        (Some(1), Some(true)),
        (Some(1), Some(false)),
        (Some(1), Some(true)),
        (Some(5), Some(true)),
        (Some(7), Some(false)),
        (Some(9), None),
        (None, Some(true)),
    ]
    .map(|(month, tornado)| WeatherStation { month, tornado });
    let counts = p
        .apply("Create", Create::new(observations.to_vec()))
        .apply("CountTornadoes", CountTornadoes);

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
