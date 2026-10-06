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

use std::time::Duration;

use beam::prelude::*;
use beam::testing::{TestPipeline, passert};
use streaming::CountTicks;

/// Ticks at 0 s to 4 s fall in the 2-second windows of their event times.
#[tokio::test]
async fn counts_even_and_odd_ticks_per_window() {
    let p = TestPipeline::new();

    let counts = p
        .apply("Ticks", Create::new(vec![0_i64, 1, 2, 3, 4]))
        .par_do_fn("AtTickSecond", |tick: i64, ctx| {
            ctx.output(tick)
                .at(Instant::from_millis(tick * 1_000))
                .emit()
        })
        .apply(
            "CountTicks",
            CountTicks {
                window_size: Duration::from_secs(2),
            },
        );

    let window = |from: i64| {
        IntervalWindow::new(
            Instant::from_millis(from * 1_000),
            Instant::from_millis((from + 2) * 1_000),
        )
    };
    passert::that("AssertCounts", &counts).contains_in_any_order([
        format!("{} even: 1", window(0)),
        format!("{} odd: 1", window(0)),
        format!("{} even: 1", window(2)),
        format!("{} odd: 1", window(2)),
        format!("{} even: 1", window(4)),
    ]);

    p.run().await.expect("pipeline and assertions must pass");
}
