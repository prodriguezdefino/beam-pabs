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
use windowed_wordcount::WindowedWordCount;

/// Equal lines fall in the same window, so their words are counted together.
#[tokio::test]
async fn counts_words_per_window() {
    let p = TestPipeline::new();
    let lines = ["hello window", "hello prism", "hello window"].map(String::from);

    let counts = p.apply("Create", Create::new(lines.to_vec())).apply(
        "WindowedWordCount",
        WindowedWordCount {
            window_size: Duration::from_secs(60),
            base_timestamp: Instant::from_millis(1_000_000),
        },
    );

    passert::that("AssertCounts", &counts).contains_in_any_order(
        [
            "[1970-01-01T00:16:00.000Z..1970-01-01T00:17:00.000Z) hello: 1",
            "[1970-01-01T00:16:00.000Z..1970-01-01T00:17:00.000Z) prism: 1",
            "[1970-01-01T00:18:00.000Z..1970-01-01T00:19:00.000Z) hello: 2",
            "[1970-01-01T00:18:00.000Z..1970-01-01T00:19:00.000Z) window: 2",
        ]
        .map(String::from),
    );

    p.run().await.expect("pipeline and assertions must pass");
}
