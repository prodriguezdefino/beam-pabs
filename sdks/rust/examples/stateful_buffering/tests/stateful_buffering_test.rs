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

//! Tests for `BufferPerKey` on Prism with a `TestStream`: a watermark advance flushes at the
//! end of the window, and a processing-time advance flushes on timeout.

use std::time::Duration;

use beam::prelude::*;
use beam::testing::{TestPipeline, TestStream, passert};
use stateful_buffering::{Batch, BufferPerKey, Flush};

const WINDOW: Duration = Duration::from_secs(10);
const TIMEOUT: Duration = Duration::from_secs(60);

fn reading(key: &str, value: i64, millis: i64) -> ((String, i64), Instant) {
    ((key.to_string(), value), Instant::from_millis(millis))
}

fn batch(key: &str, values: &[i64], reason: Flush) -> Batch {
    Batch {
        key: key.to_string(),
        values: values.to_vec(),
        sum: values.iter().sum(),
        reason,
    }
}

#[tokio::test]
async fn the_watermark_at_the_window_end_flushes_each_key() {
    let p = TestPipeline::new();
    let batches = p
        .apply(
            "Readings",
            TestStream::new()
                .add_timestamped_elements([
                    reading("a", 1, 1_000),
                    reading("a", 2, 2_000),
                    reading("b", 5, 3_000),
                ])
                .advance_watermark_to(Instant::from_millis(10_000))
                .advance_watermark_to_infinity(),
        )
        .apply("BufferPerKey", BufferPerKey::new(WINDOW, TIMEOUT));

    passert::that("AssertBatches", &batches).contains_in_any_order([
        batch("a", &[1, 2], Flush::WindowEnd),
        batch("b", &[5], Flush::WindowEnd),
    ]);
    p.run().await.expect("pipeline and assertions pass");
}

/// Prism reads an advance of `i64::MAX / 1000` milliseconds as processing time infinity. It
/// fires every processing-time timer.
const PROCESSING_TIME_INFINITY: Duration = Duration::from_millis((i64::MAX / 1000) as u64);

/// The timeout flushes the batch before the watermark reaches the end of the window.
#[tokio::test]
async fn a_processing_time_advance_past_the_timeout_flushes_the_batch() {
    let p = TestPipeline::new();
    let batches = p
        .apply(
            "Readings",
            TestStream::new()
                .add_timestamped_elements([reading("a", 1, 1_000), reading("a", 2, 2_000)])
                // A watermark advance makes the runner process the elements and set the timers.
                .advance_watermark_to(Instant::from_millis(5_000))
                .advance_processing_time(PROCESSING_TIME_INFINITY)
                .advance_watermark_to_infinity(),
        )
        .apply("BufferPerKey", BufferPerKey::new(WINDOW, TIMEOUT));

    passert::that("AssertBatches", &batches).contains_in_any_order([batch(
        "a",
        &[1, 2],
        Flush::Timeout,
    )]);
    p.run().await.expect("pipeline and assertions pass");
}
