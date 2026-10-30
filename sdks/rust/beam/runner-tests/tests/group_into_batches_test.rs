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

//! `GroupIntoBatches` on Prism: the count and byte limits, the end of the window, the
//! buffering duration and sharded keys.

use std::time::Duration;

use beam::coders::ShardedKey;
use beam::prelude::*;
use beam::testing::{TestPipeline, TestStream, passert};
use tests::prism_runner_from_env;

fn s(value: &str) -> String {
    value.to_string()
}

/// `count` values for each of the keys `a` and `b`.
fn keyed_values(count: i64) -> Vec<(String, i64)> {
    (0..count)
        .flat_map(|v| [(s("a"), v), (s("b"), 100 + v)])
        .collect()
}

/// Each batch with its values in order, so a batch compares without the bag order.
fn sorted_batches<K: beam::coders::DefaultCoder>(
    batches: &PCollection<(K, Vec<i64>)>,
    name: &str,
) -> PCollection<(K, Vec<i64>)> {
    batches.map(name, |(key, mut values): (K, Vec<i64>)| {
        values.sort_unstable();
        (key, values)
    })
}

fn explode(batches: &PCollection<(String, Vec<i64>)>, name: &str) -> PCollection<(String, i64)> {
    batches.flat_map(name, |(key, values): (String, Vec<i64>)| {
        values
            .into_iter()
            .map(|v| (key.clone(), v))
            .collect::<Vec<_>>()
    })
}

async fn run(p: TestPipeline) {
    p.run_with(&prism_runner_from_env())
        .await
        .expect("pipeline and assertions pass");
}

#[tokio::test]
async fn group_into_batches_emits_full_batches_by_count() {
    let p = TestPipeline::new();
    let batches = p
        .apply("Create", Create::new(keyed_values(10)))
        .group_into_batches("Batch", 4);

    passert::that("AssertBatchSize", &batches)
        .all("at most 4 values", |(_, values)| {
            (1..=4).contains(&values.len())
        })
        // 10 values per key: two full batches and one batch of 2, for each key.
        .has_count(6);
    passert::that("AssertEachValueOnce", &explode(&batches, "Explode"))
        .contains_in_any_order(keyed_values(10));
    run(p).await;
}

#[tokio::test]
async fn group_into_batches_emits_a_batch_when_the_byte_size_is_reached() {
    let p = TestPipeline::new();
    // Each string value encodes to 3 bytes, so the third value reaches 7 bytes.
    let words: Vec<(String, String)> = (0..7).map(|i| (s("k"), format!("w{i}"))).collect();
    let batches = p
        .apply("Create", Create::new(words.clone()))
        .apply("Batch", GroupIntoBatches::new(100).with_byte_size(7));

    passert::that("AssertBatchBytes", &batches)
        .all("at most 3 values", |(_, values)| {
            (1..=3).contains(&values.len())
        })
        .has_count(3);
    let values = batches.flat_map("Explode", |(key, values): (String, Vec<String>)| {
        values
            .into_iter()
            .map(|v| (key.clone(), v))
            .collect::<Vec<_>>()
    });
    passert::that("AssertEachValueOnce", &values).contains_in_any_order(words);
    run(p).await;
}

#[tokio::test]
async fn group_into_batches_emits_a_partial_batch_at_the_end_of_each_window() {
    let window = |start: i64| IntervalWindow {
        start: Instant::from_millis(start),
        end: Instant::from_millis(start + 10_000),
    };
    let at = |key: &str, value: i64, millis: i64| ((s(key), value), Instant::from_millis(millis));
    let p = TestPipeline::new();
    let batches = p
        .apply(
            "TestStream",
            TestStream::new()
                .add_timestamped_elements([
                    at("a", 1, 1_000),
                    at("a", 2, 2_000),
                    at("b", 3, 3_000),
                    at("a", 11, 11_000),
                ])
                .advance_watermark_to(Instant::from_millis(12_000))
                .add_timestamped_elements([at("a", 12, 12_500), at("b", 13, 13_000)])
                .advance_watermark_to_infinity(),
        )
        .apply(
            "FixedWindows",
            WindowInto::new(FixedWindows::of(Duration::from_secs(10))),
        )
        .group_into_batches("Batch", 100);
    let batches = sorted_batches(&batches, "Sort");

    passert::that("AssertFirstWindow", &batches)
        .in_window(window(0))
        .contains_in_any_order([(s("a"), vec![1, 2]), (s("b"), vec![3])]);
    passert::that("AssertSecondWindow", &batches)
        .in_window(window(10_000))
        .contains_in_any_order([(s("a"), vec![11, 12]), (s("b"), vec![13])]);
    run(p).await;
}

#[tokio::test]
async fn group_into_batches_with_sharded_key_keeps_the_user_key() {
    let p = TestPipeline::new();
    let batches = p
        .apply("Create", Create::new(keyed_values(10)))
        .apply("Batch", GroupIntoBatches::new(3).with_sharded_key());

    passert::that("AssertShardIds", &batches).all(
        "batches with an 8-byte shard id and at most 3 values",
        |(key, values): &(ShardedKey<String>, Vec<i64>)| {
            key.shard_id.len() == 8 && (1..=3).contains(&values.len())
        },
    );
    let unsharded = batches.map(
        "DropShard",
        |(key, values): (ShardedKey<String>, Vec<i64>)| (key.key, values),
    );
    passert::that("AssertEachValueOnce", &explode(&unsharded, "Explode"))
        .contains_in_any_order(keyed_values(10));
    run(p).await;
}

// Prism can run all TestStream events before the batching stage sets its timer, so the
// processing-time advance comes too early and the run gets one batch [1, 2, 3].
#[tokio::test]
#[ignore = "Prism does not order a TestStream processing-time advance after a timer is set"]
async fn group_into_batches_emits_a_batch_when_the_buffering_duration_ends() {
    let at = |value: i64, millis: i64| ((s("k"), value), Instant::from_millis(millis));
    let p = TestPipeline::new();
    let batches = p
        .apply(
            "TestStream",
            TestStream::new()
                .add_timestamped_elements([at(1, 1_000), at(2, 2_000)])
                .advance_processing_time(Duration::from_secs(60))
                .add_timestamped_elements([at(3, 3_000)])
                .advance_watermark_to_infinity(),
        )
        .apply(
            "Batch",
            GroupIntoBatches::new(100).with_max_buffering_duration(Duration::from_secs(10)),
        );

    // Without the buffering timer, all three values are in one batch at the window end.
    passert::that("AssertBatches", &sorted_batches(&batches, "Sort"))
        .contains_in_any_order([(s("k"), vec![1, 2]), (s("k"), vec![3])]);
    run(p).await;
}
