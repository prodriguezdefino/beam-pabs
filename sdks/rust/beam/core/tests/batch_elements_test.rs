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

//! Tests for when `BatchElements` emits a batch.

use std::time::Duration;

use beam::coders::DefaultCoder;
use beam::internals::HandlerContext;
use beam::pipeline::Pipeline;
use beam::prelude::*;
use beam::transforms::BatchElements;

/// Runs one bundle of `0..count` through `batcher`. Returns the batches emitted during
/// processing and those flushed at bundle end.
fn run_bundle(batcher: BatchElements<i64>, count: i64) -> (Vec<Vec<i64>>, Vec<Vec<i64>>) {
    let p = Pipeline::new();
    let batches = p.apply(Create::new("Create", vec![0i64])).apply(batcher);
    let id = p
        .producer_transform_id(batches.id())
        .expect("BatchElements transform must exist");
    let handler = p
        .transform_handlers()
        .remove(&id)
        .expect("BatchElements handler must be registered");

    let mut stage = handler.instantiate();
    let decode = |sink: &[Vec<u8>]| -> Vec<Vec<i64>> {
        sink.iter()
            .map(|bytes| Vec::<i64>::decode(bytes).expect("decode"))
            .collect()
    };
    let mut processed = Vec::<Vec<u8>>::new();
    let mut ctx = HandlerContext::new(&mut processed);
    for value in 0..count {
        let element = value.encode().expect("encode");
        stage.process(&element, &mut ctx).expect("process");
    }
    drop(ctx);
    let mut flushed = Vec::<Vec<u8>>::new();
    stage
        .finish_bundle(&mut HandlerContext::new(&mut flushed))
        .expect("finish_bundle");
    (decode(&processed), decode(&flushed))
}

#[test]
fn a_batch_is_emitted_when_it_reaches_the_max_size() {
    // Without a duration limit, reaching the minimum size does not trigger emission.
    let (processed, flushed) = run_bundle(BatchElements::new("Batch", 2, 3), 7);
    assert_eq!(processed, vec![vec![0, 1, 2], vec![3, 4, 5]]);
    assert_eq!(flushed, vec![vec![6]]);
}

#[test]
fn an_expired_batch_is_emitted_once_it_reaches_the_min_size() {
    // With zero duration, every batch expires immediately and emits at the minimum size.
    let expired = BatchElements::new("Batch", 2, 10).with_max_batch_duration(Duration::ZERO);
    let (processed, flushed) = run_bundle(expired, 5);
    assert_eq!(processed, vec![vec![0, 1], vec![2, 3]]);
    assert_eq!(flushed, vec![vec![4]]);

    let unexpired =
        BatchElements::new("Batch", 2, 10).with_max_batch_duration(Duration::from_secs(3_600));
    let (processed, flushed) = run_bundle(unexpired, 5);
    assert!(processed.is_empty(), "{processed:?}");
    assert_eq!(flushed, vec![vec![0, 1, 2, 3, 4]]);
}
