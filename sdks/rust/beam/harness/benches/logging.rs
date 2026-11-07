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

//! The per-event cost of logging through the global handle of the Fn API logging layer.
//!
//! - `has_client`: the check that the stdout filter makes on every event.
//! - `send`: one entry to a connected client. A task drains the client's batches, so its
//!   queue never fills.
//! - `tracing_event`: one `tracing::info!` event through the logging layer to the client.
//!
//! ```text
//! ./gradlew :sdks:rust:harnessBenchmark -Pbench=logging
//! ```

use std::hint::black_box;

use criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main};
use tokio::sync::mpsc;
use tracing_subscriber::layer::SubscriberExt;

use beam_harness::logging::{
    BeamFnLoggingHandle, BeamFnLoggingLayer, LOG_QUEUE_BATCHES, LoggingClient,
};
use beam_model::fn_execution::LogEntry;

fn logging(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .expect("runtime");
    let _guard = runtime.enter();
    let (tx, mut rx) = mpsc::channel(LOG_QUEUE_BATCHES);
    let client = LoggingClient::new(tx);
    runtime.spawn(async move { while rx.recv().await.is_some() {} });
    let handle = BeamFnLoggingHandle::new();
    let mut group = c.benchmark_group("logging");
    group.throughput(Throughput::Elements(1));

    group.bench_function("has_client", |b| b.iter(|| black_box(handle.has_client())));

    handle.set_client(client);
    let entry = LogEntry {
        message: "processed element 42 of the bundle".to_string(),
        ..Default::default()
    };
    group.bench_function("send", |b| {
        b.iter_batched(
            || entry.clone(),
            |entry| handle.send(entry),
            BatchSize::SmallInput,
        );
    });

    let subscriber = tracing_subscriber::registry().with(BeamFnLoggingLayer::new(handle.clone()));
    tracing::subscriber::with_default(subscriber, || {
        group.bench_function("tracing_event", |b| {
            b.iter(|| tracing::info!(element = 42, "processed element of the bundle"));
        });
    });
    group.finish();
}

criterion_group!(benches, logging);
criterion_main!(benches);
