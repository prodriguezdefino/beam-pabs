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

//! The data plane without user code: inbound routing, outbound buffering, and how long one
//! slow bundle holds up another bundle on the same stream.
//!
//! ```text
//! ./gradlew :sdks:rust:harnessBenchmark -Pbench=data_plane
//! ```

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use futures::future::join_all;
use tokio::sync::mpsc;

use harness::data::{DataManager, InboundReceiver};
use model::fn_execution::{Elements, elements};

const CHUNK_BYTES: usize = 4 * 1024;
const CHUNKS: usize = 1_000;
/// Chunks the slow bundle receives before the fast one. More than one queue holds.
const SLOW_CHUNKS: usize = 300;
const FAST_CHUNKS: usize = 10;
/// How long the slow bundle takes to read one chunk.
const SLOW_READ: Duration = Duration::from_millis(1);

static NEXT_ID: AtomicUsize = AtomicUsize::new(0);

fn instruction_ids(count: usize) -> Vec<String> {
    (0..count)
        .map(|_| format!("instruction-{}", NEXT_ID.fetch_add(1, Ordering::Relaxed)))
        .collect()
}

fn message(instruction_id: &str, data: Vec<u8>, is_last: bool) -> Elements {
    Elements {
        data: vec![elements::Data {
            instruction_id: instruction_id.to_string(),
            transform_id: "source".to_string(),
            data,
            is_last,
        }],
        timers: Vec::new(),
    }
}

/// A data manager whose outbound messages are read and dropped.
fn data_manager() -> DataManager {
    let (tx, mut rx) = mpsc::channel::<Elements>(64);
    tokio::spawn(async move { while rx.recv().await.is_some() {} });
    DataManager::new(tx)
}

/// Reads `rx` to its end, waiting `delay` for each chunk. Returns the chunks read.
async fn read_all(mut rx: InboundReceiver<Vec<u8>>, delay: Duration) -> usize {
    let mut chunks = 0;
    while let Some(Some(_)) = rx.recv().await {
        chunks += 1;
        if !delay.is_zero() {
            tokio::time::sleep(delay).await;
        }
    }
    chunks
}

/// Sends `CHUNKS` chunks round robin to `bundles` registered instructions, as one stream.
async fn route(dm: &DataManager, bundles: usize) {
    let ids = instruction_ids(bundles);
    let mut readers = Vec::with_capacity(bundles);
    for id in &ids {
        readers.push(tokio::spawn(read_all(
            dm.register_inbound(id),
            Duration::ZERO,
        )));
    }
    for i in 0..CHUNKS {
        dm.handle_inbound_elements(message(&ids[i % bundles], vec![0; CHUNK_BYTES], false))
            .await;
    }
    for id in &ids {
        dm.handle_inbound_elements(message(id, Vec::new(), true))
            .await;
    }
    let read: usize = join_all(readers)
        .await
        .into_iter()
        .map(|r| r.expect("reader task"))
        .sum();
    assert_eq!(read, CHUNKS);
    for id in &ids {
        dm.unregister_inbound(id);
    }
}

/// Writes `CHUNKS` elements of `CHUNK_BYTES` through one bundle's outbound buffer.
async fn write(dm: &DataManager) {
    let id = &instruction_ids(1)[0];
    let mut outbound = dm.outbound("", id, ["sink"], []);
    let element = vec![7u8; CHUNK_BYTES];
    for _ in 0..CHUNKS {
        outbound
            .write(0, |buf| {
                buf.extend_from_slice(&element);
                Ok(())
            })
            .expect("buffered write");
        if outbound.has_pending() {
            outbound.drain().await.expect("drain");
        }
    }
    outbound.finish().await.expect("finish");
}

/// The time a fast bundle waits for its data while a slow bundle on the same stream reads
/// its backlog. Ideally near zero: the fast bundle's data does not depend on the slow one.
async fn head_of_line(dm: &DataManager) -> Duration {
    let ids = instruction_ids(2);
    let (slow, fast) = (&ids[0], &ids[1]);
    let slow_reader = tokio::spawn(read_all(dm.register_inbound(slow), SLOW_READ));
    let fast_rx = dm.register_inbound(fast);
    let started = Instant::now();
    let fast_reader = tokio::spawn(async move {
        let chunks = read_all(fast_rx, Duration::ZERO).await;
        (chunks, started.elapsed())
    });
    let feed = async {
        for _ in 0..SLOW_CHUNKS {
            dm.handle_inbound_elements(message(slow, vec![0; CHUNK_BYTES], false))
                .await;
        }
        for _ in 0..FAST_CHUNKS {
            dm.handle_inbound_elements(message(fast, vec![0; CHUNK_BYTES], false))
                .await;
        }
        dm.handle_inbound_elements(message(fast, Vec::new(), true))
            .await;
        dm.handle_inbound_elements(message(slow, Vec::new(), true))
            .await;
    };
    let ((), fast_done, slow_done) = tokio::join!(feed, fast_reader, slow_reader);
    let (fast_chunks, waited) = fast_done.expect("fast reader");
    assert_eq!(fast_chunks, FAST_CHUNKS);
    assert_eq!(slow_done.expect("slow reader"), SLOW_CHUNKS);
    for id in &ids {
        dm.unregister_inbound(id);
    }
    waited
}

fn data_plane(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().expect("tokio runtime");
    let dm = runtime.block_on(async { data_manager() });
    let mut group = c.benchmark_group("data_plane");

    group.throughput(Throughput::Bytes((CHUNKS * CHUNK_BYTES) as u64));
    for bundles in [1, 16] {
        group.bench_function(BenchmarkId::new("inbound_routing", bundles), |b| {
            b.to_async(&runtime).iter(|| route(&dm, bundles));
        });
    }
    group.bench_function("outbound_buffering", |b| {
        b.to_async(&runtime).iter(|| write(&dm));
    });
    group.finish();

    let mut group = c.benchmark_group("data_plane");
    group.sample_size(10);
    group.bench_function("head_of_line_wait", |b| {
        b.to_async(&runtime).iter_custom(|iterations| {
            let dm = dm.clone();
            async move {
                let mut waited = Duration::ZERO;
                for _ in 0..iterations {
                    waited += head_of_line(&dm).await;
                }
                waited
            }
        });
    });
    group.finish();
}

criterion_group!(benches, data_plane);
criterion_main!(benches);
