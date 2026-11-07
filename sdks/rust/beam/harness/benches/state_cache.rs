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

//! Multi-threaded throughput of [`StateCache`] across 1, 4, 8 and 16 worker threads.
//!
//! - `uniform_read_hit`: reads across 4,096 cached bag cells that fit in the cache.
//! - `hot_key_read_hit`: 75% of reads hit 16 hot keys, 25% sweep 4,096 keys.
//! - `mixed_read_write`: 90% cached bag reads and 10% bag updates on 4,096 keys.
//! - `eviction_churn`: 75% reads and 25% inserts over 8,192 keys in a 1 MiB cache.
//!
//! ```text
//! ./gradlew :sdks:rust:harnessBenchmark -Pbench=state_cache -PcargoProfile=worker
//! ```

use std::hint::black_box;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier};
use std::thread::{self, JoinHandle};

use beam::internals::StateBag;
use beam_harness::state::{DEFAULT_STATE_CACHE_BYTES, StateCache};
use bytes::Bytes;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};

const TRANSFORM: &str = "stateful";
const STATE: &str = "bag";
const WINDOW: &[u8] = b"\x80\x00\x00\x00\x00\x00\x27\x10\x27\x10";
const TOKEN: &[u8] = b"runner-user-state-token-0001";

const FIT_KEYS: usize = 4_096;
const HOT_KEYS: usize = 16;
const CHURN_KEYS: usize = 8_192;
const CHURN_CACHE_BYTES: usize = 1 << 20;
const OPS_PER_ROUND: usize = 65_536;
const CHURN_OPS_PER_ROUND: usize = 32_768;
const THREAD_COUNTS: [usize; 4] = [1, 4, 8, 16];

fn make_keys(count: usize) -> Arc<[[u8; 8]]> {
    (0..count)
        .map(|i| (i as u64).to_be_bytes())
        .collect::<Vec<_>>()
        .into()
}

fn make_bags(count: usize) -> Arc<[StateBag]> {
    (0..count)
        .map(|i| {
            let byte = (i & 0xff) as u8;
            StateBag::from_packed(Bytes::from(vec![byte; 64]), Box::new([0..32, 32..64]))
                .expect("valid bag")
        })
        .collect::<Vec<_>>()
        .into()
}

fn seed_cache(max_bytes: usize, keys: &[[u8; 8]], bags: &[StateBag]) -> Arc<StateCache> {
    let cache = Arc::new(StateCache::new(max_bytes));
    let token = Bytes::from_static(TOKEN);
    for (key, bag) in keys.iter().zip(bags) {
        cache.insert_bag(token.clone(), TRANSFORM, STATE, WINDOW, key, bag.clone());
    }
    cache
}

/// Persistent worker pool so `b.iter` measures cache operations instead of thread creation.
struct BenchPool {
    threads: usize,
    start: Arc<Barrier>,
    done: Arc<Barrier>,
    stop: Arc<AtomicBool>,
    handles: Vec<JoinHandle<()>>,
    local_work: Box<dyn Fn() + Send + Sync>,
}

impl BenchPool {
    fn spawn<F>(threads: usize, work: F) -> Self
    where
        F: Fn(usize) + Send + Sync + 'static,
    {
        let work = Arc::new(work);
        let start = Arc::new(Barrier::new(threads));
        let done = Arc::new(Barrier::new(threads));
        let stop = Arc::new(AtomicBool::new(false));
        let handles = (1..threads)
            .map(|tid| {
                let start = Arc::clone(&start);
                let done = Arc::clone(&done);
                let stop = Arc::clone(&stop);
                let work = Arc::clone(&work);
                thread::spawn(move || {
                    loop {
                        start.wait();
                        if stop.load(Ordering::Acquire) {
                            return;
                        }
                        work(tid);
                        done.wait();
                    }
                })
            })
            .collect();
        let main_work = Arc::clone(&work);
        Self {
            threads,
            start,
            done,
            stop,
            handles,
            local_work: Box::new(move || main_work(0)),
        }
    }

    fn run_round(&self) {
        if self.threads == 1 {
            (self.local_work)();
            return;
        }
        self.start.wait();
        (self.local_work)();
        self.done.wait();
    }
}

impl Drop for BenchPool {
    fn drop(&mut self) {
        if self.threads == 1 {
            return;
        }
        self.stop.store(true, Ordering::Release);
        self.start.wait();
        for handle in self.handles.drain(..) {
            let _ = handle.join();
        }
    }
}

#[inline]
fn step_index(tid: usize, i: usize) -> usize {
    // Prime stride spreads threads across the key ring without shared RNG state.
    tid.wrapping_mul(1_315_423_911)
        .wrapping_add(i.wrapping_mul(2_654_435_761))
}

fn bench_uniform_read_hit(c: &mut Criterion) {
    let keys = make_keys(FIT_KEYS);
    let bags = make_bags(FIT_KEYS);
    let cache = seed_cache(DEFAULT_STATE_CACHE_BYTES, &keys, &bags);

    let mut group = c.benchmark_group("state_cache/uniform_read_hit");
    group.throughput(Throughput::Elements(OPS_PER_ROUND as u64));

    for threads in THREAD_COUNTS {
        let ops_per_thread = OPS_PER_ROUND / threads;
        let cache = Arc::clone(&cache);
        let keys = Arc::clone(&keys);
        let pool = BenchPool::spawn(threads, move |tid| {
            let mut total = 0usize;
            for i in 0..ops_per_thread {
                let idx = step_index(tid, i) & (FIT_KEYS - 1);
                if let Some(found) = cache.get_bag(TOKEN, TRANSFORM, STATE, WINDOW, &keys[idx]) {
                    total = total.wrapping_add(found.byte_len());
                }
            }
            black_box(total);
        });
        group.bench_function(BenchmarkId::from_parameter(threads), |b| {
            b.iter(|| pool.run_round());
        });
    }
    group.finish();
}

fn bench_hot_key_read_hit(c: &mut Criterion) {
    let keys = make_keys(FIT_KEYS);
    let bags = make_bags(FIT_KEYS);
    let cache = seed_cache(DEFAULT_STATE_CACHE_BYTES, &keys, &bags);

    let mut group = c.benchmark_group("state_cache/hot_key_read_hit");
    group.throughput(Throughput::Elements(OPS_PER_ROUND as u64));

    for threads in THREAD_COUNTS {
        let ops_per_thread = OPS_PER_ROUND / threads;
        let cache = Arc::clone(&cache);
        let keys = Arc::clone(&keys);
        let pool = BenchPool::spawn(threads, move |tid| {
            let mut total = 0usize;
            for i in 0..ops_per_thread {
                let h = step_index(tid, i);
                let idx = if i & 3 != 0 {
                    h & (HOT_KEYS - 1)
                } else {
                    h & (FIT_KEYS - 1)
                };
                if let Some(found) = cache.get_bag(TOKEN, TRANSFORM, STATE, WINDOW, &keys[idx]) {
                    total = total.wrapping_add(found.byte_len());
                }
            }
            black_box(total);
        });
        group.bench_function(BenchmarkId::from_parameter(threads), |b| {
            b.iter(|| pool.run_round());
        });
    }
    group.finish();
}

fn bench_mixed_read_write(c: &mut Criterion) {
    let keys = make_keys(FIT_KEYS);
    let bags = make_bags(FIT_KEYS);

    let mut group = c.benchmark_group("state_cache/mixed_read_write");
    group.throughput(Throughput::Elements(OPS_PER_ROUND as u64));

    for threads in THREAD_COUNTS {
        let ops_per_thread = OPS_PER_ROUND / threads;
        let cache = seed_cache(DEFAULT_STATE_CACHE_BYTES, &keys, &bags);
        let keys = Arc::clone(&keys);
        let bags = Arc::clone(&bags);
        let token = Bytes::from_static(TOKEN);
        let pool = BenchPool::spawn(threads, move |tid| {
            let mut total = 0usize;
            for i in 0..ops_per_thread {
                let idx = step_index(tid, i) & (FIT_KEYS - 1);
                if i % 10 == 0 {
                    cache.insert_bag(
                        token.clone(),
                        TRANSFORM,
                        STATE,
                        WINDOW,
                        &keys[idx],
                        bags[idx].clone(),
                    );
                } else if let Some(found) =
                    cache.get_bag(TOKEN, TRANSFORM, STATE, WINDOW, &keys[idx])
                {
                    total = total.wrapping_add(found.byte_len());
                }
            }
            black_box(total);
        });
        group.bench_function(BenchmarkId::from_parameter(threads), |b| {
            b.iter(|| pool.run_round());
        });
    }
    group.finish();
}

fn bench_eviction_churn(c: &mut Criterion) {
    let keys = make_keys(CHURN_KEYS);
    let bags = make_bags(CHURN_KEYS);

    let mut group = c.benchmark_group("state_cache/eviction_churn");
    group.throughput(Throughput::Elements(CHURN_OPS_PER_ROUND as u64));

    for threads in THREAD_COUNTS {
        let ops_per_thread = CHURN_OPS_PER_ROUND / threads;
        let cache = seed_cache(CHURN_CACHE_BYTES, &keys, &bags);
        let keys = Arc::clone(&keys);
        let bags = Arc::clone(&bags);
        let token = Bytes::from_static(TOKEN);
        let pool = BenchPool::spawn(threads, move |tid| {
            let mut total = 0usize;
            for i in 0..ops_per_thread {
                let idx = step_index(tid, i) & (CHURN_KEYS - 1);
                if i & 3 == 0 {
                    cache.insert_bag(
                        token.clone(),
                        TRANSFORM,
                        STATE,
                        WINDOW,
                        &keys[idx],
                        bags[idx].clone(),
                    );
                } else if let Some(found) =
                    cache.get_bag(TOKEN, TRANSFORM, STATE, WINDOW, &keys[idx])
                {
                    total = total.wrapping_add(found.byte_len());
                }
            }
            black_box(total);
        });
        group.bench_function(BenchmarkId::from_parameter(threads), |b| {
            b.iter(|| pool.run_round());
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_uniform_read_hit,
    bench_hot_key_read_hit,
    bench_mixed_read_write,
    bench_eviction_churn
);
criterion_main!(benches);
