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

//! Bag, value, multimap, set, and ordered-list user state of one bundle against an
//! in-process state server: the first read of each key, appends, cached reads, the first
//! read of a bag of many pages, and the commit at the end of the bundle. The
//! `multimap_new_keys` case lists the keys of a multimap, puts to new keys and reads them
//! back. The `set_insert_contains` case reads a set, clears it, inserts unique elements,
//! checks hits and misses, and commits. The `ordered_list_add_read_range` case reads a
//! timestamp slice from the server, clears the list, adds out-of-order entries, and commits.
//! The `user_state_cpu` group runs the same benchmarks and reports the CPU time of the
//! process. For `cached_reads` and `value_read_write`, it reports the CPU time of the bench
//! thread, so the threads of earlier benchmarks do not count.
//!
//! The `cached_append_commit` cases run one bundle with a cache token. The bundle appends one
//! element to a bag of 10 or 1,000 elements that the state cache holds, and commits. It does
//! not read the bag. The `value_read_write` case reads and writes one value cell 1,000 times
//! in one bundle.
//!
//! The `paged_read_latency` cases read the same bag from a second server that waits
//! [`GET_DELAY`] before each GET reply, as a runner with latency does. The `pages_*` cases
//! read the bag one page at a time and decode each page before they ask for the next one.
//! `pages_work` also spins for [`PAGE_WORK`] on each page, as the work of a DoFn.
//!
//! ```text
//! ./gradlew :sdks:rust:harnessBenchmark -Pbench=user_state
//! ./gradlew :sdks:rust:harnessBenchmark -Pbench=user_state -Pfilter=paged_read_latency
//! ```

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use criterion::measurement::Measurement;
use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use prost::Message;

use beam::internals::{StateBinding, StateCell};
use beam::pipeline::URN_PAR_DO;
use beam::transforms::{OrderedListState, SetState, ValueState};
use beam_harness::state::{StateCache, StateClients};
use beam_harness::user_state::{BundleUserState, UserStateCache, UserStateSpecs};
use beam_model::fn_execution::{ProcessBundleDescriptor, StateKey, state_key};
use beam_model::pipeline::{self as proto, ApiServiceDescriptor, FunctionSpec};

#[path = "../tests/state/state_mock.rs"]
mod state_mock;
use state_mock::{FullKey, StrictStateServer};

#[path = "support/cpu_time.rs"]
mod cpu_time;
use cpu_time::CpuTime;

#[path = "support/thread_cpu_time.rs"]
mod thread_cpu_time;
use thread_cpu_time::ThreadCpuTime;

const TRANSFORM: &str = "stateful";
const STATE: &str = "bag";
const MULTIMAP: &str = "multimap";
const SET_STATE: &str = "set";
const ORDERED_LIST: &str = "ordered_list";
const KEYS: usize = 100;
const APPENDS_PER_KEY: usize = 10;
const CACHED_READS: usize = 1_000;
/// The key and the sizes of the bag of `cached_append_commit`, and the token of its bundles.
const CACHED_KEY: &[u8] = b"cached";
const CACHED_BAG_SIZES: [usize; 2] = [10, 1_000];
const CACHE_TOKEN: &[u8] = b"token";
/// The key of the cell of `value_read_write`.
const VALUE_KEY: &[u8] = b"value";
/// The bag of `paged_read`: [`PAGES`] pages of [`PAGE_BYTES`] one-byte varints.
const PAGED_KEY: &[u8] = b"paged";
const PAGES: usize = 64;
const PAGE_BYTES: usize = 4 << 10;
/// The wait of the latency server before each GET reply.
const GET_DELAY: Duration = Duration::from_millis(1);
/// The CPU work of `pages_work` on each page, after the decode.
const PAGE_WORK: Duration = Duration::from_micros(500);
/// Samples of each latency case. One iteration takes about 64 x [`GET_DELAY`].
const LATENCY_SAMPLES: usize = 10;

static NEXT_BUNDLE: AtomicUsize = AtomicUsize::new(0);

fn descriptor(endpoint: &str) -> ProcessBundleDescriptor {
    let bag = proto::StateSpec {
        protocol: Some(FunctionSpec {
            urn: "beam:user_state:bag:v1".to_string(),
            payload: Vec::new(),
        }),
        spec: Some(proto::state_spec::Spec::BagSpec(proto::BagStateSpec {
            element_coder_id: "varint".to_string(),
        })),
    };
    let multimap = proto::StateSpec {
        protocol: Some(FunctionSpec {
            urn: "beam:user_state:multimap:v1".to_string(),
            payload: Vec::new(),
        }),
        spec: Some(proto::state_spec::Spec::MultimapSpec(
            proto::MultimapStateSpec {
                key_coder_id: "varint".to_string(),
                value_coder_id: "varint".to_string(),
            },
        )),
    };
    let set = proto::StateSpec {
        protocol: Some(FunctionSpec {
            urn: "beam:user_state:multimap:v1".to_string(),
            payload: Vec::new(),
        }),
        spec: Some(proto::state_spec::Spec::SetSpec(proto::SetStateSpec {
            element_coder_id: "varint".to_string(),
        })),
    };
    let ordered_list = proto::StateSpec {
        protocol: Some(FunctionSpec {
            urn: "beam:user_state:ordered_list:v1".to_string(),
            payload: Vec::new(),
        }),
        spec: Some(proto::state_spec::Spec::OrderedListSpec(
            proto::OrderedListStateSpec {
                element_coder_id: "varint".to_string(),
            },
        )),
    };
    let pardo = proto::ParDoPayload {
        state_specs: HashMap::from([
            (STATE.to_string(), bag),
            (MULTIMAP.to_string(), multimap),
            (SET_STATE.to_string(), set),
            (ORDERED_LIST.to_string(), ordered_list),
        ]),
        ..Default::default()
    };
    ProcessBundleDescriptor {
        id: "stateful".to_string(),
        state_api_service_descriptor: Some(ApiServiceDescriptor {
            url: endpoint.to_string(),
            ..Default::default()
        }),
        transforms: HashMap::from([(
            TRANSFORM.to_string(),
            proto::PTransform {
                unique_name: TRANSFORM.to_string(),
                spec: Some(FunctionSpec {
                    urn: URN_PAR_DO.to_string(),
                    payload: pardo.encode_to_vec(),
                }),
                ..Default::default()
            },
        )]),
        coders: HashMap::from([(
            "varint".to_string(),
            proto::Coder {
                spec: Some(FunctionSpec {
                    urn: "beam:coder:varint:v1".to_string(),
                    payload: Vec::new(),
                }),
                component_coder_ids: Vec::new(),
            },
        )]),
        ..Default::default()
    }
}

/// The user state of a new bundle. The specs come from the descriptor once, as in a worker.
/// With `cache`, the bundle has [`CACHE_TOKEN`] and uses that state cache.
fn bundle_state(
    clients: &StateClients,
    descriptor: &ProcessBundleDescriptor,
    specs: &Arc<UserStateSpecs>,
    cache: Option<&Arc<StateCache>>,
) -> BundleUserState {
    let instruction = format!("bundle-{}", NEXT_BUNDLE.fetch_add(1, Ordering::Relaxed));
    let channel = clients.channel(&instruction, descriptor).expect("channel");
    let cache = cache.map(|cache| UserStateCache {
        cache: Arc::clone(cache),
        token: Bytes::from_static(CACHE_TOKEN),
    });
    BundleUserState::new(Arc::clone(specs), channel, cache)
}

/// Decodes the varints of one page and returns their count and their sum.
fn decode_page(page: &[u8]) -> (u64, u64) {
    let (mut count, mut sum, mut value, mut shift) = (0u64, 0u64, 0u64, 0u32);
    for &byte in page {
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            count += 1;
            sum = sum.wrapping_add(value);
            value = 0;
            shift = 0;
        } else {
            shift += 7;
        }
    }
    (count, sum)
}

/// Keeps the CPU busy for `work`, as a DoFn that processes the elements of a page.
fn spin(work: Duration) {
    let started = Instant::now();
    while started.elapsed() < work {
        std::hint::spin_loop();
    }
}

/// Reads the bag at `PAGED_KEY` one page at a time. Each page is decoded, then `work` runs,
/// then the next page is requested.
fn read_pages(clients: &StateClients, endpoint: &str, window: &[u8], work: Duration) -> u64 {
    let instruction = format!("bundle-{}", NEXT_BUNDLE.fetch_add(1, Ordering::Relaxed));
    let channel = clients.channel_to(&instruction, endpoint);
    let key = StateKey {
        r#type: Some(state_key::Type::BagUserState(state_key::BagUserState {
            transform_id: TRANSFORM.to_string(),
            user_state_id: STATE.to_string(),
            window: window.to_vec(),
            key: PAGED_KEY.to_vec(),
        })),
    };
    let mut elements = 0;
    for page in channel.stream_pages(key).expect("page stream") {
        let (count, sum) = decode_page(&page.expect("page"));
        std::hint::black_box(sum);
        elements += count;
        if !work.is_zero() {
            spin(work);
        }
    }
    assert_eq!(
        elements,
        (PAGES * PAGE_BYTES) as u64,
        "every element of the bag"
    );
    elements
}

/// A state server, a descriptor that points to it, the state clients and the user state specs.
fn setup() -> (
    StrictStateServer,
    ProcessBundleDescriptor,
    StateClients,
    Arc<UserStateSpecs>,
) {
    let server = StrictStateServer::start();
    let descriptor = descriptor(server.endpoint());
    let clients = StateClients::new("worker".to_string());
    let specs = Arc::new(
        UserStateSpecs::from_descriptor(&descriptor, &Arc::new(descriptor.coders.clone()))
            .expect("the descriptor declares user state"),
    );
    (server, descriptor, clients, specs)
}

fn user_state<M: Measurement>(c: &mut Criterion<M>, group_name: &str) {
    let (server, descriptor, clients, specs) = setup();
    let window: Vec<u8> = Vec::new();
    let keys: Vec<Vec<u8>> = (0..KEYS).map(|k| format!("key-{k}").into_bytes()).collect();
    // The bags of the other benchmarks are smaller than one page.
    let paged: Vec<u8> = (0..PAGES * PAGE_BYTES).map(|i| (i % 128) as u8).collect();
    server
        .backend
        .preload(FullKey::bag(TRANSFORM, STATE, &window, PAGED_KEY), &paged);
    server.backend.set_page_size(PAGE_BYTES);
    let mut group = c.benchmark_group(group_name);

    group.throughput(Throughput::Elements((KEYS * APPENDS_PER_KEY) as u64));
    group.bench_function("read_append_commit", |b| {
        b.iter(|| {
            let state = bundle_state(&clients, &descriptor, &specs, None);
            let backend = state.scoped(TRANSFORM);
            for key in &keys {
                backend.get_state(STATE, &window, key).expect("get");
                // Clearing first keeps each bag at APPENDS_PER_KEY items across iterations.
                backend.clear_state(STATE, &window, key).expect("clear");
                for i in 0..APPENDS_PER_KEY {
                    backend
                        .append_state(STATE, &window, key, vec![i as u8])
                        .expect("append");
                }
            }
            state.commit().expect("commit");
        });
    });

    // The benchmark does not commit the bundle, so each iteration starts from the same
    // empty multimap.
    let map_keys: Vec<Vec<u8>> = (0..KEYS).map(|k| vec![k as u8]).collect();
    group.throughput(Throughput::Elements((KEYS * APPENDS_PER_KEY) as u64));
    group.bench_function("multimap_new_keys", |b| {
        b.iter(|| {
            let state = bundle_state(&clients, &descriptor, &specs, None);
            let backend = state.scoped(TRANSFORM);
            let user_key = &keys[0];
            backend
                .get_map_keys(MULTIMAP, &window, user_key)
                .expect("keys");
            for map_key in &map_keys {
                for i in 0..APPENDS_PER_KEY {
                    backend
                        .append_map_state(
                            MULTIMAP,
                            &window,
                            user_key,
                            map_key.clone(),
                            vec![i as u8],
                        )
                        .expect("put");
                }
                let values = backend
                    .get_map_state(MULTIMAP, &window, user_key, map_key)
                    .expect("get");
                std::hint::black_box(values);
            }
        });
    });

    {
        let seed_state = bundle_state(&clients, &descriptor, &specs, None);
        let binding = StateBinding::new(Some(seed_state.scoped(TRANSFORM)), &window, &keys[0]);
        let mut set = <SetState<i64> as StateCell>::bind(SET_STATE, &binding).expect("bind set");
        for i in 0..KEYS {
            set.insert(&(i as i64)).expect("seed set");
        }
        let mut ordered = <OrderedListState<i64> as StateCell>::bind(ORDERED_LIST, &binding)
            .expect("bind ordered list");
        for i in 0..KEYS {
            ordered
                .add(beam::Instant::from_millis((i * 10) as i64), &(i as i64))
                .expect("seed ordered list");
        }
        seed_state.commit().expect("seed commit");
    }

    group.throughput(Throughput::Elements(KEYS as u64));
    group.bench_function("set_insert_contains", |b| {
        b.iter(|| {
            let state = bundle_state(&clients, &descriptor, &specs, None);
            let binding = StateBinding::new(Some(state.scoped(TRANSFORM)), &window, &keys[0]);
            let mut set =
                <SetState<i64> as StateCell>::bind(SET_STATE, &binding).expect("bind set");
            let existing = set.read().expect("read set");
            assert_eq!(existing.len(), KEYS);
            set.clear().expect("clear set");
            for i in 0..KEYS {
                let value = i as i64;
                assert!(set.insert_if_absent(&value).expect("insert"));
                assert!(set.contains(&value).expect("contains hit"));
                assert!(!set.contains(&(value + KEYS as i64)).expect("contains miss"));
            }
            state.commit().expect("commit");
        });
    });

    group.throughput(Throughput::Elements(KEYS as u64));
    group.bench_function("ordered_list_add_read_range", |b| {
        b.iter(|| {
            let state = bundle_state(&clients, &descriptor, &specs, None);
            let binding = StateBinding::new(Some(state.scoped(TRANSFORM)), &window, &keys[0]);
            let mut ordered = <OrderedListState<i64> as StateCell>::bind(ORDERED_LIST, &binding)
                .expect("bind ordered list");
            let slice = ordered
                .read_range(beam::Instant::from_millis(250)..beam::Instant::from_millis(750))
                .expect("read range");
            assert_eq!(slice.len(), KEYS / 2);
            std::hint::black_box(slice);
            ordered.clear().expect("clear ordered list");
            for i in 0..KEYS {
                let millis = (((i * 37) % KEYS) * 10) as i64;
                ordered
                    .add(beam::Instant::from_millis(millis), &(i as i64))
                    .expect("add");
            }
            state.commit().expect("commit");
        });
    });

    // Each bundle reads the bag from the runner, since a bundle without a cache keeps nothing.
    group.throughput(Throughput::Elements(paged.len() as u64));
    group.bench_function("paged_read", |b| {
        b.iter(|| {
            let state = bundle_state(&clients, &descriptor, &specs, None);
            let values = state
                .scoped(TRANSFORM)
                .get_state(STATE, &window, PAGED_KEY)
                .expect("paged get");
            std::hint::black_box(values);
        });
    });

    // The setup bundle clears the bag and appends `size` elements, so each measured bundle
    // starts from the same bag in the state cache.
    let cache = Arc::new(StateCache::default());
    group.throughput(Throughput::Elements(1));
    for size in CACHED_BAG_SIZES {
        group.bench_function(BenchmarkId::new("cached_append_commit", size), |b| {
            b.iter_batched(
                || {
                    let state = bundle_state(&clients, &descriptor, &specs, Some(&cache));
                    let backend = state.scoped(TRANSFORM);
                    backend
                        .clear_state(STATE, &window, CACHED_KEY)
                        .expect("clear");
                    for i in 0..size {
                        backend
                            .append_state(STATE, &window, CACHED_KEY, vec![(i % 128) as u8])
                            .expect("append");
                    }
                    state.commit().expect("commit");
                    // The mock records each request. Drop the records, so they do not add up.
                    drop(server.backend.take_calls());
                },
                |()| {
                    let state = bundle_state(&clients, &descriptor, &specs, Some(&cache));
                    state
                        .scoped(TRANSFORM)
                        .append_state(STATE, &window, CACHED_KEY, vec![0])
                        .expect("append");
                    state.commit().expect("commit");
                },
                BatchSize::PerIteration,
            );
        });
    }
    group.finish();

    // A second server, so that the delay does not change the cases above.
    let slow = StrictStateServer::start();
    slow.backend
        .preload(FullKey::bag(TRANSFORM, STATE, &window, PAGED_KEY), &paged);
    slow.backend.set_page_size(PAGE_BYTES);
    slow.backend.set_get_delay(GET_DELAY);
    let slow_descriptor = self::descriptor(slow.endpoint());
    let mut group = c.benchmark_group(format!("{group_name}/paged_read_latency"));
    group.sample_size(LATENCY_SAMPLES);
    group.throughput(Throughput::Elements(paged.len() as u64));
    // The user state API joins all pages before it decodes, so no work runs between pages.
    group.bench_function("user_state", |b| {
        b.iter(|| {
            let state = bundle_state(&clients, &slow_descriptor, &specs, None);
            let values = state
                .scoped(TRANSFORM)
                .get_state(STATE, &window, PAGED_KEY)
                .expect("paged get");
            std::hint::black_box(values);
        });
    });
    group.bench_function("pages_no_work", |b| {
        b.iter(|| read_pages(&clients, slow.endpoint(), &window, Duration::ZERO));
    });
    group.bench_function("pages_work_500us", |b| {
        b.iter(|| read_pages(&clients, slow.endpoint(), &window, PAGE_WORK));
    });
    group.finish();
}

/// Reads of one bag of [`APPENDS_PER_KEY`] values from the cache of the bundle, and reads and
/// writes of one value cell. No request goes to the runner. All the work runs on the bench
/// thread, so the CPU group measures it with [`ThreadCpuTime`].
fn cached_reads<M: Measurement>(c: &mut Criterion<M>, group_name: &str) {
    let (server, descriptor, clients, specs) = setup();
    let window: Vec<u8> = Vec::new();
    let key = b"key-0";
    let values: Vec<u8> = (0..APPENDS_PER_KEY as u8).collect();
    server
        .backend
        .preload(FullKey::bag(TRANSFORM, STATE, &window, key), &values);
    let mut group = c.benchmark_group(group_name);
    group.throughput(Throughput::Elements(CACHED_READS as u64));
    group.bench_function("cached_reads", |b| {
        let state = bundle_state(&clients, &descriptor, &specs, None);
        let backend = state.scoped(TRANSFORM);
        backend.get_state(STATE, &window, key).expect("first get");
        b.iter(|| {
            for _ in 0..CACHED_READS {
                backend.get_state(STATE, &window, key).expect("cached get");
            }
        });
    });
    group.bench_function("value_read_write", |b| {
        let state = bundle_state(&clients, &descriptor, &specs, None);
        let binding = StateBinding::new(Some(state.scoped(TRANSFORM)), &window, VALUE_KEY);
        let mut value = <ValueState<i64> as StateCell>::bind(STATE, &binding).expect("bind");
        // A write clears the cell, so the reads after it do not ask the runner.
        value.write(&0).expect("first write");
        b.iter(|| {
            for _ in 0..CACHED_READS {
                let next = (value.read().expect("read").unwrap_or(0) + 1) % 100;
                value.write(&next).expect("write");
            }
        });
    });
    group.finish();
}

fn wall_time(c: &mut Criterion) {
    user_state(c, "user_state");
    cached_reads(c, "user_state");
}

fn cpu_time(c: &mut Criterion<CpuTime>) {
    user_state(c, "user_state_cpu");
}

fn thread_cpu_time(c: &mut Criterion<ThreadCpuTime>) {
    cached_reads(c, "user_state_cpu");
}

criterion_group!(benches, wall_time);
criterion_group! {
    name = cpu_benches;
    config = Criterion::default().with_measurement(CpuTime);
    targets = cpu_time
}
criterion_group! {
    name = thread_cpu_benches;
    config = Criterion::default().with_measurement(ThreadCpuTime);
    targets = thread_cpu_time
}
criterion_main!(benches, cpu_benches, thread_cpu_benches);
