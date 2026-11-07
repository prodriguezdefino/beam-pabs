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

//! Bag user state of one bundle against an in-process state server: the first read of each
//! key, appends, cached reads, the first read of a bag of many pages, and the commit at the
//! end of the bundle. The `user_state_cpu` group runs the same benchmarks and reports the
//! CPU time of the process.
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

use criterion::measurement::Measurement;
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use prost::Message;

use beam::pipeline::URN_PAR_DO;
use harness::state::StateClients;
use harness::user_state::{BundleUserState, UserStateSpecs};
use model::fn_execution::{ProcessBundleDescriptor, StateKey, state_key};
use model::pipeline::{self as proto, ApiServiceDescriptor, FunctionSpec};

#[path = "../tests/state/state_mock.rs"]
mod state_mock;
use state_mock::{FullKey, StrictStateServer};

#[path = "support/cpu_time.rs"]
mod cpu_time;
use cpu_time::CpuTime;

const TRANSFORM: &str = "stateful";
const STATE: &str = "bag";
const KEYS: usize = 100;
const APPENDS_PER_KEY: usize = 10;
const CACHED_READS: usize = 1_000;
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
    let pardo = proto::ParDoPayload {
        state_specs: HashMap::from([(STATE.to_string(), bag)]),
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
fn bundle_state(
    clients: &StateClients,
    descriptor: &ProcessBundleDescriptor,
    specs: &Arc<UserStateSpecs>,
) -> BundleUserState {
    let instruction = format!("bundle-{}", NEXT_BUNDLE.fetch_add(1, Ordering::Relaxed));
    let channel = clients.channel(&instruction, descriptor).expect("channel");
    BundleUserState::new(Arc::clone(specs), channel, None)
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

fn user_state<M: Measurement>(c: &mut Criterion<M>, group_name: &str) {
    let server = StrictStateServer::start();
    let descriptor = descriptor(server.endpoint());
    let clients = StateClients::new("worker".to_string());
    let specs = Arc::new(
        UserStateSpecs::from_descriptor(&descriptor, Arc::new(descriptor.coders.clone()))
            .expect("the descriptor declares user state"),
    );
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
            let state = bundle_state(&clients, &descriptor, &specs);
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

    group.throughput(Throughput::Elements(CACHED_READS as u64));
    group.bench_function("cached_reads", |b| {
        let state = bundle_state(&clients, &descriptor, &specs);
        let backend = state.scoped(TRANSFORM);
        backend
            .get_state(STATE, &window, &keys[0])
            .expect("first get");
        b.iter(|| {
            for _ in 0..CACHED_READS {
                backend
                    .get_state(STATE, &window, &keys[0])
                    .expect("cached get");
            }
        });
    });

    // Each bundle reads the bag from the runner, since a bundle without a cache keeps nothing.
    group.throughput(Throughput::Elements(paged.len() as u64));
    group.bench_function("paged_read", |b| {
        b.iter(|| {
            let state = bundle_state(&clients, &descriptor, &specs);
            let values = state
                .scoped(TRANSFORM)
                .get_state(STATE, &window, PAGED_KEY)
                .expect("paged get");
            std::hint::black_box(values);
        });
    });
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
            let state = bundle_state(&clients, &slow_descriptor, &specs);
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

fn wall_time(c: &mut Criterion) {
    user_state(c, "user_state");
}

fn cpu_time(c: &mut Criterion<CpuTime>) {
    user_state(c, "user_state_cpu");
}

criterion_group!(benches, wall_time);
criterion_group! {
    name = cpu_benches;
    config = Criterion::default().with_measurement(CpuTime);
    targets = cpu_time
}
criterion_main!(benches, cpu_benches);
