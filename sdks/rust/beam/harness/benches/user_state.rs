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
//! key, appends, cached reads, and the commit at the end of the bundle. The `user_state_cpu`
//! group runs the same benchmarks and reports the CPU time of the process.
//!
//! ```text
//! ./gradlew :sdks:rust:harnessBenchmark -Pbench=user_state
//! ```

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use criterion::measurement::Measurement;
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use prost::Message;

use beam::pipeline::URN_PAR_DO;
use harness::state::StateClients;
use harness::user_state::{BundleUserState, UserStateSpecs};
use model::fn_execution::ProcessBundleDescriptor;
use model::pipeline::{self as proto, ApiServiceDescriptor, FunctionSpec};

#[path = "../tests/common/mod.rs"]
mod common;
use common::state_mock::StrictStateServer;

#[path = "support/cpu_time.rs"]
mod cpu_time;
use cpu_time::CpuTime;

const TRANSFORM: &str = "stateful";
const STATE: &str = "bag";
const KEYS: usize = 100;
const APPENDS_PER_KEY: usize = 10;
const CACHED_READS: usize = 1_000;

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
