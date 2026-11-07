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

//! Reads of a singleton side input against an in-process state server, as a `DoFn` that
//! reads the side input for each element. A read takes the iterable of the window and reads
//! its one element, then checks that no second element follows.
//!
//! - `singleton_cached_read`: a read in a bundle that read the same window before, so the
//!   page comes from the cache of the bundle.
//! - `singleton_first_read`: the first read of a bundle, which fetches the page from the
//!   runner.
//!
//! The `side_input_cpu` group runs the same benchmarks and reports the CPU time of the
//! process.
//!
//! ```text
//! ./gradlew :sdks:rust:harnessBenchmark -Pbench=side_input
//! ```

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use criterion::measurement::Measurement;
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use prost::Message;

use beam::internals::SideInputReader;
use beam::pipeline::URN_PAR_DO;
use beam_harness::state::{FnApiSideInputReader, SideInputSpecs, StateClients};
use beam_model::fn_execution::ProcessBundleDescriptor;
use beam_model::pipeline::{self as proto, ApiServiceDescriptor, FunctionSpec};

#[path = "../tests/state/state_mock.rs"]
mod state_mock;
use state_mock::{FullKey, StrictStateServer};

#[path = "support/cpu_time.rs"]
mod cpu_time;
use cpu_time::CpuTime;

const TRANSFORM: &str = "select";
const SIDE_INPUT: &str = "max_price";
/// The encoded window of the reads.
const WINDOW: &[u8] = b"\x80\x00\x00\x00\x00\x00\x27\x10\x27\x10";

static NEXT_BUNDLE: AtomicUsize = AtomicUsize::new(0);

fn descriptor(endpoint: &str) -> ProcessBundleDescriptor {
    let varint = proto::Coder {
        spec: Some(FunctionSpec {
            urn: "beam:coder:varint:v1".to_string(),
            payload: Vec::new(),
        }),
        component_coder_ids: Vec::new(),
    };
    let pardo = proto::ParDoPayload {
        side_inputs: HashMap::from([(SIDE_INPUT.to_string(), proto::SideInput::default())]),
        ..Default::default()
    };
    ProcessBundleDescriptor {
        id: "side_input".to_string(),
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
                inputs: HashMap::from([
                    ("main".to_string(), "bids".to_string()),
                    (SIDE_INPUT.to_string(), "prices".to_string()),
                ]),
                ..Default::default()
            },
        )]),
        pcollections: HashMap::from([(
            "prices".to_string(),
            proto::PCollection {
                coder_id: "varint".to_string(),
                ..Default::default()
            },
        )]),
        coders: HashMap::from([("varint".to_string(), varint)]),
        ..Default::default()
    }
}

/// The side input reader of a new bundle. The specs come from the descriptor once, as in a
/// worker.
fn bundle_reader(
    clients: &StateClients,
    descriptor: &ProcessBundleDescriptor,
    specs: &Arc<SideInputSpecs>,
) -> FnApiSideInputReader {
    let instruction = format!("bundle-{}", NEXT_BUNDLE.fetch_add(1, Ordering::Relaxed));
    let channel = clients.channel(&instruction, descriptor);
    FnApiSideInputReader::new(Arc::clone(specs), channel)
}

/// Reads the one element of the side input in [`WINDOW`], as a singleton view does.
fn read_singleton(reader: &FnApiSideInputReader) -> usize {
    let mut elements = reader
        .get_iterable_for_transform(TRANSFORM, SIDE_INPUT, WINDOW)
        .expect("the side input is declared");
    let single = elements
        .next()
        .expect("one element")
        .expect("a readable element");
    assert!(elements.next().is_none(), "a singleton has one element");
    single.len()
}

fn side_input<M: Measurement>(c: &mut Criterion<M>, group_name: &str) {
    let server = StrictStateServer::start();
    let descriptor = descriptor(server.endpoint());
    let clients = StateClients::new("worker".to_string());
    let specs = Arc::new(
        SideInputSpecs::from_descriptor(&descriptor, &Arc::new(descriptor.coders.clone()))
            .expect("the descriptor declares a side input"),
    );
    server.backend.preload(
        FullKey::iterable_side_input(TRANSFORM, SIDE_INPUT, WINDOW),
        &[0xe8, 0x07],
    );

    let mut group = c.benchmark_group(group_name);
    group.throughput(Throughput::Elements(1));
    let cached = bundle_reader(&clients, &descriptor, &specs);
    read_singleton(&cached);
    group.bench_function("singleton_cached_read", |b| {
        b.iter(|| read_singleton(&cached));
    });
    group.bench_function("singleton_first_read", |b| {
        b.iter(|| read_singleton(&bundle_reader(&clients, &descriptor, &specs)));
    });
    group.finish();
}

fn wall_time(c: &mut Criterion) {
    side_input(c, "side_input");
}

fn cpu_time(c: &mut Criterion<CpuTime>) {
    side_input(c, "side_input_cpu");
}

criterion_group!(benches, wall_time);
criterion_group! {
    name = cpu_benches;
    config = Criterion::default().with_measurement(CpuTime);
    targets = cpu_time
}
criterion_main!(benches, cpu_benches);
