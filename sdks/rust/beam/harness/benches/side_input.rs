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

//! Reads of singleton and map side inputs against an in-process state server, as a `DoFn`
//! that reads a side input for each element.
//!
//! - `singleton_cached_read`: a read in a bundle that read the same window before, so the
//!   page comes from the cache of the bundle.
//! - `singleton_first_read`: the first read of a bundle, which fetches the page from the
//!   runner.
//! - `singleton_row_read`: a cached read through a `SingletonView` of a row with four string
//!   fields, as `ProcessContext::side_input` does.
//! - `singleton_row_read_two_windows`: as `singleton_row_read`, but each read uses a
//!   different main window from the read before it.
//! - `map_cached_lookup`: a repeated lookup of one key through a `MapView<String, Limits>`
//!   in one bundle.
//! - `map_many_keys_lookup`: lookups cycling across 64 keys in one bundle, reading cached
//!   pages beyond the single-slot memo.
//! - `map_first_lookup`: the first lookup of a key in a new bundle, which fetches the entry
//!   from the runner and decodes the row.
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
use std::time::Duration;

use criterion::measurement::Measurement;
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use prost::Message;

use beam::coders::{DefaultCoder, PaneInfo, WindowedHeader};
use beam::internals::{HandlerContext, ScopedSideInputReader, SideInputReader};
use beam::pipeline::{Pipeline, URN_PAR_DO};
use beam::schema::BeamRow;
use beam::transforms::Create;
use beam::values::{MapView, SingletonView};
use beam::windowing::{FixedWindows, WindowFn, WindowInto};
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
/// The size of the windows of the row side input.
const ROW_WINDOW: Duration = Duration::from_secs(10);
/// Distinct keys preloaded for the map side input cases.
const MAP_KEYS: usize = 64;
const MULTIMAP_ACCESS_PATTERN: &str = "beam:side_input:multimap:v1";

static NEXT_BUNDLE: AtomicUsize = AtomicUsize::new(0);

/// The value of the row side input.
#[derive(Clone, Debug, PartialEq, BeamRow)]
struct Limits {
    category: String,
    currency: String,
    region: String,
    seller: String,
    max_price: i64,
}

fn limits() -> Limits {
    Limits {
        category: "collectibles-and-art".to_string(),
        currency: "USD".to_string(),
        region: "europe-west1".to_string(),
        seller: "seller-000042".to_string(),
        max_price: 1_000,
    }
}

/// The view of one [`Limits`] in fixed windows, and the coders of its pipeline.
fn limits_view() -> (SingletonView<Limits>, HashMap<String, proto::Coder>) {
    let p = Pipeline::new();
    let view = p
        .apply("Limits", Create::new(vec![limits()]))
        .apply("Window", WindowInto::new(FixedWindows::of(ROW_WINDOW)))
        .as_singleton();
    let coders = p.to_proto().components.expect("components").coders;
    (view, coders)
}

/// The map view of [`MAP_KEYS`] [`Limits`] entries in fixed windows, and its coders.
fn limits_map_view() -> (MapView<String, Limits>, HashMap<String, proto::Coder>) {
    let entries: Vec<(String, Limits)> = (0..MAP_KEYS)
        .map(|i| (format!("key-{i:04}"), limits()))
        .collect();
    let p = Pipeline::new();
    let view = p
        .apply("MapLimits", Create::new(entries))
        .apply("Window", WindowInto::new(FixedWindows::of(ROW_WINDOW)))
        .as_map();
    let coders = p.to_proto().components.expect("components").coders;
    (view, coders)
}

/// The header of a main input element at `millis`, in its window of size [`ROW_WINDOW`].
fn header_at(millis: i64) -> WindowedHeader {
    let windows = FixedWindows::of(ROW_WINDOW).assign_windows(beam::Instant::from_millis(millis));
    WindowedHeader::global(millis, PaneInfo::NO_FIRING)
        .with_windows(&windows)
        .expect("encode interval window header")
}

/// A descriptor whose transform reads `side_input`, with elements of coder `coder_id`.
fn descriptor(
    endpoint: &str,
    side_input: &str,
    coder_id: &str,
    access_pattern: Option<&str>,
    coders: HashMap<String, proto::Coder>,
) -> ProcessBundleDescriptor {
    let spec = proto::SideInput {
        access_pattern: access_pattern.map(|urn| FunctionSpec {
            urn: urn.to_string(),
            payload: Vec::new(),
        }),
        ..Default::default()
    };
    let pardo = proto::ParDoPayload {
        side_inputs: HashMap::from([(side_input.to_string(), spec)]),
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
                    (side_input.to_string(), "prices".to_string()),
                ]),
                ..Default::default()
            },
        )]),
        pcollections: HashMap::from([(
            "prices".to_string(),
            proto::PCollection {
                coder_id: coder_id.to_string(),
                ..Default::default()
            },
        )]),
        coders,
        ..Default::default()
    }
}

fn specs(descriptor: &ProcessBundleDescriptor) -> Arc<SideInputSpecs> {
    Arc::new(
        SideInputSpecs::from_descriptor(descriptor, &Arc::new(descriptor.coders.clone()))
            .expect("the descriptor declares a side input"),
    )
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

/// Reads `view` for a main input element with `header` and decodes the value, as a `DoFn`
/// does.
fn read_view(
    reader: &FnApiSideInputReader,
    view: &SingletonView<Limits>,
    header: &WindowedHeader,
) -> i64 {
    let scoped = ScopedSideInputReader::new(reader, TRANSFORM);
    let mut sink = Vec::<Vec<u8>>::new();
    let mut handler = HandlerContext::new(&mut sink)
        .with_side_inputs(Some(&scoped))
        .with_header(header);
    let ctx = handler.as_process_context::<()>();
    let Limits { max_price, .. } = ctx.side_input(view).expect("the side input value");
    max_price
}

/// Looks up `key` in `view` for a main input element with `header`, as a `DoFn` does.
fn read_map_view(
    reader: &FnApiSideInputReader,
    view: &MapView<String, Limits>,
    header: &WindowedHeader,
    key: &String,
) -> Option<i64> {
    let scoped = ScopedSideInputReader::new(reader, TRANSFORM);
    let mut sink = Vec::<Vec<u8>>::new();
    let mut handler = HandlerContext::new(&mut sink)
        .with_side_inputs(Some(&scoped))
        .with_header(header);
    let ctx = handler.as_process_context::<()>();
    let dict = ctx.side_input(view).expect("the map side input");
    dict.get(key)
        .expect("lookup succeeds")
        .map(|Limits { max_price, .. }| max_price)
}

fn side_input<M: Measurement>(c: &mut Criterion<M>, group_name: &str) {
    let server = StrictStateServer::start();
    let varint = proto::Coder {
        spec: Some(FunctionSpec {
            urn: "beam:coder:varint:v1".to_string(),
            payload: Vec::new(),
        }),
        component_coder_ids: Vec::new(),
    };
    let descriptor = descriptor(
        server.endpoint(),
        SIDE_INPUT,
        "varint",
        None,
        HashMap::from([("varint".to_string(), varint)]),
    );
    let clients = StateClients::new("worker".to_string());
    let specs = specs(&descriptor);
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

    let (view, coders) = limits_view();
    let row_descriptor =
        self::descriptor(server.endpoint(), view.tag(), view.coder_id(), None, coders);
    let row_specs = self::specs(&row_descriptor);
    // Fixed windows of one size map each main window to the same side input window.
    let headers = [header_at(0), header_at(10_000)];
    let row = limits().encode().expect("encode the side input value");
    headers.iter().for_each(|header| {
        let window = header.window_bytes();
        server.backend.preload(
            FullKey::iterable_side_input(TRANSFORM, view.tag(), window),
            &row,
        );
    });
    let reader = bundle_reader(&clients, &row_descriptor, &row_specs);
    // Read each window one time, so the cache of the bundle has its page.
    headers.iter().for_each(|header| {
        read_view(&reader, &view, header);
    });
    group.bench_function("singleton_row_read", |b| {
        b.iter(|| read_view(&reader, &view, &headers[0]));
    });
    group.bench_function("singleton_row_read_two_windows", |b| {
        let mut alternate = headers.iter().cycle();
        b.iter(|| {
            let header = alternate.next().expect("a cycle does not end");
            read_view(&reader, &view, header)
        });
    });

    let (map_view, map_coders) = limits_map_view();
    let map_descriptor = self::descriptor(
        server.endpoint(),
        map_view.tag(),
        map_view.coder_id(),
        Some(MULTIMAP_ACCESS_PATTERN),
        map_coders,
    );
    let map_specs = self::specs(&map_descriptor);
    let map_keys: Vec<String> = (0..MAP_KEYS).map(|i| format!("key-{i:04}")).collect();
    let map_window = headers[0].window_bytes();
    let mut nested_row = Vec::new();
    limits()
        .encode_element(&mut nested_row)
        .expect("encode nested side input value");
    for key in &map_keys {
        let key_bytes = DefaultCoder::encode(key).expect("encode map key");
        server.backend.preload(
            FullKey::multimap_side_input(TRANSFORM, map_view.tag(), map_window, &key_bytes),
            &nested_row,
        );
    }
    let map_reader = bundle_reader(&clients, &map_descriptor, &map_specs);
    for key in &map_keys {
        assert_eq!(
            read_map_view(&map_reader, &map_view, &headers[0], key),
            Some(1_000)
        );
    }
    group.bench_function("map_cached_lookup", |b| {
        b.iter(|| read_map_view(&map_reader, &map_view, &headers[0], &map_keys[0]));
    });
    group.bench_function("map_many_keys_lookup", |b| {
        let mut cycle = map_keys.iter().cycle();
        b.iter(|| {
            let key = cycle.next().expect("a cycle does not end");
            read_map_view(&map_reader, &map_view, &headers[0], key)
        });
    });
    group.bench_function("map_first_lookup", |b| {
        b.iter(|| {
            let fresh = bundle_reader(&clients, &map_descriptor, &map_specs);
            read_map_view(&fresh, &map_view, &headers[0], &map_keys[0])
        });
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
