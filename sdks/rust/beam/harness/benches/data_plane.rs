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

//! The data plane without user code: inbound routing, outbound buffering, outbound
//! backpressure above 80 MB, large `GroupByKey` iterables, small-element chunks and nested
//! rows at an SDK boundary, and how long one slow bundle holds up another bundle on the same
//! stream.
//!
//! ```text
//! ./gradlew :sdks:rust:harnessBenchmark -Pbench=data_plane
//! ```

use std::collections::{BTreeMap, HashMap};
use std::hint::black_box;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use futures::future::join_all;
use prost::Message;
use tokio::sync::mpsc;

use beam::coders::{
    BeamIterable, CoderError, DefaultCoder, PageStream, PaneInfo, RowCoder, StateStreamReader,
    URN_GLOBAL_WINDOW, URN_INTERVAL_WINDOW, URN_ITERABLE, URN_KV, URN_ROW, URN_STRING_UTF8,
    URN_VARINT, URN_WINDOWED_VALUE, VarIntCoder, WindowedHeader,
};
use beam::internals::block_on_io;
use beam::schema::{BeamRow, Field, FieldType, FieldValue, Row, Schema};
use beam::windowing::{FixedWindows, WindowFn};
use beam_harness::bundle_processor::PortFraming;
use beam_harness::data::{DataManager, InboundReceiver, OUTBOUND_QUEUE_MESSAGES};
use beam_model::fn_execution::{Elements, elements};
use beam_model::pipeline::{Coder as ProtoCoder, FunctionSpec};

const CHUNK_BYTES: usize = 4 * 1024;
const CHUNKS: usize = 1_000;
/// Chunks written by `outbound_backpressure_128m` (128 MiB, crossing the 80 MB pending cap).
const BACKPRESSURE_CHUNKS: usize = 32_768;
/// HTTP/2 frame slice size inspected by the backpressure consumer task.
const H2_FRAME_BYTES: usize = 16 * 1024;
/// Inbound chunk size for the large-element resumable decode benchmarks (256 chunks for
/// 16 MiB).
const RESUMABLE_CHUNK_BYTES: usize = 64 * 1024;
/// Number of 64-byte string entries in the 16 MiB resumable decode elements.
const RESUMABLE_STRINGS: usize = (16 * 1024 * 1024) / 64;
/// Values in one hot key of the `GroupByKey` iterable benchmarks.
const GBK_VALUES: usize = 100_000;
/// Inlined prefix elements before the state continuation token.
const GBK_INLINE_PREFIX: usize = 16;
/// Values per state page for the state-backed `GroupByKey` iterable.
const GBK_PAGE_VALUES: usize = 2_000;
/// Small keyed elements framed across 512 B chunks at an SDK boundary.
const SMALL_ELEMENTS: usize = 10_000;
const SMALL_CHUNK_BYTES: usize = 512;
/// Nested `BeamRow` elements framed and decoded across 4 KiB chunks at an SDK boundary.
const NESTED_ROWS: usize = 1_000;
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

/// A data manager bounded to [`OUTBOUND_QUEUE_MESSAGES`] whose consumer encodes protobuf
/// frames as a gRPC writer task does, so a 128 MiB burst crosses the 80 MB pending limit.
fn backpressure_data_manager() -> DataManager {
    let (tx, mut rx) = mpsc::channel::<Elements>(OUTBOUND_QUEUE_MESSAGES);
    tokio::spawn(async move {
        let mut wire = Vec::with_capacity(1_100_000);
        let mut frame = vec![0u8; H2_FRAME_BYTES];
        while let Some(elements) = rx.recv().await {
            wire.clear();
            elements.encode(&mut wire).expect("encode elements");
            for chunk in wire.chunks(H2_FRAME_BYTES) {
                frame[..chunk.len()].copy_from_slice(chunk);
                black_box(frame[0]);
            }
        }
    });
    DataManager::new(tx)
}

/// Writes 128 MiB synchronously from the bundle thread, letting `Outbound::flush` hold and
/// drain pending messages once buffered output passes 80 MB.
fn write_backpressure_128m(dm: &DataManager) {
    let id = &instruction_ids(1)[0];
    let mut outbound = dm.outbound("", id, ["sink"], []);
    let element = [7u8; CHUNK_BYTES];
    for _ in 0..BACKPRESSURE_CHUNKS {
        outbound
            .write(0, |buf| {
                buf.extend_from_slice(&element);
                Ok(())
            })
            .expect("buffered write");
    }
    block_on_io(outbound.finish())
        .expect("io runtime")
        .expect("finish");
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
    let backpressure_dm = runtime.block_on(async { backpressure_data_manager() });
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
    group.sample_size(20);
    group.throughput(Throughput::Bytes(
        (BACKPRESSURE_CHUNKS * CHUNK_BYTES) as u64,
    ));
    group.bench_function("outbound_backpressure_128m", |b| {
        b.iter(|| write_backpressure_128m(&backpressure_dm));
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

fn proto_coder(urn: &str, components: &[&str], payload: Vec<u8>) -> ProtoCoder {
    ProtoCoder {
        spec: Some(FunctionSpec {
            urn: urn.to_string(),
            payload,
        }),
        component_coder_ids: components.iter().map(|c| c.to_string()).collect(),
    }
}

/// Builds a ~16 MiB `WindowedValue<KV<i64, Iterable<String>>, GlobalWindow>` element and its
/// resolved [`PortFraming`].
fn kv_iterable_16m_fixture() -> (PortFraming, Vec<u8>) {
    let coders = Arc::new(HashMap::from([
        (
            "varint".to_string(),
            proto_coder(URN_VARINT, &[], Vec::new()),
        ),
        (
            "string".to_string(),
            proto_coder(URN_STRING_UTF8, &[], Vec::new()),
        ),
        (
            "iter".to_string(),
            proto_coder(URN_ITERABLE, &["string"], Vec::new()),
        ),
        (
            "kv".to_string(),
            proto_coder(URN_KV, &["varint", "iter"], Vec::new()),
        ),
        (
            "gwindow".to_string(),
            proto_coder(URN_GLOBAL_WINDOW, &[], Vec::new()),
        ),
        (
            "windowed".to_string(),
            proto_coder(URN_WINDOWED_VALUE, &["kv", "gwindow"], Vec::new()),
        ),
    ]));
    let framing = PortFraming::resolve("windowed", coders).expect("kv iterable framing");

    let header = WindowedHeader::global(0, PaneInfo::NO_FIRING);
    let entry = [b'x'; 63];
    let mut encoded = Vec::with_capacity(RESUMABLE_STRINGS * 64 + 64);
    encoded.extend_from_slice(header.as_bytes());
    VarIntCoder::encode_varint_to_vec(42, &mut encoded);
    encoded.extend_from_slice(&(RESUMABLE_STRINGS as i32).to_be_bytes());
    for _ in 0..RESUMABLE_STRINGS {
        VarIntCoder::encode_varint_to_vec(entry.len() as i64, &mut encoded);
        encoded.extend_from_slice(&entry);
    }
    (framing, encoded)
}

/// Builds a ~16 MiB `WindowedValue<Row, GlobalWindow>` with a large `Array<String>` field and
/// its resolved [`PortFraming`].
fn row_array_16m_fixture() -> (PortFraming, Vec<u8>) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", FieldType::int64()),
        Field::new("tags", FieldType::array(FieldType::string())),
    ]));
    let coders = Arc::new(HashMap::from([
        (
            "row".to_string(),
            proto_coder(URN_ROW, &[], schema.to_proto_bytes()),
        ),
        (
            "gwindow".to_string(),
            proto_coder(URN_GLOBAL_WINDOW, &[], Vec::new()),
        ),
        (
            "windowed".to_string(),
            proto_coder(URN_WINDOWED_VALUE, &["row", "gwindow"], Vec::new()),
        ),
    ]));
    let framing = PortFraming::resolve("windowed", coders).expect("row array framing");

    let item = "y".repeat(63);
    let tags = (0..RESUMABLE_STRINGS)
        .map(|_| Some(FieldValue::String(item.clone())))
        .collect();
    let row = Row::new(
        schema,
        vec![Some(FieldValue::Int64(42)), Some(FieldValue::Array(tags))],
    )
    .expect("row matches schema");
    let header = WindowedHeader::global(0, PaneInfo::NO_FIRING);
    let mut encoded = Vec::with_capacity(RESUMABLE_STRINGS * 64 + 64);
    encoded.extend_from_slice(header.as_bytes());
    RowCoder::encode_row(&row, &mut encoded).expect("row encodes");
    (framing, encoded)
}

fn resumable_decode(c: &mut Criterion) {
    let (kv_framing, kv_bytes) = kv_iterable_16m_fixture();
    let kv_chunks: Vec<&[u8]> = kv_bytes.chunks(RESUMABLE_CHUNK_BYTES).collect();

    let (row_framing, row_bytes) = row_array_16m_fixture();
    let row_chunks: Vec<&[u8]> = row_bytes.chunks(RESUMABLE_CHUNK_BYTES).collect();

    let mut group = c.benchmark_group("data_plane");
    group.throughput(Throughput::Bytes(kv_bytes.len() as u64));
    group.bench_function("resumable_kv_iterable_16m", |b| {
        b.iter(|| {
            let mut count = 0usize;
            kv_framing
                .decode_chunks(kv_chunks.iter().copied(), |header, payload| {
                    black_box(header);
                    black_box(payload.len());
                    count += 1;
                })
                .expect("kv iterable decodes");
            assert_eq!(count, 1);
        });
    });

    group.throughput(Throughput::Bytes(row_bytes.len() as u64));
    group.bench_function("resumable_row_array_16m", |b| {
        b.iter(|| {
            let mut count = 0usize;
            row_framing
                .decode_chunks(row_chunks.iter().copied(), |header, payload| {
                    black_box(header);
                    black_box(payload.len());
                    count += 1;
                })
                .expect("row array decodes");
            assert_eq!(count, 1);
        });
    });
    group.finish();
}

/// Replays pre-encoded state pages for the state-backed `GroupByKey` iterable benchmark.
struct BenchStateReader {
    pages: Vec<Vec<u8>>,
}

impl StateStreamReader for BenchStateReader {
    fn stream_runner_pages(&self, _token: &[u8]) -> Result<PageStream, CoderError> {
        Ok(Box::new(self.pages.clone().into_iter().map(Ok)))
    }
}

fn gbk_inline_fixture() -> Vec<u8> {
    let mut encoded = Vec::with_capacity(GBK_VALUES * 3 + 16);
    42i64.encode_element(&mut encoded).expect("encode key");
    encoded.extend_from_slice(&(GBK_VALUES as i32).to_be_bytes());
    for i in 0..GBK_VALUES {
        (i as i64)
            .encode_element(&mut encoded)
            .expect("encode value");
    }
    encoded
}

fn gbk_state_backed_fixture() -> (Vec<u8>, Arc<dyn StateStreamReader>) {
    let token = b"gbk-hot-key";
    let mut encoded = Vec::new();
    42i64.encode_element(&mut encoded).expect("encode key");
    encoded.extend_from_slice(&(-1i32).to_be_bytes());
    VarIntCoder::encode_varint_to_vec(GBK_INLINE_PREFIX as i64, &mut encoded);
    for i in 0..GBK_INLINE_PREFIX {
        (i as i64)
            .encode_element(&mut encoded)
            .expect("encode inline value");
    }
    VarIntCoder::encode_varint_to_vec(-1, &mut encoded);
    VarIntCoder::encode_varint_to_vec(token.len() as i64, &mut encoded);
    encoded.extend_from_slice(token);

    let suffix: Vec<i64> = (GBK_INLINE_PREFIX..GBK_VALUES).map(|i| i as i64).collect();
    let pages = suffix
        .chunks(GBK_PAGE_VALUES)
        .map(|chunk| {
            let mut page = Vec::with_capacity(chunk.len() * 3);
            for v in chunk {
                v.encode_element(&mut page).expect("encode page value");
            }
            page
        })
        .collect();
    (encoded, Arc::new(BenchStateReader { pages }))
}

fn gbk_iterable(c: &mut Criterion) {
    let inline_bytes = gbk_inline_fixture();
    let (state_bytes, state_reader) = gbk_state_backed_fixture();

    let mut group = c.benchmark_group("data_plane");
    group.throughput(Throughput::Elements(GBK_VALUES as u64));

    group.bench_function("gbk_inline_vec_100k", |b| {
        b.iter(|| {
            let (key, values) =
                <(i64, Vec<i64>)>::decode_with_context(&inline_bytes, None, None, None)
                    .expect("decode inline vec");
            assert_eq!(key, 42);
            assert_eq!(values.len(), GBK_VALUES);
            let sum: i64 = values.into_iter().fold(0i64, i64::wrapping_add);
            black_box(sum)
        });
    });

    group.bench_function("gbk_inline_iterable_100k", |b| {
        b.iter(|| {
            let (key, values) =
                <(i64, BeamIterable<i64>)>::decode_with_context(&inline_bytes, None, None, None)
                    .expect("decode inline iterable");
            assert_eq!(key, 42);
            let mut count = 0usize;
            let mut sum = 0i64;
            for item in values.try_into_iter() {
                sum = sum.wrapping_add(item.expect("value"));
                count += 1;
            }
            assert_eq!(count, GBK_VALUES);
            black_box(sum)
        });
    });

    group.bench_function("gbk_state_backed_iterable_100k", |b| {
        b.iter(|| {
            let (key, values) = <(i64, BeamIterable<i64>)>::decode_with_context(
                &state_bytes,
                None,
                None,
                Some(&state_reader),
            )
            .expect("decode state-backed iterable");
            assert_eq!(key, 42);
            let mut count = 0usize;
            let mut sum = 0i64;
            for item in values.try_into_iter() {
                sum = sum.wrapping_add(item.expect("value"));
                count += 1;
            }
            assert_eq!(count, GBK_VALUES);
            black_box(sum)
        });
    });

    group.finish();
}

#[derive(Clone, Debug, PartialEq, BeamRow)]
struct BoundaryItem {
    sku: String,
    qty: i32,
    price_cents: i64,
}

#[derive(Clone, Debug, PartialEq, BeamRow)]
struct BoundaryCustomer {
    id: i64,
    region: String,
    vip: Option<bool>,
}

#[derive(Clone, Debug, PartialEq, BeamRow)]
struct BoundaryOrder {
    order_id: i64,
    customer: BoundaryCustomer,
    items: Vec<BoundaryItem>,
    labels: BTreeMap<String, String>,
}

fn interval_header(millis: i64) -> WindowedHeader {
    let windows = FixedWindows::of(Duration::from_secs(10))
        .assign_windows(beam::Instant::from_millis(millis));
    WindowedHeader::global(millis, PaneInfo::NO_FIRING)
        .with_windows(&windows)
        .expect("interval window header")
}

fn small_elements_fixture() -> (PortFraming, Vec<u8>) {
    let coders = Arc::new(HashMap::from([
        (
            "string".to_string(),
            proto_coder(URN_STRING_UTF8, &[], Vec::new()),
        ),
        (
            "varint".to_string(),
            proto_coder(URN_VARINT, &[], Vec::new()),
        ),
        (
            "kv".to_string(),
            proto_coder(URN_KV, &["string", "varint"], Vec::new()),
        ),
        (
            "iwindow".to_string(),
            proto_coder(URN_INTERVAL_WINDOW, &[], Vec::new()),
        ),
        (
            "windowed".to_string(),
            proto_coder(URN_WINDOWED_VALUE, &["kv", "iwindow"], Vec::new()),
        ),
    ]));
    let framing = PortFraming::resolve("windowed", coders).expect("small kv framing");
    let header = interval_header(1_700_000_000_123);
    let mut encoded = Vec::with_capacity(SMALL_ELEMENTS * 36);
    for i in 0..SMALL_ELEMENTS {
        encoded.extend_from_slice(header.as_bytes());
        (format!("k-{:04}", i & 1023), i as i64)
            .encode_element(&mut encoded)
            .expect("encode small kv");
    }
    (framing, encoded)
}

fn nested_rows_fixture() -> (PortFraming, Arc<Schema>, Vec<u8>) {
    let schema = BoundaryOrder::beam_schema();
    let proto_bytes = schema.to_proto_bytes();
    let runner_schema = Arc::new(Schema::from_proto_bytes(&proto_bytes).expect("runner schema"));
    let coders = Arc::new(HashMap::from([
        ("row".to_string(), proto_coder(URN_ROW, &[], proto_bytes)),
        (
            "iwindow".to_string(),
            proto_coder(URN_INTERVAL_WINDOW, &[], Vec::new()),
        ),
        (
            "windowed".to_string(),
            proto_coder(URN_WINDOWED_VALUE, &["row", "iwindow"], Vec::new()),
        ),
    ]));
    let framing = PortFraming::resolve("windowed", coders).expect("nested row framing");
    let header = interval_header(1_700_000_000_123);
    let mut encoded = Vec::with_capacity(NESTED_ROWS * 160);
    for i in 0..NESTED_ROWS {
        let order = BoundaryOrder {
            order_id: 100_000 + i as i64,
            customer: BoundaryCustomer {
                id: (i % 64) as i64,
                region: "us-central1".to_string(),
                vip: (i % 2 == 0).then_some(true),
            },
            items: vec![
                BoundaryItem {
                    sku: "sku-alpha".to_string(),
                    qty: 2,
                    price_cents: 1_999,
                },
                BoundaryItem {
                    sku: "sku-beta".to_string(),
                    qty: 1,
                    price_cents: 4_500,
                },
            ],
            labels: BTreeMap::from([
                ("channel".to_string(), "web".to_string()),
                ("tier".to_string(), "standard".to_string()),
            ]),
        };
        encoded.extend_from_slice(header.as_bytes());
        order
            .encode_element(&mut encoded)
            .expect("encode nested order");
    }
    (framing, runner_schema, encoded)
}

fn sdk_boundary(c: &mut Criterion) {
    let (small_framing, small_bytes) = small_elements_fixture();
    let small_chunks: Vec<&[u8]> = small_bytes.chunks(SMALL_CHUNK_BYTES).collect();

    let (row_framing, runner_schema, row_bytes) = nested_rows_fixture();
    let row_chunks: Vec<&[u8]> = row_bytes.chunks(CHUNK_BYTES).collect();
    let field_map =
        BoundaryOrder::resolve_field_map(&runner_schema).expect("resolve nested field map");

    let mut group = c.benchmark_group("data_plane");
    group.throughput(Throughput::Elements(SMALL_ELEMENTS as u64));
    group.bench_function("small_element_chunks", |b| {
        b.iter(|| {
            let mut count = 0usize;
            let mut sum = 0i64;
            small_framing
                .decode_chunks(small_chunks.iter().copied(), |header, payload| {
                    black_box(header.timestamp());
                    let (key, val) =
                        <(String, i64)>::decode_with_context(payload, None, None, None)
                            .expect("decode small kv");
                    black_box(key.len());
                    sum = sum.wrapping_add(val);
                    count += 1;
                })
                .expect("small chunks decode");
            assert_eq!(count, SMALL_ELEMENTS);
            black_box(sum)
        });
    });

    group.throughput(Throughput::Elements(NESTED_ROWS as u64));
    group.bench_function("nested_row_boundary_framing", |b| {
        b.iter(|| {
            let mut count = 0usize;
            row_framing
                .decode_chunks(row_chunks.iter().copied(), |header, payload| {
                    black_box(header.timestamp());
                    black_box(payload.len());
                    count += 1;
                })
                .expect("nested row chunks frame");
            assert_eq!(count, NESTED_ROWS);
        });
    });

    group.bench_function("nested_row_boundary_roundtrip", |b| {
        let mut outbound = Vec::with_capacity(row_bytes.len());
        b.iter(|| {
            outbound.clear();
            let mut count = 0usize;
            row_framing
                .decode_chunks(row_chunks.iter().copied(), |header, payload| {
                    let order = BoundaryOrder::decode_with_context(
                        payload,
                        Some(&runner_schema),
                        field_map.as_ref(),
                        None,
                    )
                    .expect("decode nested order");
                    outbound.extend_from_slice(header.as_bytes());
                    order
                        .encode_element(&mut outbound)
                        .expect("encode nested order");
                    count += 1;
                })
                .expect("nested row chunks roundtrip");
            assert_eq!(count, NESTED_ROWS);
            black_box(outbound.len())
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    data_plane,
    resumable_decode,
    gbk_iterable,
    sdk_boundary
);
criterion_main!(benches);
