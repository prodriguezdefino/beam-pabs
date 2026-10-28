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

//! Concurrent async RPC throughput comparing serial `block_on_io`, [`InFlightWindow`], and
//! [`MapAsync`] at concurrency 32 and 64.
//!
//! - `async_rpc_10ms`: 200 mock RPCs sleeping 10 ms each.
//! - `async_rpc_0ms`: 200 instant RPCs measuring dispatch and drain overhead.
//!
//! ```text
//! ./gradlew :sdks:rust:harnessBenchmark -Pbench=async_rpc
//! ```

use std::hint::black_box;
use std::time::Duration;

use criterion::{Criterion, Throughput, criterion_group, criterion_main};

use beam::coders::{DefaultCoder, PaneInfo, WindowedHeader};
use beam::internals::{
    BundleHandler, HandlerContext, InFlightWindow, IntoParDoFn, PipelineInternals, TransformFn,
    block_on_io, io_runtime,
};
use beam::pipeline::Pipeline;
use beam::transforms::{Create, DoFn, MapAsync, ProcessContext};

#[path = "support/drop_sink.rs"]
mod drop_sink;
use drop_sink::DropSink;

const ELEMENTS: usize = 200;
const RPC_LATENCY: Duration = Duration::from_millis(10);

async fn mock_rpc(value: i64, delay: Duration) -> beam::Result<i64> {
    if !delay.is_zero() {
        tokio::time::sleep(delay).await;
    }
    Ok(value + 1)
}

#[derive(Clone)]
struct SerialRpcFn {
    delay: Duration,
}

impl DoFn for SerialRpcFn {
    type In = i64;
    type Out = i64;

    fn process_element(&mut self, value: i64, ctx: &mut ProcessContext<'_, i64>) -> beam::Result {
        let out = block_on_io(mock_rpc(value, self.delay))??;
        ctx.emit(out)
    }
}

fn map_async_handler(delay: Duration, concurrency: usize) -> TransformFn {
    let p = Pipeline::new();
    let out = p.apply("Create", Create::new(vec![0_i64])).apply(
        "MapAsync",
        MapAsync::new(move |value: i64| mock_rpc(value, delay)).with_max_in_flight(concurrency),
    );
    let id = p
        .producer_transform_id(out.id())
        .expect("MapAsync transform must exist");
    p.transform_handlers()
        .remove(&id)
        .expect("MapAsync handler must be registered")
}

fn run_bundle(handler: &mut dyn BundleHandler, elements: &[Vec<u8>], header: &WindowedHeader) {
    let mut sink = DropSink;
    handler.start_bundle().expect("start bundle");
    {
        let mut ctx = HandlerContext::new(&mut sink).with_header(header);
        for element in elements {
            handler
                .process(black_box(element), &mut ctx)
                .expect("process element");
        }
    }
    let mut ctx = HandlerContext::new(&mut sink);
    handler.finish_bundle(&mut ctx).expect("finish bundle");
}

fn run_in_flight_window(count: usize, delay: Duration, concurrency: usize) {
    let handle = io_runtime().expect("io runtime starts").handle();
    let mut window = InFlightWindow::new(concurrency, usize::MAX);
    for i in 0..count {
        window.spawn(handle, (), 0, mock_rpc(black_box(i as i64), delay));
        window
            .drain_while_full(|(), result| -> Result<(), ()> {
                black_box(result.expect("task joined").expect("rpc succeeded"));
                Ok(())
            })
            .expect("drain_while_full");
    }
    window
        .drain_all(|(), result| -> Result<(), ()> {
            black_box(result.expect("task joined").expect("rpc succeeded"));
            Ok(())
        })
        .expect("drain_all");
}

fn bench_group(c: &mut Criterion, name: &str, delay: Duration, slow: bool) {
    let mut group = c.benchmark_group(name);
    group.throughput(Throughput::Elements(ELEMENTS as u64));
    if slow {
        group.sample_size(10);
        group.warm_up_time(Duration::from_secs(1));
        group.measurement_time(Duration::from_secs(5));
    }

    let header = WindowedHeader::global(1_700_000_000_000, PaneInfo::NO_FIRING);
    let elements: Vec<Vec<u8>> = (0..ELEMENTS as i64)
        .map(|i| i.encode().expect("encode element"))
        .collect();

    let serial_proto = SerialRpcFn { delay }.into_handler();
    group.bench_function("serial_block_on_io", |b| {
        let mut handler = serial_proto.instantiate();
        b.iter(|| run_bundle(handler.as_mut(), &elements, &header));
    });

    for concurrency in [32usize, 64] {
        group.bench_function(format!("in_flight_window_{concurrency}"), |b| {
            b.iter(|| run_in_flight_window(ELEMENTS, delay, concurrency));
        });

        let proto = map_async_handler(delay, concurrency);
        group.bench_function(format!("map_async_{concurrency}"), |b| {
            let mut handler = proto.instantiate();
            b.iter(|| run_bundle(handler.as_mut(), &elements, &header));
        });
    }

    group.finish();
}

fn async_rpc(c: &mut Criterion) {
    bench_group(c, "async_rpc_10ms", RPC_LATENCY, true);
    bench_group(c, "async_rpc_0ms", Duration::ZERO, false);
}

criterion_group!(benches, async_rpc);
criterion_main!(benches);
