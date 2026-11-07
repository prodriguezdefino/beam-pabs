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

//! The `WindowInto` handler on one element, with a sink that drops the output.
//!
//! - `process_value`: a fused step before it gives the element by value, as a `ParDo` that
//!   feeds `WindowInto`. The handler encodes the element, then assigns its window.
//! - `process`: the element arrives encoded.
//! - `process_sliding`: as `process`, with 60 s sliding windows every 10 s (6 windows).
//! - `process_sessions`: as `process`, with session windows of a 10 s gap.
//!
//! The element is a `(String, String)` pair of about 200 bytes, as a keyed Nexmark event.
//!
//! ```text
//! ./gradlew :sdks:rust:harnessBenchmark -Pbench=window_into
//! ```

use std::hint::black_box;
use std::sync::Arc;
use std::time::Duration;

use criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main};

use beam::coders::{DefaultCoder, PaneInfo, WindowedHeader};
use beam::internals::{
    BundleHandler, ElementSink, HandlerContext, TypedElement, WindowIntoHandler,
};
use beam::windowing::{FixedWindows, Sessions, SlidingWindows, WindowFn};

/// Drops every element. Keeps the header and the size, so the work is not optimized out.
struct DropSink;

impl ElementSink for DropSink {
    fn push(&mut self, element: Vec<u8>) -> Result<(), String> {
        black_box(element);
        Ok(())
    }

    fn push_windowed_bytes(
        &mut self,
        header: &WindowedHeader,
        element: &[u8],
    ) -> Result<(), String> {
        black_box((header.as_bytes().len(), element.len()));
        Ok(())
    }
}

fn element() -> (String, String) {
    ("shard-00003-of-00010".to_string(), "x".repeat(180))
}

fn window_into(c: &mut Criterion) {
    let mut group = c.benchmark_group("window_into");
    group.throughput(Throughput::Elements(1));
    let header = WindowedHeader::global(1_700_000_000_123, PaneInfo::NO_FIRING);
    let fixed = || WindowIntoHandler::new(Arc::new(FixedWindows::of(Duration::from_secs(10))));

    group.bench_function("process_value", |b| {
        let mut handler = fixed();
        let mut sink = DropSink;
        let value = element();
        b.iter_batched_ref(
            || Some(value.clone()),
            |slot| {
                let mut ctx = HandlerContext::new(&mut sink).with_header(&header);
                handler
                    .process_value(TypedElement::new(slot), &mut ctx)
                    .expect("window element");
            },
            BatchSize::SmallInput,
        );
    });

    let encoded = element().encode().expect("encode element");
    let mut process = |name: &str, window_fn: Arc<dyn WindowFn>| {
        group.bench_function(name, |b| {
            let mut handler = WindowIntoHandler::new(Arc::clone(&window_fn));
            let mut sink = DropSink;
            b.iter(|| {
                let mut ctx = HandlerContext::new(&mut sink).with_header(&header);
                handler
                    .process(black_box(&encoded), &mut ctx)
                    .expect("window element");
            });
        });
    };
    process(
        "process",
        Arc::new(FixedWindows::of(Duration::from_secs(10))),
    );
    process(
        "process_sliding",
        Arc::new(SlidingWindows::of(Duration::from_secs(60)).every(Duration::from_secs(10))),
    );
    process(
        "process_sessions",
        Arc::new(Sessions::with_gap_duration(Duration::from_secs(10))),
    );

    group.finish();
}

criterion_group!(benches, window_into);
criterion_main!(benches);
