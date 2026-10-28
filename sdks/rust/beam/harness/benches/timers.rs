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

//! The timers of a `StatefulDoFn` handler, with a sink that drops the output.
//!
//! - `set`: each element sets one event-time timer. Then the bench takes the timers from
//!   the collector, as the harness does when the elements end.
//! - `fire`: each timer fires and emits its key.
//!
//! Each iteration has 1,000 elements or timers, and each has a different key.
//!
//! ```text
//! ./gradlew :sdks:rust:harnessBenchmark -Pbench=timers
//! ```

use std::hint::black_box;
use std::sync::Arc;

use criterion::{Criterion, Throughput, criterion_group, criterion_main};

use beam::coders::{DefaultCoder, PaneInfo, TimerRecord, WindowedHeader};
use beam::internals::{HandlerContext, IntoParDoFn, TimerCollector};
use beam::transforms::{StatefulContext, StatefulDoFn, Timers};

#[path = "support/drop_sink.rs"]
mod drop_sink;
use drop_sink::DropSink;

const TRANSFORM: &str = "stateful";
/// The id of the timer family of [`Flush`].
const FAMILY: &str = "flush";
const TIMERS: usize = 1_000;
const ELEMENT_MILLIS: i64 = 1_700_000_000_123;
const FIRE_MILLIS: i64 = ELEMENT_MILLIS + 60_000;

#[derive(Timers)]
enum Flush {
    #[timer(event_time, id = "flush")]
    Later,
}

/// Sets one timer at the time that the element gives. A fired timer emits its key.
#[derive(Clone)]
struct SetTimer;

impl StatefulDoFn for SetTimer {
    type Key = String;
    type Value = i64;
    type Out = String;
    type State = ();
    type Timer = Flush;

    fn process_element(
        &mut self,
        _key: &String,
        fire_millis: i64,
        _state: &mut (),
        ctx: &mut StatefulContext<'_, Self>,
    ) -> beam::Result {
        ctx.timer(Flush::Later)
            .at(beam::Instant::from_millis(fire_millis))
    }

    fn on_timer(
        &mut self,
        _timer: Flush,
        key: &String,
        _state: &mut (),
        ctx: &mut StatefulContext<'_, Self>,
    ) -> beam::Result {
        ctx.emit(key.clone())
    }
}

fn timers(c: &mut Criterion) {
    let mut group = c.benchmark_group("timers");
    group.throughput(Throughput::Elements(TIMERS as u64));
    let prototype = SetTimer.into_handler();
    let keys = (0..TIMERS)
        .map(|i| format!("user-{i:06}"))
        .collect::<Vec<_>>();

    let header = WindowedHeader::global(ELEMENT_MILLIS, PaneInfo::NO_FIRING);
    let elements = keys
        .iter()
        .map(|key| (key.clone(), FIRE_MILLIS).encode().expect("encode element"))
        .collect::<Vec<_>>();
    group.bench_function("set", |b| {
        let mut handler = prototype.instantiate();
        let collector = Arc::new(TimerCollector::new());
        let mut sink = DropSink;
        b.iter(|| {
            let mut ctx = HandlerContext::new(&mut sink)
                .with_header(&header)
                .with_transform_id(TRANSFORM)
                .with_timer_collector(Some(&collector));
            elements.iter().for_each(|element| {
                handler
                    .process(black_box(element), &mut ctx)
                    .expect("set a timer");
            });
            black_box(collector.drain());
        });
    });

    let fire = beam::Instant::from_millis(FIRE_MILLIS);
    let fired = keys
        .iter()
        .map(|key| TimerRecord::new_set(key.encode().expect("encode key"), "", fire, fire))
        .collect::<Vec<_>>();
    group.bench_function("fire", |b| {
        let mut handler = prototype.instantiate();
        let mut sink = DropSink;
        b.iter(|| {
            let mut ctx = HandlerContext::new(&mut sink).with_transform_id(TRANSFORM);
            fired.iter().for_each(|record| {
                handler
                    .on_timer(FAMILY, black_box(record), &mut ctx)
                    .expect("fire a timer");
            });
        });
    });

    group.finish();
}

criterion_group!(benches, timers);
criterion_main!(benches);
