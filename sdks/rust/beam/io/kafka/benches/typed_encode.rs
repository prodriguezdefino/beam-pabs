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

//! The encode of a `#[derive(BeamRow)]` value into a Kafka record value.
//!
//! - `*_typed`: the `Typed` codec (`Typed::new(codec)`).
//! - `*_row`: `to_row`, then the `Row` codec.
//! - `*_row_bytes`: the row coder bytes, then the `Row` codec, which decodes them.
//!
//! ```text
//! ./gradlew :sdks:rust:harnessBenchmark -Ppkg=apache-beam-io-kafka -Pbench=typed_encode -PcargoProfile=worker
//! ```

use std::hint::black_box;
use std::sync::Arc;

use beam::schema::{BeamRow, RowElement, Schema};
use beam_io_kafka::codec::{Avro, Encoder, Json, RowEncoder, Typed};
use criterion::{Criterion, Throughput, criterion_group, criterion_main};

#[derive(Clone, Debug, PartialEq, BeamRow)]
struct Address {
    city: String,
    zip: Option<i32>,
}

#[derive(Clone, Debug, PartialEq, BeamRow)]
struct Event {
    id: i64,
    user: String,
    score: f64,
    flag: bool,
    note: Option<String>,
    tags: Vec<String>,
    address: Address,
}

fn event(index: usize) -> Event {
    Event {
        id: 1_700_000_000_123 + index as i64,
        user: "user-000042".to_string(),
        score: 12.5,
        flag: index.is_multiple_of(2),
        note: (!index.is_multiple_of(3)).then(|| "x".repeat(40)),
        tags: vec!["a".to_string(), "bb".to_string(), "ccc".to_string()],
        address: Address {
            city: "Lisbon".to_string(),
            zip: Some(1100),
        },
    }
}

const EVENTS: usize = 1_000;

/// Benches the three paths of `codec` over `events`.
fn bench_codec<C: RowEncoder>(
    c: &mut Criterion,
    name: &str,
    codec: C,
    schema: &Arc<Schema>,
    events: &[Event],
) {
    let mut group = c.benchmark_group(format!("kafka_typed_encode_{name}"));
    group.throughput(Throughput::Elements(EVENTS as u64));
    let mut rows = codec.clone();
    rows.bind_schema(Arc::clone(schema))
        .expect("bind the row codec");
    let mut typed = Typed::<C, Event>::new(codec);
    typed.prepare().expect("prepare the typed codec");

    group.bench_function("typed", |b| {
        let mut state = Default::default();
        let mut buffer = Vec::new();
        b.iter(|| {
            for event in black_box(events) {
                let sent = typed.encode(&mut state, event, &mut buffer).expect("typed");
                black_box(sent.value);
            }
        });
    });
    group.bench_function("row", |b| {
        let mut state = C::State::default();
        let mut buffer = Vec::new();
        b.iter(|| {
            for event in black_box(events) {
                let row = RowElement::Row(event.to_row().expect("to_row"));
                let sent = rows.encode(&mut state, &row, &mut buffer).expect("row");
                black_box(sent.value);
            }
        });
    });
    group.bench_function("row_bytes", |b| {
        let mut state = C::State::default();
        let mut buffer = Vec::new();
        b.iter(|| {
            for event in black_box(events) {
                let mut bytes = Vec::new();
                event.encode_row_bytes(&mut bytes).expect("row bytes");
                let element = RowElement::Encoded(bytes);
                let sent = rows
                    .encode(&mut state, &element, &mut buffer)
                    .expect("row bytes");
                black_box(sent.value);
            }
        });
    });
    group.finish();
}

fn typed_encode(c: &mut Criterion) {
    let schema = Event::beam_schema();
    let events: Vec<Event> = (0..EVENTS).map(event).collect();
    bench_codec(c, "json", Json::new(), &schema, &events);
    bench_codec(c, "avro", Avro::new(), &schema, &events);
}

criterion_group!(benches, typed_encode);
criterion_main!(benches);
