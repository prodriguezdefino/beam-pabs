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

//! The row coder of a `#[derive(BeamRow)]` struct on one element.
//!
//! - `encode` / `encode_via_row`: direct wire encoding vs intermediate `Row`.
//! - `decode` / `decode_via_row`: direct wire decoding vs intermediate `Row`.
//! - `decode_with_runner_schema` / `decode_with_cached_field_map`: equal runner schema,
//!   uncached vs cached `FieldMap`.
//! - `decode_reordered_uncached` / `decode_reordered_cached_field_map`: reordered writer
//!   schema, uncached vs cached `FieldMap`.
//!
//! ```text
//! ./gradlew :sdks:rust:harnessBenchmark -Pbench=row_coder -PcargoProfile=worker
//! ```

use std::collections::BTreeMap;
use std::hint::black_box;
use std::sync::Arc;

use criterion::{Criterion, Throughput, criterion_group, criterion_main};

use beam::coders::{DefaultCoder, RowCoder};
use beam::schema::{BeamRow, Schema};
use chrono::{DateTime, Utc};

#[derive(Clone, Debug, PartialEq, BeamRow)]
struct Address {
    city: String,
    zip: Option<i32>,
}

#[derive(Clone, Debug, PartialEq, BeamRow)]
struct Pair<T> {
    key: String,
    value: T,
}

#[derive(Clone, Debug, PartialEq, BeamRow)]
struct Event {
    id: i64,
    user: String,
    score: f64,
    flag: bool,
    note: Option<String>,
    missing: Option<i64>,
    tags: Vec<String>,
    counts: BTreeMap<String, i64>,
    address: Address,
    at: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, BeamRow)]
struct EventReordered {
    at: DateTime<Utc>,
    address: Address,
    counts: BTreeMap<String, i64>,
    tags: Vec<String>,
    missing: Option<i64>,
    note: Option<String>,
    flag: bool,
    score: f64,
    user: String,
    id: i64,
}

fn event() -> Event {
    Event {
        id: 1_700_000_000_123,
        user: "user-000042".to_string(),
        score: 12.5,
        flag: true,
        note: Some("x".repeat(40)),
        missing: None,
        tags: vec!["a".to_string(), "bb".to_string(), "ccc".to_string()],
        counts: BTreeMap::from([("clicks".to_string(), 3), ("views".to_string(), 17)]),
        address: Address {
            city: "Lisbon".to_string(),
            zip: Some(1100),
        },
        at: DateTime::from_timestamp(1_700_000_000, 123_456_000).expect("valid timestamp"),
    }
}

fn event_reordered(ev: &Event) -> EventReordered {
    EventReordered {
        at: ev.at,
        address: ev.address.clone(),
        counts: ev.counts.clone(),
        tags: ev.tags.clone(),
        missing: ev.missing,
        note: ev.note.clone(),
        flag: ev.flag,
        score: ev.score,
        user: ev.user.clone(),
        id: ev.id,
    }
}

fn row_coder(c: &mut Criterion) {
    let mut group = c.benchmark_group("row_coder");
    group.throughput(Throughput::Elements(1));
    let value = event();
    let bytes = value.encode().expect("encode");

    group.bench_function("encode", |b| {
        let mut buffer = Vec::with_capacity(bytes.len());
        b.iter(|| {
            buffer.clear();
            black_box(&value)
                .encode_element(&mut buffer)
                .expect("encode");
            black_box(buffer.len());
        });
    });

    group.bench_function("encode_via_row", |b| {
        let mut buffer = Vec::with_capacity(bytes.len());
        b.iter(|| {
            buffer.clear();
            let row = black_box(&value).to_row().expect("to_row");
            RowCoder::encode_row(&row, &mut buffer).expect("encode row");
            black_box(buffer.len());
        });
    });

    group.bench_function("decode", |b| {
        b.iter(|| black_box(Event::decode(black_box(&bytes)).expect("decode")));
    });

    let runner_schema = Arc::new(
        Schema::from_proto_bytes(&Event::beam_schema().to_proto_bytes()).expect("valid schema"),
    );

    group.bench_function("decode_via_row", |b| {
        b.iter(|| {
            let mut slice = black_box(bytes.as_slice());
            let row = RowCoder::decode_row(&runner_schema, &mut slice).expect("decode row");
            black_box(Event::from_row(&row).expect("from_row"))
        });
    });

    group.bench_function("decode_with_runner_schema", |b| {
        b.iter(|| {
            black_box(
                Event::decode_with_schema(black_box(&bytes), Some(&runner_schema)).expect("decode"),
            )
        });
    });

    let same_map = Event::resolve_field_map(&runner_schema).expect("same field map");
    group.bench_function("decode_with_cached_field_map", |b| {
        b.iter(|| {
            black_box(
                Event::decode_with_context(
                    black_box(&bytes),
                    Some(&runner_schema),
                    same_map.as_ref(),
                    None,
                )
                .expect("decode"),
            )
        });
    });

    let reordered_val = event_reordered(&value);
    let reordered_bytes = reordered_val.encode().expect("encode reordered");
    let reordered_schema = Arc::new(
        Schema::from_proto_bytes(&EventReordered::beam_schema().to_proto_bytes())
            .expect("valid reordered schema"),
    );
    let reordered_map = Event::resolve_field_map(&reordered_schema).expect("reordered field map");

    group.bench_function("decode_reordered_uncached", |b| {
        b.iter(|| {
            black_box(
                Event::decode_with_schema(black_box(&reordered_bytes), Some(&reordered_schema))
                    .expect("decode reordered"),
            )
        });
    });

    group.bench_function("decode_reordered_cached_field_map", |b| {
        b.iter(|| {
            black_box(
                Event::decode_with_context(
                    black_box(&reordered_bytes),
                    Some(&reordered_schema),
                    reordered_map.as_ref(),
                    None,
                )
                .expect("decode reordered"),
            )
        });
    });

    let generic_val = Pair {
        key: "user-000042".to_string(),
        value: Address {
            city: "Lisbon".to_string(),
            zip: Some(1100),
        },
    };
    let generic_bytes = generic_val.encode().expect("encode generic");
    let generic_runner_schema = Arc::new(
        Schema::from_proto_bytes(&Pair::<Address>::beam_schema().to_proto_bytes())
            .expect("valid generic schema"),
    );
    let generic_map =
        Pair::<Address>::resolve_field_map(&generic_runner_schema).expect("generic field map");

    group.bench_function("generic_encode", |b| {
        let mut buffer = Vec::with_capacity(generic_bytes.len());
        b.iter(|| {
            buffer.clear();
            black_box(&generic_val)
                .encode_element(&mut buffer)
                .expect("encode");
            black_box(buffer.len());
        });
    });

    group.bench_function("generic_decode", |b| {
        b.iter(|| black_box(Pair::<Address>::decode(black_box(&generic_bytes)).expect("decode")));
    });

    group.bench_function("generic_decode_with_runner_schema", |b| {
        b.iter(|| {
            black_box(
                Pair::<Address>::decode_with_context(
                    black_box(&generic_bytes),
                    Some(&generic_runner_schema),
                    generic_map.as_ref(),
                    None,
                )
                .expect("decode"),
            )
        });
    });

    group.finish();
}

criterion_group!(benches, row_coder);
criterion_main!(benches);
