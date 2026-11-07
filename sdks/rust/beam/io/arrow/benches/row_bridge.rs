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

//! Batch conversion between `#[derive(BeamRow)]` structs and Arrow `RecordBatch`.
//!
//! - `to_record_batch_direct` vs `to_record_batch_via_row`: `Vec<Event>` to `RecordBatch`.
//! - `from_record_batch_direct` vs `from_record_batch_via_row`: `RecordBatch` to `Vec<Event>`.
//!
//! ```text
//! ./gradlew :sdks:rust:harnessBenchmark -Ppkg=apache-beam-io-arrow -Pbench=row_bridge -PcargoProfile=worker
//! ```

use std::collections::BTreeMap;
use std::hint::black_box;

use beam::schema::BeamRow;
use chrono::{DateTime, Utc};
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
    missing: Option<i64>,
    tags: Vec<String>,
    counts: BTreeMap<String, i64>,
    address: Address,
    at: DateTime<Utc>,
}

fn event(index: usize) -> Event {
    Event {
        id: 1_700_000_000_123 + index as i64,
        user: "user-000042".to_string(),
        score: 12.5,
        flag: index.is_multiple_of(2),
        note: (!index.is_multiple_of(3)).then(|| "x".repeat(40)),
        missing: index.is_multiple_of(3).then_some(index as i64),
        tags: vec!["a".to_string(), "bb".to_string(), "ccc".to_string()],
        counts: BTreeMap::from([("clicks".to_string(), 3), ("views".to_string(), 17)]),
        address: Address {
            city: "Lisbon".to_string(),
            zip: Some(1100),
        },
        at: DateTime::from_timestamp(1_700_000_000, 123_456_000).expect("valid timestamp"),
    }
}

fn arrow_row_bridge(c: &mut Criterion) {
    const BATCH_LEN: usize = 1_000;

    let mut group = c.benchmark_group("arrow_row_bridge");
    group.throughput(Throughput::Elements(BATCH_LEN as u64));

    let beam_schema = Event::beam_schema();
    let events: Vec<Event> = (0..BATCH_LEN).map(event).collect();
    let batch = beam_io_arrow::beam_rows_to_record_batch(&events).expect("initial record batch");

    group.bench_function("to_record_batch_direct", |b| {
        b.iter(|| {
            black_box(
                beam_io_arrow::beam_rows_to_record_batch(black_box(&events))
                    .expect("to_record_batch_direct"),
            )
        });
    });

    group.bench_function("to_record_batch_via_row", |b| {
        b.iter(|| {
            let rows: Vec<_> = black_box(&events)
                .iter()
                .map(|e| e.to_row().expect("to_row"))
                .collect();
            black_box(
                beam_io_arrow::rows_to_record_batch(&beam_schema, &rows)
                    .expect("to_record_batch_via_row"),
            )
        });
    });

    group.bench_function("from_record_batch_direct", |b| {
        b.iter(|| {
            black_box(
                beam_io_arrow::record_batch_to_beam_rows::<Event>(black_box(&batch))
                    .expect("from_record_batch_direct"),
            )
        });
    });

    group.bench_function("from_record_batch_via_row", |b| {
        b.iter(|| {
            let rows = beam_io_arrow::record_batch_to_rows(black_box(&batch), &beam_schema)
                .expect("record_batch_to_rows");
            let decoded: Vec<Event> = rows
                .iter()
                .map(|r| Event::from_row(r).expect("from_row"))
                .collect();
            black_box(decoded)
        });
    });

    group.finish();
}

criterion_group!(benches, arrow_row_bridge);
criterion_main!(benches);
