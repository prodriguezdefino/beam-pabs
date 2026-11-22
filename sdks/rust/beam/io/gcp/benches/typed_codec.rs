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

//! BigQuery typed `#[derive(BeamRow)]` read and write batch conversion.
//!
//! - `bigquery_typed_read`: Storage Read `RecordBatch` to `Vec<Event>` with `BeamRowCodec`
//!   (`typed`) vs `RowElement::Encoded` + `FromRows` (`row_bytes`) and `Row` + `from_row` (`row`).
//! - `bigquery_typed_write`: `Vec<Event>` to Arrow IPC `EncodedBatch` with `BeamRowCodec`
//!   (`typed`) vs `RowElement::Encoded` (`row_bytes`) and `to_row` (`row`).
//!
//! ```text
//! ./gradlew :sdks:rust:harnessBenchmark -Ppkg=apache-beam-io-gcp -Pbench=typed_codec -PcargoProfile=worker
//! ```

use std::hint::black_box;
use std::sync::Arc;

use beam::schema::{BeamRow, RowElement};
use beam_io_arrow::{BeamRowCodec, FieldSelection, SchemaRowCodec};
use beam_io_gcp::bigquery::schema::{
    batch_to_beam_rows, batch_to_encoded_rows, batch_to_rows, table_arrow_schema, table_fields,
};
use beam_io_gcp::bigquery::write::encode::{Encoder, schema_bytes};
use beam_io_gcp::bigquery::write::projection::Selection;
use beam_io_gcp::bigquery::write::table::Destination;
use beam_io_gcp::bigquery::write_failure_schema;
use bytes::Bytes;
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
    tags: Vec<String>,
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
        tags: vec!["a".to_string(), "bb".to_string(), "ccc".to_string()],
        address: Address {
            city: "Lisbon".to_string(),
            zip: Some(1100),
        },
        at: DateTime::from_timestamp(1_700_000_000, 123_456_000).expect("valid timestamp"),
    }
}

const EVENTS: usize = 1_000;

fn bench_typed_read(c: &mut Criterion) {
    let schema = Event::beam_schema();
    let events: Vec<Event> = (0..EVENTS).map(event).collect();
    let batch = beam_io_arrow::beam_rows_to_record_batch(&events).expect("session record batch");

    let mut group = c.benchmark_group("bigquery_typed_read");
    group.throughput(Throughput::Elements(EVENTS as u64));

    group.bench_function("typed", |b| {
        b.iter(|| black_box(batch_to_beam_rows::<Event>(black_box(&batch)).expect("typed read")));
    });

    group.bench_function("row_bytes", |b| {
        b.iter(|| {
            let encoded = batch_to_encoded_rows(black_box(&batch), &schema).expect("encoded rows");
            let items: Vec<Event> = encoded
                .iter()
                .map(|bytes| {
                    let mut slice = bytes.as_slice();
                    Event::decode_row_bytes(&mut slice).expect("decode_row_bytes")
                })
                .collect();
            black_box(items)
        });
    });

    group.bench_function("row", |b| {
        b.iter(|| {
            let rows = batch_to_rows(black_box(&batch), &schema).expect("rows");
            let items: Vec<Event> = rows
                .iter()
                .map(|row| Event::from_row(row).expect("from_row"))
                .collect();
            black_box(items)
        });
    });

    group.finish();
}

fn bench_typed_write(c: &mut Criterion) {
    let schema = Event::beam_schema();
    let events: Vec<Event> = (0..EVENTS).map(event).collect();
    let fields = table_fields(&schema).expect("table fields");
    let arrow_table = table_arrow_schema(&fields).expect("table arrow schema");
    let destination = Destination {
        table: "projects/p/datasets/d/tables/t".to_string(),
        arrow_schema: Bytes::from(schema_bytes(&arrow_table).expect("ipc schema")),
    };
    let selection =
        Selection::new(Arc::clone(&schema), &FieldSelection::All, false).expect("selection");
    let errors = Arc::new(write_failure_schema(selection.error_input()));
    let encoder = Encoder::new(&selection, errors, &destination).expect("encoder");
    let typed_codec = BeamRowCodec::<Event>::new();
    let row_codec = SchemaRowCodec::new(Arc::clone(&schema));

    let mut group = c.benchmark_group("bigquery_typed_write");
    group.throughput(Throughput::Elements(EVENTS as u64));

    group.bench_function("typed", |b| {
        b.iter(|| {
            let encoded = encoder
                .encode(&typed_codec, black_box(&events))
                .expect("typed write");
            black_box(encoded.batches)
        });
    });

    group.bench_function("row_bytes", |b| {
        b.iter(|| {
            let elements: Vec<RowElement> = black_box(&events)
                .iter()
                .map(|item| {
                    let mut bytes = Vec::new();
                    item.encode_row_bytes(&mut bytes).expect("encode_row_bytes");
                    RowElement::Encoded(bytes)
                })
                .collect();
            let encoded = encoder
                .encode(&row_codec, &elements)
                .expect("row_bytes write");
            black_box(encoded.batches)
        });
    });

    group.bench_function("row", |b| {
        b.iter(|| {
            let elements: Vec<RowElement> = black_box(&events)
                .iter()
                .map(|item| RowElement::Row(item.to_row().expect("to_row")))
                .collect();
            let encoded = encoder.encode(&row_codec, &elements).expect("row write");
            black_box(encoded.batches)
        });
    });

    group.finish();
}

criterion_group!(benches, bench_typed_read, bench_typed_write);
criterion_main!(benches);
