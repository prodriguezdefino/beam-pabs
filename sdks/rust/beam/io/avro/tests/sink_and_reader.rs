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

//! Exercises the Avro sink and splittable reader directly, without a runner.
#![expect(clippy::unwrap_used, reason = "test fixtures panic on setup failure")]

use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use arrow_io::{BeamRowCodec, RowCodec, SchemaRowCodec};
use avro_io::avroio::{AvroRecordReader, AvroSink, CompressionCodec, schema_of};
use beam::schema::{BeamRow, FieldType, FieldValue, Row, Schema};
use beam::transforms::sdf::{OffsetRange, OffsetRangeTracker};
use chrono::{DateTime, NaiveDate, Utc};
use file::filebasedsource::file_split_restriction;
use file::filesystem::LocalFileSystem;
use file::sink::FileSink;

#[derive(Debug, Clone, PartialEq, BeamRow)]
struct Address {
    city: String,
    zip: Option<i32>,
}

#[derive(Debug, Clone, PartialEq, BeamRow)]
struct Event {
    id: i64,
    small: i8,
    medium: i16,
    user: String,
    score: Option<f64>,
    ratio: f32,
    active: bool,
    tags: Vec<String>,
    attrs: BTreeMap<String, i64>,
    home: Address,
    previous: Option<Address>,
    day: NaiveDate,
    at: DateTime<Utc>,
    #[beam(bytes)]
    blob: Vec<u8>,
}

fn events(n: i64) -> Vec<Event> {
    (0..n)
        .map(|id| Event {
            id,
            small: (id % 100) as i8,
            medium: (id % 30_000) as i16,
            user: format!("user-{}", id % 17),
            score: (id % 3 != 0).then_some(id as f64 / 2.0),
            ratio: id as f32 / 4.0,
            active: id % 2 == 0,
            tags: (0..id % 4).map(|t| format!("t{t}")).collect(),
            attrs: (0..id % 3).map(|k| (format!("k{k}"), k * id)).collect(),
            home: Address {
                city: format!("city-{}", id % 5),
                zip: (id % 2 == 1).then_some(id as i32),
            },
            previous: (id % 4 == 0).then(|| Address {
                city: "old".into(),
                zip: None,
            }),
            day: NaiveDate::from_ymd_opt(2024, 1, 1).unwrap() + chrono::Days::new(id as u64 % 365),
            at: DateTime::from_timestamp(1_700_000_000 + id, (id as u32 % 1000) * 1000).unwrap(),
            blob: vec![id as u8; (id % 5) as usize],
        })
        .collect()
}

struct TempDir(PathBuf);

impl TempDir {
    fn new(name: &str) -> Self {
        static COUNTER: AtomicU32 = AtomicU32::new(0);
        let path = std::env::temp_dir().join(format!(
            "beam_avro_{name}_{}_{}",
            std::process::id(),
            COUNTER.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir_all(&path).unwrap();
        Self(path)
    }

    fn file(&self, name: &str) -> String {
        self.0.join(name).to_str().unwrap().to_string()
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn write_file<T: 'static>(sink: &AvroSink<T>, path: &str, items: &[T]) {
    let out = Box::new(fs::File::create(path).unwrap());
    let mut writer = sink.open(out).unwrap();
    for item in items {
        writer.write(item).unwrap();
    }
    writer.finish().unwrap();
}

fn read_range<T: 'static>(
    codec: Arc<dyn RowCodec<T>>,
    path: &str,
    range: OffsetRange,
) -> (Vec<T>, OffsetRangeTracker) {
    let reader = AvroRecordReader::new(codec, 100);
    let tracker = OffsetRangeTracker::new(range);
    let mut out = Vec::new();
    reader
        .read_with_tracker(&LocalFileSystem::new(), path, &tracker, |item| {
            out.push(item);
            Ok(())
        })
        .unwrap();
    (out, tracker)
}

fn file_len(path: &str) -> i64 {
    i64::try_from(fs::metadata(path).unwrap().len()).unwrap()
}

fn codec() -> Arc<dyn RowCodec<Event>> {
    Arc::new(BeamRowCodec::<Event>::new())
}

#[test]
fn derive_round_trip_across_blocks() {
    let dir = TempDir::new("roundtrip");
    let path = dir.file("events.avro");
    let items = events(1_000);
    write_file(&AvroSink::<Event>::new().with_block_size(64), &path, &items);

    let (read, tracker) = read_range(codec(), &path, OffsetRange::new(0, file_len(&path)));
    assert_eq!(read, items);
    tracker.check_done().unwrap();
}

#[test]
fn splits_partition_blocks_exactly_once() {
    for compression in [
        None,
        Some(CompressionCodec::Snappy),
        Some(CompressionCodec::Deflate),
        Some(CompressionCodec::ZStandard),
    ] {
        let dir = TempDir::new("splits");
        let path = dir.file("events.avro");
        let items = events(600);
        let sink = AvroSink::<Event>::new()
            .with_block_size(50)
            .with_compression(compression);
        write_file(&sink, &path, &items);

        let whole = OffsetRange::new(0, file_len(&path));
        for split_size in [211, 1_000, 4_096] {
            let mut all = Vec::new();
            let mut non_empty = 0;
            for split in file_split_restriction(&whole, split_size) {
                let (read, tracker) = read_range(codec(), &path, split);
                tracker.check_done().unwrap();
                if !read.is_empty() {
                    non_empty += 1;
                }
                all.extend(read);
            }
            assert!(
                non_empty > 1,
                "{compression:?}/{split_size}: blocks should spread"
            );
            all.sort_by_key(|e| e.id);
            assert_eq!(all, items, "{compression:?}/{split_size}");
        }
    }
}

#[test]
fn empty_file_is_a_valid_container() {
    let dir = TempDir::new("empty");
    let path = dir.file("empty.avro");
    write_file::<Event>(&AvroSink::new(), &path, &[]);
    let (read, tracker) = read_range(codec(), &path, OffsetRange::new(0, file_len(&path)));
    assert!(read.is_empty());
    tracker.check_done().unwrap();
    // The header alone carries the full schema; only BYTE/INT16 widen to INT32.
    let mut expected = (**Event::beam_schema()).clone();
    for field in &mut expected.fields {
        if field.name == "small" || field.name == "medium" {
            field.field_type = FieldType::int32();
        }
    }
    assert_eq!(schema_of(&path).unwrap(), expected);
}

#[test]
fn rows_with_explicit_schema() {
    let dir = TempDir::new("rows");
    let path = dir.file("rows.avro");
    let schema = Arc::new(
        Schema::builder()
            .field("k", FieldType::string())
            .nullable_field("v", FieldType::int64())
            .field("list", FieldType::array(FieldType::double()))
            .build(),
    );
    let rows: Vec<Row> = (0..25)
        .map(|i| {
            Row::new(
                Arc::clone(&schema),
                vec![
                    Some(FieldValue::String(format!("k{i}"))),
                    (i % 2 == 0).then_some(FieldValue::Int64(i)),
                    Some(FieldValue::Array(
                        (0..i % 3)
                            .map(|x| Some(FieldValue::Double(x as f64)))
                            .collect(),
                    )),
                ],
            )
            .unwrap()
        })
        .collect();
    write_file(&AvroSink::for_rows(Arc::clone(&schema)), &path, &rows);
    assert_eq!(schema_of(&path).unwrap(), *schema);

    let (read, _) = read_range(
        Arc::new(SchemaRowCodec::new(Arc::clone(&schema))),
        &path,
        OffsetRange::new(0, file_len(&path)),
    );
    assert_eq!(read, rows);
}

#[test]
fn schema_of_widens_small_integers() {
    let dir = TempDir::new("schema");
    let path = dir.file("events.avro");
    write_file(&AvroSink::<Event>::new(), &path, &events(3));
    let schema = schema_of(&path).unwrap();
    // Avro has no 8/16-bit integers: they come back as INT32 unless read with a
    // Beam schema asking for the narrower type.
    assert_eq!(
        schema.field("small").unwrap().field_type,
        FieldType::int32()
    );
    assert_eq!(schema.field("id").unwrap().field_type, FieldType::int64());
    assert!(schema.field("score").unwrap().field_type.nullable);
}
