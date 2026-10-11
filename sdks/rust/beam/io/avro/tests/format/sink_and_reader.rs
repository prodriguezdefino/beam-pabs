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

use std::collections::BTreeMap;
use std::fs;
use std::sync::Arc;

use arrow_io::{BeamRowCodec, RowCodec, RowOutput, SchemaRowCodec};
use avro_io::avroio::{AvroRecordReader, AvroSink, CompressionCodec, schema_of};
use beam::schema::{BeamRow, FieldType, FieldValue, Row, RowElement, Schema};
use beam::transforms::sdf::{OffsetRange, OffsetRangeTracker, RestrictionTracker};
use chrono::{DateTime, NaiveDate, Utc};
use file::LocalFileSystem;
use file::file_split_restriction;

use crate::common::{TempDir, write_file};

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
    let elements: Vec<RowElement> = rows
        .iter()
        .enumerate()
        .map(|(i, row)| {
            if i % 2 == 0 {
                RowElement::Encoded(row.to_row_bytes().unwrap())
            } else {
                RowElement::Row(row.clone())
            }
        })
        .collect();
    write_file(&AvroSink::for_rows(Arc::clone(&schema)), &path, &elements);
    assert_eq!(schema_of(&path).unwrap(), *schema);

    let read = |output| {
        let codec = SchemaRowCodec::new(Arc::clone(&schema)).with_output(output);
        read_range(Arc::new(codec), &path, OffsetRange::new(0, file_len(&path))).0
    };
    let values: Vec<RowElement> = rows.iter().cloned().map(RowElement::Row).collect();
    assert_eq!(read(RowOutput::Values), values);
    let encoded: Vec<RowElement> = rows
        .iter()
        .map(|row| RowElement::Encoded(row.to_row_bytes().unwrap()))
        .collect();
    assert_eq!(read(RowOutput::Encoded), encoded);
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

/// An Avro file inside a stream codec fails with the byte-range error.
#[test]
fn stream_compressed_avro_fails_with_byte_range_error() {
    use std::io::Write;

    use beam::internals::HandlerContext;
    use beam::transforms::sdf::SplittableDoFn;
    use file::{Compression, FileBasedSourceFn};

    let dir = TempDir::new("stream_compressed");
    let plain = dir.file("events.avro");
    write_file(&AvroSink::<Event>::new(), &plain, &events(10));
    let path = dir.file("events.avro.zst");
    let mut zst = Compression::Zstd
        .writer(fs::File::create(&path).unwrap())
        .unwrap();
    zst.write_all(&fs::read(&plain).unwrap()).unwrap();
    zst.finish().unwrap();

    let reader = AvroRecordReader::new(Arc::new(BeamRowCodec::<Event>::new()), 64);
    let mut dofn = FileBasedSourceFn::new(1 << 20, Arc::new(reader));
    let tracker = dofn.create_tracker(&dofn.initial_restriction(&path).unwrap());
    let mut sink: Vec<Vec<u8>> = Vec::new();
    let mut hctx = HandlerContext::new(&mut sink);
    let err = dofn
        .process_element(path.clone(), &tracker, &mut hctx.as_process_context())
        .map(drop)
        .expect_err("a compressed Avro file must fail");
    let expected =
        format!("'{path}' is ZSTD compressed, and compressed files cannot be read by byte range");
    assert!(format!("{err:#}").contains(&expected), "{err:#}");
}
