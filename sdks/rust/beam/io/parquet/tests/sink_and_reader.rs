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

//! Exercises the Parquet sink and splittable reader directly, without a runner.
#![expect(clippy::unwrap_used, reason = "test fixtures panic on setup failure")]

use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use arrow_io::{BeamRowCodec, RowCodec, SchemaRowCodec};
use beam::schema::{BeamRow, FieldType, FieldValue, Row, Schema};
use beam::transforms::sdf::{OffsetRange, OffsetRangeTracker};
use file::filebasedsource::file_split_restriction;
use file::sink::FileSink;
use parquet_io::parquet::file::reader::{FileReader, SerializedFileReader};
use parquet_io::parquetio::{
    Compression, FileSystemChunkReader, ParquetRecordReader, ParquetSink, ZstdLevel, schema_of,
};

#[derive(Debug, Clone, PartialEq, BeamRow)]
struct Event {
    id: i64,
    user: String,
    score: Option<f64>,
    tags: Vec<String>,
}

fn events(n: i64) -> Vec<Event> {
    (0..n)
        .map(|id| Event {
            id,
            user: format!("user-{}", id % 17),
            score: (id % 3 != 0).then_some(id as f64 / 2.0),
            tags: (0..id % 4).map(|t| format!("t{t}")).collect(),
        })
        .collect()
}

struct TempDir(PathBuf);

impl TempDir {
    fn new(name: &str) -> Self {
        static COUNTER: AtomicU32 = AtomicU32::new(0);
        let path = std::env::temp_dir().join(format!(
            "beam_parquet_{name}_{}_{}",
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

fn write_file<T: 'static>(sink: &ParquetSink<T>, path: &str, items: &[T]) {
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
    let reader = ParquetRecordReader::new(codec, 64);
    let tracker = OffsetRangeTracker::new(range);
    let mut out = Vec::new();
    reader
        .read_with_tracker(path, &tracker, |item| {
            out.push(item);
            Ok(())
        })
        .unwrap();
    (out, tracker)
}

fn file_len(path: &str) -> i64 {
    i64::try_from(fs::metadata(path).unwrap().len()).unwrap()
}

fn row_group_count(path: &str) -> usize {
    SerializedFileReader::new(fs::File::open(path).unwrap())
        .unwrap()
        .metadata()
        .num_row_groups()
}

#[test]
fn derive_round_trip_with_many_row_groups() {
    let dir = TempDir::new("roundtrip");
    let path = dir.file("events.parquet");
    let items = events(2_500);
    let sink = ParquetSink::<Event>::new()
        .with_batch_size(100)
        .with_row_group_size(500);
    write_file(&sink, &path, &items);
    assert_eq!(row_group_count(&path), 5);

    let (read, tracker) = read_range(
        Arc::new(BeamRowCodec::<Event>::new()),
        &path,
        OffsetRange::new(0, file_len(&path)),
    );
    assert_eq!(read, items);
    tracker.check_done().unwrap();
}

#[test]
fn splits_partition_row_groups_exactly_once() {
    let dir = TempDir::new("splits");
    let path = dir.file("events.parquet");
    let items = events(3_000);
    let sink = ParquetSink::<Event>::new().with_row_group_size(250);
    write_file(&sink, &path, &items);
    assert_eq!(row_group_count(&path), 12);

    let whole = OffsetRange::new(0, file_len(&path));
    let splits = file_split_restriction(&whole, 512);
    assert!(splits.len() > 12, "want more splits than row groups");

    let mut all = Vec::new();
    let mut non_empty = 0;
    for split in splits {
        let (read, tracker) = read_range(Arc::new(BeamRowCodec::<Event>::new()), &path, split);
        tracker.check_done().unwrap();
        if !read.is_empty() {
            non_empty += 1;
        }
        all.extend(read);
    }
    assert!(non_empty > 1, "row groups should spread across splits");
    all.sort_by_key(|e| e.id);
    assert_eq!(all, items);
}

#[test]
fn stops_when_claim_is_refused() {
    let dir = TempDir::new("claim");
    let path = dir.file("events.parquet");
    let items = events(1_000);
    write_file(
        &ParquetSink::<Event>::new().with_row_group_size(100),
        &path,
        &items,
    );
    // A range ending before the second row group reads only the first.
    let metadata = SerializedFileReader::new(fs::File::open(&path).unwrap())
        .unwrap()
        .metadata()
        .clone();
    let second = metadata.row_group(1).column(0).byte_range().0;
    assert!(i64::try_from(second).unwrap() < file_len(&path));
    let (read, _) = read_range(
        Arc::new(BeamRowCodec::<Event>::new()),
        &path,
        OffsetRange::new(0, i64::try_from(second).unwrap()),
    );
    assert_eq!(read, items[..100].to_vec());
}

#[test]
fn empty_file_is_valid_parquet() {
    let dir = TempDir::new("empty");
    let path = dir.file("empty.parquet");
    write_file::<Event>(&ParquetSink::new(), &path, &[]);
    assert_eq!(&schema_of(&path).unwrap(), &**Event::beam_schema());
    let (read, _) = read_range(
        Arc::new(BeamRowCodec::<Event>::new()),
        &path,
        OffsetRange::new(0, file_len(&path)),
    );
    assert!(read.is_empty());
}

#[test]
fn rows_with_explicit_schema_and_zstd() {
    let dir = TempDir::new("rows");
    let path = dir.file("rows.parquet");
    let schema = Arc::new(
        Schema::builder()
            .field("k", FieldType::string())
            .nullable_field("v", FieldType::int32())
            .build(),
    );
    let rows: Vec<Row> = (0..10)
        .map(|i| {
            Row::new(
                Arc::clone(&schema),
                vec![
                    Some(FieldValue::String(format!("k{i}"))),
                    (i % 2 == 0).then_some(FieldValue::Int32(i)),
                ],
            )
            .unwrap()
        })
        .collect();
    let sink = ParquetSink::for_rows(Arc::clone(&schema))
        .with_compression(Compression::ZSTD(ZstdLevel::default()));
    write_file(&sink, &path, &rows);

    assert_eq!(schema_of(&path).unwrap(), *schema);
    let (read, _) = read_range(
        Arc::new(SchemaRowCodec::new(Arc::clone(&schema))),
        &path,
        OffsetRange::new(0, file_len(&path)),
    );
    assert_eq!(read, rows);
}

#[test]
fn projection_reads_subset_of_columns() {
    #[derive(Debug, Clone, PartialEq, BeamRow)]
    struct IdOnly {
        id: i64,
        missing: Option<String>,
    }

    let dir = TempDir::new("projection");
    let path = dir.file("events.parquet");
    let items = events(50);
    write_file(&ParquetSink::<Event>::new(), &path, &items);
    let (read, _) = read_range(
        Arc::new(BeamRowCodec::<IdOnly>::new()),
        &path,
        OffsetRange::new(0, file_len(&path)),
    );
    let expected: Vec<IdOnly> = items
        .iter()
        .map(|e| IdOnly {
            id: e.id,
            missing: None,
        })
        .collect();
    assert_eq!(read, expected);
}

#[test]
fn chunk_reader_serves_ranges() {
    use parquet_io::parquet::file::reader::{ChunkReader, Length};
    use std::io::Read;

    let dir = TempDir::new("chunk");
    let path = dir.file("bytes.bin");
    fs::write(&path, b"0123456789").unwrap();
    let chunk = FileSystemChunkReader::open(&path).unwrap();
    assert_eq!(chunk.len(), 10);
    assert_eq!(&chunk.get_bytes(3, 4).unwrap()[..], b"3456");
    assert!(chunk.get_bytes(0, 0).unwrap().is_empty());
    assert!(chunk.get_bytes(8, 5).is_err());
    let mut rest = String::new();
    chunk
        .get_read(7)
        .unwrap()
        .read_to_string(&mut rest)
        .unwrap();
    assert_eq!(rest, "789");
}

fn row_group_starts(path: &str) -> Vec<i64> {
    SerializedFileReader::new(fs::File::open(path).unwrap())
        .unwrap()
        .metadata()
        .row_groups()
        .iter()
        .map(|rg| i64::try_from(rg.column(0).byte_range().0).unwrap())
        .collect()
}

#[test]
fn ranges_claim_row_groups_by_first_byte() {
    let dir = TempDir::new("aligned");
    let path = dir.file("events.parquet");
    let items = events(600);
    write_file(
        &ParquetSink::<Event>::new().with_row_group_size(100),
        &path,
        &items,
    );
    let starts = row_group_starts(&path);
    assert_eq!(starts.len(), 6);
    let cases = [
        (
            "aligned to row group starts",
            starts[2],
            starts[4],
            200..400,
        ),
        (
            "one byte past row group starts",
            starts[2] + 1,
            starts[4] + 1,
            300..500,
        ),
        (
            "ends one byte into a row group",
            starts[2],
            starts[3] + 1,
            200..400,
        ),
    ];
    for (name, start, end, ids) in cases {
        let (read, tracker) = read_range(
            Arc::new(BeamRowCodec::<Event>::new()),
            &path,
            OffsetRange::new(start, end),
        );
        tracker.check_done().unwrap();
        assert_eq!(read, items[ids].to_vec(), "{name}");
    }
}

#[test]
fn dynamic_split_during_read_hands_off_remaining_row_groups() {
    let dir = TempDir::new("dynamic_split");
    let path = dir.file("events.parquet");
    let items = events(500);
    write_file(
        &ParquetSink::<Event>::new().with_row_group_size(100),
        &path,
        &items,
    );
    let reader = ParquetRecordReader::new(Arc::new(BeamRowCodec::<Event>::new()), 64);
    let tracker = OffsetRangeTracker::new(OffsetRange::new(0, file_len(&path)));
    let mut primary = Vec::new();
    let mut residual = None;
    reader
        .read_with_tracker(&path, &tracker, |item| {
            if residual.is_none() {
                residual = tracker.try_split(0.0).map(|(_, r)| r);
            }
            primary.push(item);
            Ok(())
        })
        .unwrap();
    tracker.check_done().unwrap();
    assert_eq!(primary, items[..100].to_vec());

    let (rest, rest_tracker) = read_range(
        Arc::new(BeamRowCodec::<Event>::new()),
        &path,
        residual.unwrap(),
    );
    rest_tracker.check_done().unwrap();
    assert_eq!(rest, items[100..].to_vec());
}
