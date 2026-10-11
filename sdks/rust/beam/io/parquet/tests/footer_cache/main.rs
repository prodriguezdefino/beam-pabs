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

//! The worker-level footer cache: the reads that each split makes.
//!
//! The cache is shared by the whole process. This binary has no other Parquet reads, so no
//! other test can evict a footer while a test counts reads.

mod common;

use std::fs;
use std::sync::Arc;

use beam::schema::BeamRow;
use beam::transforms::sdf::{OffsetRange, OffsetRangeTracker, RestrictionTracker};
use beam_io_arrow::BeamRowCodec;
use beam_io_parquet::parquetio::{ParquetRecordReader, ParquetSink};

use common::{CountingFileSystem, TempDir, write_file};

#[derive(Debug, Clone, PartialEq, BeamRow)]
struct Id {
    id: i64,
}

fn ids(range: std::ops::Range<i64>) -> Vec<Id> {
    range.map(|id| Id { id }).collect()
}

/// Reads `[start, end)` of `uri` and checks that the tracker is done.
fn read_split(uri: &str, start: i64, end: i64) -> Vec<i64> {
    let reader = ParquetRecordReader::new(Arc::new(BeamRowCodec::<Id>::new()), 64);
    let tracker = OffsetRangeTracker::new(OffsetRange::new(start, end));
    let mut out = Vec::new();
    reader
        .read_with_tracker(uri, &tracker, |Id { id }| {
            out.push(id);
            Ok(())
        })
        .unwrap();
    tracker.check_done().unwrap();
    out
}

/// The reads of the footer: the metadata and its length at the end of the file.
fn footer_reads(path: &str, reads: &[(u64, u64)]) -> usize {
    let file = fs::read(path).unwrap();
    let tail: [u8; 4] = file[file.len() - 8..file.len() - 4].try_into().unwrap();
    let footer_start = (file.len() - 8 - u32::from_le_bytes(tail) as usize) as u64;
    reads
        .iter()
        .filter(|(start, _)| *start >= footer_start)
        .count()
}

#[test]
fn splits_of_one_file_read_the_footer_once() {
    let dir = TempDir::new("footer_once");
    let path = dir.file("ids.parquet");
    write_file(
        &ParquetSink::<Id>::new().with_row_group_size(100),
        &path,
        &ids(0..500),
    );
    let len = fs::metadata(&path).unwrap().len();
    let uri = CountingFileSystem::uri(&path);
    let counting = CountingFileSystem::get();

    let end = i64::try_from(len).unwrap();
    let step = end / 5 + 1;
    let mut all = read_split(&uri, 0, step);
    let first_footer_reads = footer_reads(&path, &counting.reads(&uri));
    assert!(first_footer_reads > 0);
    all.extend((1..5).flat_map(|i| read_split(&uri, i * step, ((i + 1) * step).min(end))));
    all.sort_unstable();
    assert_eq!(all, (0..500).collect::<Vec<_>>());

    // Each split looks up the size once; only the first split reads the footer.
    assert_eq!(counting.sizes(&uri), 5);
    assert_eq!(
        footer_reads(&path, &counting.reads(&uri)),
        first_footer_reads
    );

    // A split with no row group start reads no bytes once the footer is cached.
    let before = counting.reads(&uri).len();
    assert!(read_split(&uri, end - 8, end).is_empty());
    assert_eq!(counting.reads(&uri).len(), before);
    assert_eq!(counting.sizes(&uri), 6);
}

#[test]
fn a_file_written_again_with_another_size_reads_the_new_footer() {
    let dir = TempDir::new("footer_size_key");
    let path = dir.file("ids.parquet");
    let uri = CountingFileSystem::uri(&path);
    let sink = ParquetSink::<Id>::new().with_row_group_size(100);

    write_file(&sink, &path, &ids(0..10));
    let len = i64::try_from(fs::metadata(&path).unwrap().len()).unwrap();
    assert_eq!(read_split(&uri, 0, len), (0..10).collect::<Vec<_>>());

    // The old footer would give 10 rows; the new file has 300 in 3 row groups.
    write_file(&sink, &path, &ids(0..300));
    let end = i64::try_from(fs::metadata(&path).unwrap().len()).unwrap();
    assert_ne!(end, len);
    assert_eq!(read_split(&uri, 0, end), (0..300).collect::<Vec<_>>());
}
