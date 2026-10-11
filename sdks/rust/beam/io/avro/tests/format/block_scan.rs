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

//! Split-start sync-marker scanning and block-header varint decoding.

use std::fs;
use std::sync::Arc;

use arrow_io::BeamRowCodec;
use avro_io::avroio::{AvroRecordReader, AvroSink};
use beam::transforms::sdf::{OffsetRange, OffsetRangeTracker};
use file::filesystem::LocalFileSystem;

use crate::common::{
    Simple, TempDir, long, parse_blocks, parse_header, read_all, simple, write_to_bytes,
};

/// Bytes read per chunk by the reader's sync-marker scan.
const SCAN_CHUNK: u64 = 64 * 1024;

/// Reads `[start, end)` of `path`, returning the decoded ids or the error text.
fn read_split(path: &str, start: u64, end: u64) -> Result<Vec<i64>, String> {
    let reader = AvroRecordReader::new(Arc::new(BeamRowCodec::<Simple>::new()), 100);
    let tracker = OffsetRangeTracker::new(OffsetRange::new(start as i64, end as i64));
    let mut ids = Vec::new();
    reader
        .read_with_tracker(&LocalFileSystem::new(), path, &tracker, |s| {
            ids.push(s.id);
            Ok(())
        })
        .map_err(|e| e.to_string())?;
    tracker.check_done().map_err(|e| e.to_string())?;
    Ok(ids)
}

#[test]
fn split_start_finds_sync_marker_across_scan_chunks() {
    // Record 0 is a block larger than two scan chunks; records 1..=3 are small blocks.
    let mut items: Vec<Simple> = (0..4).map(|id| Simple { id, ..simple() }).collect();
    items[0].name = "x".repeat(3 * SCAN_CHUNK as usize);
    let buf = write_to_bytes(
        &AvroSink::<Simple>::new()
            .with_block_size(1)
            .with_compression(None),
        &items,
    );
    let header = parse_header(&buf);
    assert_eq!(parse_blocks(&buf, &header).len(), 4);
    let first_end = buf[header.len..]
        .windows(16)
        .position(|w| w == header.sync)
        .map(|p| (header.len + p + 16) as u64)
        .unwrap();
    let len = buf.len() as u64;
    let dir = TempDir::new("scan");
    let path = dir.file("scan.avro");
    fs::write(&path, &buf).unwrap();

    // The scan starts at `start - 16`; offsets place the marker ending at
    // `first_end` relative to the scan's chunk boundaries.
    let cases = [
        (
            "marker straddles first chunk boundary",
            first_end - SCAN_CHUNK + 8,
        ),
        ("marker starts second chunk", first_end - SCAN_CHUNK),
        (
            "marker straddles second chunk boundary",
            first_end - 2 * SCAN_CHUNK + 8,
        ),
        ("marker ends second chunk", first_end + 16 - 2 * SCAN_CHUNK),
        ("split starts at block", first_end),
    ];
    for (name, start) in cases {
        assert!(start > header.len as u64, "{name}");
        assert_eq!(read_split(&path, start, len), Ok(vec![1, 2, 3]), "{name}");
        assert_eq!(read_split(&path, 0, start), Ok(vec![0]), "{name}");
    }
    // A split starting one byte into block 1 begins at block 2.
    assert_eq!(read_split(&path, first_end + 1, len), Ok(vec![2, 3]));
    // The last marker ends the file, so no block starts in the split.
    assert_eq!(read_split(&path, len - 1, len), Ok(vec![]));
    // No marker at or after the scan start.
    assert_eq!(read_split(&path, len + 20, len + 30), Ok(vec![]));
    assert_eq!(
        read_all(Arc::new(BeamRowCodec::<Simple>::new()), &path),
        items
    );
}

#[test]
fn block_header_varints_decode_zigzag_values() {
    let header_buf = write_to_bytes(&AvroSink::<Simple>::new(), &[]);
    let header_len = header_buf.len() as u64;
    let dir = TempDir::new("varint");
    let overlong = vec![0xff; 10];
    let cases: Vec<(&str, Vec<u8>, &str)> = vec![
        (
            "negative size",
            [long(1), long(-5)].concat(),
            "Invalid Avro block size -5 ",
        ),
        (
            "multi-byte negative size",
            [long(1), long(-1_000_000)].concat(),
            "Invalid Avro block size -1000000 ",
        ),
        (
            "ten-byte negative size",
            [long(1), long(i64::MIN)].concat(),
            "Invalid Avro block size -9223372036854775808 ",
        ),
        (
            "ten-byte count",
            [long(i64::MIN), long(-7)].concat(),
            "Invalid Avro block size -7 ",
        ),
        ("overlong count", overlong, "Overlong varint"),
        ("truncated count", vec![0x80], "Truncated varint"),
        (
            "truncated size",
            [long(1), vec![0x80]].concat(),
            "Truncated varint",
        ),
        ("missing size", long(1), "Truncated Avro block header"),
    ];
    assert_eq!(long(i64::MIN).len(), 10);
    for (name, block, expected) in cases {
        let path = dir.file(&format!("{}.avro", name.replace(' ', "_")));
        fs::write(&path, [header_buf.clone(), block].concat()).unwrap();
        let len = fs::metadata(&path).unwrap().len();
        let err = read_split(&path, header_len, len).unwrap_err();
        assert!(err.contains(expected), "{name}: {err}");
    }
}

#[test]
fn range_past_end_of_file_stops_cleanly() {
    let items: Vec<Simple> = (0..3).map(|id| Simple { id, ..simple() }).collect();
    let buf = write_to_bytes(&AvroSink::<Simple>::new().with_block_size(1), &items);
    let dir = TempDir::new("past_eof");
    let path = dir.file("past_eof.avro");
    fs::write(&path, &buf).unwrap();
    let len = buf.len() as u64;
    assert_eq!(read_split(&path, 0, len + 100), Ok(vec![0, 1, 2]));
}
