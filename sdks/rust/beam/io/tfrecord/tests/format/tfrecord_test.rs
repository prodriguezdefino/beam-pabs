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

//! The TFRecord framing against reference values written by TensorFlow.

use std::io;
use std::sync::Arc;

use beam::coders::DefaultCoder;
use beam::internals::HandlerContext;
use beam::prelude::*;
use beam::transforms::sdf::{OffsetRange, OffsetRangeTracker};
use beam_io_file::{Compression, FileRecordReader, FileSystem};
use beam_io_tfrecord::tfrecordio::{
    Read, ReadFiles, RecordReader, TFRecordReader, masked_crc32c, write_record,
};
use beam_test_utils::InMemoryFileSystem;

/// The TFRecord file of the record `foo`.
const FOO: [u8; 19] = [
    0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xb0, 0x99, 0x49, 0x0e, 0x66, 0x6f, 0x6f, 0x61,
    0x8a, 0xbe, 0xfe,
];

/// The TFRecord file of the records `foo` and `bar`.
const FOO_BAR: [u8; 38] = [
    0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xb0, 0x99, 0x49, 0x0e, 0x66, 0x6f, 0x6f, 0x61,
    0x8a, 0xbe, 0xfe, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xb0, 0x99, 0x49, 0x0e, 0x62,
    0x61, 0x72, 0x46, 0x00, 0xe5, 0xc8,
];

fn read_all(bytes: &[u8]) -> Result<Vec<(u64, Vec<u8>)>> {
    RecordReader::new(bytes).collect()
}

fn framed_header_with_valid_crc(raw_length: u64, trailing: &[u8]) -> Vec<u8> {
    let len_bytes = raw_length.to_le_bytes();
    let crc_bytes = masked_crc32c(&len_bytes).to_le_bytes();
    let mut buf = Vec::with_capacity(12 + trailing.len());
    buf.extend_from_slice(&len_bytes);
    buf.extend_from_slice(&crc_bytes);
    buf.extend_from_slice(trailing);
    buf
}

fn read_range(fs: &dyn FileSystem, path: &str, start: i64, end: i64) -> Result<Vec<Vec<u8>>> {
    let tracker = OffsetRangeTracker::new(OffsetRange::new(start, end));
    let mut sink: Vec<Vec<u8>> = Vec::new();
    let mut hctx = HandlerContext::new(&mut sink);
    let mut ctx = hctx.as_process_context();
    TFRecordReader.read_records(fs, path, &tracker, &mut ctx)?;
    sink.into_iter()
        .map(|bytes| Vec::<u8>::decode(&bytes).map_err(Error::from))
        .collect()
}

#[test]
fn masked_crc32c_matches_the_reference_values() {
    assert_eq!(masked_crc32c(&[0x00; 32]), 0x0fd7_fffa);
    assert_eq!(masked_crc32c(&[0xff; 32]), 0xf909_b029);
    assert_eq!(masked_crc32c(b"foo"), 0xfebe_8a61);
    assert_eq!(masked_crc32c(&[0x03, 0, 0, 0, 0, 0, 0, 0]), 0x0e49_99b0);
}

#[test]
fn write_record_writes_the_tensorflow_bytes() -> Result {
    let mut written = Vec::new();
    write_record(&mut written, b"foo")?;
    assert_eq!(written, FOO);
    write_record(&mut written, b"bar")?;
    assert_eq!(written, FOO_BAR);
    Ok(())
}

#[test]
fn the_reader_gives_each_record_and_its_offset() -> Result {
    assert_eq!(
        read_all(&FOO_BAR)?,
        vec![(0, b"foo".to_vec()), (19, b"bar".to_vec())]
    );
    assert_eq!(read_all(&[])?, Vec::new());
    let mut empty_record = Vec::new();
    write_record(&mut empty_record, b"")?;
    assert_eq!(read_all(&empty_record)?, vec![(0, Vec::new())]);
    Ok(())
}

#[test]
fn a_corrupt_record_is_an_error() {
    let mut bad_data = FOO_BAR;
    bad_data[32] ^= 0x01;
    let err = read_all(&bad_data).expect_err("corrupt data");
    assert!(
        err.to_string().contains("byte 19") && err.to_string().contains("data"),
        "{err}"
    );

    let mut bad_length = FOO;
    bad_length[0] = 0x04;
    let err = read_all(&bad_length).expect_err("corrupt length");
    assert!(err.to_string().contains("length"), "{err}");
}

#[test]
fn a_truncated_record_is_an_error() {
    for end in [4, 10, 14, 17] {
        let err = read_all(&FOO[..end]).expect_err("truncated");
        assert!(err.to_string().contains("truncated"), "{end}: {err}");
    }
}

#[test]
fn oversized_length_header_fails_without_huge_allocation() {
    let max_header = framed_header_with_valid_crc(u64::MAX, b"short");
    let err = read_all(&max_header).expect_err("u64::MAX length must fail");
    let io_err = err
        .source()
        .and_then(|s| s.downcast_ref::<io::Error>())
        .expect("expected underlying io::Error");
    assert_eq!(io_err.kind(), io::ErrorKind::InvalidData);

    // 100 GiB fits in 64-bit isize::MAX yet has only 5 payload bytes in the stream.
    let huge_len: u64 = 100 * 1024 * 1024 * 1024;
    let huge_header = framed_header_with_valid_crc(huge_len, b"short");
    let err = read_all(&huge_header).expect_err("100 GiB truncated length must fail");
    let io_err = err
        .source()
        .and_then(|s| s.downcast_ref::<io::Error>())
        .expect("expected underlying io::Error");
    assert!(
        matches!(
            io_err.kind(),
            io::ErrorKind::UnexpectedEof | io::ErrorKind::InvalidData
        ),
        "unexpected error kind: {:?}",
        io_err.kind()
    );
}

#[test]
fn tfrecord_reader_splits_subranges_without_gaps_or_duplicates() -> Result {
    let expected: Vec<Vec<u8>> = vec![
        b"alpha".to_vec(),
        b"bravo-payload".to_vec(),
        Vec::new(),
        b"delta-0123456789".to_vec(),
        b"echo".to_vec(),
        b"foxtrot-final-record".to_vec(),
    ];

    let mut file_bytes = Vec::new();
    for record in &expected {
        write_record(&mut file_bytes, record)?;
    }
    let file_len = i64::try_from(file_bytes.len()).expect("file length fits in i64");

    let fs = Arc::new(InMemoryFileSystem::new());
    let path = "mem-tfr:///records.tfrecord";
    fs.insert_file(path, file_bytes);

    for split_offset in 1..file_len {
        let mut combined = read_range(fs.as_ref(), path, 0, split_offset)?;
        combined.extend(read_range(fs.as_ref(), path, split_offset, file_len)?);
        assert_eq!(
            combined, expected,
            "mismatch when splitting at byte {split_offset}"
        );
    }

    Ok(())
}

#[test]
fn read_and_read_files_record_display_data() {
    let p = Pipeline::new();
    let _ = p.apply(
        "ReadRecords",
        Read::new("/tmp/*.tfrecord").with_compression(Compression::Gzip),
    );
    let paths = p.apply("Paths", Create::new(["/tmp/a.tfrecord".to_string()]));
    let _ = paths.apply(
        "ReadFromPaths",
        ReadFiles::new().with_compression(Compression::Deflate),
    );

    let components = p.to_proto().components.expect("pipeline components");
    for unique_name in ["ReadRecords", "ReadFromPaths"] {
        let transform = components
            .transforms
            .values()
            .find(|t| t.unique_name == unique_name)
            .unwrap_or_else(|| panic!("missing transform {unique_name}"));
        assert!(
            !transform.display_data.is_empty(),
            "{unique_name} must attach display_data"
        );
    }
}
