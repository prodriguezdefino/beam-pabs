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

use beam::prelude::*;
use tfrecord_io::tfrecordio::{RecordReader, masked_crc32c, write_record};

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
