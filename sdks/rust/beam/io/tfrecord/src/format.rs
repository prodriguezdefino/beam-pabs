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

//! The TFRecord framing: each record is
//!
//! ```text
//! u64 length (little endian) | u32 masked CRC32C of the length bytes | data | u32 masked CRC32C of the data
//! ```
//!
//! The masked CRC is the CRC32C rotated right by 15 bits plus `0xa282ead8`, as in
//! TensorFlow and the Java and Python SDKs.

use std::io::{self, Read, Write};

/// The constant that the mask adds to the rotated CRC.
const MASK_DELTA: u32 = 0xa282_ead8;

/// The bytes of a record other than its data: the length, and the two CRCs.
pub const RECORD_OVERHEAD: u64 = 16;

/// The masked CRC32C of `bytes`.
pub fn masked_crc32c(bytes: &[u8]) -> u32 {
    crc32c::crc32c(bytes)
        .rotate_right(15)
        .wrapping_add(MASK_DELTA)
}

/// Writes `data` as one record.
///
/// # Errors
///
/// Returns the error of `writer`.
pub fn write_record(writer: &mut dyn Write, data: &[u8]) -> io::Result<()> {
    let length = (data.len() as u64).to_le_bytes();
    writer.write_all(&length)?;
    writer.write_all(&masked_crc32c(&length).to_le_bytes())?;
    writer.write_all(data)?;
    writer.write_all(&masked_crc32c(data).to_le_bytes())
}

/// Reads the records of a TFRecord stream in order, and checks both CRCs of each.
pub struct RecordReader<R> {
    reader: R,
    offset: u64,
}

impl<R: Read> RecordReader<R> {
    /// Reads records from the start of `reader`.
    pub fn new(reader: R) -> Self {
        Self { reader, offset: 0 }
    }

    /// The next record and the byte offset where it starts, or `None` at the end of the
    /// stream.
    ///
    /// # Errors
    ///
    /// Returns an error if the stream ends inside a record, if a CRC does not match, or if
    /// the reader fails.
    pub fn next_record(&mut self) -> beam::Result<Option<(u64, Vec<u8>)>> {
        let start = self.offset;
        let mut length = [0u8; 8];
        if !self.fill(&mut length, true)? {
            return Ok(None);
        }
        let mut length_crc = [0u8; 4];
        self.fill(&mut length_crc, false)?;
        if u32::from_le_bytes(length_crc) != masked_crc32c(&length) {
            return Err(corrupt(start, "length"));
        }
        let size =
            usize::try_from(u64::from_le_bytes(length)).map_err(|_| corrupt(start, "length"))?;
        let mut data = vec![0u8; size];
        self.fill(&mut data, false)?;
        let mut data_crc = [0u8; 4];
        self.fill(&mut data_crc, false)?;
        if u32::from_le_bytes(data_crc) != masked_crc32c(&data) {
            return Err(corrupt(start, "data"));
        }
        Ok(Some((start, data)))
    }

    /// Fills `buffer`. Returns `false` if the stream ends before the first byte and
    /// `at_record_start` is set; any other early end is a truncated record.
    fn fill(&mut self, buffer: &mut [u8], at_record_start: bool) -> beam::Result<bool> {
        let mut read = 0;
        while read < buffer.len() {
            match self.reader.read(&mut buffer[read..]) {
                Ok(0) if read == 0 && at_record_start => return Ok(false),
                Ok(0) => {
                    return Err(beam::Error::msg(format!(
                        "TFRecord at byte {} is truncated",
                        self.offset
                    )));
                }
                Ok(n) => read += n,
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(e) => {
                    return Err(beam::Error::msg(format!(
                        "Failed to read the TFRecord at byte {}: {e}",
                        self.offset
                    )));
                }
            }
        }
        self.offset += read as u64;
        Ok(true)
    }
}

impl<R: Read> Iterator for RecordReader<R> {
    type Item = beam::Result<(u64, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_record().transpose()
    }
}

fn corrupt(offset: u64, part: &str) -> beam::Error {
    beam::Error::msg(format!(
        "The TFRecord at byte {offset} is corrupt: the CRC of its {part} does not match"
    ))
}
