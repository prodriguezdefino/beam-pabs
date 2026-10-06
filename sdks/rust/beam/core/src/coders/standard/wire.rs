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

//! Wire helpers that the coders share: byte reads, counts, lengths and timestamps.

use std::io::Write;

use super::VarIntCoder;
use crate::coders::URN_STATE_BACKED_ITERABLE;
use crate::coders::traits::{CoderError, Context};

pub(super) fn unexpected_eof(wanted: usize, left: usize) -> std::io::Error {
    std::io::Error::new(
        std::io::ErrorKind::UnexpectedEof,
        format!("expected {wanted} bytes, {left} left"),
    )
}

/// Splits the next `len` bytes off `reader` and returns them, borrowed.
pub fn take_bytes<'a>(reader: &mut &'a [u8], len: usize) -> Result<&'a [u8], std::io::Error> {
    if reader.len() < len {
        return Err(unexpected_eof(len, reader.len()));
    }
    let (head, rest) = reader.split_at(len);
    *reader = rest;
    Ok(head)
}

/// The bytes between `start` and `rest`, where `rest` is what is left of `start` after reads.
pub fn consumed<'a>(start: &'a [u8], rest: &[u8]) -> &'a [u8] {
    &start[..start.len() - rest.len()]
}

thread_local! {
    static SCRATCH: std::cell::RefCell<Vec<u8>> = const { std::cell::RefCell::new(Vec::new()) };
}

/// Runs `f` with an empty buffer of this thread, kept between calls so that encoding to
/// measure or hash a value does not allocate. A nested call gets a new buffer.
pub(crate) fn with_scratch_buffer<R>(f: impl FnOnce(&mut Vec<u8>) -> R) -> R {
    SCRATCH.with(|cell| match cell.try_borrow_mut() {
        Ok(mut buf) => {
            buf.clear();
            f(&mut buf)
        }
        Err(_) => f(&mut Vec::new()),
    })
}

pub(in crate::coders) fn read_array<const N: usize>(
    reader: &mut &[u8],
) -> Result<[u8; N], std::io::Error> {
    let bytes = take_bytes(reader, N)?;
    let mut buf = [0u8; N];
    buf.copy_from_slice(bytes);
    Ok(buf)
}

/// Encodes big-endian milliseconds with the sign bit flipped, so byte order matches time order.
pub(in crate::coders) fn encode_timestamp(millis: i64) -> [u8; 8] {
    ((millis as u64) ^ (1 << 63)).to_be_bytes()
}

pub(in crate::coders) fn decode_timestamp(bytes: [u8; 8]) -> i64 {
    (u64::from_be_bytes(bytes) ^ (1 << 63)) as i64
}

/// Reads a timestamp written by [`encode_timestamp`].
pub(in crate::coders) fn read_timestamp(reader: &mut &[u8]) -> Result<i64, std::io::Error> {
    read_array(reader).map(decode_timestamp)
}

/// Reads `len` bytes into a new buffer. The length is checked against the input first, so a
/// corrupt length cannot allocate more than the input holds.
pub(in crate::coders) fn read_exact_vec(
    reader: &mut &[u8],
    len: usize,
) -> Result<Vec<u8>, std::io::Error> {
    take_bytes(reader, len).map(<[u8]>::to_vec)
}

pub(in crate::coders) fn read_be_i32(reader: &mut &[u8]) -> Result<i32, std::io::Error> {
    Ok(i32::from_be_bytes(read_array(reader)?))
}

/// Reads a 32-bit big-endian element count. A negative count is an error: it would read zero
/// elements and leave the reader on the wrong byte.
pub(in crate::coders) fn read_count(reader: &mut &[u8]) -> Result<usize, CoderError> {
    let count = read_be_i32(reader)?;
    usize::try_from(count)
        .map_err(|_| CoderError::Format(format!("Negative element count: {count}")))
}

/// Writes a collection length as the 32-bit big-endian count of the wire format.
pub(in crate::coders) fn write_count(len: usize, writer: &mut Vec<u8>) -> Result<(), CoderError> {
    let count = i32::try_from(len)
        .map_err(|_| CoderError::Format(format!("length {len} exceeds the i32 wire limit")))?;
    Ok(writer.write_all(&count.to_be_bytes())?)
}

/// Reads a varint length prefix. A negative length is an error, not a huge `usize`.
pub(in crate::coders) fn read_varint_len(reader: &mut &[u8]) -> Result<usize, CoderError> {
    let len = VarIntCoder::decode_varint(reader)?;
    usize::try_from(len).map_err(|_| CoderError::Format(format!("Negative length: {len}")))
}

/// Writes a byte payload, with a varint length prefix in `Nested` context.
pub(in crate::coders) fn write_bytes(
    value: &[u8],
    writer: &mut Vec<u8>,
    context: Context,
) -> Result<(), CoderError> {
    if context == Context::Nested {
        VarIntCoder::encode_varint(value.len() as i64, writer)?;
    }
    writer.write_all(value)?;
    Ok(())
}

/// Reads a byte payload: varint length-prefixed in `Nested` context, else the rest of the stream.
pub(in crate::coders) fn read_bytes(
    reader: &mut &[u8],
    context: Context,
) -> Result<Vec<u8>, CoderError> {
    match context {
        Context::Nested => {
            let len = read_varint_len(reader)?;
            Ok(read_exact_vec(reader, len)?)
        }
        Context::WholeStream => Ok(std::mem::take(reader).to_vec()),
    }
}

/// Decodes an iterable without a state channel.
pub(in crate::coders) fn decode_iterable<T>(
    reader: &mut &[u8],
    decode_element: impl FnMut(&mut &[u8]) -> Result<T, CoderError>,
) -> Result<Vec<T>, CoderError> {
    crate::coders::iterable::decode_iterable_with_state(reader, decode_element, None)
}

/// Error message for a state-backed iterable when no state channel is configured.
pub(in crate::coders) fn state_backed_iterable_message(decoded_so_far: usize) -> String {
    format!(
        "iterable is state-backed ('{URN_STATE_BACKED_ITERABLE}'): {decoded_so_far} element(s) \
         were inlined and the rest require a runner state channel"
    )
}
