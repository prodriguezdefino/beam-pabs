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

//! Standard primitive Beam coders and the wire helpers that the other coders share.

use std::io::{Read, Write};

use super::traits::{Coder, CoderError, CoderRegistry, Context, DefaultCoder};
use super::{
    URN_BOOL, URN_BYTES, URN_DOUBLE, URN_STATE_BACKED_ITERABLE, URN_STRING_UTF8, URN_VARINT,
};

pub(super) fn read_array<const N: usize>(reader: &mut dyn Read) -> Result<[u8; N], std::io::Error> {
    let mut buf = [0u8; N];
    reader.read_exact(&mut buf)?;
    Ok(buf)
}

/// Encodes big-endian milliseconds with the sign bit flipped, so byte order matches time order.
pub(super) fn encode_timestamp(millis: i64) -> [u8; 8] {
    ((millis as u64) ^ (1 << 63)).to_be_bytes()
}

pub(super) fn decode_timestamp(bytes: [u8; 8]) -> i64 {
    (u64::from_be_bytes(bytes) ^ (1 << 63)) as i64
}

/// Reads a timestamp written by [`encode_timestamp`].
pub(super) fn read_timestamp(reader: &mut dyn Read) -> Result<i64, std::io::Error> {
    read_array(reader).map(decode_timestamp)
}

/// Largest buffer to reserve in advance for a length read from the wire.
const MAX_PREALLOCATED_BYTES: usize = 64 * 1024;

/// Reads `len` bytes, capping initial allocation at [`MAX_PREALLOCATED_BYTES`].
pub(super) fn read_exact_vec(reader: &mut dyn Read, len: usize) -> Result<Vec<u8>, std::io::Error> {
    let mut buf = Vec::with_capacity(len.min(MAX_PREALLOCATED_BYTES));
    Read::take(&mut *reader, len as u64).read_to_end(&mut buf)?;
    if buf.len() < len {
        return Err(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            format!("expected {len} bytes, stream ended after {}", buf.len()),
        ));
    }
    Ok(buf)
}

pub(super) fn read_be_i32(reader: &mut dyn Read) -> Result<i32, std::io::Error> {
    Ok(i32::from_be_bytes(read_array(reader)?))
}

/// Writes a byte payload, with a varint length prefix in `Nested` context.
fn write_bytes(value: &[u8], writer: &mut dyn Write, context: Context) -> Result<(), CoderError> {
    if context == Context::Nested {
        VarIntCoder::encode_varint(value.len() as i64, writer)?;
    }
    writer.write_all(value)?;
    Ok(())
}

/// Reads a byte payload: varint length-prefixed in `Nested` context, else the rest of the stream.
fn read_bytes(reader: &mut dyn Read, context: Context) -> Result<Vec<u8>, CoderError> {
    match context {
        Context::Nested => {
            let len = VarIntCoder::decode_varint(reader)? as usize;
            Ok(read_exact_vec(reader, len)?)
        }
        Context::WholeStream => {
            let mut buf = Vec::new();
            reader.read_to_end(&mut buf)?;
            Ok(buf)
        }
    }
}

/// Decodes an iterable without a state channel.
pub(super) fn decode_iterable<T>(
    reader: &mut dyn Read,
    decode_element: impl FnMut(&mut dyn Read) -> Result<T, CoderError>,
) -> Result<Vec<T>, CoderError> {
    super::iterable::decode_iterable_with_state(reader, decode_element, None)
}

/// Error message for a state-backed iterable when no state channel is configured.
pub(super) fn state_backed_iterable_message(decoded_so_far: usize) -> String {
    format!(
        "iterable is state-backed ('{URN_STATE_BACKED_ITERABLE}'): {decoded_so_far} element(s) \
         were inlined and the rest require a runner state channel"
    )
}

/// Coder for UTF-8 strings.
#[derive(Clone, Debug, Default)]
pub struct StringUtf8Coder;

impl Coder<String> for StringUtf8Coder {
    fn urn(&self) -> &'static str {
        URN_STRING_UTF8
    }

    fn encode(
        &self,
        value: &String,
        writer: &mut dyn Write,
        context: Context,
    ) -> Result<(), CoderError> {
        write_bytes(value.as_bytes(), writer, context)
    }

    fn decode(&self, reader: &mut dyn Read, context: Context) -> Result<String, CoderError> {
        let bytes = read_bytes(reader, context)?;
        Ok(String::from_utf8(bytes)?)
    }
}

impl DefaultCoder for String {
    type Coder = StringUtf8Coder;
    fn coder() -> Self::Coder {
        StringUtf8Coder
    }

    fn encode_element(&self, writer: &mut dyn Write) -> Result<(), CoderError> {
        StringUtf8Coder.encode(self, writer, Context::Nested)
    }

    fn decode_element(reader: &mut dyn Read) -> Result<Self, CoderError> {
        StringUtf8Coder.decode(reader, Context::Nested)
    }

    fn register_coder<R: CoderRegistry + ?Sized>(registry: &R) -> String {
        registry.register_coder(URN_STRING_UTF8, Vec::new())
    }
}

/// Coder for raw byte vectors.
#[derive(Clone, Debug, Default)]
pub struct BytesCoder;

impl Coder<Vec<u8>> for BytesCoder {
    fn urn(&self) -> &'static str {
        URN_BYTES
    }

    fn encode(
        &self,
        value: &Vec<u8>,
        writer: &mut dyn Write,
        context: Context,
    ) -> Result<(), CoderError> {
        write_bytes(value, writer, context)
    }

    fn decode(&self, reader: &mut dyn Read, context: Context) -> Result<Vec<u8>, CoderError> {
        read_bytes(reader, context)
    }
}

impl DefaultCoder for Vec<u8> {
    type Coder = BytesCoder;
    fn coder() -> Self::Coder {
        BytesCoder
    }

    fn encode_element(&self, writer: &mut dyn Write) -> Result<(), CoderError> {
        BytesCoder.encode(self, writer, Context::Nested)
    }

    fn decode_element(reader: &mut dyn Read) -> Result<Self, CoderError> {
        BytesCoder.decode(reader, Context::Nested)
    }

    fn register_coder<R: CoderRegistry + ?Sized>(registry: &R) -> String {
        registry.register_coder(URN_BYTES, Vec::new())
    }
}

/// Coder for booleans, as one byte: 0 is false and 1 is true. Other values are an error.
#[derive(Clone, Debug, Default)]
pub struct BoolCoder;

impl Coder<bool> for BoolCoder {
    fn urn(&self) -> &'static str {
        URN_BOOL
    }

    fn encode(
        &self,
        value: &bool,
        writer: &mut dyn Write,
        _context: Context,
    ) -> Result<(), CoderError> {
        writer.write_all(&[u8::from(*value)])?;
        Ok(())
    }

    fn decode(&self, reader: &mut dyn Read, _context: Context) -> Result<bool, CoderError> {
        let [byte] = read_array(reader)?;
        match byte {
            0 => Ok(false),
            1 => Ok(true),
            other => Err(CoderError::Format(format!("Invalid boolean byte: {other}"))),
        }
    }
}

impl DefaultCoder for bool {
    type Coder = BoolCoder;
    fn coder() -> Self::Coder {
        BoolCoder
    }

    fn encode_element(&self, writer: &mut dyn Write) -> Result<(), CoderError> {
        BoolCoder.encode(self, writer, Context::WholeStream)
    }

    fn decode_element(reader: &mut dyn Read) -> Result<Self, CoderError> {
        BoolCoder.decode(reader, Context::WholeStream)
    }

    fn register_coder<R: CoderRegistry + ?Sized>(registry: &R) -> String {
        registry.register_coder(URN_BOOL, Vec::new())
    }
}

/// Coder for 64-bit IEEE 754 floating-point values, big-endian.
#[derive(Clone, Debug, Default)]
pub struct DoubleCoder;

impl Coder<f64> for DoubleCoder {
    fn urn(&self) -> &'static str {
        URN_DOUBLE
    }

    fn encode(
        &self,
        value: &f64,
        writer: &mut dyn Write,
        _context: Context,
    ) -> Result<(), CoderError> {
        writer.write_all(&value.to_be_bytes())?;
        Ok(())
    }

    fn decode(&self, reader: &mut dyn Read, _context: Context) -> Result<f64, CoderError> {
        Ok(f64::from_be_bytes(read_array(reader)?))
    }
}

impl DefaultCoder for f64 {
    type Coder = DoubleCoder;
    fn coder() -> Self::Coder {
        DoubleCoder
    }

    fn encode_element(&self, writer: &mut dyn Write) -> Result<(), CoderError> {
        DoubleCoder.encode(self, writer, Context::WholeStream)
    }

    fn decode_element(reader: &mut dyn Read) -> Result<Self, CoderError> {
        DoubleCoder.decode(reader, Context::WholeStream)
    }

    fn register_coder<R: CoderRegistry + ?Sized>(registry: &R) -> String {
        registry.register_coder(URN_DOUBLE, Vec::new())
    }
}

/// Coder for 64-bit varints. A negative value encodes as its two's-complement `u64` (10 bytes).
#[derive(Clone, Debug, Default)]
pub struct VarIntCoder;

impl VarIntCoder {
    pub fn encode_varint(value: i64, writer: &mut dyn Write) -> Result<(), std::io::Error> {
        let mut v = value as u64;
        loop {
            if (v & !0x7F) == 0 {
                writer.write_all(&[v as u8])?;
                return Ok(());
            }
            writer.write_all(&[((v & 0x7F) as u8) | 0x80])?;
            v >>= 7;
        }
    }

    /// Returns an error if the varint is longer than 64 bits.
    pub fn decode_varint(reader: &mut dyn Read) -> Result<i64, std::io::Error> {
        let mut result: u64 = 0;
        let mut shift = 0;
        let mut buf = [0u8; 1];
        loop {
            reader.read_exact(&mut buf)?;
            let byte = buf[0];
            let bits = (byte & 0x7F) as u64;
            if shift >= 64 || (shift == 63 && bits > 1) {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "varint too long or out of range",
                ));
            }
            result |= bits << shift;
            shift += 7;
            if (byte & 0x80) == 0 {
                break;
            }
        }
        Ok(result as i64)
    }
}

impl Coder<i64> for VarIntCoder {
    fn urn(&self) -> &'static str {
        URN_VARINT
    }

    fn encode(
        &self,
        value: &i64,
        writer: &mut dyn Write,
        _context: Context,
    ) -> Result<(), CoderError> {
        Self::encode_varint(*value, writer)?;
        Ok(())
    }

    fn decode(&self, reader: &mut dyn Read, _context: Context) -> Result<i64, CoderError> {
        let v = Self::decode_varint(reader)?;
        Ok(v)
    }
}

impl DefaultCoder for i64 {
    type Coder = VarIntCoder;
    fn coder() -> Self::Coder {
        VarIntCoder
    }

    fn encode_element(&self, writer: &mut dyn Write) -> Result<(), CoderError> {
        Coder::<i64>::encode(&VarIntCoder, self, writer, Context::WholeStream)
    }

    fn decode_element(reader: &mut dyn Read) -> Result<Self, CoderError> {
        Coder::<i64>::decode(&VarIntCoder, reader, Context::WholeStream)
    }

    fn register_coder<R: CoderRegistry + ?Sized>(registry: &R) -> String {
        registry.register_coder(URN_VARINT, Vec::new())
    }
}

impl Coder<i32> for VarIntCoder {
    fn urn(&self) -> &'static str {
        URN_VARINT
    }

    fn encode(
        &self,
        value: &i32,
        writer: &mut dyn Write,
        _context: Context,
    ) -> Result<(), CoderError> {
        Self::encode_varint(*value as i64, writer)?;
        Ok(())
    }

    fn decode(&self, reader: &mut dyn Read, _context: Context) -> Result<i32, CoderError> {
        let v = Self::decode_varint(reader)?;
        Ok(v as i32)
    }
}

impl DefaultCoder for i32 {
    type Coder = VarIntCoder;
    fn coder() -> Self::Coder {
        VarIntCoder
    }

    fn encode_element(&self, writer: &mut dyn Write) -> Result<(), CoderError> {
        Coder::<i32>::encode(&VarIntCoder, self, writer, Context::WholeStream)
    }

    fn decode_element(reader: &mut dyn Read) -> Result<Self, CoderError> {
        Coder::<i32>::decode(&VarIntCoder, reader, Context::WholeStream)
    }

    fn register_coder<R: CoderRegistry + ?Sized>(registry: &R) -> String {
        registry.register_coder(URN_VARINT, Vec::new())
    }
}

/// Coder for the unit type `()`. It uses the bytes coder URN and encodes an empty payload.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct UnitCoder;

impl Coder<()> for UnitCoder {
    fn urn(&self) -> &'static str {
        URN_BYTES
    }

    fn encode(
        &self,
        _value: &(),
        writer: &mut dyn Write,
        context: Context,
    ) -> Result<(), CoderError> {
        write_bytes(&[], writer, context)
    }

    fn decode(&self, reader: &mut dyn Read, context: Context) -> Result<(), CoderError> {
        let _ = read_bytes(reader, context)?;
        Ok(())
    }
}

impl DefaultCoder for () {
    type Coder = UnitCoder;

    fn coder() -> Self::Coder {
        UnitCoder
    }

    fn encode_element(&self, writer: &mut dyn Write) -> Result<(), CoderError> {
        UnitCoder.encode(self, writer, Context::Nested)
    }

    fn decode_element(reader: &mut dyn Read) -> Result<Self, CoderError> {
        UnitCoder.decode(reader, Context::Nested)
    }

    fn register_coder<R: CoderRegistry + ?Sized>(registry: &R) -> String {
        registry.register_coder(URN_BYTES, Vec::new())
    }
}
