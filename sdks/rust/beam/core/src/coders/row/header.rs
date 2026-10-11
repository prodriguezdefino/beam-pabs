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

//! The `beam:coder:row:v1` header: the field count and the null bitmask. [`RowCoder`] and
//! the code that `#[derive(BeamRow)]` generates use these functions, so both write the same
//! bytes.
//!
//! [`RowCoder`]: super::RowCoder

use crate::coders::standard::{VarIntCoder, read_varint_len, take_bytes};
use crate::coders::traits::CoderError;

/// Writes the field count and the null bitmask. `is_null` gives the flag at a wire position.
/// With no null field, the bitmask is empty.
pub fn write_row_header(num_fields: usize, is_null: impl Fn(usize) -> bool, writer: &mut Vec<u8>) {
    VarIntCoder::encode_varint_to_vec(num_fields as i64, writer);
    if (0..num_fields).any(&is_null) {
        let bitmask_bytes = num_fields.div_ceil(8);
        VarIntCoder::encode_varint_to_vec(bitmask_bytes as i64, writer);
        let start = writer.len();
        writer.resize(start + bitmask_bytes, 0);
        (0..num_fields)
            .filter(|&position| is_null(position))
            .for_each(|position| writer[start + position / 8] |= 1 << (position % 8));
    } else {
        writer.push(0);
    }
}

/// Reads the field count and borrows the null bitmask. Returns an error if the field count
/// is not `expected_fields`.
pub fn read_row_header<'a>(
    expected_fields: usize,
    reader: &mut &'a [u8],
) -> Result<&'a [u8], CoderError> {
    let num_fields = read_varint_len(reader)?;
    if num_fields != expected_fields {
        return Err(CoderError::Format(format!(
            "Row field count mismatch: expected {expected_fields}, got {num_fields}"
        )));
    }
    let bitmask_len = read_varint_len(reader)?;
    Ok(take_bytes(reader, bitmask_len)?)
}

/// Returns true when wire `position` is marked null in `null_bits`. A bitmask shorter than
/// the field count marks the missing positions as not null.
#[inline]
pub fn bit_is_null(null_bits: &[u8], position: usize) -> bool {
    null_bits
        .get(position / 8)
        .is_some_and(|byte| byte & (1 << (position % 8)) != 0)
}
