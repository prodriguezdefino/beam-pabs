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

//! The runner framing of state and side input values.

use std::borrow::Cow;
use std::collections::HashMap;
use std::ops::Range;
use std::sync::Arc;

use beam::coders::{
    SkipPlan, URN_LENGTH_PREFIX, VarIntCoder, add_nested_row_length_prefixes, coder_urn,
    has_nested_row_length_prefix, strip_nested_row_length_prefixes, take_bytes,
};
use beam_model::pipeline::Coder;

/// How the values of one runner coder are delimited in concatenated bytes.
enum Delimiter {
    /// Each value is a varint length followed by that many bytes.
    LengthPrefix,
    /// Each value is one value of the coder, read to find its end.
    Coder { coder_id: String, plan: SkipPlan },
}

/// A coder with runner length prefixes around nested rows, and the coders to read it.
struct NestedRows {
    coder_id: String,
    coders: Arc<HashMap<String, Coder>>,
}

/// The framing that a runner coder adds to the SDK encoding of a value: an outer length
/// prefix, length prefixes around nested rows, or both. A descriptor resolves it once for
/// each coder, so a value needs no coder lookup.
pub(crate) struct RunnerFraming {
    delimiter: Delimiter,
    nested_rows: Option<NestedRows>,
}

impl RunnerFraming {
    /// The framing of runner coder `coder_id`.
    pub(crate) fn new(coder_id: &str, coders: &Arc<HashMap<String, Coder>>) -> Self {
        let (delimiter, inner_id) = match coders
            .get(coder_id)
            .filter(|c| coder_urn(c) == URN_LENGTH_PREFIX)
        {
            Some(c) => (
                Delimiter::LengthPrefix,
                c.component_coder_ids.first().map_or("", String::as_str),
            ),
            None => (
                Delimiter::Coder {
                    coder_id: coder_id.to_string(),
                    plan: SkipPlan::new(coder_id, coders),
                },
                coder_id,
            ),
        };
        let nested_rows = has_nested_row_length_prefix(inner_id, coders).then(|| NestedRows {
            coder_id: inner_id.to_string(),
            coders: Arc::clone(coders),
        });
        Self {
            delimiter,
            nested_rows,
        }
    }

    /// The runner encoding of the SDK-encoded `value`. Returns `value` with no copy when the
    /// runner adds no framing.
    pub(crate) fn frame<'a>(&self, value: Cow<'a, [u8]>) -> Result<Cow<'a, [u8]>, String> {
        let payload = self.frame_rows(value)?;
        match self.delimiter {
            Delimiter::LengthPrefix => {
                let mut framed = Vec::with_capacity(payload.len() + 10);
                VarIntCoder::encode_varint_to_vec(payload.len() as i64, &mut framed);
                framed.extend_from_slice(&payload);
                Ok(Cow::Owned(framed))
            }
            Delimiter::Coder { .. } => Ok(payload),
        }
    }

    /// [`Self::frame`] without the outer length prefix. Use it where the protocol gives the
    /// length of each value, as an ordered list entry does.
    pub(crate) fn frame_rows<'a>(&self, value: Cow<'a, [u8]>) -> Result<Cow<'a, [u8]>, String> {
        match &self.nested_rows {
            Some(NestedRows { coder_id, coders }) => {
                add_nested_row_length_prefixes(&value, coder_id, coders)
                    .map(Cow::Owned)
                    .map_err(|e| format!("Failed to add the nested row prefixes: {e}"))
            }
            None => Ok(value),
        }
    }

    /// The SDK encoding of one runner `value` without its outer length prefix. Returns
    /// `value` with no copy when it has no nested row prefixes.
    pub(crate) fn unframe_rows<T: AsRef<[u8]> + From<Vec<u8>>>(
        &self,
        value: T,
    ) -> Result<T, String> {
        match &self.nested_rows {
            Some(NestedRows { coder_id, coders }) => {
                strip_nested_row_length_prefixes(value.as_ref(), coder_id, coders)
                    .map(T::from)
                    .map_err(|e| format!("Failed to strip the nested row prefixes: {e}"))
            }
            None => Ok(value),
        }
    }

    /// The first value of `data`: the range of its payload, and the number of bytes it uses.
    /// An error means that `data` does not hold a complete value.
    pub(crate) fn first_value(&self, data: &[u8]) -> Result<(Range<usize>, usize), String> {
        let mut cursor = data;
        let payload = self.take_value(&mut cursor)?.len();
        let used = data.len() - cursor.len();
        Ok((used - payload..used, used))
    }

    /// Splits concatenated runner `data` into SDK-encoded values.
    pub(crate) fn split(&self, data: &[u8]) -> Result<Vec<Vec<u8>>, String> {
        let values = match &self.delimiter {
            Delimiter::LengthPrefix => split_length_prefixed(data),
            Delimiter::Coder { coder_id, plan } => split_with_plan(data, coder_id, plan),
        };
        match &self.nested_rows {
            Some(_) => values?
                .into_iter()
                .map(|value| self.unframe_rows(value))
                .collect(),
            None => values,
        }
    }

    /// Takes the payload of the first value from `cursor`, without its outer length prefix.
    fn take_value<'a>(&self, cursor: &mut &'a [u8]) -> Result<&'a [u8], String> {
        match &self.delimiter {
            Delimiter::LengthPrefix => {
                let len = VarIntCoder::decode_varint(cursor)
                    .map_err(|e| format!("Failed to read length prefix: {e}"))?;
                let left = cursor.len();
                usize::try_from(len)
                    .ok()
                    .and_then(|len| take_bytes(cursor, len).ok())
                    .ok_or_else(|| {
                        format!("Length prefix {len} exceeds remaining data length {left}")
                    })
            }
            Delimiter::Coder { coder_id, plan } => {
                let start = *cursor;
                plan.skip(cursor, true)
                    .map_err(|e| format!("Failed to frame value with coder '{coder_id}': {e}"))?;
                Ok(&start[..start.len() - cursor.len()])
            }
        }
    }
}

/// Splits `data` into the payloads of varint length-prefixed values.
fn split_length_prefixed(data: &[u8]) -> Result<Vec<Vec<u8>>, String> {
    let mut cursor = data;
    std::iter::from_fn(|| {
        (!cursor.is_empty()).then(|| {
            let len = VarIntCoder::decode_varint(&mut cursor)
                .map_err(|e| format!("Failed to read length prefix: {e}"))?;
            let left = cursor.len();
            usize::try_from(len)
                .ok()
                .and_then(|len| take_bytes(&mut cursor, len).ok())
                .map(<[u8]>::to_vec)
                .ok_or_else(|| format!("Length prefix {len} exceeds remaining data length {left}"))
        })
    })
    .collect()
}

/// Splits `data` into values of coder `coder_id`, which `plan` skips.
fn split_with_plan(data: &[u8], coder_id: &str, plan: &SkipPlan) -> Result<Vec<Vec<u8>>, String> {
    let mut cursor = data;
    std::iter::from_fn(|| {
        let start = data.len() - cursor.len();
        (start < data.len()).then(|| {
            plan.skip(&mut cursor, true)
                .map_err(|e| format!("Failed to frame value with coder '{coder_id}': {e}"))
                .map(|()| data[start..data.len() - cursor.len()].to_vec())
        })
    })
    .collect()
}
