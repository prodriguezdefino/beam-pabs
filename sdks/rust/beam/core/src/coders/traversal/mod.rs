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

//! Walks and skips encoded values of the Beam standard coders, driven by coder protos.

mod keys;
mod plan;

pub use keys::{
    extract_kv_key_bytes, extract_row_schema, key_slice, kv_key_coder_id, kv_key_slice,
};
pub use plan::SkipPlan;

use std::collections::HashMap;

use beam_model::pipeline::Coder as ProtoCoder;

use crate::schema::Schema;

use super::pane::PaneInfo;
use super::standard::{VarIntCoder, read_array, read_be_i32, take_bytes};

use super::{
    RowCoder, URN_BOOL, URN_BYTES, URN_DOUBLE, URN_GLOBAL_WINDOW, URN_INTERVAL_WINDOW,
    URN_ITERABLE, URN_KV, URN_LENGTH_PREFIX, URN_NULLABLE, URN_PARAM_WINDOWED_VALUE, URN_ROW,
    URN_SHARDED_KEY, URN_STATE_BACKED_ITERABLE, URN_STRING_UTF8, URN_TIMER, URN_VARINT,
    URN_WINDOWED_VALUE,
};

/// Returns the URN of a coder, or `""` (an unknown URN) if it has no spec.
pub fn coder_urn(coder: &ProtoCoder) -> &str {
    coder.spec.as_ref().map_or("", |s| s.urn.as_str())
}

/// Returns the component of a length prefix coder, or `None` for other coders or a missing one.
pub fn length_prefix_component<'a>(
    coder: &ProtoCoder,
    coders: &'a HashMap<String, ProtoCoder>,
) -> Option<&'a ProtoCoder> {
    (coder_urn(coder) == URN_LENGTH_PREFIX)
        .then(|| coder.component_coder_ids.first())
        .flatten()
        .and_then(|id| coders.get(id))
}

/// Returns the coder under all the length prefixes around `coder` that resolve in `coders`.
pub fn peel_length_prefixes<'a>(
    coder: &'a ProtoCoder,
    coders: &'a HashMap<String, ProtoCoder>,
) -> &'a ProtoCoder {
    std::iter::successors(Some(coder), |c| length_prefix_component(c, coders))
        .last()
        .unwrap_or(coder)
}

/// Reads a varint length prefix and returns the bytes that it delimits. `cursor` ends after them.
pub fn read_length_prefixed_slice<'a>(cursor: &mut &'a [u8]) -> Result<&'a [u8], std::io::Error> {
    let len = VarIntCoder::decode_varint(cursor)?;
    let len = usize::try_from(len).map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("Negative length: {len}"),
        )
    })?;
    take_bytes(cursor, len)
}

/// Moves `cursor` past a varint length prefix and the bytes that it delimits.
pub fn skip_length_prefixed(cursor: &mut &[u8]) -> Result<(), std::io::Error> {
    read_length_prefixed_slice(cursor).map(|_| ())
}

/// Moves `cursor` past an encoded `PaneInfo` and the element metadata after it, if any.
pub fn skip_pane_info(cursor: &mut &[u8]) -> Result<(), std::io::Error> {
    let (_, element_metadata) = PaneInfo::decode(cursor)?;
    if element_metadata {
        skip_length_prefixed(cursor)?;
    }
    Ok(())
}

fn skip_to_end(cursor: &mut &[u8]) {
    *cursor = &[];
}

/// Returns the component coder ID at `index`, or `""` if there is none.
fn component_coder_id(coder: &ProtoCoder, index: usize) -> &str {
    coder
        .component_coder_ids
        .get(index)
        .map_or("", String::as_str)
}

/// Deepest coder nesting that [`skip_coder_value`] follows. A runner coder graph can have a
/// cycle, and unbounded recursion would overflow the stack and abort the whole harness.
const MAX_SKIP_DEPTH: usize = 64;

/// Reads the window count of a windowed value or timer. A negative count is an error: it would
/// skip zero windows and leave the cursor on the wrong byte.
fn read_window_count(cursor: &mut &[u8]) -> Result<i32, std::io::Error> {
    let count = read_be_i32(cursor)?;
    if count < 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("Negative window count: {count}"),
        ));
    }
    Ok(count)
}

/// Skips an iterable body, with a count prefix or with varint-counted chunks. `skip_element`
/// skips one element.
fn skip_iterable_elements(
    cursor: &mut &[u8],
    mut skip_element: impl FnMut(&mut &[u8]) -> Result<(), std::io::Error>,
) -> Result<(), std::io::Error> {
    let count = read_be_i32(cursor)?;
    if count >= 0 {
        return (0..count).try_for_each(|_| skip_element(cursor));
    }
    if count != -1 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("Invalid iterable count: {count}"),
        ));
    }
    // Count `-1` starts the chunked encoding: varint chunk sizes, and a 0 chunk ends it.
    loop {
        let chunk_len = VarIntCoder::decode_varint(cursor)?;
        if chunk_len == 0 {
            return Ok(());
        }
        if chunk_len < 0 {
            if chunk_len == -1 {
                let token_len = VarIntCoder::decode_varint(cursor)?;
                if token_len < 0 {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("Invalid continuation token length: {token_len}"),
                    ));
                }
                return take_bytes(cursor, token_len as usize)
                    .map(|_| ())
                    .map_err(|_| {
                        std::io::Error::new(
                            std::io::ErrorKind::UnexpectedEof,
                            "Truncated continuation token",
                        )
                    });
            }
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Invalid iterable chunk header: {chunk_len}"),
            ));
        }
        (0..chunk_len).try_for_each(|_| skip_element(cursor))?;
    }
}

/// Moves `cursor` past one value encoded with `coder_id`. An unknown coder ID or URN, or a Row
/// coder without a schema, skips to the end of the slice: the runner sends such a coder only
/// behind a length prefix.
pub fn skip_coder_value(
    cursor: &mut &[u8],
    coder_id: &str,
    coders: &HashMap<String, ProtoCoder>,
    nested: bool,
) -> Result<(), std::io::Error> {
    skip_value(cursor, coder_id, coders, nested, 0)
}

/// Returns `true` if [`skip_coder_value`] finds the end of a nested value of `coder_id`
/// exactly. It is `false` for a coder that skips to the end of the slice: an unknown URN, a
/// missing coder, or a Row coder without a schema.
pub fn skips_exactly(coder_id: &str, coders: &HashMap<String, ProtoCoder>) -> bool {
    skips_exactly_at(coder_id, coders, 0)
}

fn skips_exactly_at(coder_id: &str, coders: &HashMap<String, ProtoCoder>, depth: usize) -> bool {
    let Some(coder) = coders.get(coder_id).filter(|_| depth <= MAX_SKIP_DEPTH) else {
        return false;
    };
    let components = || {
        coder
            .component_coder_ids
            .iter()
            .all(|id| skips_exactly_at(id, coders, depth + 1))
    };
    match coder_urn(coder) {
        URN_VARINT | URN_DOUBLE | URN_BOOL | URN_STRING_UTF8 | URN_BYTES | URN_LENGTH_PREFIX
        | URN_GLOBAL_WINDOW | URN_INTERVAL_WINDOW => true,
        URN_KV | URN_ITERABLE | URN_STATE_BACKED_ITERABLE | URN_NULLABLE | URN_SHARDED_KEY => {
            components()
        }
        URN_ROW => coder.spec.as_ref().is_some_and(|s| !s.payload.is_empty()),
        _ => false,
    }
}

fn skip_value(
    cursor: &mut &[u8],
    coder_id: &str,
    coders: &HashMap<String, ProtoCoder>,
    nested: bool,
    depth: usize,
) -> Result<(), std::io::Error> {
    if depth > MAX_SKIP_DEPTH {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("coder '{coder_id}' nests deeper than {MAX_SKIP_DEPTH} levels"),
        ));
    }
    let Some(coder) = coders.get(coder_id) else {
        skip_to_end(cursor);
        return Ok(());
    };

    let urn = coder_urn(coder);
    match urn {
        URN_VARINT => {
            VarIntCoder::decode_varint(cursor)?;
            Ok(())
        }
        URN_DOUBLE => {
            read_array::<8>(cursor)?;
            Ok(())
        }
        URN_BOOL => {
            read_array::<1>(cursor)?;
            Ok(())
        }
        URN_STRING_UTF8 | URN_BYTES => {
            if nested {
                skip_length_prefixed(cursor)
            } else {
                skip_to_end(cursor);
                Ok(())
            }
        }
        URN_LENGTH_PREFIX => skip_length_prefixed(cursor),
        URN_KV => {
            let [key_id, value_id, ..] = coder.component_coder_ids.as_slice() else {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("KV coder '{coder_id}' has fewer than 2 components"),
                ));
            };
            skip_value(cursor, key_id, coders, true, depth + 1)?;
            skip_value(cursor, value_id, coders, nested, depth + 1)
        }
        URN_ITERABLE | URN_STATE_BACKED_ITERABLE => {
            let elem_coder_id = component_coder_id(coder, 0);
            skip_iterable_elements(cursor, |cursor| {
                skip_value(cursor, elem_coder_id, coders, true, depth + 1)
            })
        }
        URN_NULLABLE => {
            let [tag] = read_array(cursor)?;
            let inner_id = coder.component_coder_ids.first().filter(|_| tag != 0);
            match inner_id {
                Some(inner_id) => skip_value(cursor, inner_id, coders, nested, depth + 1),
                None => Ok(()),
            }
        }
        URN_SHARDED_KEY => {
            let [key_id, ..] = coder.component_coder_ids.as_slice() else {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("sharded key coder '{coder_id}' has no key component"),
                ));
            };
            skip_length_prefixed(cursor)?;
            skip_value(cursor, key_id, coders, true, depth + 1)
        }
        URN_GLOBAL_WINDOW => Ok(()),
        URN_INTERVAL_WINDOW => {
            read_array::<8>(cursor)?;
            VarIntCoder::decode_varint(cursor)?;
            Ok(())
        }
        URN_WINDOWED_VALUE => {
            read_array::<8>(cursor)?; // timestamp
            let win_count = read_window_count(cursor)?;
            let win_coder_id = component_coder_id(coder, 1);
            (0..win_count)
                .try_for_each(|_| skip_value(cursor, win_coder_id, coders, true, depth + 1))?;
            skip_pane_info(cursor)?;
            skip_value(
                cursor,
                component_coder_id(coder, 0),
                coders,
                nested,
                depth + 1,
            )
        }
        // A param windowed value has only the inner element on the wire.
        URN_PARAM_WINDOWED_VALUE => skip_value(
            cursor,
            component_coder_id(coder, 0),
            coders,
            nested,
            depth + 1,
        ),
        URN_TIMER => {
            skip_value(
                cursor,
                component_coder_id(coder, 0),
                coders,
                true,
                depth + 1,
            )?;
            skip_length_prefixed(cursor)?;
            let win_count = read_window_count(cursor)?;
            let win_coder_id = component_coder_id(coder, 1);
            (0..win_count)
                .try_for_each(|_| skip_value(cursor, win_coder_id, coders, true, depth + 1))?;
            let [clear] = read_array::<1>(cursor)?;
            if clear == 0 {
                read_array::<8>(cursor)?;
                read_array::<8>(cursor)?;
                skip_pane_info(cursor)?;
            }
            Ok(())
        }
        URN_ROW => {
            let schema_bytes = coder
                .spec
                .as_ref()
                .map(|s| s.payload.as_slice())
                .unwrap_or(&[]);
            if schema_bytes.is_empty() {
                skip_to_end(cursor);
                return Ok(());
            }
            // A caller on a hot path parses the schema once and calls `RowCoder::skip_row`.
            let schema = Schema::from_proto_bytes(schema_bytes)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
            RowCoder::skip_row(&schema, cursor)
        }
        _ => {
            skip_to_end(cursor);
            Ok(())
        }
    }
}
