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

use std::collections::HashMap;
use std::io::Cursor;

use model::pipeline::Coder as ProtoCoder;

use super::pane::PaneInfo;
use super::standard::{VarIntCoder, read_array, read_be_i32};

use super::{
    URN_BOOL, URN_BYTES, URN_DOUBLE, URN_GLOBAL_WINDOW, URN_INTERVAL_WINDOW, URN_ITERABLE, URN_KV,
    URN_LENGTH_PREFIX, URN_NULLABLE, URN_PARAM_WINDOWED_VALUE, URN_ROW, URN_STATE_BACKED_ITERABLE,
    URN_STRING_UTF8, URN_TIMER, URN_VARINT, URN_WINDOWED_VALUE,
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
pub fn read_length_prefixed_slice<'a>(
    cursor: &mut Cursor<&'a [u8]>,
) -> Result<&'a [u8], std::io::Error> {
    let data: &'a [u8] = cursor.get_ref();
    let len = VarIntCoder::decode_varint(cursor)? as usize;
    let start = cursor.position() as usize;
    let end = start
        .checked_add(len)
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidData, "Length overflow"))?;
    if end > data.len() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "failed to fill whole buffer",
        ));
    }
    cursor.set_position(end as u64);
    Ok(&data[start..end])
}

/// Moves `cursor` past a varint length prefix and the bytes that it delimits.
pub fn skip_length_prefixed(cursor: &mut Cursor<&[u8]>) -> Result<(), std::io::Error> {
    read_length_prefixed_slice(cursor).map(|_| ())
}

/// Moves `cursor` past an encoded `PaneInfo` and the element metadata after it, if any.
pub fn skip_pane_info(cursor: &mut Cursor<&[u8]>) -> Result<(), std::io::Error> {
    let (_, element_metadata) = PaneInfo::decode(cursor)?;
    if element_metadata {
        skip_length_prefixed(cursor)?;
    }
    Ok(())
}

fn skip_to_end(cursor: &mut Cursor<&[u8]>) {
    let end = cursor.get_ref().len() as u64;
    cursor.set_position(end);
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
fn read_window_count(cursor: &mut Cursor<&[u8]>) -> Result<i32, std::io::Error> {
    let count = read_be_i32(cursor)?;
    if count < 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("Negative window count: {count}"),
        ));
    }
    Ok(count)
}

/// Skips an iterable body, with a count prefix or with varint-counted chunks.
fn skip_iterable_elements(
    cursor: &mut Cursor<&[u8]>,
    elem_coder_id: &str,
    coders: &HashMap<String, ProtoCoder>,
    depth: usize,
) -> Result<(), std::io::Error> {
    let count = read_be_i32(cursor)?;
    if count >= 0 {
        return (0..count)
            .try_for_each(|_| skip_value(cursor, elem_coder_id, coders, true, depth + 1));
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
                let token_len = token_len as usize;
                let cur_pos = cursor.position() as usize;
                let data_len = cursor.get_ref().len();
                // A huge `token_len` would wrap in release builds and pass the bounds check.
                let token_end = cur_pos
                    .checked_add(token_len)
                    .filter(|end| *end <= data_len)
                    .ok_or_else(|| {
                        std::io::Error::new(
                            std::io::ErrorKind::UnexpectedEof,
                            "Truncated continuation token",
                        )
                    })?;
                cursor.set_position(token_end as u64);
                return Ok(());
            }
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Invalid iterable chunk header: {chunk_len}"),
            ));
        }
        (0..chunk_len)
            .try_for_each(|_| skip_value(cursor, elem_coder_id, coders, true, depth + 1))?;
    }
}

/// Moves `cursor` past one value encoded with `coder_id`. An unknown coder ID or URN, or a Row
/// coder without a schema, skips to the end of the slice: the runner sends such a coder only
/// behind a length prefix.
pub fn skip_coder_value(
    cursor: &mut Cursor<&[u8]>,
    coder_id: &str,
    coders: &HashMap<String, ProtoCoder>,
    nested: bool,
) -> Result<(), std::io::Error> {
    skip_value(cursor, coder_id, coders, nested, 0)
}

fn skip_value(
    cursor: &mut Cursor<&[u8]>,
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
            if let [key_id, value_id, ..] = coder.component_coder_ids.as_slice() {
                skip_value(cursor, key_id, coders, true, depth + 1)?;
                skip_value(cursor, value_id, coders, nested, depth + 1)?;
            }
            Ok(())
        }
        URN_ITERABLE | URN_STATE_BACKED_ITERABLE => {
            skip_iterable_elements(cursor, component_coder_id(coder, 0), coders, depth)
        }
        URN_NULLABLE => {
            let [tag] = read_array(cursor)?;
            let inner_id = coder.component_coder_ids.first().filter(|_| tag != 0);
            match inner_id {
                Some(inner_id) => skip_value(cursor, inner_id, coders, nested, depth + 1),
                None => Ok(()),
            }
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
                read_array::<1>(cursor)?;
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
            let schema = crate::schema::Schema::from_proto_bytes(schema_bytes)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
            let schema_arc = std::sync::Arc::new(schema);
            super::RowCoder::decode_row(&schema_arc, cursor)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
            Ok(())
        }
        _ => {
            skip_to_end(cursor);
            Ok(())
        }
    }
}

/// Copies the key out of an encoded `KV` element. `coder_id` can be a windowed value coder.
pub fn extract_kv_key_bytes(
    element: &[u8],
    coder_id: &str,
    coders: &HashMap<String, ProtoCoder>,
) -> Option<Vec<u8>> {
    kv_key_slice(element, coder_id, coders).map(<[u8]>::to_vec)
}

/// Borrows the encoded key out of `element`. [`extract_kv_key_bytes`] copies it.
pub fn kv_key_slice<'e>(
    element: &'e [u8],
    coder_id: &str,
    coders: &HashMap<String, ProtoCoder>,
) -> Option<&'e [u8]> {
    key_slice(element, kv_key_coder_id(coder_id, coders)?, coders)
}

/// Returns the key coder ID of a `KV` coder, also inside a windowed value coder, or `None`.
/// Resolve it once per collection, then call [`key_slice`] for each element.
pub fn kv_key_coder_id<'c>(
    coder_id: &str,
    coders: &'c HashMap<String, ProtoCoder>,
) -> Option<&'c str> {
    let coder = coders.get(coder_id)?;
    let kv_coder = match coder_urn(coder) {
        URN_KV => Some(coder),
        URN_WINDOWED_VALUE | URN_PARAM_WINDOWED_VALUE => coder
            .component_coder_ids
            .first()
            .and_then(|inner_id| coders.get(inner_id))
            .filter(|inner| coder_urn(inner) == URN_KV),
        _ => None,
    }?;
    kv_coder.component_coder_ids.first().map(String::as_str)
}

/// Borrows the leading key out of an encoded KV `element`. See [`kv_key_coder_id`].
pub fn key_slice<'e>(
    element: &'e [u8],
    key_coder_id: &str,
    coders: &HashMap<String, ProtoCoder>,
) -> Option<&'e [u8]> {
    if element.is_empty() {
        return None;
    }
    let mut cursor = Cursor::new(element);
    skip_coder_value(&mut cursor, key_coder_id, coders, true).ok()?;
    let key_len = cursor.position() as usize;
    element.get(..key_len)
}

/// Returns the Row [`Schema`](crate::schema::Schema) of a Row coder, also inside a length
/// prefix, windowed value or nullable coder. Returns `None` if there is no valid Row schema.
pub fn extract_row_schema(
    coder_id: &str,
    coders: &HashMap<String, ProtoCoder>,
) -> Option<std::sync::Arc<crate::schema::Schema>> {
    let mut current_id = coder_id;
    let mut visited = std::collections::HashSet::new();
    loop {
        if !visited.insert(current_id) {
            return None;
        }
        let coder = coders.get(current_id)?;
        let urn = coder_urn(coder);
        match urn {
            URN_ROW => {
                let payload = coder
                    .spec
                    .as_ref()
                    .map(|s| s.payload.as_slice())
                    .unwrap_or(&[]);
                if payload.is_empty() {
                    return None;
                }
                let schema = crate::schema::Schema::from_proto_bytes(payload).ok()?;
                return Some(std::sync::Arc::new(schema));
            }
            URN_WINDOWED_VALUE | URN_PARAM_WINDOWED_VALUE | URN_LENGTH_PREFIX | URN_NULLABLE => {
                current_id = coder.component_coder_ids.first()?.as_str();
            }
            _ => return None,
        }
    }
}
