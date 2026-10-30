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

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use model::pipeline::Coder as ProtoCoder;

use crate::schema::Schema;

use super::pane::PaneInfo;
use super::standard::{VarIntCoder, consumed, read_array, read_be_i32, take_bytes};

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
    let mut cursor = element;
    skip_coder_value(&mut cursor, key_coder_id, coders, true).ok()?;
    Some(consumed(element, cursor))
}

/// Returns the Row [`Schema`] of the rows in an element of `coder_id`.
///
/// Looks through length prefix, windowed value, nullable, iterable and key-value coders. The
/// harness gives this one schema to every row in the element. Returns `None` if the coder has
/// no Row coder with a valid schema, or Row coders with different schemas.
pub fn extract_row_schema(
    coder_id: &str,
    coders: &HashMap<String, ProtoCoder>,
) -> Option<Arc<Schema>> {
    let mut payloads = Vec::new();
    collect_row_payloads(coder_id, coders, &mut HashSet::new(), &mut payloads);
    match payloads.as_slice() {
        [payload] => Schema::from_proto_bytes(payload).ok().map(Arc::new),
        _ => None,
    }
}

/// Adds the distinct schema payloads of the Row coders under `coder_id` to `payloads`. Visits
/// each coder once, so a cyclic coder graph ends.
fn collect_row_payloads<'a>(
    coder_id: &'a str,
    coders: &'a HashMap<String, ProtoCoder>,
    visited: &mut HashSet<&'a str>,
    payloads: &mut Vec<&'a [u8]>,
) {
    if !visited.insert(coder_id) {
        return;
    }
    let Some(coder) = coders.get(coder_id) else {
        return;
    };
    match coder_urn(coder) {
        URN_ROW => {
            let payload = coder
                .spec
                .as_ref()
                .map_or(&[][..], |s| s.payload.as_slice());
            // A Row coder with no payload has no schema to give.
            if !payload.is_empty() && !payloads.contains(&payload) {
                payloads.push(payload);
            }
        }
        URN_WINDOWED_VALUE
        | URN_PARAM_WINDOWED_VALUE
        | URN_LENGTH_PREFIX
        | URN_NULLABLE
        | URN_SHARDED_KEY
        | URN_ITERABLE
        | URN_STATE_BACKED_ITERABLE
        | URN_KV => coder
            .component_coder_ids
            .iter()
            .for_each(|id| collect_row_payloads(id, coders, visited, payloads)),
        _ => {}
    }
}

/// A coder compiled for skipping encoded values.
///
/// [`skip_coder_value`] looks up the coder and compares its URN for every value. A plan does
/// that once, so a hot path builds the plan once per coder and calls [`skip`](Self::skip) for
/// each value. Both find the same end of each value and fail on the same input.
#[derive(Clone, Debug)]
pub struct SkipPlan(Arc<SkipNode>);

#[derive(Debug)]
enum SkipNode {
    VarInt,
    Double,
    Bool,
    /// A string or bytes value: length-prefixed when nested, else the rest of the input.
    Bytes,
    LengthPrefix,
    GlobalWindow,
    IntervalWindow,
    Kv(Arc<SkipNode>, Arc<SkipNode>),
    Iterable(Arc<SkipNode>),
    /// A nullable value. `None` when the coder has no component: a present value is empty.
    Nullable(Option<Arc<SkipNode>>),
    /// A nested shard id, then the key.
    ShardedKey(Arc<SkipNode>),
    WindowedValue {
        window: Arc<SkipNode>,
        value: Arc<SkipNode>,
    },
    Timer {
        key: Arc<SkipNode>,
        window: Arc<SkipNode>,
    },
    Row(Arc<Schema>),
    /// An unknown URN, a missing coder or a Row coder without a schema: the value takes the
    /// rest of the input.
    ToEnd,
    /// A coder that [`skip_coder_value`] rejects when it reaches it. The plan fails at the
    /// same value, not when it is built.
    Invalid(Arc<str>),
}

impl SkipPlan {
    /// Compiles `coder_id`. A coder that occurs more than once at the same depth shares one
    /// plan, so a cycle unrolls to the depth limit with at most one node per coder and depth.
    pub fn new(coder_id: &str, coders: &HashMap<String, ProtoCoder>) -> Self {
        let mut built = HashMap::new();
        Self(compile(coder_id, coders, 0, &mut built))
    }

    /// Moves `cursor` past one value. `nested` has the meaning of [`skip_coder_value`].
    pub fn skip(&self, cursor: &mut &[u8], nested: bool) -> Result<(), std::io::Error> {
        skip_node(&self.0, cursor, nested)
    }

    /// Borrows the leading key out of an encoded KV `element`, with this plan as the key
    /// coder. The same as [`key_slice`].
    pub fn key_slice<'e>(&self, element: &'e [u8]) -> Option<&'e [u8]> {
        if element.is_empty() {
            return None;
        }
        let mut cursor = element;
        self.skip(&mut cursor, true).ok()?;
        Some(consumed(element, cursor))
    }
}

fn invalid_data(message: &str) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, message.to_string())
}

/// Compiles one coder at `depth`. `built` holds the nodes compiled so far, by coder ID and
/// depth. The depth is part of the key because the depth limit makes the same coder end at a
/// different level of a cycle.
fn compile<'a>(
    coder_id: &'a str,
    coders: &'a HashMap<String, ProtoCoder>,
    depth: usize,
    built: &mut HashMap<(&'a str, usize), Arc<SkipNode>>,
) -> Arc<SkipNode> {
    if depth > MAX_SKIP_DEPTH {
        return Arc::new(SkipNode::Invalid(Arc::from(format!(
            "coder '{coder_id}' nests deeper than {MAX_SKIP_DEPTH} levels"
        ))));
    }
    if let Some(node) = built.get(&(coder_id, depth)) {
        return Arc::clone(node);
    }
    let Some(coder) = coders.get(coder_id) else {
        return Arc::new(SkipNode::ToEnd);
    };
    let component = |index: usize, built: &mut HashMap<(&'a str, usize), Arc<SkipNode>>| {
        compile(component_coder_id(coder, index), coders, depth + 1, built)
    };
    let node = match coder_urn(coder) {
        URN_VARINT => SkipNode::VarInt,
        URN_DOUBLE => SkipNode::Double,
        URN_BOOL => SkipNode::Bool,
        URN_STRING_UTF8 | URN_BYTES => SkipNode::Bytes,
        URN_LENGTH_PREFIX => SkipNode::LengthPrefix,
        URN_KV if coder.component_coder_ids.len() < 2 => SkipNode::Invalid(Arc::from(format!(
            "KV coder '{coder_id}' has fewer than 2 components"
        ))),
        URN_KV => SkipNode::Kv(component(0, built), component(1, built)),
        URN_ITERABLE | URN_STATE_BACKED_ITERABLE => SkipNode::Iterable(component(0, built)),
        URN_NULLABLE => {
            SkipNode::Nullable((!coder.component_coder_ids.is_empty()).then(|| component(0, built)))
        }
        URN_SHARDED_KEY if coder.component_coder_ids.is_empty() => SkipNode::Invalid(Arc::from(
            format!("sharded key coder '{coder_id}' has no key component"),
        )),
        URN_SHARDED_KEY => SkipNode::ShardedKey(component(0, built)),
        URN_GLOBAL_WINDOW => SkipNode::GlobalWindow,
        URN_INTERVAL_WINDOW => SkipNode::IntervalWindow,
        URN_WINDOWED_VALUE => SkipNode::WindowedValue {
            window: component(1, built),
            value: component(0, built),
        },
        // A param windowed value has only the inner element on the wire.
        URN_PARAM_WINDOWED_VALUE => {
            let inner = component(0, built);
            built.insert((coder_id, depth), Arc::clone(&inner));
            return inner;
        }
        URN_TIMER => SkipNode::Timer {
            key: component(0, built),
            window: component(1, built),
        },
        URN_ROW => {
            let schema_bytes = coder
                .spec
                .as_ref()
                .map_or(&[][..], |s| s.payload.as_slice());
            if schema_bytes.is_empty() {
                SkipNode::ToEnd
            } else {
                match Schema::from_proto_bytes(schema_bytes) {
                    Ok(schema) => SkipNode::Row(Arc::new(schema)),
                    Err(e) => SkipNode::Invalid(Arc::from(e.to_string())),
                }
            }
        }
        _ => SkipNode::ToEnd,
    };
    let node = Arc::new(node);
    built.insert((coder_id, depth), Arc::clone(&node));
    node
}

fn skip_node(node: &SkipNode, cursor: &mut &[u8], nested: bool) -> Result<(), std::io::Error> {
    match node {
        SkipNode::VarInt => VarIntCoder::decode_varint(cursor).map(|_| ()),
        SkipNode::Double => read_array::<8>(cursor).map(|_| ()),
        SkipNode::Bool => read_array::<1>(cursor).map(|_| ()),
        SkipNode::Bytes if nested => skip_length_prefixed(cursor),
        SkipNode::Bytes | SkipNode::ToEnd => {
            skip_to_end(cursor);
            Ok(())
        }
        SkipNode::LengthPrefix => skip_length_prefixed(cursor),
        SkipNode::Kv(key, value) => {
            skip_node(key, cursor, true)?;
            skip_node(value, cursor, nested)
        }
        SkipNode::Iterable(element) => {
            skip_iterable_elements(cursor, |cursor| skip_node(element, cursor, true))
        }
        SkipNode::Nullable(inner) => {
            let [tag] = read_array(cursor)?;
            match inner.as_deref().filter(|_| tag != 0) {
                Some(inner) => skip_node(inner, cursor, nested),
                None => Ok(()),
            }
        }
        SkipNode::ShardedKey(key) => {
            skip_length_prefixed(cursor)?;
            skip_node(key, cursor, true)
        }
        SkipNode::GlobalWindow => Ok(()),
        SkipNode::IntervalWindow => {
            read_array::<8>(cursor)?;
            VarIntCoder::decode_varint(cursor).map(|_| ())
        }
        SkipNode::WindowedValue { window, value } => {
            read_array::<8>(cursor)?; // timestamp
            let win_count = read_window_count(cursor)?;
            (0..win_count).try_for_each(|_| skip_node(window, cursor, true))?;
            skip_pane_info(cursor)?;
            skip_node(value, cursor, nested)
        }
        SkipNode::Timer { key, window } => {
            skip_node(key, cursor, true)?;
            skip_length_prefixed(cursor)?;
            let win_count = read_window_count(cursor)?;
            (0..win_count).try_for_each(|_| skip_node(window, cursor, true))?;
            let [clear] = read_array::<1>(cursor)?;
            if clear == 0 {
                read_array::<8>(cursor)?;
                read_array::<8>(cursor)?;
                skip_pane_info(cursor)?;
            }
            Ok(())
        }
        SkipNode::Row(schema) => RowCoder::skip_row(schema, cursor),
        SkipNode::Invalid(message) => Err(invalid_data(message)),
    }
}
