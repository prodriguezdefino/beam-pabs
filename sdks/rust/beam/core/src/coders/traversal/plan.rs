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

//! [`SkipPlan`]: a coder compiled once to skip many encoded values.

use std::borrow::Borrow;
use std::collections::HashMap;
use std::io;
use std::sync::Arc;

use beam_model::pipeline::Coder as ProtoCoder;

use super::cursor::SkipCursor;
use super::{
    MAX_SKIP_DEPTH, coder_urn, component_coder_id, read_window_count, skip_iterable_elements,
    skip_length_prefixed, skip_pane_info, skip_to_end,
};
use crate::coders::row::header::{bit_is_null, read_row_header};
use crate::coders::row::{FieldWire, encoding_order, field_wire, into_io_error};
use crate::coders::standard::{VarIntCoder, consumed, read_array, read_be_i32, take_bytes};
use crate::coders::{
    URN_BOOL, URN_BYTES, URN_DOUBLE, URN_GLOBAL_WINDOW, URN_INTERVAL_WINDOW, URN_ITERABLE, URN_KV,
    URN_LENGTH_PREFIX, URN_NULLABLE, URN_PARAM_WINDOWED_VALUE, URN_ROW, URN_SHARDED_KEY,
    URN_STATE_BACKED_ITERABLE, URN_STRING_UTF8, URN_TIMER, URN_VARINT, URN_WINDOWED_VALUE,
};
use crate::schema::{FieldType, Schema};

/// A coder compiled for skipping encoded values.
///
/// [`skip_coder_value`](super::skip_coder_value) looks up the coder and compares its URN
/// for every value. A plan does that once, so a hot path builds the plan once per coder and
/// calls [`skip`](Self::skip) for each value. Both find the same end of each value and fail
/// on the same input.
#[derive(Clone, Debug)]
pub struct SkipPlan(Arc<SkipNode>);

/// A compiled row schema with its field plans in wire `encoding_position` order.
#[derive(Debug)]
pub(super) struct RowSkipNode {
    pub(super) fields: Box<[Arc<SkipNode>]>,
}

#[derive(Debug)]
pub(super) enum SkipNode {
    VarInt,
    Fixed(usize),
    /// A string or bytes value: length-prefixed when nested, else the rest of the input.
    Bytes,
    LengthPrefix,
    GlobalWindow,
    IntervalWindow,
    Kv(Arc<SkipNode>, Arc<SkipNode>),
    Iterable(Arc<SkipNode>),
    /// A nullable value. `None` when the coder has no component: a present value is empty.
    Nullable(Option<Arc<SkipNode>>),
    /// A row container nullable: 0 is absent, 1 is present, any other byte is invalid data.
    StrictNullable(Arc<SkipNode>),
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
    /// A row map: 32-bit big-endian entry count, then that many key-value pairs.
    Map {
        key: Arc<SkipNode>,
        value: Arc<SkipNode>,
    },
    /// A row decimal logical type: varint scale, then length-prefixed big integer.
    Decimal,
    Row(Arc<RowSkipNode>),
    /// An unknown URN, a missing coder or a Row coder without a schema: the value takes the
    /// rest of the input.
    ToEnd,
    /// A coder that [`skip_coder_value`](super::skip_coder_value) rejects when it reaches it.
    /// The plan fails at the same value, not when it is built.
    Invalid(Arc<str>),
}

impl SkipPlan {
    /// Compiles `coder_id`. A coder that occurs more than once at the same depth shares one
    /// plan, so a cycle unrolls to the depth limit with at most one node per coder and depth.
    pub fn new(coder_id: &str, coders: &HashMap<String, ProtoCoder>) -> Self {
        let mut built = HashMap::new();
        Self(compile(coder_id, coders, 0, &mut built))
    }

    /// Compiles a row `schema` for skipping encoded rows.
    pub fn for_row(schema: impl Borrow<Schema>) -> Self {
        Self(Arc::new(SkipNode::Row(compile_row(schema.borrow()))))
    }

    /// Moves `cursor` past one value. `nested` has the meaning of
    /// [`skip_coder_value`](super::skip_coder_value).
    pub fn skip(&self, cursor: &mut &[u8], nested: bool) -> Result<(), io::Error> {
        skip_node(&self.0, cursor, nested)
    }

    /// Starts a resumable skip at byte offset `start` in an element buffer. Use this only
    /// after [`skip`](Self::skip) returns `UnexpectedEof` on a partial element.
    pub fn cursor(&self, start: usize, nested: bool) -> SkipCursor {
        SkipCursor::new(Arc::clone(&self.0), start, nested)
    }

    /// Borrows the leading key out of an encoded KV `element`, with this plan as the key
    /// coder. The same as [`key_slice`](super::key_slice).
    pub fn key_slice<'e>(&self, element: &'e [u8]) -> Option<&'e [u8]> {
        if element.is_empty() {
            return None;
        }
        let mut cursor = element;
        self.skip(&mut cursor, true).ok()?;
        Some(consumed(element, cursor))
    }
}

pub(super) fn invalid_data(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.to_string())
}

pub(crate) fn skip_row_field(field_type: &FieldType, cursor: &mut &[u8]) -> io::Result<()> {
    skip_node(&compile_row_field_node(field_type), cursor, true)
}

fn compile_row(schema: &Schema) -> Arc<RowSkipNode> {
    let order = encoding_order(schema);
    let fields = (0..schema.num_fields())
        .map(|position| {
            let index = order.as_ref().map_or(position, |o| o[position]);
            Arc::new(compile_row_field_node(&schema.fields[index].field_type))
        })
        .collect();
    Arc::new(RowSkipNode { fields })
}

fn compile_row_field_node(field_type: &FieldType) -> SkipNode {
    match field_wire(field_type) {
        FieldWire::Fixed(len) => SkipNode::Fixed(len),
        FieldWire::VarInt => SkipNode::VarInt,
        FieldWire::LengthPrefixed => SkipNode::LengthPrefix,
        FieldWire::Decimal => SkipNode::Decimal,
        FieldWire::Iterable(elem) => SkipNode::Iterable(compile_row_container_element(elem)),
        FieldWire::Map(key, value) => SkipNode::Map {
            key: Arc::new(compile_row_field_node(key)),
            value: compile_row_container_element(value),
        },
        FieldWire::Row(inner) => SkipNode::Row(compile_row(inner)),
    }
}

fn compile_row_container_element(elem: &FieldType) -> Arc<SkipNode> {
    let inner = Arc::new(compile_row_field_node(elem));
    if elem.nullable {
        Arc::new(SkipNode::StrictNullable(inner))
    } else {
        inner
    }
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
        URN_DOUBLE => SkipNode::Fixed(8),
        URN_BOOL => SkipNode::Fixed(1),
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
                    Ok(schema) => SkipNode::Row(compile_row(&schema)),
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

fn skip_node(node: &SkipNode, cursor: &mut &[u8], nested: bool) -> Result<(), io::Error> {
    match node {
        SkipNode::VarInt => VarIntCoder::decode_varint(cursor).map(|_| ()),
        SkipNode::Fixed(len) => take_bytes(cursor, *len).map(|_| ()),
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
        SkipNode::StrictNullable(inner) => match read_array::<1>(cursor)? {
            [0] => Ok(()),
            [1] => skip_node(inner, cursor, true),
            [b] => Err(invalid_data(&format!(
                "Invalid nullable presence byte: {b}"
            ))),
        },
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
        SkipNode::Map { key, value } => {
            let count = read_be_i32(cursor)?;
            let count = usize::try_from(count)
                .map_err(|_| invalid_data(&format!("Negative element count: {count}")))?;
            (0..count).try_for_each(|_| {
                skip_node(key, cursor, true)?;
                skip_node(value, cursor, true)
            })
        }
        SkipNode::Decimal => {
            VarIntCoder::decode_varint(cursor)?;
            skip_length_prefixed(cursor)
        }
        SkipNode::Row(row) => {
            let null_bits = read_row_header(row.fields.len(), cursor).map_err(into_io_error)?;
            row.fields
                .iter()
                .enumerate()
                .filter(|&(idx, _)| !bit_is_null(null_bits, idx))
                .try_for_each(|(_, field)| skip_node(field, cursor, true))
        }
        SkipNode::Invalid(message) => Err(invalid_data(message)),
    }
}
