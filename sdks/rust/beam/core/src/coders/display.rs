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

//! Human-readable rendering of encoded elements from runner coder protos.
//!
//! [`ElementFormatter`] compiles a coder graph into a tree of rendering steps once per bundle
//! processor. Coders outside the standard set render as length-prefixed escaped byte strings.

use std::collections::HashMap;
use std::fmt;
use std::io::{Cursor, Error, ErrorKind};
use std::sync::Arc;

use model::pipeline::Coder as ProtoCoder;

use super::pane::PaneInfo;
use super::standard::{VarIntCoder, read_array, read_be_i32, read_timestamp};
use super::traversal::{coder_urn, read_length_prefixed_slice, skip_length_prefixed};
use super::{
    RowCoder, URN_BOOL, URN_BYTES, URN_DOUBLE, URN_GLOBAL_WINDOW, URN_INTERVAL_WINDOW,
    URN_ITERABLE, URN_KV, URN_LENGTH_PREFIX, URN_NULLABLE, URN_PARAM_WINDOWED_VALUE, URN_ROW,
    URN_STATE_BACKED_ITERABLE, URN_STRING_UTF8, URN_VARINT, URN_WINDOWED_VALUE,
};
use crate::schema::Schema;

/// Deepest coder nesting to compile, as a runner coder graph can have a cycle. Deeper coders
/// render as opaque bytes.
const MAX_DEPTH: usize = 64;

/// One rendering step. Each variant matches the coder it was compiled from.
#[derive(Debug)]
enum Node {
    VarInt,
    Double,
    Bool,
    /// If `nested`, a varint length prefix delimits the value, else it runs to the input end.
    Utf8 {
        nested: bool,
    },
    Bytes {
        nested: bool,
    },
    LengthPrefix(Box<Node>),
    Kv(Box<Node>, Box<Node>),
    Iterable(Box<Node>),
    Nullable(Box<Node>),
    GlobalWindow,
    IntervalWindow,
    WindowedValue {
        value: Box<Node>,
        window: Box<Node>,
    },
    Row(Arc<Schema>),
    /// A coder that this SDK cannot interpret. Renders the remaining bytes, escaped.
    Opaque,
}

/// Renders encoded elements of one coder as text. Build one per coder and share it in an `Arc`.
#[derive(Debug)]
pub struct ElementFormatter {
    root: Node,
}

impl ElementFormatter {
    /// Compiles the formatter for `coder_id`, in the nested context of Fn API elements.
    pub fn new(coder_id: &str, coders: &HashMap<String, ProtoCoder>) -> Self {
        Self {
            root: compile(coder_id, coders, true, 0),
        }
    }

    /// Writes the value at `cursor` to `out`, leaving `cursor` just past it. To find where a
    /// leading value ends, write to a sink that discards its input.
    pub fn write(&self, cursor: &mut Cursor<&[u8]>, out: &mut dyn fmt::Write) -> Result<(), Error> {
        render(&self.root, cursor, out, false)
    }

    /// Renders all of `encoded` and never fails: bytes that do not match the coder show escaped.
    pub fn format(&self, encoded: &[u8]) -> String {
        let mut out = String::with_capacity(encoded.len() + 2);
        match self.write(&mut Cursor::new(encoded), &mut out) {
            Ok(()) => out,
            Err(_) => format!("<undecodable {}>", ByteStr(encoded)),
        }
    }
}

/// Builds the rendering step for `coder_id`. For a runner coder graph, the depth limit prevents
/// stack overflow and the step limit prevents exponential growth (a cyclic `KV<c, c>` doubles at
/// each level). Coders beyond either limit render as opaque bytes.
fn compile(
    coder_id: &str,
    coders: &HashMap<String, ProtoCoder>,
    nested: bool,
    depth: usize,
) -> Node {
    Compiler {
        coders,
        budget: MAX_NODES,
    }
    .node(coder_id, nested, depth)
}

/// Maximum number of rendering steps in one formatter. Real element coders need only a few.
const MAX_NODES: usize = 1024;

struct Compiler<'c> {
    coders: &'c HashMap<String, ProtoCoder>,
    /// Steps left to compile. At zero, the remaining coders become [`Node::Opaque`].
    budget: usize,
}

impl Compiler<'_> {
    fn node(&mut self, coder_id: &str, nested: bool, depth: usize) -> Node {
        let coders = self.coders;
        let Some(coder) = coders
            .get(coder_id)
            .filter(|_| depth <= MAX_DEPTH && self.budget > 0)
        else {
            return Node::Opaque;
        };
        self.budget -= 1;
        let mut component = |index: usize, nested: bool| {
            coder
                .component_coder_ids
                .get(index)
                .map_or(Node::Opaque, |id| self.node(id, nested, depth + 1))
        };
        match coder_urn(coder) {
            URN_VARINT => Node::VarInt,
            URN_DOUBLE => Node::Double,
            URN_BOOL => Node::Bool,
            URN_STRING_UTF8 => Node::Utf8 { nested },
            URN_BYTES => Node::Bytes { nested },
            // The wrapped value uses the whole-stream context inside the delimited bytes.
            URN_LENGTH_PREFIX => Node::LengthPrefix(Box::new(component(0, false))),
            URN_KV => {
                let key = component(0, true);
                Node::Kv(Box::new(key), Box::new(component(1, nested)))
            }
            URN_ITERABLE | URN_STATE_BACKED_ITERABLE => {
                Node::Iterable(Box::new(component(0, true)))
            }
            URN_NULLABLE => Node::Nullable(Box::new(component(0, nested))),
            URN_GLOBAL_WINDOW => Node::GlobalWindow,
            URN_INTERVAL_WINDOW => Node::IntervalWindow,
            URN_WINDOWED_VALUE => {
                let value = component(0, nested);
                Node::WindowedValue {
                    value: Box::new(value),
                    window: Box::new(component(1, true)),
                }
            }
            // Only the element is on the wire. The coder payload holds the other fields.
            URN_PARAM_WINDOWED_VALUE => component(0, nested),
            URN_ROW => coder
                .spec
                .as_ref()
                .filter(|spec| !spec.payload.is_empty())
                .and_then(|spec| Schema::from_proto_bytes(&spec.payload).ok())
                .map_or(Node::Opaque, |schema| Node::Row(Arc::new(schema))),
            _ => Node::Opaque,
        }
    }
}

fn fmt_err(_: fmt::Error) -> Error {
    Error::other("failed to write rendered element")
}

/// Writes one value. If `quoted` is true, strings get quotes, as inside a composite.
fn render(
    node: &Node,
    cursor: &mut Cursor<&[u8]>,
    out: &mut dyn fmt::Write,
    quoted: bool,
) -> Result<(), Error> {
    match node {
        Node::VarInt => write!(out, "{}", VarIntCoder::decode_varint(cursor)?).map_err(fmt_err),
        Node::Double => {
            let value = f64::from_be_bytes(read_array::<8>(cursor)?);
            write!(out, "{value:?}").map_err(fmt_err)
        }
        Node::Bool => {
            let [byte] = read_array::<1>(cursor)?;
            write!(out, "{}", byte != 0).map_err(fmt_err)
        }
        Node::Utf8 { nested } => {
            let bytes = take(cursor, *nested)?;
            let text =
                std::str::from_utf8(bytes).map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
            if quoted {
                write!(out, "{text:?}")
            } else {
                out.write_str(text)
            }
            .map_err(fmt_err)
        }
        Node::Bytes { nested } => {
            write!(out, "{}", ByteStr(take(cursor, *nested)?)).map_err(fmt_err)
        }
        Node::LengthPrefix(inner) => {
            let mut framed = Cursor::new(read_length_prefixed_slice(cursor)?);
            render(inner, &mut framed, out, quoted)
        }
        Node::Kv(key, value) => {
            out.write_char('(').map_err(fmt_err)?;
            render(key, cursor, out, true)?;
            out.write_str(", ").map_err(fmt_err)?;
            render(value, cursor, out, true)?;
            out.write_char(')').map_err(fmt_err)
        }
        Node::Iterable(element) => render_iterable(element, cursor, out),
        Node::Nullable(inner) => match read_array::<1>(cursor)? {
            [0] => out.write_str("null").map_err(fmt_err),
            _ => render(inner, cursor, out, quoted),
        },
        Node::GlobalWindow => out.write_str("GlobalWindow").map_err(fmt_err),
        Node::IntervalWindow => {
            let end = read_timestamp(cursor)?;
            let span = VarIntCoder::decode_varint(cursor)?;
            write!(out, "[{}, {end})", end.saturating_sub(span)).map_err(fmt_err)
        }
        Node::WindowedValue { value, window } => {
            let timestamp = read_timestamp(cursor)?;
            let count = read_be_i32(cursor)?;
            if count < 0 {
                return Err(Error::new(ErrorKind::InvalidData, "negative window count"));
            }
            let mut windows = String::new();
            (0..count).try_for_each(|i| {
                if i > 0 {
                    windows.push_str(", ");
                }
                render(window, cursor, &mut windows, true)
            })?;
            let (pane, has_metadata) = PaneInfo::decode(cursor)?;
            if has_metadata {
                skip_length_prefixed(cursor)?;
            }
            render(value, cursor, out, true)?;
            write!(out, " @ {timestamp} in [{windows}] {pane:?}").map_err(fmt_err)
        }
        Node::Row(schema) => {
            let row = RowCoder::decode_row(schema, cursor)
                .map_err(|e| Error::new(ErrorKind::InvalidData, e.to_string()))?;
            out.write_str("Row{").map_err(fmt_err)?;
            schema
                .fields
                .iter()
                .zip(row.values())
                .enumerate()
                .try_for_each(|(i, (field, value))| {
                    let sep = if i == 0 { "" } else { ", " };
                    match value {
                        Some(value) => write!(out, "{sep}{}: {value}", field.name),
                        None => write!(out, "{sep}{}: null", field.name),
                    }
                })
                .map_err(fmt_err)?;
            out.write_char('}').map_err(fmt_err)
        }
        Node::Opaque => {
            let rest = take(cursor, false)?;
            write!(out, "{}", ByteStr(rest)).map_err(fmt_err)
        }
    }
}

/// Writes an iterable body: a 32-bit count, or `-1` and then varint-sized chunks.
fn render_iterable(
    element: &Node,
    cursor: &mut Cursor<&[u8]>,
    out: &mut dyn fmt::Write,
) -> Result<(), Error> {
    out.write_char('[').map_err(fmt_err)?;
    let mut written = 0usize;
    match read_be_i32(cursor)? {
        count if count >= 0 => {
            render_items(element, i64::from(count), &mut written, cursor, out)?;
        }
        -1 => loop {
            match VarIntCoder::decode_varint(cursor)? {
                0 => break,
                // A continuation token: the remaining elements are in runner state.
                -1 => {
                    skip_length_prefixed(cursor)?;
                    out.write_str(if written > 0 { ", ..." } else { "..." })
                        .map_err(fmt_err)?;
                    break;
                }
                chunk if chunk > 0 => render_items(element, chunk, &mut written, cursor, out)?,
                chunk => {
                    return Err(Error::new(
                        ErrorKind::InvalidData,
                        format!("invalid iterable chunk header: {chunk}"),
                    ));
                }
            }
        },
        count => {
            return Err(Error::new(
                ErrorKind::InvalidData,
                format!("invalid iterable count: {count}"),
            ));
        }
    }
    out.write_char(']').map_err(fmt_err)
}

/// Writes `n` iterable elements. `written` counts elements already shown, for separators.
fn render_items(
    element: &Node,
    n: i64,
    written: &mut usize,
    cursor: &mut Cursor<&[u8]>,
    out: &mut dyn fmt::Write,
) -> Result<(), Error> {
    (0..n).try_for_each(|_| {
        if *written > 0 {
            out.write_str(", ").map_err(fmt_err)?;
        }
        *written += 1;
        render(element, cursor, out, true)
    })
}

/// Borrows a string or bytes value: varint-delimited if `nested`, otherwise the rest.
fn take<'a>(cursor: &mut Cursor<&'a [u8]>, nested: bool) -> Result<&'a [u8], Error> {
    if nested {
        return read_length_prefixed_slice(cursor);
    }
    let data: &'a [u8] = cursor.get_ref();
    let start = usize::try_from(cursor.position())
        .unwrap_or(data.len())
        .min(data.len());
    cursor.set_position(data.len() as u64);
    Ok(&data[start..])
}

/// Displays bytes as a Rust byte string literal, `b"..."`, escaping non-printable bytes.
struct ByteStr<'a>(&'a [u8]);

impl fmt::Display for ByteStr<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "b\"{}\"", self.0.escape_ascii())
    }
}
