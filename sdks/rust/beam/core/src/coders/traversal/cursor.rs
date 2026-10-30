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

//! [`SkipCursor`]: resumable stepper that finds the end of a value split across chunks.

use std::io::{self, ErrorKind};
use std::sync::Arc;

use super::plan::{RowSkipNode, SkipNode, invalid_data};
use crate::coders::PaneInfo;
use crate::coders::row::header::{bit_is_null, read_row_header};
use crate::coders::row::into_io_error;
use crate::coders::standard::VarIntCoder;

/// Outcome of one [`SkipCursor::advance`] call.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SkipStep {
    /// The value ended at this byte offset in `buf`.
    Complete(usize),
    /// `buf` ended before the value finished. Append more bytes and call `advance` again.
    NeedMore,
}

/// Resumable stepper that finds the end of one encoded value across chunks.
///
/// Build a cursor with [`SkipPlan::cursor`](super::SkipPlan::cursor) only after a fast-path
/// skip hits `UnexpectedEof`. Each [`advance`](Self::advance) call resumes at the start of
/// the interrupted token, so the cursor scans each byte once instead of restarting from byte
/// 0 on every chunk.
#[derive(Debug)]
pub struct SkipCursor {
    pos: usize,
    stack: Vec<SkipFrame>,
}

#[derive(Debug)]
enum SkipFrame {
    /// Skip one compiled node.
    Node { node: Arc<SkipNode>, nested: bool },
    /// Skip `remaining` raw bytes starting at `self.pos`.
    FixedBytes(usize),
    /// Skip one varint.
    VarInt,
    /// Skip one varint length prefix and its payload bytes.
    LengthPrefix,
    /// Skip a `PaneInfo` and any trailing length-prefixed element metadata.
    PaneInfo,
    /// Skip `remaining` nested elements of `element`.
    Repeat {
        element: Arc<SkipNode>,
        remaining: u64,
    },
    /// Read the next chunk header of a chunked (`-1`) iterable.
    ChunkedIterable { element: Arc<SkipNode> },
    /// Skip the continuation token length prefix and bytes after a `-1` chunk header.
    ContinuationToken,
    /// Skip `remaining` map entries `(key, value)` (both nested).
    MapEntries {
        key: Arc<SkipNode>,
        value: Arc<SkipNode>,
        remaining: usize,
    },
    /// Skip the remaining fields of a row whose null bitmask lives at
    /// `buf[bitmask_start..bitmask_end]`.
    RowFields {
        row: Arc<RowSkipNode>,
        bitmask_start: usize,
        bitmask_end: usize,
        next_field: usize,
    },
    /// Tail of a `WindowedValue` after the 8-byte timestamp.
    WindowedValueWindows {
        window: Arc<SkipNode>,
        value: Arc<SkipNode>,
        nested: bool,
    },
    /// Tail of a `Timer` after key and dynamic tag.
    TimerAfterTag { window: Arc<SkipNode> },
    /// Tail of a `Timer` after its windows: read clear flag and optional timestamps and pane.
    TimerClear,
}

/// Result of the inline leaf loop in [`SkipCursor::step_repeat`].
enum RepeatOutcome {
    /// Every requested element completed.
    Done,
    /// Input ended before the current element's leading token completed; `remaining > 0`.
    NeedMore,
    /// Consumed the current element's prefix and decremented `remaining`; `top` finishes it.
    NeedMoreWithTop(SkipFrame),
    /// The element is composite and runs on `self.stack`.
    Composite,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum RepeatProgress {
    Done,
    Yielded,
    NeedMore,
}

impl SkipCursor {
    pub(super) fn new(node: Arc<SkipNode>, start: usize, nested: bool) -> Self {
        Self {
            pos: start,
            stack: vec![SkipFrame::Node { node, nested }],
        }
    }

    /// Advances through every complete token in `buf`, stopping at the start of the first
    /// incomplete token. `buf` must hold all accumulated bytes from the start offset through
    /// the latest chunk.
    ///
    /// # Errors
    ///
    /// Returns [`ErrorKind::InvalidData`] if the bytes violate the coder wire format or if
    /// `buf` is shorter than the cursor's saved offset.
    pub fn advance(&mut self, buf: &[u8]) -> io::Result<SkipStep> {
        if buf.len() < self.pos {
            return Err(invalid_data("buffer shrank below skip cursor offset"));
        }
        while let Some(frame) = self.stack.pop() {
            match frame {
                SkipFrame::FixedBytes(len) => {
                    if !self.try_skip_fixed(buf, len) {
                        self.stack.push(SkipFrame::FixedBytes(len));
                        return Ok(SkipStep::NeedMore);
                    }
                }
                SkipFrame::VarInt => {
                    if self.try_read_varint(buf)?.is_none() {
                        self.stack.push(SkipFrame::VarInt);
                        return Ok(SkipStep::NeedMore);
                    }
                }
                SkipFrame::LengthPrefix => {
                    if !self.skip_length_prefixed(buf)? {
                        return Ok(SkipStep::NeedMore);
                    }
                }
                SkipFrame::PaneInfo => {
                    if !self.skip_pane_info(buf)? {
                        return Ok(SkipStep::NeedMore);
                    }
                }
                SkipFrame::Repeat { element, remaining } => {
                    let progress = self.run_repeat(&element, remaining, false, buf)?;
                    if progress == RepeatProgress::NeedMore {
                        return Ok(SkipStep::NeedMore);
                    }
                }
                SkipFrame::ChunkedIterable { element } => {
                    if !self.step_chunked_iterable(element, buf)? {
                        return Ok(SkipStep::NeedMore);
                    }
                }
                SkipFrame::ContinuationToken => {
                    if !self.step_continuation_token(buf)? {
                        return Ok(SkipStep::NeedMore);
                    }
                }
                SkipFrame::MapEntries {
                    key,
                    value,
                    remaining,
                } => self.push_map_entry(key, value, remaining),
                SkipFrame::RowFields {
                    row,
                    bitmask_start,
                    bitmask_end,
                    next_field,
                } => self.push_next_row_field(row, bitmask_start, bitmask_end, next_field, buf),
                SkipFrame::WindowedValueWindows {
                    window,
                    value,
                    nested,
                } => {
                    let Some(win_count) = self.try_read_be_i32(buf) else {
                        self.stack.push(SkipFrame::WindowedValueWindows {
                            window,
                            value,
                            nested,
                        });
                        return Ok(SkipStep::NeedMore);
                    };
                    if win_count < 0 {
                        return Err(invalid_data(&format!("Negative window count: {win_count}")));
                    }
                    self.stack.push(SkipFrame::Node {
                        node: value,
                        nested,
                    });
                    self.stack.push(SkipFrame::PaneInfo);
                    if win_count > 0 {
                        self.stack.push(SkipFrame::Repeat {
                            element: window,
                            remaining: win_count as u64,
                        });
                    }
                }
                SkipFrame::TimerAfterTag { window } => {
                    let Some(win_count) = self.try_read_be_i32(buf) else {
                        self.stack.push(SkipFrame::TimerAfterTag { window });
                        return Ok(SkipStep::NeedMore);
                    };
                    if win_count < 0 {
                        return Err(invalid_data(&format!("Negative window count: {win_count}")));
                    }
                    self.stack.push(SkipFrame::TimerClear);
                    if win_count > 0 {
                        self.stack.push(SkipFrame::Repeat {
                            element: window,
                            remaining: win_count as u64,
                        });
                    }
                }
                SkipFrame::TimerClear => {
                    let Some(clear) = self.try_read_u8(buf) else {
                        self.stack.push(SkipFrame::TimerClear);
                        return Ok(SkipStep::NeedMore);
                    };
                    if clear == 0 {
                        if self.try_skip_fixed(buf, 16) {
                            if !self.skip_pane_info(buf)? {
                                return Ok(SkipStep::NeedMore);
                            }
                        } else {
                            self.stack.push(SkipFrame::PaneInfo);
                            self.stack.push(SkipFrame::FixedBytes(16));
                            return Ok(SkipStep::NeedMore);
                        }
                    }
                }
                SkipFrame::Node { node, nested } => {
                    if !self.step_node(node, nested, buf)? {
                        return Ok(SkipStep::NeedMore);
                    }
                }
            }
        }
        Ok(SkipStep::Complete(self.pos))
    }

    fn step_node(&mut self, node: Arc<SkipNode>, nested: bool, buf: &[u8]) -> io::Result<bool> {
        match node.as_ref() {
            SkipNode::VarInt => {
                if self.try_read_varint(buf)?.is_none() {
                    self.stack.push(SkipFrame::VarInt);
                    return Ok(false);
                }
            }
            SkipNode::Fixed(len) => {
                let len = *len;
                if !self.try_skip_fixed(buf, len) {
                    self.stack.push(SkipFrame::FixedBytes(len));
                    return Ok(false);
                }
            }
            SkipNode::Bytes if nested => return self.skip_length_prefixed(buf),
            SkipNode::Bytes | SkipNode::ToEnd => self.pos = buf.len(),
            SkipNode::LengthPrefix => return self.skip_length_prefixed(buf),
            SkipNode::GlobalWindow => {}
            SkipNode::IntervalWindow => {
                if !self.try_skip_fixed(buf, 8) {
                    self.stack.push(SkipFrame::Node { node, nested });
                    return Ok(false);
                }
                if self.try_read_varint(buf)?.is_none() {
                    self.stack.push(SkipFrame::VarInt);
                    return Ok(false);
                }
            }
            SkipNode::Kv(key, value) => {
                self.stack.push(SkipFrame::Node {
                    node: Arc::clone(value),
                    nested,
                });
                self.stack.push(SkipFrame::Node {
                    node: Arc::clone(key),
                    nested: true,
                });
            }
            SkipNode::Iterable(element) => {
                let Some(count) = self.try_read_be_i32(buf) else {
                    self.stack.push(SkipFrame::Node { node, nested });
                    return Ok(false);
                };
                if count > 0 {
                    let progress = self.run_repeat(element, count as u64, false, buf)?;
                    return Ok(progress != RepeatProgress::NeedMore);
                }
                if count == -1 {
                    return self.step_chunked_iterable(Arc::clone(element), buf);
                }
                if count != 0 {
                    return Err(invalid_data(&format!("Invalid iterable count: {count}")));
                }
            }
            SkipNode::Nullable(inner) => {
                let Some(tag) = self.try_read_u8(buf) else {
                    self.stack.push(SkipFrame::Node { node, nested });
                    return Ok(false);
                };
                if let Some(inner) = inner.as_ref().filter(|_| tag != 0) {
                    self.stack.push(SkipFrame::Node {
                        node: Arc::clone(inner),
                        nested,
                    });
                }
            }
            SkipNode::StrictNullable(inner) => {
                let Some(tag) = self.try_read_u8(buf) else {
                    self.stack.push(SkipFrame::Node { node, nested });
                    return Ok(false);
                };
                match tag {
                    0 => {}
                    1 => self.stack.push(SkipFrame::Node {
                        node: Arc::clone(inner),
                        nested: true,
                    }),
                    other => {
                        return Err(invalid_data(&format!(
                            "Invalid nullable presence byte: {other}"
                        )));
                    }
                }
            }
            SkipNode::ShardedKey(key) => {
                self.stack.push(SkipFrame::Node {
                    node: Arc::clone(key),
                    nested: true,
                });
                return self.skip_length_prefixed(buf);
            }
            SkipNode::WindowedValue { window, value } => {
                if !self.try_skip_fixed(buf, 8) {
                    self.stack.push(SkipFrame::Node { node, nested });
                    return Ok(false);
                }
                self.stack.push(SkipFrame::WindowedValueWindows {
                    window: Arc::clone(window),
                    value: Arc::clone(value),
                    nested,
                });
            }
            SkipNode::Timer { key, window } => {
                self.stack.push(SkipFrame::TimerAfterTag {
                    window: Arc::clone(window),
                });
                self.stack.push(SkipFrame::LengthPrefix);
                self.stack.push(SkipFrame::Node {
                    node: Arc::clone(key),
                    nested: true,
                });
            }
            SkipNode::Map { key, value } => {
                let Some(count) = self.try_read_be_i32(buf) else {
                    self.stack.push(SkipFrame::Node { node, nested });
                    return Ok(false);
                };
                let Ok(count) = usize::try_from(count) else {
                    return Err(invalid_data(&format!("Negative element count: {count}")));
                };
                if count > 0 {
                    self.push_map_entry(Arc::clone(key), Arc::clone(value), count);
                }
            }
            SkipNode::Decimal => {
                if self.try_read_varint(buf)?.is_none() {
                    self.stack.push(SkipFrame::Node { node, nested });
                    return Ok(false);
                }
                return self.skip_length_prefixed(buf);
            }
            SkipNode::Row(row) => {
                let mut rest = buf.get(self.pos..).unwrap_or_default();
                match read_row_header(row.fields.len(), &mut rest).map_err(into_io_error) {
                    Ok(null_bits) => {
                        let header_end = buf.len() - rest.len();
                        let bitmask_start = header_end - null_bits.len();
                        self.pos = header_end;
                        self.push_next_row_field(
                            Arc::clone(row),
                            bitmask_start,
                            header_end,
                            0,
                            buf,
                        );
                    }
                    Err(e) if e.kind() == ErrorKind::UnexpectedEof => {
                        self.stack.push(SkipFrame::Node { node, nested });
                        return Ok(false);
                    }
                    Err(e) => return Err(e),
                }
            }
            SkipNode::Invalid(message) => return Err(invalid_data(message)),
        }
        Ok(true)
    }

    fn run_repeat(
        &mut self,
        element: &Arc<SkipNode>,
        mut remaining: u64,
        chunked: bool,
        buf: &[u8],
    ) -> io::Result<RepeatProgress> {
        match self.step_repeat(element, &mut remaining, buf)? {
            RepeatOutcome::Done => Ok(RepeatProgress::Done),
            RepeatOutcome::NeedMore => {
                if chunked {
                    self.stack.push(SkipFrame::ChunkedIterable {
                        element: Arc::clone(element),
                    });
                }
                self.stack.push(SkipFrame::Repeat {
                    element: Arc::clone(element),
                    remaining,
                });
                Ok(RepeatProgress::NeedMore)
            }
            RepeatOutcome::NeedMoreWithTop(top) => {
                if chunked {
                    self.stack.push(SkipFrame::ChunkedIterable {
                        element: Arc::clone(element),
                    });
                }
                if remaining > 0 {
                    self.stack.push(SkipFrame::Repeat {
                        element: Arc::clone(element),
                        remaining,
                    });
                }
                self.stack.push(top);
                Ok(RepeatProgress::NeedMore)
            }
            RepeatOutcome::Composite => {
                if chunked {
                    self.stack.push(SkipFrame::ChunkedIterable {
                        element: Arc::clone(element),
                    });
                }
                if remaining > 1 {
                    self.stack.push(SkipFrame::Repeat {
                        element: Arc::clone(element),
                        remaining: remaining - 1,
                    });
                }
                self.stack.push(SkipFrame::Node {
                    node: Arc::clone(element),
                    nested: true,
                });
                Ok(RepeatProgress::Yielded)
            }
        }
    }

    /// Scans leaf elements inline without touching `self.stack`, so large iterables of
    /// strings, bytes, varints or fixed-width primitives skip with no per-element `Arc` clone.
    fn step_repeat(
        &mut self,
        element: &SkipNode,
        remaining: &mut u64,
        buf: &[u8],
    ) -> io::Result<RepeatOutcome> {
        match element {
            SkipNode::GlobalWindow => {
                *remaining = 0;
                Ok(RepeatOutcome::Done)
            }
            SkipNode::ToEnd => {
                self.pos = buf.len();
                *remaining = 0;
                Ok(RepeatOutcome::Done)
            }
            SkipNode::Fixed(0) => {
                *remaining = 0;
                Ok(RepeatOutcome::Done)
            }
            SkipNode::Fixed(len) => {
                let avail = buf.len().saturating_sub(self.pos);
                let take = ((avail / *len) as u64).min(*remaining);
                self.pos += (take as usize) * *len;
                *remaining -= take;
                if *remaining == 0 {
                    Ok(RepeatOutcome::Done)
                } else {
                    Ok(RepeatOutcome::NeedMore)
                }
            }
            SkipNode::VarInt => {
                while *remaining > 0 {
                    if self.try_read_varint(buf)?.is_none() {
                        return Ok(RepeatOutcome::NeedMore);
                    }
                    *remaining -= 1;
                }
                Ok(RepeatOutcome::Done)
            }
            SkipNode::LengthPrefix | SkipNode::Bytes => {
                while *remaining > 0 {
                    let Some(len) = self.try_read_varint(buf)? else {
                        return Ok(RepeatOutcome::NeedMore);
                    };
                    let len = usize::try_from(len)
                        .map_err(|_| invalid_data(&format!("Negative length: {len}")))?;
                    if self.try_skip_fixed(buf, len) {
                        *remaining -= 1;
                    } else {
                        *remaining -= 1;
                        return Ok(RepeatOutcome::NeedMoreWithTop(SkipFrame::FixedBytes(len)));
                    }
                }
                Ok(RepeatOutcome::Done)
            }
            SkipNode::IntervalWindow => {
                while *remaining > 0 {
                    let Some(after_ts) = self.pos.checked_add(8).filter(|&end| end <= buf.len())
                    else {
                        return Ok(RepeatOutcome::NeedMore);
                    };
                    let mut rest = buf.get(after_ts..).unwrap_or_default();
                    match VarIntCoder::decode_varint(&mut rest) {
                        Ok(_) => {
                            self.pos = buf.len() - rest.len();
                            *remaining -= 1;
                        }
                        Err(e) if e.kind() == ErrorKind::UnexpectedEof => {
                            self.pos = after_ts;
                            *remaining -= 1;
                            return Ok(RepeatOutcome::NeedMoreWithTop(SkipFrame::VarInt));
                        }
                        Err(e) => return Err(e),
                    }
                }
                Ok(RepeatOutcome::Done)
            }
            SkipNode::Invalid(message) => Err(invalid_data(message)),
            SkipNode::Kv(..)
            | SkipNode::Iterable(_)
            | SkipNode::Nullable(_)
            | SkipNode::StrictNullable(_)
            | SkipNode::ShardedKey(_)
            | SkipNode::WindowedValue { .. }
            | SkipNode::Timer { .. }
            | SkipNode::Map { .. }
            | SkipNode::Decimal
            | SkipNode::Row(_) => Ok(RepeatOutcome::Composite),
        }
    }

    fn step_chunked_iterable(&mut self, element: Arc<SkipNode>, buf: &[u8]) -> io::Result<bool> {
        loop {
            let Some(chunk_len) = self.try_read_varint(buf)? else {
                self.stack.push(SkipFrame::ChunkedIterable { element });
                return Ok(false);
            };
            if chunk_len == 0 {
                return Ok(true);
            }
            if chunk_len == -1 {
                return self.step_continuation_token(buf);
            }
            if chunk_len < 0 {
                return Err(invalid_data(&format!(
                    "Invalid iterable chunk header: {chunk_len}"
                )));
            }
            match self.run_repeat(&element, chunk_len as u64, true, buf)? {
                RepeatProgress::Done => {}
                RepeatProgress::Yielded => return Ok(true),
                RepeatProgress::NeedMore => return Ok(false),
            }
        }
    }

    fn step_continuation_token(&mut self, buf: &[u8]) -> io::Result<bool> {
        let Some(token_len) = self.try_read_varint(buf)? else {
            self.stack.push(SkipFrame::ContinuationToken);
            return Ok(false);
        };
        let Ok(token_len) = usize::try_from(token_len) else {
            return Err(invalid_data(&format!(
                "Invalid continuation token length: {token_len}"
            )));
        };
        if self.try_skip_fixed(buf, token_len) {
            Ok(true)
        } else {
            self.stack.push(SkipFrame::FixedBytes(token_len));
            Ok(false)
        }
    }

    fn push_map_entry(&mut self, key: Arc<SkipNode>, value: Arc<SkipNode>, remaining: usize) {
        if remaining > 1 {
            self.stack.push(SkipFrame::MapEntries {
                key: Arc::clone(&key),
                value: Arc::clone(&value),
                remaining: remaining - 1,
            });
        }
        self.stack.push(SkipFrame::Node {
            node: value,
            nested: true,
        });
        self.stack.push(SkipFrame::Node {
            node: key,
            nested: true,
        });
    }

    fn push_next_row_field(
        &mut self,
        row: Arc<RowSkipNode>,
        bitmask_start: usize,
        bitmask_end: usize,
        mut next_field: usize,
        buf: &[u8],
    ) {
        let null_bits = buf.get(bitmask_start..bitmask_end).unwrap_or_default();
        while next_field < row.fields.len() && bit_is_null(null_bits, next_field) {
            next_field += 1;
        }
        let Some(field) = row.fields.get(next_field).map(Arc::clone) else {
            return;
        };
        next_field += 1;
        while next_field < row.fields.len() && bit_is_null(null_bits, next_field) {
            next_field += 1;
        }
        if next_field < row.fields.len() {
            self.stack.push(SkipFrame::RowFields {
                row,
                bitmask_start,
                bitmask_end,
                next_field,
            });
        }
        self.stack.push(SkipFrame::Node {
            node: field,
            nested: true,
        });
    }

    fn skip_length_prefixed(&mut self, buf: &[u8]) -> io::Result<bool> {
        let Some(len) = self.try_read_varint(buf)? else {
            self.stack.push(SkipFrame::LengthPrefix);
            return Ok(false);
        };
        let len =
            usize::try_from(len).map_err(|_| invalid_data(&format!("Negative length: {len}")))?;
        if self.try_skip_fixed(buf, len) {
            Ok(true)
        } else {
            self.stack.push(SkipFrame::FixedBytes(len));
            Ok(false)
        }
    }

    fn skip_pane_info(&mut self, buf: &[u8]) -> io::Result<bool> {
        let mut rest = buf.get(self.pos..).unwrap_or_default();
        match PaneInfo::decode(&mut rest) {
            Ok((_, has_metadata)) => {
                self.pos = buf.len() - rest.len();
                if has_metadata {
                    self.skip_length_prefixed(buf)
                } else {
                    Ok(true)
                }
            }
            Err(e) if e.kind() == ErrorKind::UnexpectedEof => {
                self.stack.push(SkipFrame::PaneInfo);
                Ok(false)
            }
            Err(e) => Err(e),
        }
    }

    fn try_skip_fixed(&mut self, buf: &[u8], len: usize) -> bool {
        let Some(avail) = buf.len().checked_sub(self.pos) else {
            return false;
        };
        if avail < len {
            return false;
        }
        self.pos += len;
        true
    }

    fn try_read_varint(&mut self, buf: &[u8]) -> io::Result<Option<i64>> {
        let Some(mut rest) = buf.get(self.pos..) else {
            return Ok(None);
        };
        match VarIntCoder::decode_varint(&mut rest) {
            Ok(value) => {
                self.pos = buf.len() - rest.len();
                Ok(Some(value))
            }
            Err(e) if e.kind() == ErrorKind::UnexpectedEof => Ok(None),
            Err(e) => Err(e),
        }
    }

    fn try_read_be_i32(&mut self, buf: &[u8]) -> Option<i32> {
        let bytes = buf.get(self.pos..)?.first_chunk::<4>()?;
        self.pos += 4;
        Some(i32::from_be_bytes(*bytes))
    }

    fn try_read_u8(&mut self, buf: &[u8]) -> Option<u8> {
        let &byte = buf.get(self.pos)?;
        self.pos += 1;
        Some(byte)
    }
}
