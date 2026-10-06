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

//! [`WindowedHeader`]: the encoded timestamp, windows, pane and metadata that precede an
//! element in a `beam:coder:windowed_value:v1` encoding.

use std::borrow::Cow;
use std::io::Write;

use super::metadata::ElementMetadata;
use super::pane::PaneInfo;
use super::standard::{decode_timestamp, encode_timestamp};

/// Longest header kept inline. A global-window header is 13 bytes and a one-interval-window
/// header is about 26. Only headers with many windows, such as sliding windows, use the heap.
const INLINE_HEADER: usize = 30;

/// Header bytes, inline if short and shared otherwise. A clone never copies a heap buffer.
#[derive(Clone)]
enum HeaderBytes {
    Inline { len: u8, buf: [u8; INLINE_HEADER] },
    Shared(std::sync::Arc<[u8]>),
}

impl HeaderBytes {
    const EMPTY: Self = Self::Inline {
        len: 0,
        buf: [0; INLINE_HEADER],
    };

    fn from_slice(bytes: &[u8]) -> Self {
        let mut buf = [0; INLINE_HEADER];
        match buf.get_mut(..bytes.len()) {
            Some(dst) => {
                dst.copy_from_slice(bytes);
                Self::Inline {
                    len: bytes.len() as u8,
                    buf,
                }
            }
            None => Self::Shared(bytes.into()),
        }
    }

    fn as_slice(&self) -> &[u8] {
        match self {
            Self::Inline { len, buf } => buf.get(..*len as usize).unwrap_or_default(),
            Self::Shared(bytes) => bytes,
        }
    }
}

/// Builds header bytes on the stack. Spills to a `Vec` only past [`INLINE_HEADER`] bytes.
struct HeaderWriter {
    buf: [u8; INLINE_HEADER],
    len: usize,
    spill: Option<Vec<u8>>,
}

impl HeaderWriter {
    fn new() -> Self {
        Self {
            buf: [0; INLINE_HEADER],
            len: 0,
            spill: None,
        }
    }

    fn len(&self) -> usize {
        self.spill.as_ref().map_or(self.len, Vec::len)
    }

    /// Appends `data`. Writing to memory cannot fail.
    fn put(&mut self, data: &[u8]) {
        let end = self.len + data.len();
        if let (None, Some(dst)) = (&self.spill, self.buf.get_mut(self.len..end)) {
            dst.copy_from_slice(data);
            self.len = end;
        } else {
            let inline = self.buf.get(..self.len).unwrap_or_default();
            self.spill
                .get_or_insert_with(|| inline.to_vec())
                .extend_from_slice(data);
        }
    }

    fn finish(self) -> HeaderBytes {
        match self.spill {
            Some(spill) => HeaderBytes::Shared(spill.into()),
            None => HeaderBytes::Inline {
                len: self.len as u8,
                buf: self.buf,
            },
        }
    }
}

impl Write for HeaderWriter {
    fn write(&mut self, data: &[u8]) -> std::io::Result<usize> {
        self.put(data);
        Ok(data.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// The header of a `beam:coder:windowed_value:v1` encoding: all bytes before the element.
/// It holds the wire bytes and the pane offset. The pane has a variable length and the window
/// coder is not known at every place, so record the pane offset where it is known: counting
/// back from the end is correct only for a one-byte pane. Short bytes are inline and long
/// bytes are shared, so a per-element clone never allocates.
#[derive(Clone)]
pub struct WindowedHeader {
    bytes: HeaderBytes,
    pane_start: usize,
}

impl Default for WindowedHeader {
    fn default() -> Self {
        Self::EMPTY.clone()
    }
}

impl PartialEq for WindowedHeader {
    fn eq(&self, other: &Self) -> bool {
        self.pane_start == other.pane_start && self.as_bytes() == other.as_bytes()
    }
}

impl Eq for WindowedHeader {}

impl std::fmt::Debug for WindowedHeader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WindowedHeader")
            .field("bytes", &self.as_bytes())
            .field("pane_start", &self.pane_start)
            .finish()
    }
}

impl WindowedHeader {
    /// Offset of the first window: an 8-byte timestamp followed by a 4-byte window count.
    pub const WINDOWS_START: usize = 12;

    /// No header, for coders that carry no per-element metadata.
    pub const EMPTY: &'static Self = &Self {
        bytes: HeaderBytes::EMPTY,
        pane_start: 0,
    };

    /// The global window as an encoded window list: one window, zero bytes wide. This is not
    /// an element in no window, which the encoder must not write.
    const GLOBAL_WINDOWS: &'static [Vec<u8>] = &[Vec::new()];

    /// Encodes a header from encoded `windows`. An empty `windows` means a lost window set,
    /// not the global window: use [`WindowedHeader::global`] for that.
    pub fn new(timestamp_millis: i64, windows: &[Vec<u8>], pane: PaneInfo) -> Self {
        Self::with_metadata(timestamp_millis, windows, pane, &ElementMetadata::default())
    }

    /// Encodes a header for an element in the global window.
    pub fn global(timestamp_millis: i64, pane: PaneInfo) -> Self {
        Self::new(timestamp_millis, Self::GLOBAL_WINDOWS, pane)
    }

    /// Encodes a header with element metadata for an element in the global window.
    pub fn global_with_metadata(
        timestamp_millis: i64,
        pane: PaneInfo,
        metadata: &ElementMetadata,
    ) -> Self {
        Self::with_metadata(timestamp_millis, Self::GLOBAL_WINDOWS, pane, metadata)
    }

    /// Encodes a header with element metadata from already-encoded `windows`.
    pub fn with_metadata(
        timestamp_millis: i64,
        windows: &[Vec<u8>],
        pane: PaneInfo,
        metadata: &ElementMetadata,
    ) -> Self {
        let mut bytes = HeaderWriter::new();
        bytes.put(&encode_timestamp(timestamp_millis));
        bytes.put(&(windows.len() as i32).to_be_bytes());
        windows.iter().for_each(|w| bytes.put(w));
        Self::finish_with_pane(bytes, pane, metadata)
    }

    /// Appends the pane and metadata to a header whose windows are already written.
    fn finish_with_pane(
        mut bytes: HeaderWriter,
        pane: PaneInfo,
        metadata: &ElementMetadata,
    ) -> Self {
        let pane_start = bytes.len();
        let has_metadata = !metadata.is_default();
        let _ = pane.encode(has_metadata, &mut bytes);
        if has_metadata {
            let _ = metadata.encode(&mut bytes);
        }
        Self {
            bytes: bytes.finish(),
            pane_start,
        }
    }

    /// Wraps header bytes read from the wire. The pane starts at offset `pane_start`.
    pub fn from_wire(bytes: &[u8], pane_start: usize) -> Self {
        Self {
            bytes: HeaderBytes::from_slice(bytes),
            pane_start,
        }
    }

    /// Returns true if there is no header, as for a coder with no per-element metadata.
    pub fn is_empty(&self) -> bool {
        self.as_bytes().is_empty()
    }

    /// Returns the header exactly as it is on the wire.
    pub fn as_bytes(&self) -> &[u8] {
        self.bytes.as_slice()
    }

    /// Returns the event timestamp, or 0 if there is no header.
    pub fn timestamp_millis(&self) -> i64 {
        self.as_bytes()
            .first_chunk::<8>()
            .map_or(0, |ts| decode_timestamp(*ts))
    }

    /// Returns the number of windows in the header, or 0 if there is no header.
    pub fn window_count(&self) -> usize {
        self.as_bytes()
            .get(8..Self::WINDOWS_START)
            .and_then(|b| b.first_chunk::<4>())
            .map_or(0, |c| i32::from_be_bytes(*c).max(0) as usize)
    }

    /// Returns true if the element is in more than one window, as after `SlidingWindows`.
    pub fn is_multi_window(&self) -> bool {
        self.window_count() > 1
    }

    /// Returns the encoded windows as opaque bytes, because the window coder is not known.
    pub fn window_bytes(&self) -> &[u8] {
        self.as_bytes()
            .get(Self::WINDOWS_START..self.pane_start)
            .unwrap_or_default()
    }

    /// Returns one single-window header per window, or `self` if it has at most one window.
    /// Fails if the windows are not interval windows, the only kind an element can have many of.
    pub fn explode(&self) -> Result<impl Iterator<Item = Cow<'_, Self>>, String> {
        let windows = match self.is_multi_window() {
            true => window_slices(self.window_bytes(), self.window_count())?,
            false => Vec::new(),
        };
        let unchanged = windows.is_empty().then_some(Cow::Borrowed(self));
        let split = windows
            .into_iter()
            .map(|w| Cow::Owned(self.with_single_window(w)));
        Ok(unchanged.into_iter().chain(split))
    }

    /// Returns `self` with only `window`, one of its windows, which bounds the slices below.
    fn with_single_window(&self, window: &[u8]) -> Self {
        let raw = self.as_bytes();
        let mut bytes = HeaderWriter::new();
        bytes.put(&raw[..8]);
        bytes.put(&1i32.to_be_bytes());
        bytes.put(window);
        let pane_start = bytes.len();
        bytes.put(&raw[self.pane_start..]);
        Self {
            bytes: bytes.finish(),
            pane_start,
        }
    }

    /// Returns the pane, or [`PaneInfo::NO_FIRING`] if there is no header.
    pub fn pane(&self) -> PaneInfo {
        self.as_bytes()
            .get(self.pane_start..)
            .and_then(|mut rest| PaneInfo::decode(&mut rest).ok())
            .map_or(PaneInfo::NO_FIRING, |(pane, _)| pane)
    }

    /// Returns the element metadata, or the default if the runner sent none.
    pub fn metadata(&self) -> ElementMetadata {
        let Some(mut rest) = self.as_bytes().get(self.pane_start..) else {
            return ElementMetadata::default();
        };
        match PaneInfo::decode(&mut rest) {
            Ok((_, true)) => ElementMetadata::decode(&mut rest).unwrap_or_default(),
            _ => ElementMetadata::default(),
        }
    }

    /// Returns a header with the same windows and a new timestamp, pane and metadata. The
    /// windows are copied byte for byte, because the window coder is not known.
    pub fn rebuilt(
        &self,
        timestamp_millis: i64,
        pane: PaneInfo,
        metadata: &ElementMetadata,
    ) -> Self {
        let windows = self.as_bytes().get(8..self.pane_start);
        let Some(windows) = windows.filter(|_| self.pane_start >= Self::WINDOWS_START) else {
            // There is no header to copy, so the element goes to the global window.
            return Self::global_with_metadata(timestamp_millis, pane, metadata);
        };
        let mut bytes = HeaderWriter::new();
        bytes.put(&encode_timestamp(timestamp_millis));
        bytes.put(windows);
        Self::finish_with_pane(bytes, pane, metadata)
    }
}

/// Returns the next nested-context interval window: an 8-byte end, then a varint span.
fn next_interval_window<'a>(cursor: &mut std::io::Cursor<&'a [u8]>) -> Option<&'a [u8]> {
    let data = *cursor.get_ref();
    let start = cursor.position() as usize;
    let after_ts = start.checked_add(8).filter(|&end| end <= data.len())?;
    cursor.set_position(after_ts as u64);
    super::standard::VarIntCoder::decode_varint(cursor).ok()?;
    data.get(start..cursor.position() as usize)
}

/// Splits `bytes` into `count` interval windows that use every byte.
fn window_slices(bytes: &[u8], count: usize) -> Result<Vec<&[u8]>, String> {
    let mut cursor = std::io::Cursor::new(bytes);
    (0..count)
        .map(|_| next_interval_window(&mut cursor))
        .collect::<Option<Vec<_>>>()
        .filter(|_| cursor.position() as usize == bytes.len())
        .ok_or_else(|| format!("Cannot split {count} windows: they are not interval windows"))
}
