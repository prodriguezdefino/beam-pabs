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

//! Window types and coders for the global window, interval windows and windowed values.

use std::io::{Cursor, Read, Write};

use super::metadata::ElementMetadata;
use super::pane::PaneInfo;
use super::standard::{VarIntCoder, encode_timestamp, read_be_i32, read_timestamp};
use super::traits::{Coder, CoderError, Context};
use super::traversal::skip_length_prefixed;
use super::{URN_GLOBAL_WINDOW, URN_INTERVAL_WINDOW, URN_PARAM_WINDOWED_VALUE, URN_WINDOWED_VALUE};

/// An interval window `[start_millis, end_millis)`, in milliseconds.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct IntervalWindow {
    pub start_millis: i64,
    pub end_millis: i64,
}

impl IntervalWindow {
    pub fn new(start_millis: i64, end_millis: i64) -> Self {
        Self {
            start_millis,
            end_millis,
        }
    }

    pub fn from_end_and_span(end_millis: i64, span_millis: i64) -> Self {
        Self {
            start_millis: end_millis - span_millis,
            end_millis,
        }
    }

    pub fn span_millis(&self) -> i64 {
        self.end_millis - self.start_millis
    }

    /// Returns the maximum inclusive event timestamp that belongs to this window.
    pub fn max_timestamp(&self) -> i64 {
        self.end_millis.saturating_sub(1)
    }

    pub fn contains(&self, timestamp_millis: i64) -> bool {
        self.start_millis <= timestamp_millis && timestamp_millis < self.end_millis
    }

    pub fn intersects(&self, other: &IntervalWindow) -> bool {
        self.start_millis < other.end_millis && other.start_millis < self.end_millis
    }

    /// Returns the minimal window that spans both `self` and `other`.
    pub fn span(&self, other: &IntervalWindow) -> IntervalWindow {
        IntervalWindow::new(
            self.start_millis.min(other.start_millis),
            self.end_millis.max(other.end_millis),
        )
    }
}

/// Coder for [`IntervalWindow`]: `end_millis` as an 8-byte Beam timestamp, then `span_millis`
/// as a varint.
#[derive(Clone, Debug, Default)]
pub struct IntervalWindowCoder;

impl Coder<IntervalWindow> for IntervalWindowCoder {
    fn urn(&self) -> &'static str {
        URN_INTERVAL_WINDOW
    }

    fn encode(
        &self,
        value: &IntervalWindow,
        writer: &mut dyn Write,
        _context: Context,
    ) -> Result<(), CoderError> {
        writer.write_all(&encode_timestamp(value.end_millis))?;
        VarIntCoder::encode_varint(value.span_millis(), writer)?;
        Ok(())
    }

    fn decode(
        &self,
        reader: &mut dyn Read,
        _context: Context,
    ) -> Result<IntervalWindow, CoderError> {
        let end_millis = read_timestamp(reader)?;
        let span_millis = VarIntCoder::decode_varint(reader)?;
        Ok(IntervalWindow::from_end_and_span(end_millis, span_millis))
    }
}

/// The global window. Its coder writes 0 bytes.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct GlobalWindow;

#[derive(Clone, Debug, Default)]
pub struct GlobalWindowCoder;

impl Coder<GlobalWindow> for GlobalWindowCoder {
    fn urn(&self) -> &'static str {
        URN_GLOBAL_WINDOW
    }

    fn encode(
        &self,
        _value: &GlobalWindow,
        _writer: &mut dyn Write,
        _context: Context,
    ) -> Result<(), CoderError> {
        Ok(())
    }

    fn decode(
        &self,
        _reader: &mut dyn Read,
        _context: Context,
    ) -> Result<GlobalWindow, CoderError> {
        Ok(GlobalWindow)
    }
}

/// A value with its timestamp, windows, pane and element metadata.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WindowedValue<T, W = GlobalWindow> {
    pub value: T,
    pub timestamp_millis: i64,
    pub windows: Vec<W>,
    pub pane: PaneInfo,
    pub metadata: ElementMetadata,
}

impl<T> WindowedValue<T, GlobalWindow> {
    pub fn global(value: T, timestamp_millis: i64) -> Self {
        Self::new(
            value,
            timestamp_millis,
            vec![GlobalWindow],
            PaneInfo::NO_FIRING,
        )
    }
}

impl<T, W> WindowedValue<T, W> {
    pub fn new(value: T, timestamp_millis: i64, windows: Vec<W>, pane: PaneInfo) -> Self {
        Self {
            value,
            timestamp_millis,
            windows,
            pane,
            metadata: ElementMetadata::default(),
        }
    }

    pub fn with_metadata(mut self, metadata: ElementMetadata) -> Self {
        self.metadata = metadata;
        self
    }

    /// Copies this value's windowing and metadata onto a different value.
    pub fn with_value<U>(&self, value: U) -> WindowedValue<U, W>
    where
        W: Clone,
    {
        WindowedValue {
            value,
            timestamp_millis: self.timestamp_millis,
            windows: self.windows.clone(),
            pane: self.pane,
            metadata: self.metadata.clone(),
        }
    }

    /// Writes timestamp, windows, pane and element metadata. Skips default metadata.
    fn encode_header<WC: Coder<W>>(
        &self,
        window_coder: &WC,
        writer: &mut dyn Write,
    ) -> Result<(), CoderError> {
        writer.write_all(&encode_timestamp(self.timestamp_millis))?;

        // A 32-bit big-endian window count, then each window in the nested context.
        writer.write_all(&(self.windows.len() as i32).to_be_bytes())?;
        self.windows
            .iter()
            .try_for_each(|w| window_coder.encode(w, writer, Context::Nested))?;

        let has_metadata = !self.metadata.is_default();
        self.pane.encode(has_metadata, writer)?;
        if has_metadata {
            self.metadata.encode(writer)?;
        }
        Ok(())
    }
}

impl<W> WindowedValue<(), W> {
    /// Reads all fields before the value, and leaves `reader` at the start of the value.
    fn decode_header<WC: Coder<W>>(
        reader: &mut dyn Read,
        window_coder: &WC,
    ) -> Result<Self, CoderError> {
        let timestamp_millis = read_timestamp(reader)?;

        // A negative count would read zero windows and start the pane read at the wrong byte.
        let win_count = read_be_i32(reader)?;
        let win_count = usize::try_from(win_count).map_err(|_| {
            CoderError::Format(format!("windowed value declares {win_count} windows"))
        })?;
        let windows = (0..win_count)
            .map(|_| window_coder.decode(&mut *reader, Context::Nested))
            .collect::<Result<Vec<_>, _>>()?;

        let (pane, has_metadata) = PaneInfo::decode(reader)?;
        let metadata = has_metadata
            .then(|| ElementMetadata::decode(reader))
            .transpose()?
            .unwrap_or_default();

        Ok(Self {
            value: (),
            timestamp_millis,
            windows,
            pane,
            metadata,
        })
    }
}
/// Coder for [`WindowedValue`], with an element coder and a window coder.
#[derive(Clone, Debug)]
pub struct WindowedValueCoder<T, C: Coder<T>, W = GlobalWindow, WC: Coder<W> = GlobalWindowCoder> {
    element_coder: C,
    window_coder: WC,
    _marker: std::marker::PhantomData<(T, W)>,
}

impl<T, C: Coder<T>> WindowedValueCoder<T, C, GlobalWindow, GlobalWindowCoder> {
    pub fn new(element_coder: C) -> Self {
        Self {
            element_coder,
            window_coder: GlobalWindowCoder,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<T, C: Coder<T>, W, WC: Coder<W>> WindowedValueCoder<T, C, W, WC> {
    pub fn with_window_coder(element_coder: C, window_coder: WC) -> Self {
        Self {
            element_coder,
            window_coder,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<T: Send + Sync + 'static, C: Coder<T>, W: Clone + Send + Sync + 'static, WC: Coder<W>>
    Coder<WindowedValue<T, W>> for WindowedValueCoder<T, C, W, WC>
{
    fn urn(&self) -> &'static str {
        URN_WINDOWED_VALUE
    }

    fn encode(
        &self,
        value: &WindowedValue<T, W>,
        writer: &mut dyn Write,
        context: Context,
    ) -> Result<(), CoderError> {
        value.encode_header(&self.window_coder, writer)?;
        self.element_coder.encode(&value.value, writer, context)
    }

    fn decode(
        &self,
        reader: &mut dyn Read,
        context: Context,
    ) -> Result<WindowedValue<T, W>, CoderError> {
        let header = WindowedValue::decode_header(&mut *reader, &self.window_coder)?;
        let value = self.element_coder.decode(reader, context)?;
        Ok(header.with_value(value))
    }
}

/// Coder for windowed values whose timestamp, windows and pane are constants of the coder.
/// Only the element is on the wire. The payload holds the constants as a
/// `beam:coder:windowed_value:v1` encoding with a bytes element and this coder's window coder.
#[derive(Clone, Debug)]
pub struct ParamWindowedValueCoder<T, C: Coder<T>, W = GlobalWindow> {
    element_coder: C,
    constants: WindowedValue<(), W>,
    payload: Option<Vec<u8>>,
    _marker: std::marker::PhantomData<T>,
}

impl<T, C: Coder<T>, W> ParamWindowedValueCoder<T, C, W> {
    /// Builds the coder from the given constants, without a payload.
    pub fn new(element_coder: C, constants: WindowedValue<(), W>) -> Self {
        Self {
            element_coder,
            constants,
            payload: None,
            _marker: std::marker::PhantomData,
        }
    }

    /// Builds the coder from a `beam:coder:param_windowed_value:v1` payload. Accepts trailing
    /// bytes after the placeholder element, because other SDKs do not check for them.
    pub fn from_payload<WC: Coder<W>>(
        element_coder: C,
        window_coder: &WC,
        payload: &[u8],
    ) -> Result<Self, CoderError> {
        let mut cursor = Cursor::new(payload);
        let constants = WindowedValue::decode_header(&mut cursor, window_coder)?;

        // Skip the placeholder bytes element. The read checks that the payload has this shape.
        skip_length_prefixed(&mut cursor)?;

        Ok(Self {
            element_coder,
            constants,
            payload: Some(payload.to_vec()),
            _marker: std::marker::PhantomData,
        })
    }

    /// Returns the timestamp, windows, pane and metadata that every decoded element gets.
    pub fn constants(&self) -> &WindowedValue<(), W> {
        &self.constants
    }

    /// Returns the parsed payload, so the proto can be written again byte for byte, or `None`
    /// if the coder was built from constants.
    pub fn payload(&self) -> Option<&[u8]> {
        self.payload.as_deref()
    }
}

impl<T: Send + Sync + 'static, C: Coder<T>, W: Clone + Send + Sync + 'static>
    Coder<WindowedValue<T, W>> for ParamWindowedValueCoder<T, C, W>
{
    fn urn(&self) -> &'static str {
        URN_PARAM_WINDOWED_VALUE
    }

    fn encode(
        &self,
        value: &WindowedValue<T, W>,
        writer: &mut dyn Write,
        context: Context,
    ) -> Result<(), CoderError> {
        // Do not write the header. The coder payload holds it.
        self.element_coder.encode(&value.value, writer, context)
    }

    fn decode(
        &self,
        reader: &mut dyn Read,
        context: Context,
    ) -> Result<WindowedValue<T, W>, CoderError> {
        let value = self.element_coder.decode(reader, context)?;
        Ok(self.constants.with_value(value))
    }
}
