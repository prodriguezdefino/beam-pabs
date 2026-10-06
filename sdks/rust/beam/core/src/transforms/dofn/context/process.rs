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

//! [`ProcessContext`] builders, which a runner calls before the `DoFn`, and accessors for the
//! timestamp, pane, metadata and key of the current element.

use std::borrow::Cow;

use super::ProcessContext;
use crate::Instant;
use crate::coders::{ElementMetadata, PaneInfo, WindowedHeader};

impl<'a, T> ProcessContext<'a, T> {
    /// Sets the encoded key of the current element.
    pub(crate) fn with_key_bytes(mut self, key_bytes: impl Into<Cow<'a, [u8]>>) -> Self {
        self.key_bytes = Some(key_bytes.into());
        self
    }

    pub(crate) fn with_header(mut self, header: &'a WindowedHeader) -> Self {
        self.header = header;
        self
    }

    /// The encoded header of the current element. User code reads the window through
    /// [`Windowed`](crate::transforms::Windowed); SDK crates use
    /// [`internals::element_header`](crate::internals::element_header).
    pub(crate) fn header(&self) -> &'a WindowedHeader {
        self.header
    }

    /// The event timestamp of the element.
    pub fn timestamp(&self) -> Instant {
        Instant::from_millis(self.header.timestamp_millis())
    }

    /// The encoded windows of the current element: one window when the element was split per
    /// window, several otherwise.
    pub(crate) fn window_bytes(&self) -> &'a [u8] {
        self.header.window_bytes()
    }

    /// Returns the trigger firing this element belongs to.
    pub fn pane(&self) -> PaneInfo {
        self.header.pane()
    }

    /// Returns the element metadata the runner attached, if any.
    pub fn metadata(&self) -> ElementMetadata {
        self.header.metadata()
    }

    /// Returns `true` during a pipeline drain, which stops the sources and lets in-flight work
    /// finish. A `DoFn` that schedules more work, such as a far-future timer, should stop then.
    pub fn is_draining(&self) -> bool {
        self.header.metadata().is_draining()
    }
}

impl<T> std::fmt::Debug for ProcessContext<'_, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProcessContext")
            .field("timestamp", &self.header.timestamp_millis())
            .field("pane", &self.header.pane())
            .finish_non_exhaustive()
    }
}
