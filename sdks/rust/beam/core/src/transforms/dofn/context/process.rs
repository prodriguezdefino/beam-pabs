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
//! timestamp, window, pane, metadata and key of the current element.

use std::borrow::Cow;
use std::marker::PhantomData;
use std::sync::Arc;

use super::ProcessContext;
use crate::coders::{
    Coder, Context, DefaultCoder, ElementMetadata, IntervalWindow, IntervalWindowCoder, PaneInfo,
    WindowedHeader,
};
use crate::transforms::ElementSink;
use crate::transforms::dofn::side_input::SideInputReader;
use crate::transforms::dofn::state::UserStateReader;
use crate::transforms::dofn::timer::TimerCollector;

impl<'a, T> ProcessContext<'a, T> {
    /// Creates a context for `sink` with no side input reader and an empty header.
    pub fn new(sink: &'a mut dyn ElementSink) -> Self {
        Self {
            sink,
            reader: None,
            state_reader: None,
            timer_collector: None,
            residual_collector: None,
            metrics_container: None,
            bundle_finalizer: None,
            transform_id: Cow::Borrowed(""),
            header: WindowedHeader::EMPTY,
            key_bytes: None,
            _marker: PhantomData,
        }
    }

    pub fn with_context(
        sink: &'a mut dyn ElementSink,
        reader: Option<&'a dyn SideInputReader>,
        header: &'a WindowedHeader,
    ) -> Self {
        Self {
            sink,
            reader,
            state_reader: None,
            timer_collector: None,
            residual_collector: None,
            metrics_container: None,
            bundle_finalizer: None,
            transform_id: Cow::Borrowed(""),
            header,
            key_bytes: None,
            _marker: PhantomData,
        }
    }

    pub fn with_side_inputs(mut self, reader: &'a dyn SideInputReader) -> Self {
        self.reader = Some(reader);
        self
    }

    pub fn with_state_reader(mut self, reader: &'a Arc<dyn UserStateReader>) -> Self {
        self.state_reader = Some(reader);
        self
    }

    pub fn with_timer_collector(mut self, collector: &'a Arc<TimerCollector>) -> Self {
        self.timer_collector = Some(collector);
        self
    }

    pub fn with_metrics_container(
        mut self,
        metrics_container: Option<&'a Arc<crate::metrics::MetricsContainer>>,
    ) -> Self {
        self.metrics_container = metrics_container;
        self
    }

    pub fn with_bundle_finalizer(
        mut self,
        bundle_finalizer: Option<&'a Arc<super::BundleFinalizerCollector>>,
    ) -> Self {
        self.bundle_finalizer = bundle_finalizer;
        self
    }

    pub fn with_transform_id(mut self, transform_id: impl Into<Cow<'a, str>>) -> Self {
        self.transform_id = transform_id.into();
        self
    }

    pub fn transform_id(&self) -> &str {
        &self.transform_id
    }

    /// Sets the encoded key of the current element.
    pub fn with_key_bytes(mut self, key_bytes: impl Into<Cow<'a, [u8]>>) -> Self {
        self.key_bytes = Some(key_bytes.into());
        self
    }

    pub fn with_header(mut self, header: &'a WindowedHeader) -> Self {
        self.header = header;
        self
    }

    pub fn header(&self) -> &'a WindowedHeader {
        self.header
    }

    /// Returns the event timestamp of the element in milliseconds.
    pub fn timestamp(&self) -> i64 {
        self.header.timestamp_millis()
    }

    /// Returns the encoded window bytes of the element.
    pub fn window(&self) -> &'a [u8] {
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

    /// Decodes the window as an [`IntervalWindow`]. Returns `None` if it is empty or not an
    /// interval window.
    pub fn interval_window(&self) -> Option<IntervalWindow> {
        let window = self.window();
        if window.is_empty() {
            None
        } else {
            IntervalWindowCoder
                .decode(&mut std::io::Cursor::new(window), Context::Nested)
                .ok()
        }
    }

    /// Decodes the current key as `K`. Returns an error if there is no key: a key is present
    /// only for keyed input and timer firings.
    pub fn current_key<K: DefaultCoder>(&self) -> crate::Result<K> {
        let bytes = self
            .key_bytes
            .as_deref()
            .ok_or_else(|| "No active key in ProcessContext".to_string())?;
        K::decode(bytes).map_err(|e| crate::Error::from(e).context("Failed to decode current key"))
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
