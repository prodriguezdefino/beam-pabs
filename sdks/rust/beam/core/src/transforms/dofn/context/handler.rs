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

//! The byte-level context for a [`BundleHandler`](crate::internals::BundleHandler): the
//! untyped form of [`ProcessContext`]. Convert it with [`HandlerContext::as_process_context`].

use std::borrow::Cow;
use std::marker::PhantomData;
use std::sync::Arc;

use super::{BundleFinalizerCollector, ProcessContext, ResidualApplication, ResidualCollector};
use crate::coders::{ElementMetadata, PaneInfo, WindowedHeader};
use crate::transforms::ElementSink;
use crate::transforms::dofn::side_input::SideInputReader;
use crate::transforms::dofn::state::UserStateReader;
use crate::transforms::dofn::timer::TimerCollector;

/// Carries the output sink, the readers, the collectors, and the windowed-value header and
/// encoded key of the element. It uses no thread-local state.
pub struct HandlerContext<'a> {
    pub sink: &'a mut dyn ElementSink,
    pub side_inputs: Option<&'a dyn SideInputReader>,
    pub state_reader: Option<&'a Arc<dyn UserStateReader>>,
    pub timer_collector: Option<&'a Arc<TimerCollector>>,
    pub residual_collector: Option<&'a Arc<ResidualCollector>>,
    pub metrics_container: Option<&'a Arc<crate::metrics::MetricsContainer>>,
    pub bundle_finalizer: Option<&'a Arc<BundleFinalizerCollector>>,
    pub state_stream_reader: Option<&'a Arc<dyn crate::coders::StateStreamReader>>,
    /// Borrowed from the operator graph on the hot path; owned only when a caller gives a `String`.
    pub transform_id: Cow<'a, str>,
    /// Timestamp, windows, pane and element metadata for the element being processed.
    pub header: &'a WindowedHeader,
    /// Borrowed from the current element when possible, so keyed input does not copy the key.
    pub key_bytes: Option<Cow<'a, [u8]>>,
    pub input_schema: Option<&'a Arc<crate::schema::Schema>>,
}

impl<'a> HandlerContext<'a> {
    /// Creates a `HandlerContext` for `sink` with no readers, no collectors and an empty header.
    pub fn new(sink: &'a mut dyn ElementSink) -> Self {
        Self {
            sink,
            side_inputs: None,
            state_reader: None,
            timer_collector: None,
            residual_collector: None,
            metrics_container: None,
            bundle_finalizer: None,
            state_stream_reader: None,
            transform_id: Cow::Borrowed(""),
            header: WindowedHeader::EMPTY,
            key_bytes: None,
            input_schema: None,
        }
    }

    pub fn with_header(mut self, header: &'a WindowedHeader) -> Self {
        self.header = header;
        self
    }

    pub fn with_side_inputs(mut self, side_inputs: Option<&'a dyn SideInputReader>) -> Self {
        self.side_inputs = side_inputs;
        self
    }

    pub fn with_state_reader(mut self, state_reader: Option<&'a Arc<dyn UserStateReader>>) -> Self {
        self.state_reader = state_reader;
        self
    }

    pub fn with_timer_collector(
        mut self,
        timer_collector: Option<&'a Arc<TimerCollector>>,
    ) -> Self {
        self.timer_collector = timer_collector;
        self
    }

    pub fn with_residual_collector(
        mut self,
        residual_collector: Option<&'a Arc<ResidualCollector>>,
    ) -> Self {
        self.residual_collector = residual_collector;
        self
    }

    /// Attaches a state stream reader for runner-backed continuation streams.
    pub fn with_state_stream_reader(
        mut self,
        state_stream_reader: Option<&'a Arc<dyn crate::coders::StateStreamReader>>,
    ) -> Self {
        self.state_stream_reader = state_stream_reader;
        self
    }

    pub fn with_transform_id(mut self, transform_id: impl Into<Cow<'a, str>>) -> Self {
        self.transform_id = transform_id.into();
        self
    }

    /// Adds a residual application to return to the runner.
    pub fn add_residual(&self, application: ResidualApplication) {
        if let Some(c) = self.residual_collector {
            c.add(application);
        }
    }

    pub fn transform_id(&self) -> &str {
        &self.transform_id
    }

    /// Sets the encoded key of the current element, for a keyed PCollection or a timer.
    pub fn with_key_bytes(mut self, key_bytes: Option<impl Into<Cow<'a, [u8]>>>) -> Self {
        self.key_bytes = key_bytes.map(Into::into);
        self
    }

    /// Sets the Row schema of the current element.
    pub fn with_input_schema(
        mut self,
        input_schema: Option<&'a Arc<crate::schema::Schema>>,
    ) -> Self {
        self.input_schema = input_schema;
        self
    }

    /// Returns the event timestamp in milliseconds.
    pub fn timestamp(&self) -> i64 {
        self.header.timestamp_millis()
    }

    /// Returns the encoded window bytes.
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

    pub fn with_metrics_container(
        mut self,
        metrics_container: Option<&'a Arc<crate::metrics::MetricsContainer>>,
    ) -> Self {
        self.metrics_container = metrics_container;
        self
    }

    pub fn metrics_container(&self) -> Option<&Arc<crate::metrics::MetricsContainer>> {
        self.metrics_container
    }

    pub fn with_bundle_finalizer(
        mut self,
        bundle_finalizer: Option<&'a Arc<BundleFinalizerCollector>>,
    ) -> Self {
        self.bundle_finalizer = bundle_finalizer;
        self
    }

    pub fn header(&self) -> &WindowedHeader {
        self.header
    }

    /// Converts to a typed [`ProcessContext`] for a `DoFn`.
    pub fn as_process_context<T>(&mut self) -> ProcessContext<'_, T> {
        ProcessContext {
            sink: self.sink,
            reader: self.side_inputs,
            state_reader: self.state_reader,
            timer_collector: self.timer_collector,
            residual_collector: self.residual_collector,
            metrics_container: self.metrics_container,
            bundle_finalizer: self.bundle_finalizer,
            transform_id: Cow::Borrowed(&self.transform_id),
            header: self.header,
            key_bytes: self.key_bytes.as_deref().map(Cow::Borrowed),
            _marker: PhantomData,
        }
    }
}
