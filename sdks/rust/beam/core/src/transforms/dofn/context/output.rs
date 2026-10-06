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

//! Output emission from a `DoFn`. [`OutputBuilder`] overrides fields for one element.

use super::ProcessContext;
use crate::Instant;
use crate::coders::{CausedByDrain, ElementMetadata, PaneInfo, ValueKind, WindowedHeader};
use crate::transforms::dofn::outputs::{Emitter, Outputs};

impl<'a, T: Outputs> ProcessContext<'a, T> {
    /// Emits `value` with the timestamp, window and pane of the current element.
    pub fn emit(&mut self, value: T) -> crate::Result {
        self.output(value).emit()
    }

    pub fn emit_all<I: IntoIterator<Item = T>>(&mut self, values: I) -> crate::Result {
        values.into_iter().try_for_each(|v| self.emit(v))
    }

    /// Starts an output element whose timestamp, pane or metadata can change.
    /// `ctx.output(v).emit()` is the same as `ctx.emit(v)`.
    pub fn output(&mut self, value: T) -> OutputBuilder<'_, 'a, T> {
        OutputBuilder::new(self, value)
    }
}

/// An output element under construction. Unset fields come from the current element.
pub struct OutputBuilder<'ctx, 'a, T> {
    ctx: &'ctx mut ProcessContext<'a, T>,
    value: T,
    header: Option<&'ctx WindowedHeader>,
    timestamp: Option<Instant>,
    pane: Option<PaneInfo>,
    metadata: Option<ElementMetadata>,
}

impl<'ctx, 'a, T> OutputBuilder<'ctx, 'a, T> {
    fn new(ctx: &'ctx mut ProcessContext<'a, T>, value: T) -> Self {
        Self {
            ctx,
            value,
            header: None,
            timestamp: None,
            pane: None,
            metadata: None,
        }
    }

    /// Returns the header that unset fields inherit from.
    fn base_header(&self) -> &WindowedHeader {
        self.header.unwrap_or(self.ctx.header)
    }

    /// Overrides the event timestamp.
    pub fn at(mut self, timestamp: Instant) -> Self {
        self.timestamp = Some(timestamp);
        self
    }

    /// Uses `header` as the base instead of the header of the current element.
    pub(crate) fn windowed(mut self, header: &'ctx WindowedHeader) -> Self {
        self.header = Some(header);
        self
    }

    /// Overrides the trigger firing that the element belongs to.
    pub fn with_pane(mut self, pane: PaneInfo) -> Self {
        self.pane = Some(pane);
        self
    }

    fn metadata_mut(&mut self) -> &mut ElementMetadata {
        if self.metadata.is_none() {
            self.metadata = Some(self.base_header().metadata());
        }
        self.metadata.get_or_insert_with(ElementMetadata::default)
    }

    /// Marks the element as produced by a drain, or not.
    pub fn with_drain(mut self, drain: CausedByDrain) -> Self {
        self.metadata_mut().drain = drain;
        self
    }

    /// Sets the change-data-capture operation the element represents.
    pub fn with_value_kind(mut self, value_kind: ValueKind) -> Self {
        self.metadata_mut().value_kind = value_kind;
        self
    }

    /// Attaches a W3C trace context to the element.
    pub fn with_trace(
        mut self,
        traceparent: impl Into<String>,
        tracestate: Option<String>,
    ) -> Self {
        let metadata = self.metadata_mut();
        metadata.traceparent = Some(traceparent.into());
        metadata.tracestate = tracestate;
        self
    }

    /// Replaces all element metadata.
    pub fn with_metadata(mut self, metadata: ElementMetadata) -> Self {
        self.metadata = Some(metadata);
        self
    }
}

impl<T: Outputs> OutputBuilder<'_, '_, T> {
    pub fn emit(self) -> crate::Result {
        let overridden = self.timestamp.is_some() || self.pane.is_some() || self.metadata.is_some();
        let rebuilt = overridden.then(|| {
            let base = self.base_header();
            base.rebuilt(
                self.timestamp
                    .map_or_else(|| base.timestamp_millis(), Instant::as_millis),
                self.pane.unwrap_or_else(|| base.pane()),
                &self.metadata.clone().unwrap_or_else(|| base.metadata()),
            )
        });
        let header = rebuilt.as_ref().or(self.header).or_else(|| {
            // Without a header, the sink supplies its own, as `ProcessContext::emit` does.
            (!self.ctx.header.is_empty()).then_some(self.ctx.header)
        });
        Ok(self.value.emit_to(Emitter::new(self.ctx.sink, header))?)
    }
}
