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

use std::borrow::Cow;

use super::ProcessContext;
use crate::coders::{
    CausedByDrain, DefaultCoder, ElementMetadata, PaneInfo, ValueKind, WindowedHeader,
};
use crate::transforms::TypedElement;
use crate::transforms::failure::{FAILURES_TAG, Failure};

/// An output tag of a multi-output `DoFn`, for [`OutputBuilder::to`]. Build it from a name,
/// or from an index for the tags `"0"`, `"1"`, ..., as in
/// [`Partition`](crate::transforms::Partition).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OutputTag<'t>(Cow<'t, str>);

impl OutputTag<'_> {
    /// Returns the tag as it appears in the pipeline graph.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl<'t> From<&'t str> for OutputTag<'t> {
    fn from(tag: &'t str) -> Self {
        Self(Cow::Borrowed(tag))
    }
}

impl<'t> From<&'t String> for OutputTag<'t> {
    fn from(tag: &'t String) -> Self {
        Self(Cow::Borrowed(tag))
    }
}

impl From<String> for OutputTag<'_> {
    fn from(tag: String) -> Self {
        Self(Cow::Owned(tag))
    }
}

impl From<usize> for OutputTag<'_> {
    fn from(index: usize) -> Self {
        Self(Cow::Owned(index.to_string()))
    }
}

impl<'a, T: DefaultCoder> ProcessContext<'a, T> {
    /// Emits `value` to the main output with the header of the current element.
    pub fn emit(&mut self, value: T) -> crate::Result {
        self.output(value).emit()
    }

    pub fn emit_all<I: IntoIterator<Item = T>>(&mut self, values: I) -> crate::Result {
        values.into_iter().try_for_each(|v| self.emit(v))
    }

    /// Routes `input` and `error` to the failures output of a
    /// [`TryParDo`](crate::transforms::TryParDo). The types must match the `Failure<I, E>` of
    /// the transform; for the default `Failure<I>`, pass the error as a `String`.
    pub fn emit_failure<I: DefaultCoder, E: DefaultCoder>(
        &mut self,
        input: I,
        error: E,
    ) -> crate::Result {
        self.output_to(FAILURES_TAG, Failure { input, error })
            .emit()
    }

    /// Starts an output element whose tag, timestamp, windows, pane or metadata can change.
    /// `ctx.output(v).emit()` is the same as `ctx.emit(v)`.
    pub fn output(&mut self, value: T) -> OutputBuilder<'_, 'a, T> {
        OutputBuilder::new(self, value, None)
    }

    /// Starts an output element for `tag`. `U` must be the element type of the `PCollection`
    /// behind `tag`, and can differ from the main output type.
    pub fn output_to<'s, U: DefaultCoder>(
        &'s mut self,
        tag: impl Into<OutputTag<'s>>,
        value: U,
    ) -> OutputBuilder<'s, 'a, T, U> {
        OutputBuilder::new(self, value, Some(tag.into()))
    }
}

/// An output element under construction. Unset fields come from the current element. `T` is
/// the main output type of the `DoFn`; `V` is the type of the emitted element.
pub struct OutputBuilder<'ctx, 'a, T, V = T> {
    ctx: &'ctx mut ProcessContext<'a, T>,
    value: V,
    tag: Option<OutputTag<'ctx>>,
    header: Option<&'ctx WindowedHeader>,
    timestamp: Option<i64>,
    pane: Option<PaneInfo>,
    metadata: Option<ElementMetadata>,
}

impl<'ctx, 'a, T, V> OutputBuilder<'ctx, 'a, T, V> {
    fn new(ctx: &'ctx mut ProcessContext<'a, T>, value: V, tag: Option<OutputTag<'ctx>>) -> Self {
        Self {
            ctx,
            value,
            tag,
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

    /// Sends the element to output `tag` (name or index) instead of the main output.
    pub fn to(mut self, tag: impl Into<OutputTag<'ctx>>) -> Self {
        self.tag = Some(tag.into());
        self
    }

    /// Overrides the event timestamp, in milliseconds.
    pub fn at(mut self, timestamp_millis: i64) -> Self {
        self.timestamp = Some(timestamp_millis);
        self
    }

    /// Uses `header` (timestamp, windows, pane, metadata) as the base instead of the header of
    /// the current element. Other overrides apply on top of it.
    ///
    /// Use it to flush buffered elements in
    /// [`finish_bundle`](crate::transforms::DoFn::finish_bundle): there the inherited header is
    /// the last one that the runner sent, under fusion often the impulse in the global window.
    /// Store the header of each element and pass it here.
    pub fn windowed(mut self, header: &'ctx WindowedHeader) -> Self {
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

impl<T, V: DefaultCoder> OutputBuilder<'_, '_, T, V> {
    pub fn emit(self) -> crate::Result {
        let overridden = self.timestamp.is_some() || self.pane.is_some() || self.metadata.is_some();
        let rebuilt = overridden.then(|| {
            let base = self.base_header();
            base.rebuilt(
                self.timestamp.unwrap_or_else(|| base.timestamp_millis()),
                self.pane.unwrap_or_else(|| base.pane()),
                &self.metadata.clone().unwrap_or_else(|| base.metadata()),
            )
        });
        let header = rebuilt.as_ref().or(self.header).or_else(|| {
            // Without a header, the sink supplies its own, as `ProcessContext::emit` does.
            (!self.ctx.header.is_empty()).then_some(self.ctx.header)
        });
        Ok(self.ctx.sink.push_value(
            self.tag.as_ref().map(OutputTag::as_str),
            header,
            TypedElement::new(&mut Some(self.value)),
        )?)
    }
}
