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

//! The context of `finish_bundle`, which has no current element.

use super::{OutputBuilder, ProcessContext};
use crate::Instant;
use crate::coders::{PaneInfo, WindowedHeader};
use crate::transforms::dofn::outputs::Outputs;
use crate::windowing::BoundedWindow;

/// The context of `finish_bundle`. There is no current element to inherit a window from, so
/// every output names its window and timestamp. Keep the window of buffered elements by
/// taking [`Windowed<T>`](crate::transforms::Windowed) as input.
///
/// ```
/// # use beam::prelude::*;
/// # #[derive(Clone, Default)]
/// # struct Buffer { pending: Vec<(String, BoundedWindow)> }
/// # impl Buffer {
/// fn finish_bundle(&mut self, ctx: &mut FinishBundleContext<'_, String>) -> Result {
///     for (value, window) in self.pending.drain(..) {
///         ctx.emit(value, window, window.max_timestamp())?;
///     }
///     Ok(())
/// }
/// # }
/// ```
pub struct FinishBundleContext<'a, T> {
    pub(super) inner: ProcessContext<'a, T>,
}

impl<'a, T: Outputs> FinishBundleContext<'a, T> {
    pub(crate) fn new(inner: ProcessContext<'a, T>) -> Self {
        Self { inner }
    }

    /// Emits `value` in `window` at `timestamp`.
    pub fn emit(&mut self, value: T, window: BoundedWindow, timestamp: Instant) -> crate::Result {
        self.output(value, window, timestamp).emit()
    }

    /// Starts an output in `window` at `timestamp`, whose pane can change.
    pub fn output(
        &mut self,
        value: T,
        window: BoundedWindow,
        timestamp: Instant,
    ) -> FinishOutput<'_, 'a, T> {
        FinishOutput {
            ctx: &mut self.inner,
            value,
            window,
            timestamp,
            pane: PaneInfo::NO_FIRING,
        }
    }

    /// Output under a stored header. SDK crates use
    /// [`internals::finish_output_windowed`](crate::internals::finish_output_windowed).
    pub(crate) fn output_windowed<'h>(
        &'h mut self,
        value: T,
        header: &'h WindowedHeader,
    ) -> OutputBuilder<'h, 'a, T> {
        self.inner.output(value).windowed(header)
    }
}

/// An output of `finish_bundle` under construction.
pub struct FinishOutput<'c, 'a, T> {
    ctx: &'c mut ProcessContext<'a, T>,
    value: T,
    window: BoundedWindow,
    timestamp: Instant,
    pane: PaneInfo,
}

impl<T: Outputs> FinishOutput<'_, '_, T> {
    /// Sets the trigger firing the element belongs to. The default is no firing.
    pub fn with_pane(mut self, pane: PaneInfo) -> Self {
        self.pane = pane;
        self
    }

    pub fn emit(self) -> crate::Result {
        let mut window = Vec::new();
        self.window
            .encode(&mut window)
            .map_err(|e| crate::Error::from(e).context("Failed to encode the output window"))?;
        let header = WindowedHeader::new(self.timestamp.as_millis(), &[window], self.pane);
        self.ctx.output(self.value).windowed(&header).emit()
    }
}

/// The windows of the elements of a bundle, each with its earliest element timestamp.
///
/// A `DoFn` that buffers outputs adds each element in `process_element`. Then
/// `finish_bundle` emits the outputs of each window at the earliest timestamp of the window,
/// so the outputs are not later than their inputs.
///
/// ```
/// # use beam::prelude::*;
/// # use beam::transforms::BundleWindows;
/// # use beam::windowing::GlobalWindow;
/// # #[derive(Clone, Default)]
/// # struct Buffer { pending: Vec<String>, windows: BundleWindows }
/// # impl Buffer {
/// fn finish_bundle(&mut self, ctx: &mut FinishBundleContext<'_, String>) -> Result {
///     let window = BoundedWindow::Global(GlobalWindow);
///     let Some(timestamp) = self.windows.take(&window) else {
///         return Ok(());
///     };
///     self.pending
///         .drain(..)
///         .try_for_each(|value| ctx.emit(value, window, timestamp))
/// }
/// # }
/// ```
#[derive(Clone, Debug, Default)]
pub struct BundleWindows {
    windows: Vec<(BoundedWindow, Instant)>,
}

impl BundleWindows {
    /// Creates a value with no windows.
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds an element in `window` at `timestamp`.
    pub fn add(&mut self, window: BoundedWindow, timestamp: Instant) {
        // A bundle has few windows, and the last window is the most likely.
        match self
            .windows
            .iter_mut()
            .rev()
            .find(|(seen, _)| *seen == window)
        {
            Some((_, earliest)) => *earliest = (*earliest).min(timestamp),
            None => self.windows.push((window, timestamp)),
        }
    }

    /// Removes `window` and returns its earliest timestamp, or `None` if no element was in it.
    pub fn take(&mut self, window: &BoundedWindow) -> Option<Instant> {
        let index = self.windows.iter().position(|(seen, _)| seen == window)?;
        Some(self.windows.swap_remove(index).1)
    }

    /// Removes every window and returns each with its earliest timestamp, in the order of the
    /// first element of each window. A `take` can move the last window into the place of the
    /// window that it removes.
    pub fn drain(&mut self) -> impl Iterator<Item = (BoundedWindow, Instant)> + '_ {
        self.windows.drain(..)
    }
}
