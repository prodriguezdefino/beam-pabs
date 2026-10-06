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

//! Assertions about `(element, window)` pairs, and decoding of interval windows.

use std::fmt::{self, Debug};

use beam::coders::{
    Coder, Context, DefaultCoder, IntervalWindow, IntervalWindowCoder, WindowedHeader,
};
use beam::transforms::ParDo;
use beam::values::PCollection;

use super::builder::{PAssert, that};

/// Starts an assertion about `(element, window)` pairs: every element of `pcoll`,
/// paired with each [`IntervalWindow`] it is assigned to.
///
/// An element in several windows (for instance sliding windows) appears once per
/// window. Pane information is not included; combine [`PAssert::in_final_pane`] and
/// friends with [`that`] for that.
///
/// Adds a step reading each element's windows. It fails the pipeline if an element is
/// in the global window or its windows are not interval windows.
///
/// ```no_run
/// # use beam::prelude::*;
/// # use testing::{TestPipeline, passert};
/// # fn example(p: &TestPipeline, counts: &PCollection<(String, i64)>) {
/// passert::that_windowed("PAssert", counts).contains_in_any_order([
///     (("a".to_string(), 2), IntervalWindow::new(0, 10_000)),
///     (("a".to_string(), 1), IntervalWindow::new(10_000, 20_000)),
/// ]);
/// # }
/// ```
#[must_use = "an assertion checks nothing until a method such as `contains_in_any_order` is called"]
pub fn that_windowed<T: DefaultCoder + Clone>(
    name: impl Into<String>,
    pcoll: &PCollection<T>,
) -> WindowedAssert<T> {
    let assertion = name.into();
    let name = pcoll
        .pipeline()
        .unique_transform_name("PAssertReifyWindows");
    let reified = pcoll.apply(ParDo::from_fn(name.clone(), move |element: T, ctx| {
        let windows = interval_windows(ctx.header()).map_err(|e| format!("{name}: {e}"))?;
        // Each window except the last gets a clone. The last window takes the element.
        let Some((last, rest)) = windows.split_last() else {
            return Err(format!("{name}: the element is assigned to no window").into());
        };
        rest.iter()
            .try_for_each(|w| ctx.emit((element.clone(), (w.start_millis, w.end_millis))))?;
        ctx.emit((element, (last.start_millis, last.end_millis)))
    }));
    WindowedAssert {
        inner: that(assertion, &reified),
    }
}

/// An assertion about `(element, window)` pairs, built by [`that_windowed`].
#[derive(Clone)]
pub struct WindowedAssert<T> {
    inner: PAssert<(T, (i64, i64))>,
}

impl<T: 'static> Debug for WindowedAssert<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WindowedAssert")
            .field("inner", &self.inner)
            .finish()
    }
}

impl<T: DefaultCoder> WindowedAssert<T> {
    /// Asserts that the collection holds exactly the `(element, window)` pairs
    /// `expected`, in any order.
    pub fn contains_in_any_order<I>(self, expected: I) -> Self
    where
        I: IntoIterator<Item = (T, IntervalWindow)>,
        T: PartialEq + Debug,
    {
        let expected: Vec<(T, (i64, i64))> = expected
            .into_iter()
            .map(|(e, w)| (e, (w.start_millis, w.end_millis)))
            .collect();
        Self {
            inner: self.inner.contains_in_any_order(expected),
        }
    }

    /// Asserts that `check` accepts every `(element, window)` pair at once.
    pub fn satisfies<F>(self, check: F) -> Self
    where
        F: Fn(&[(T, IntervalWindow)]) -> beam::Result + Send + Sync + 'static,
    {
        let inner = self.inner.apply_owned_check(move |pairs| {
            let pairs: Vec<(T, IntervalWindow)> = pairs
                .into_iter()
                .map(|(e, (start, end))| (e, IntervalWindow::new(start, end)))
                .collect();
            check(&pairs)
        });
        Self { inner }
    }
}

/// Decodes the interval windows to which `header` assigns its element.
///
/// # Errors
///
/// Returns an error, and not fewer windows, when:
///
/// - there is no header;
/// - the element is in the global window;
/// - the windows are not interval windows, so they do not decode exactly.
///
/// Core does not decode windows from a [`WindowedHeader`], because it does not know the
/// window coder. This function reads the layout that [`WindowedHeader`] documents: an
/// 8-byte timestamp, a 4-byte big-endian window count at byte 8, then the windows
/// ([`WindowedHeader::window_bytes`]).
pub fn interval_windows(header: &WindowedHeader) -> Result<Vec<IntervalWindow>, String> {
    if header.is_empty() {
        return Err("the element carries no window information".to_string());
    }
    let count = header
        .as_bytes()
        .get(8..WindowedHeader::WINDOWS_START)
        .and_then(|b| <[u8; 4]>::try_from(b).ok())
        .map(i32::from_be_bytes)
        .ok_or_else(|| {
            format!(
                "the window header is truncated ({} byte(s))",
                header.as_bytes().len()
            )
        })?;
    let mut window_bytes = header.window_bytes();
    if count == 1 && window_bytes.is_empty() {
        return Err(
            "the element is in the global window; window and pane selection only apply to \
             interval-windowed collections, such as the output of a windowed aggregation"
                .to_string(),
        );
    }
    let count = usize::try_from(count)
        .map_err(|_| format!("the window header holds a negative window count ({count})"))?;
    let windows = (0..count)
        .map(|i| {
            IntervalWindowCoder
                .decode(&mut window_bytes, Context::Nested)
                .map_err(|e| {
                    format!(
                        "window {} of {count} is not a decodable interval window: {e}",
                        i + 1
                    )
                })
        })
        .collect::<Result<Vec<_>, _>>()?;
    if !window_bytes.is_empty() {
        return Err(format!(
            "{} byte(s) left over after decoding {count} interval window(s); the windows \
             are not interval windows",
            window_bytes.len()
        ));
    }
    Ok(windows)
}
