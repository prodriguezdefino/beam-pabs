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

//! The input of a `DoFn`: the element `T`, or [`Windowed<T>`] to also receive its window. The
//! type is the declaration: only a `Windowed` input is processed once per window.

use crate::coders::{DefaultCoder, WindowedHeader};
use crate::windowing::BoundedWindow;

/// An element with the one window it is processed in. Take it as the input of a closure or
/// [`DoFn`](crate::transforms::DoFn) to read the window.
///
/// ```
/// use beam::transforms::{ParDo, Windowed};
///
/// let tag = ParDo::from_fn("Tag", |Windowed { value, window }: Windowed<String>, ctx| {
///     ctx.emit(format!("{value} in {window:?}"))
/// });
/// # let _ = tag;
/// ```
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Windowed<T> {
    /// The element.
    pub value: T,
    /// The global window or one interval window.
    pub window: BoundedWindow,
}

mod sealed {
    pub trait Sealed {}
}

/// The input type of a `DoFn`: `T` or [`Windowed<T>`]. Sealed.
pub trait DoFnInput: sealed::Sealed + Sized + Send + 'static {
    /// The element type of the input `PCollection`.
    type Value: DefaultCoder;

    /// Whether a multi-window element is split per window.
    #[doc(hidden)]
    const OBSERVES_WINDOW: bool;

    /// Builds the input from the decoded element and its header.
    #[doc(hidden)]
    fn from_element(value: Self::Value, header: &WindowedHeader) -> crate::Result<Self>;

    /// The element, without the window.
    #[doc(hidden)]
    fn into_element(self) -> Self::Value;
}

impl<T: DefaultCoder> sealed::Sealed for T {}

impl<T: DefaultCoder> DoFnInput for T {
    type Value = T;
    const OBSERVES_WINDOW: bool = false;

    #[inline]
    fn from_element(value: T, _header: &WindowedHeader) -> crate::Result<T> {
        Ok(value)
    }

    #[inline]
    fn into_element(self) -> T {
        self
    }
}

impl<T: DefaultCoder> sealed::Sealed for Windowed<T> {}

impl<T: DefaultCoder> DoFnInput for Windowed<T> {
    type Value = T;
    const OBSERVES_WINDOW: bool = true;

    fn from_element(value: T, header: &WindowedHeader) -> crate::Result<Self> {
        Ok(Windowed {
            value,
            window: window_of(header)?,
        })
    }

    #[inline]
    fn into_element(self) -> T {
        self.value
    }
}

/// Decodes the single window of `header`.
pub(crate) fn window_of(header: &WindowedHeader) -> crate::Result<BoundedWindow> {
    Ok(header.single_window()?)
}

/// SDK-internal input: like [`Windowed<T>`], processed once per window, but the window stays
/// encoded in the element header and is not decoded.
#[doc(hidden)]
pub struct PerWindow<T>(pub T);

impl<T: DefaultCoder> sealed::Sealed for PerWindow<T> {}

impl<T: DefaultCoder> DoFnInput for PerWindow<T> {
    type Value = T;
    const OBSERVES_WINDOW: bool = true;

    #[inline]
    fn from_element(value: T, _header: &WindowedHeader) -> crate::Result<Self> {
        Ok(PerWindow(value))
    }

    #[inline]
    fn into_element(self) -> T {
        self.0
    }
}
