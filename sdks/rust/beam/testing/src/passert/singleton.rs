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

//! Assertions about a collection expected to hold exactly one element.

use std::fmt::{self, Debug};

use beam::coders::{DefaultCoder, IntervalWindow};
use beam::values::PCollection;

use super::builder::{PAssert, that};
use super::mismatch::Shown;

/// Starts an assertion about a collection expected to hold exactly one element.
///
/// Like [`that`], the builder panics if dropped without a check.
#[must_use = "an assertion checks nothing until a method such as `is_equal_to` is called"]
pub fn that_singleton<T: DefaultCoder>(
    name: impl Into<String>,
    pcoll: &PCollection<T>,
) -> SingletonAssert<T> {
    SingletonAssert {
        inner: that(name, pcoll),
    }
}

/// An assertion about a collection expected to hold exactly one element, built by
/// [`that_singleton`].
#[derive(Clone)]
pub struct SingletonAssert<T> {
    inner: PAssert<T>,
}

impl<T: 'static> Debug for SingletonAssert<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SingletonAssert")
            .field("inner", &self.inner)
            .finish()
    }
}

impl<T: DefaultCoder> SingletonAssert<T> {
    /// Considers only the elements assigned to `window`.
    #[must_use]
    pub fn in_window(mut self, window: IntervalWindow) -> Self {
        self.inner = self.inner.in_window(window);
        self
    }

    /// Considers only the elements of `window` emitted by its on-time firing.
    #[must_use]
    pub fn in_on_time_pane(mut self, window: IntervalWindow) -> Self {
        self.inner = self.inner.in_on_time_pane(window);
        self
    }

    /// Considers only the elements of `window` emitted by its final firing.
    #[must_use]
    pub fn in_final_pane(mut self, window: IntervalWindow) -> Self {
        self.inner = self.inner.in_final_pane(window);
        self
    }

    /// Considers only the elements of `window` emitted by its early (speculative) firings.
    #[must_use]
    pub fn in_early_panes(mut self, window: IntervalWindow) -> Self {
        self.inner = self.inner.in_early_panes(window);
        self
    }

    /// Considers only the elements of `window` emitted by its late firings.
    #[must_use]
    pub fn in_late_panes(mut self, window: IntervalWindow) -> Self {
        self.inner = self.inner.in_late_panes(window);
        self
    }

    /// Asserts that the collection holds exactly one element, equal to `expected`.
    pub fn is_equal_to(self, expected: T) -> Self
    where
        T: PartialEq + Debug,
    {
        self.satisfies(move |actual| {
            if *actual == expected {
                Ok(())
            } else {
                Err(format!("expected {expected:?}, but got {actual:?}").into())
            }
        })
    }

    /// Asserts that the collection holds exactly one element, accepted by `check`.
    pub fn satisfies<F>(self, check: F) -> Self
    where
        F: Fn(&T) -> beam::Result + Send + Sync + 'static,
        T: Debug,
    {
        let inner = self.inner.satisfies(move |actual| match actual {
            [single] => check(single),
            _ => Err(format!(
                "expected exactly one element, but got {}: {}",
                actual.len(),
                Shown(actual)
            )
            .into()),
        });
        Self { inner }
    }
}
