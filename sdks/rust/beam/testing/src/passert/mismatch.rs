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

//! Formatting of mismatches between actual and expected elements.

use std::fmt::{self, Debug};

/// At most this many elements are printed when an assertion describes a collection.
const MAX_ELEMENTS_SHOWN: usize = 25;

/// Returns the elements of `expected` absent from `actual`, and the elements of
/// `actual` absent from `expected`, counting duplicates.
pub(super) fn multiset_difference<'a, T: PartialEq>(
    actual: &'a [T],
    expected: &'a [T],
) -> (Vec<&'a T>, Vec<&'a T>) {
    let mut matched = vec![false; actual.len()];
    let missing = expected
        .iter()
        .filter(|wanted| {
            let found = actual
                .iter()
                .enumerate()
                .position(|(i, got)| !matched[i] && got == *wanted);
            match found {
                Some(i) => {
                    matched[i] = true;
                    false
                }
                None => true,
            }
        })
        .collect();
    let unexpected = actual
        .iter()
        .zip(&matched)
        .filter(|(_, m)| !**m)
        .map(|(e, _)| e)
        .collect();
    (missing, unexpected)
}

pub(super) fn describe_elements<T: Debug>(label: &str, elements: &[T]) -> String {
    if elements.is_empty() {
        String::new()
    } else {
        format!("; {label}: {}", Shown(elements))
    }
}

/// Formats a slice of elements, eliding all but the first [`MAX_ELEMENTS_SHOWN`].
pub(super) struct Shown<'a, T>(pub(super) &'a [T]);

impl<T: Debug> fmt::Display for Shown<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let elements = self.0;
        if elements.len() <= MAX_ELEMENTS_SHOWN {
            write!(f, "{elements:?}")
        } else {
            let shown = &elements[..MAX_ELEMENTS_SHOWN];
            write!(
                f,
                "{shown:?} (and {} more)",
                elements.len() - MAX_ELEMENTS_SHOWN
            )
        }
    }
}
