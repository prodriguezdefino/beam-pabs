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

//! Orderings for [`Top`](super::Top) and [`ApproximateQuantiles`](super::ApproximateQuantiles).

use std::cmp::Ordering;

use serde::{Deserialize, Serialize};

/// An ordering of `T` values.
///
/// A closure `Fn(&T, &T) -> Ordering` is an ordering. A closure cannot be registered, so a
/// combine function that holds one uses the rebuild path. [`Natural`] and [`Reversed`] are
/// named orderings that serialize, so a combine function that holds them can be registered.
pub trait Compare<T>: Send + Sync + 'static {
    /// Compares `left` with `right`.
    fn compare(&self, left: &T, right: &T) -> Ordering;
}

impl<T, F> Compare<T> for F
where
    F: Fn(&T, &T) -> Ordering + Send + Sync + 'static,
{
    fn compare(&self, left: &T, right: &T) -> Ordering {
        self(left, right)
    }
}

/// The order of [`Ord`]: the greatest value is the largest.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Natural;

impl<T: Ord> Compare<T> for Natural {
    fn compare(&self, left: &T, right: &T) -> Ordering {
        left.cmp(right)
    }
}

/// The reverse of [`Ord`]: the greatest value is the smallest.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Reversed;

impl<T: Ord> Compare<T> for Reversed {
    fn compare(&self, left: &T, right: &T) -> Ordering {
        right.cmp(left)
    }
}
