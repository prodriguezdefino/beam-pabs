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

//! Indexes the elements of a collection so that a side input reads them by position.

use std::collections::HashMap;
use std::hash::{BuildHasher, RandomState};
use std::marker::PhantomData;

use super::composite::expand_as;
use super::sdf::OffsetRange;
use super::{DoFn, FinishBundleContext, PTransform, ParDo, ProcessContext, Windowed};
use crate::coders::DefaultCoder;
use crate::values::{LIST_RANGES_KEY, ListEntry, PCollection};
use crate::windowing::BoundedWindow;

/// Keys each element by a list position, so that
/// [`as_list`](PCollection::as_list) reads the window as a list by index.
///
/// Each bundle uses a range of positions in each window, from a random start. It adds the
/// range under [`LIST_RANGES_KEY`]. A read sorts the ranges of the window, so a read by index
/// needs one multimap read and does not load the whole window.
///
/// ```
/// # use beam_core as beam;
/// use beam::prelude::*;
///
/// # let p = Pipeline::default();
/// let words = p.apply("Words", Create::new(vec!["a".to_string(), "b".to_string()]));
/// let list = words.apply("AsList", AsList::new()).as_list();
/// # let _ = list;
/// ```
pub struct AsList<T> {
    _marker: PhantomData<fn(T)>,
}

impl<T> AsList<T> {
    /// Creates the transform.
    pub fn new() -> Self {
        Self {
            _marker: PhantomData,
        }
    }
}

impl<T> Default for AsList<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: DefaultCoder> PTransform<PCollection<T>> for AsList<T> {
    type Output = PCollection<(i64, ListEntry<T>)>;

    fn expand(self, input: &PCollection<T>) -> Self::Output {
        expand_as(input, ParDo::new(IndexElementsFn::<T>::new()))
    }
}

/// Emits each element at the next position of the range of its window, and the ranges of the
/// bundle when the bundle ends.
struct IndexElementsFn<T> {
    ranges: HashMap<BoundedWindow, OffsetRange>,
    _marker: PhantomData<fn(T)>,
}

impl<T> IndexElementsFn<T> {
    fn new() -> Self {
        Self {
            ranges: HashMap::new(),
            _marker: PhantomData,
        }
    }
}

/// A copy starts with no ranges: the ranges belong to the bundle of one instance.
impl<T> Clone for IndexElementsFn<T> {
    fn clone(&self) -> Self {
        Self::new()
    }
}

/// An empty range at a random start, with room for `u32::MAX` positions. A random start makes
/// the ranges of two bundles unlikely to overlap. Positions never reach [`LIST_RANGES_KEY`].
fn random_range(window: &BoundedWindow) -> OffsetRange {
    let low = i128::from(LIST_RANGES_KEY) + 1;
    let high = i128::from(i64::MAX) - i128::from(u32::MAX);
    let random = i128::from(RandomState::new().hash_one(window));
    // The start lies in [low, high), so it fits in i64.
    let start = (low + random % (high - low)) as i64;
    OffsetRange::new(start, start)
}

impl<T: DefaultCoder> DoFn for IndexElementsFn<T> {
    type In = Windowed<T>;
    type Out = (i64, ListEntry<T>);

    fn process_element(
        &mut self,
        Windowed { value, window }: Windowed<T>,
        ctx: &mut ProcessContext<'_, Self::Out>,
    ) -> crate::Result {
        let range = self.ranges.entry(window).or_insert_with_key(random_range);
        let position = range.end;
        range.end += 1;
        ctx.emit((position, ListEntry::Value(value)))
    }

    fn finish_bundle(&mut self, ctx: &mut FinishBundleContext<'_, Self::Out>) -> crate::Result {
        self.ranges.drain().try_for_each(|(window, range)| {
            ctx.emit(
                (LIST_RANGES_KEY, ListEntry::Range(range)),
                window,
                window.max_timestamp(),
            )
        })
    }
}
