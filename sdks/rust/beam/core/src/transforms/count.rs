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

//! Counting the occurrences of elements.

use std::hash::Hash;
use std::marker::PhantomData;

use super::{CombineGlobally, CombinePerKey, Map, PTransform, Sum};
use crate::coders::DefaultCoder;
use crate::values::PCollection;

/// Counts how many times each distinct element occurs.
///
/// Uses [`CombinePerKey`], so each worker counts before the shuffle and sends one number
/// per value, not one `1` per occurrence.
pub struct CountPerElement<T> {
    name: String,
    _marker: PhantomData<T>,
}

impl<T> CountPerElement<T> {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            _marker: PhantomData,
        }
    }
}

impl<T> PTransform<PCollection<T>> for CountPerElement<T>
where
    T: DefaultCoder + Eq + Hash,
{
    type Output = PCollection<(T, i64)>;

    fn expand(&self, input: &PCollection<T>) -> PCollection<(T, i64)> {
        input
            .apply(Map::new(
                format!("{}/PairWithOne", self.name),
                |element: T| (element, 1i64),
            ))
            .apply(CombinePerKey::new(format!("{}/Sum", self.name), Sum))
    }
}

/// Counts the total number of elements in a PCollection.
///
/// Uses [`CombineGlobally`], so each worker counts before the shuffle. With defaults,
/// `expand` panics if the input is not in the global window; see
/// [`without_defaults`](Self::without_defaults).
pub struct CountGlobally<T> {
    name: String,
    insert_default: bool,
    _marker: PhantomData<T>,
}

impl<T> CountGlobally<T> {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            insert_default: true,
            _marker: PhantomData,
        }
    }

    /// Emits nothing for an empty window, not zero. Call this for input in non-global
    /// windows, such as fixed or sliding windows.
    pub fn without_defaults(mut self) -> Self {
        self.insert_default = false;
        self
    }
}

impl<T> PTransform<PCollection<T>> for CountGlobally<T>
where
    T: DefaultCoder,
{
    type Output = PCollection<i64>;

    fn expand(&self, input: &PCollection<T>) -> PCollection<i64> {
        let mut combine = CombineGlobally::new(format!("{}/Sum", self.name), Sum);
        if !self.insert_default {
            combine = combine.without_defaults();
        }
        input
            .apply(Map::new(format!("{}/ToOne", self.name), |_element: T| 1i64))
            .apply(combine)
    }
}
