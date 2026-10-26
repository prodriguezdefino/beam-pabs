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

//! Fluent method syntax for `Distinct` and the set operations.

use std::hash::Hash;

use beam::coders::DefaultCoder;
use beam::transforms::{Distinct, DistinctBy, Sets};
use beam::values::PCollection;

/// Shorthand methods that remove duplicates and combine two collections as sets. See
/// [`Distinct`] and [`Sets`].
pub trait SetsExt<T: DefaultCoder> {
    /// Keeps one element of each group of equal elements. See [`Distinct::new`].
    fn distinct(&self, name: impl Into<String>) -> PCollection<T>
    where
        T: Eq + Hash;

    /// Keeps one element for each value of `key_fn`. See
    /// [`DistinctBy`].
    fn distinct_by<K, F>(&self, name: impl Into<String>, key_fn: F) -> PCollection<T>
    where
        K: DefaultCoder + Eq + Hash,
        F: Fn(&T) -> K + Send + Sync + 'static;

    /// The elements of either collection, each once. See [`Sets::union_distinct`].
    fn union_distinct(&self, name: impl Into<String>, other: &PCollection<T>) -> PCollection<T>
    where
        T: Eq + Hash + Clone;

    /// The elements of both collections, with all duplicates. See [`Sets::union_all`].
    fn union_all(&self, name: impl Into<String>, other: &PCollection<T>) -> PCollection<T>
    where
        T: Eq + Hash + Clone;

    /// The elements in both collections, each once. See [`Sets::intersect_distinct`].
    fn intersect_distinct(&self, name: impl Into<String>, other: &PCollection<T>) -> PCollection<T>
    where
        T: Eq + Hash + Clone;

    /// The elements in both collections, as many times as the smaller count. See
    /// [`Sets::intersect_all`].
    fn intersect_all(&self, name: impl Into<String>, other: &PCollection<T>) -> PCollection<T>
    where
        T: Eq + Hash + Clone;

    /// The elements that are not in `other`, each once. See [`Sets::except_distinct`].
    fn except_distinct(&self, name: impl Into<String>, other: &PCollection<T>) -> PCollection<T>
    where
        T: Eq + Hash + Clone;

    /// The elements, as many times as their count minus their count in `other`. See
    /// [`Sets::except_all`].
    fn except_all(&self, name: impl Into<String>, other: &PCollection<T>) -> PCollection<T>
    where
        T: Eq + Hash + Clone;
}

impl<T: DefaultCoder> SetsExt<T> for PCollection<T> {
    fn distinct(&self, name: impl Into<String>) -> PCollection<T>
    where
        T: Eq + Hash,
    {
        self.apply(name, Distinct::new())
    }

    fn distinct_by<K, F>(&self, name: impl Into<String>, key_fn: F) -> PCollection<T>
    where
        K: DefaultCoder + Eq + Hash,
        F: Fn(&T) -> K + Send + Sync + 'static,
    {
        self.apply(name, DistinctBy::new(key_fn))
    }

    fn union_distinct(&self, name: impl Into<String>, other: &PCollection<T>) -> PCollection<T>
    where
        T: Eq + Hash + Clone,
    {
        self.apply(name, Sets::union_distinct(other))
    }

    fn union_all(&self, name: impl Into<String>, other: &PCollection<T>) -> PCollection<T>
    where
        T: Eq + Hash + Clone,
    {
        self.apply(name, Sets::union_all(other))
    }

    fn intersect_distinct(&self, name: impl Into<String>, other: &PCollection<T>) -> PCollection<T>
    where
        T: Eq + Hash + Clone,
    {
        self.apply(name, Sets::intersect_distinct(other))
    }

    fn intersect_all(&self, name: impl Into<String>, other: &PCollection<T>) -> PCollection<T>
    where
        T: Eq + Hash + Clone,
    {
        self.apply(name, Sets::intersect_all(other))
    }

    fn except_distinct(&self, name: impl Into<String>, other: &PCollection<T>) -> PCollection<T>
    where
        T: Eq + Hash + Clone,
    {
        self.apply(name, Sets::except_distinct(other))
    }

    fn except_all(&self, name: impl Into<String>, other: &PCollection<T>) -> PCollection<T>
    where
        T: Eq + Hash + Clone,
    {
        self.apply(name, Sets::except_all(other))
    }
}
