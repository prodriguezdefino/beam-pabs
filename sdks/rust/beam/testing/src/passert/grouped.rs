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

//! Assertions about `GroupByKey` output, comparing each group's values as a multiset.

use std::fmt::{self, Debug};

use beam::coders::{BeamIterable, DefaultCoder, IntervalWindow};
use beam::values::PCollection;

use super::builder::{PAssert, that};
use super::mismatch::{Shown, describe_elements, multiset_difference};

/// Starts an assertion about the output of a `GroupByKey`, where the order of the
/// values within a group is unspecified.
///
/// Like [`that`], the builder panics if dropped without a check.
#[must_use = "an assertion checks nothing until a method such as `contains_in_any_order` is called"]
pub fn that_grouped<K, V>(
    name: impl Into<String>,
    pcoll: &PCollection<(K, BeamIterable<V>)>,
) -> GroupedAssert<K, V>
where
    K: DefaultCoder,
    V: DefaultCoder,
{
    GroupedAssert {
        inner: that(name, pcoll),
    }
}

/// An assertion about the groups a `GroupByKey` produced, built by [`that_grouped`].
///
/// Groups are compared as multisets of values: the order of values within a group is
/// ignored, duplicates are not.
#[derive(Clone)]
pub struct GroupedAssert<K, V> {
    inner: PAssert<(K, BeamIterable<V>)>,
}

impl<K: 'static, V: 'static> Debug for GroupedAssert<K, V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GroupedAssert")
            .field("inner", &self.inner)
            .finish()
    }
}

impl<K: DefaultCoder, V: DefaultCoder> GroupedAssert<K, V> {
    /// Considers only the groups assigned to `window`.
    #[must_use]
    pub fn in_window(mut self, window: IntervalWindow) -> Self {
        self.inner = self.inner.in_window(window);
        self
    }

    /// Considers only the groups of `window` emitted by its on-time firing.
    #[must_use]
    pub fn in_on_time_pane(mut self, window: IntervalWindow) -> Self {
        self.inner = self.inner.in_on_time_pane(window);
        self
    }

    /// Asserts that the collection holds exactly the groups `expected`, in any order,
    /// each with exactly its listed values, in any order.
    pub fn contains_in_any_order<I>(self, expected: I) -> Self
    where
        I: IntoIterator<Item = (K, Vec<V>)>,
        K: PartialEq + Debug,
        V: PartialEq + Debug,
    {
        let expected: Vec<Group<K, V>> = expected
            .into_iter()
            .map(|(key, values)| Group { key, values })
            .collect();
        self.satisfies(move |actual| {
            let actual: Vec<&Group<K, V>> = actual.iter().collect();
            let expected: Vec<&Group<K, V>> = expected.iter().collect();
            let (missing, unexpected) = multiset_difference(&actual, &expected);
            if missing.is_empty() && unexpected.is_empty() {
                Ok(())
            } else {
                Err(format!(
                    "expected groups {} in any order, but got {}{}{}",
                    Shown(&expected),
                    Shown(&actual),
                    describe_elements("missing", &missing),
                    describe_elements("unexpected", &unexpected),
                )
                .into())
            }
        })
    }

    /// Asserts that `check` accepts every group, each with its values read into a
    /// `Vec` (in no particular order).
    pub fn satisfies<F>(self, check: F) -> Self
    where
        F: Fn(&[Group<K, V>]) -> beam::Result + Send + Sync + 'static,
    {
        let inner = self.inner.apply_owned_check(move |grouped| {
            let groups = grouped
                .into_iter()
                .map(|(key, values)| {
                    values
                        .into_vec()
                        .map(|values| Group { key, values })
                        .map_err(|e| {
                            beam::Error::from(e).context("failed to read a group's values")
                        })
                })
                .collect::<Result<Vec<_>, _>>()?;
            check(&groups)
        });
        Self { inner }
    }
}

/// One group of a `GroupByKey` output, as [`GroupedAssert`] checks it.
///
/// Two groups are equal when their keys are equal and their values are the same
/// multiset, whatever their order.
#[derive(Clone, Debug)]
pub struct Group<K, V> {
    /// The key of the group.
    pub key: K,
    /// The values of the group, in the order the runner supplied them.
    pub values: Vec<V>,
}

impl<K: PartialEq, V: PartialEq> PartialEq for Group<K, V> {
    fn eq(&self, other: &Self) -> bool {
        if self.key != other.key || self.values.len() != other.values.len() {
            return false;
        }
        let (missing, unexpected) = multiset_difference(&self.values, &other.values);
        missing.is_empty() && unexpected.is_empty()
    }
}
