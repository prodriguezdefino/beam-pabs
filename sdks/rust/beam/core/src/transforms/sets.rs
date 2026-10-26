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

//! Set operations on two collections of the same element type.

use std::hash::Hash;

use super::co_group_by_key::{CoGbkResult, CoGroupByKey, KeyedPCollectionTuple};
use super::{ClosureFn, CountPerElement, Distinct, Flatten, PTransform, ParDo};
use crate::coders::DefaultCoder;
use crate::values::{PCollection, PCollectionList};

/// The operation of a [`Sets`] transform.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Operation {
    UnionDistinct,
    UnionAll,
    Counted(CountedOperation),
}

/// An operation that compares the count of each element in the two collections.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CountedOperation {
    IntersectDistinct,
    IntersectAll,
    ExceptDistinct,
    ExceptAll,
}

impl CountedOperation {
    /// How many times an element that occurs `left` times in the input and `right` times in
    /// the other collection occurs in the output.
    fn occurrences(self, left: i64, right: i64) -> i64 {
        match self {
            CountedOperation::IntersectDistinct => i64::from(left > 0 && right > 0),
            CountedOperation::IntersectAll => left.min(right),
            CountedOperation::ExceptDistinct => i64::from(left > 0 && right == 0),
            CountedOperation::ExceptAll => (left - right).max(0),
        }
    }
}

/// A set operation between the input collection and another collection of the same element
/// type, in each window, as in SQL.
///
/// The `_distinct` forms give each element once. The `_all` forms keep the duplicates: a
/// union keeps all of them, an intersection keeps the smaller count, and a difference keeps
/// the count of the input minus the count of the other collection. Two elements are equal
/// when their encodings are equal. Both collections must have the same windowing.
///
/// ```
/// use beam::prelude::*;
/// use beam::transforms::Sets;
///
/// let p = Pipeline::new();
/// let a = p.apply("A", Create::new(vec![1i64, 2, 2, 3]));
/// let b = p.apply("B", Create::new(vec![2i64, 3, 4]));
/// let both = a.apply("Both", Sets::intersect_distinct(&b)); // 2, 3
/// let only_a = a.apply("OnlyA", Sets::except_all(&b)); // 1, 2
/// # let _ = (both, only_a);
/// ```
pub struct Sets<T> {
    other: PCollection<T>,
    operation: Operation,
}

impl<T> Sets<T> {
    fn new(other: &PCollection<T>, operation: Operation) -> Self {
        Self {
            other: other.clone(),
            operation,
        }
    }

    /// The elements of either collection, each once.
    pub fn union_distinct(other: &PCollection<T>) -> Self {
        Self::new(other, Operation::UnionDistinct)
    }

    /// The elements of both collections, with all duplicates.
    pub fn union_all(other: &PCollection<T>) -> Self {
        Self::new(other, Operation::UnionAll)
    }

    /// The elements in both collections, each once.
    pub fn intersect_distinct(other: &PCollection<T>) -> Self {
        Self::new(
            other,
            Operation::Counted(CountedOperation::IntersectDistinct),
        )
    }

    /// The elements in both collections, as many times as the smaller count.
    pub fn intersect_all(other: &PCollection<T>) -> Self {
        Self::new(other, Operation::Counted(CountedOperation::IntersectAll))
    }

    /// The elements of the input that are not in `other`, each once.
    pub fn except_distinct(other: &PCollection<T>) -> Self {
        Self::new(other, Operation::Counted(CountedOperation::ExceptDistinct))
    }

    /// The elements of the input, as many times as their count in the input minus their count
    /// in `other`.
    pub fn except_all(other: &PCollection<T>) -> Self {
        Self::new(other, Operation::Counted(CountedOperation::ExceptAll))
    }
}

impl<T> PTransform<PCollection<T>> for Sets<T>
where
    T: DefaultCoder + Eq + Hash + Clone,
{
    type Output = PCollection<T>;

    fn expand(&self, input: &PCollection<T>) -> PCollection<T> {
        let flatten = || {
            PCollectionList::of(input.clone())
                .and(self.other.clone())
                .apply("Flatten", Flatten::new())
        };
        match self.operation {
            Operation::UnionAll => flatten(),
            Operation::UnionDistinct => flatten().apply("Distinct", Distinct::new()),
            Operation::Counted(operation) => {
                let left = input.apply("CountLeft", CountPerElement::new());
                let right = self.other.apply("CountRight", CountPerElement::new());
                KeyedPCollectionTuple::of("left", &left)
                    .and("right", &right)
                    .apply("CoGbk", CoGroupByKey::new())
                    .apply(
                        "Emit",
                        ParDo::new(ClosureFn::new(
                            "Sets",
                            move |(element, counts): (T, CoGbkResult), ctx| {
                                let left = total(&counts, 0)?;
                                let right = total(&counts, 1)?;
                                let times = usize::try_from(operation.occurrences(left, right))
                                    .unwrap_or_default();
                                ctx.emit_all(std::iter::repeat_n(element, times))
                            },
                        )),
                    )
            }
        }
    }
}

/// The count of one side of the cogroup: the side holds at most one count per window.
fn total(counts: &CoGbkResult, side: i32) -> crate::Result<i64> {
    counts.get_by_index::<i64>(side)?.into_iter().sum()
}
