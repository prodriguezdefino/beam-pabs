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

//! [`ToRows`]: the elements of a `#[derive(BeamRow)]` type as [`Row`]s.

use std::marker::PhantomData;

use super::composite::expand_as;
use super::{ClosureFn, PTransform, ParDo};
use crate::coders::DefaultCoder;
use crate::schema::{BeamRow, Row};
use crate::values::PCollection;

/// Converts each element of a `#[derive(BeamRow)]` type to a [`Row`]. The output coder
/// carries the schema of `T`, so other SDKs and schema transforms can read the rows.
///
/// ```
/// use beam::prelude::*;
/// use beam::schema::BeamRow;
/// use beam::transforms::ToRows;
///
/// #[derive(BeamRow, Clone, Debug, PartialEq)]
/// struct Count {
///     word: String,
///     count: i64,
/// }
///
/// # let p = Pipeline::default();
/// let counts = p.apply("Counts", Create::new(vec![Count { word: "a".into(), count: 1 }]));
/// let rows = counts.apply("ToRows", ToRows::new());
/// assert!(rows.row_schema().is_some());
/// ```
pub struct ToRows<T>(PhantomData<fn(T)>);

impl<T> ToRows<T> {
    pub fn new() -> Self {
        Self(PhantomData)
    }
}

impl<T> Default for ToRows<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: BeamRow + DefaultCoder> PTransform<PCollection<T>> for ToRows<T> {
    type Output = PCollection<Row>;

    fn expand(&self, input: &PCollection<T>) -> PCollection<Row> {
        expand_as(
            input,
            ParDo::new(ClosureFn::new("ToRows", |element: T, out| {
                out.emit(element.to_row()?)
            })),
        )
        .with_row_schema(T::beam_schema())
    }
}
