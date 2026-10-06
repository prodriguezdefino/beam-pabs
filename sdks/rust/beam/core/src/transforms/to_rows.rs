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

//! [`ToRows`] and [`FromRows`]: convert between a `#[derive(BeamRow)]` type and [`Row`].

use std::marker::PhantomData;
use std::sync::Arc;

use super::composite::expand_as;
use super::{ClosureFn, DoFn, PTransform, ParDo, ProcessContext};
use crate::coders::{DefaultCoder, FieldMapCache};
use crate::schema::{BeamRow, Row, RowElement, Schema};
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

    fn expand(self, input: &PCollection<T>) -> PCollection<Row> {
        let schema = T::beam_schema();
        match input.as_row_elements() {
            // The bytes are already rows of `T`. The step emits them again, so the output is
            // a new PCollection of `Row`.
            Some(raw) => expand_as(
                &raw,
                ParDo::new(ClosureFn::new("ToRows", |el: RowElement, out| out.emit(el))),
            )
            .as_rows()
            .with_row_schema(&schema),
            None => expand_as(
                input,
                ParDo::new(ClosureFn::new("ToRows", |element: T, out| {
                    out.emit(element.to_row()?)
                })),
            )
            .with_row_schema(&schema),
        }
    }
}

/// Converts each [`Row`] element to `T`. When the schemas differ, it matches fields by name.
///
/// `FromRows` skips writer fields that `T` does not have, and sets a missing `Option<U>`
/// field to `None`. When the input has a row schema, a missing required field fails
/// `Pipeline::validate`. Else the bundle fails with
/// [`SchemaError::FieldNotFound`](crate::schema::SchemaError::FieldNotFound).
///
/// ```
/// use beam::prelude::*;
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
/// let typed: PCollection<Count> = rows.apply("FromRows", FromRows::new());
/// # let _ = typed;
/// ```
pub struct FromRows<T>(PhantomData<fn() -> T>);

impl<T> FromRows<T> {
    /// Creates the transform. The output type `T` gives the target schema.
    pub fn new() -> Self {
        Self(PhantomData)
    }
}

impl<T> Default for FromRows<T> {
    fn default() -> Self {
        Self::new()
    }
}

struct FromRowsFn<T> {
    input_schema: Option<Arc<Schema>>,
    field_map: FieldMapCache,
    _marker: PhantomData<fn() -> T>,
}

impl<T> Clone for FromRowsFn<T> {
    fn clone(&self) -> Self {
        Self {
            input_schema: self.input_schema.clone(),
            field_map: self.field_map.clone(),
            _marker: PhantomData,
        }
    }
}

impl<T: BeamRow + DefaultCoder> DoFn for FromRowsFn<T> {
    type In = RowElement;
    type Out = T;

    /// A reordered schema decodes through a [`Row`]: `T` has no decoder for another layout.
    fn process_element(
        &mut self,
        element: RowElement,
        ctx: &mut ProcessContext<'_, Self::Out>,
    ) -> crate::Result {
        let Self {
            input_schema,
            field_map,
            ..
        } = self;
        ctx.emit(field_map.from_row_element::<T>(element, input_schema.as_ref())?)
    }
}

impl<T: BeamRow + DefaultCoder> PTransform<PCollection<Row>> for FromRows<T> {
    type Output = PCollection<T>;

    fn expand(self, input: &PCollection<Row>) -> PCollection<T> {
        let input_schema = input.row_schema();
        if let Some(Err(e)) = input_schema.as_deref().map(T::field_map) {
            input
                .pipeline()
                .record_invalid_construction(format_args!("the input rows do not convert: {e}"));
        }
        let raw = input.as_row_elements();
        expand_as(
            &raw,
            ParDo::new(FromRowsFn::<T> {
                input_schema,
                field_map: FieldMapCache::new(),
                _marker: PhantomData,
            }),
        )
    }
}
