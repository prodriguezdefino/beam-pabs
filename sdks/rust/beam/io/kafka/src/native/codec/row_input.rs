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

//! What the encoders of `Row` inputs share: the schema of the input, and rows that reach the
//! writer still encoded.

use std::borrow::Cow;
use std::sync::Arc;

use beam::schema::{FieldType, FieldValue, Row, RowElement, Schema};
use beam::values::PCollection;

use super::CodecError;

/// The schema of the `Row` input of a write, once the encoder got it.
#[derive(Clone, Debug, Default)]
pub(super) struct RowInput {
    schema: Option<Arc<Schema>>,
}

impl RowInput {
    /// Keeps the row schema of `input`, and gives the writer the rows as `RowElement`s.
    ///
    /// # Errors
    ///
    /// Fails if `input` has no row schema.
    pub(super) fn bind(
        &mut self,
        input: &PCollection<Row>,
    ) -> beam::Result<(Arc<Schema>, PCollection<RowElement>)> {
        let schema = input
            .row_schema()
            .ok_or_else(|| beam::Error::msg("The input has no row schema"))?;
        self.schema = Some(Arc::clone(&schema));
        Ok((schema, input.as_row_elements()))
    }

    pub(super) fn schema(&self) -> Result<&Arc<Schema>, CodecError> {
        self.schema
            .as_ref()
            .ok_or_else(|| CodecError::new("The encoder has no input schema yet"))
    }

    /// The row of `element`: the row itself, or the row that its bytes encode.
    pub(super) fn row<'e>(&self, element: &'e RowElement) -> Result<Cow<'e, Row>, CodecError> {
        match element {
            RowElement::Row(row) => Ok(Cow::Borrowed(row)),
            RowElement::Encoded(bytes) => Row::from_row_bytes(self.schema()?, bytes)
                .map(Cow::Owned)
                .map_err(|e| CodecError::new(format!("Cannot decode an input row: {e}"))),
        }
    }

    /// The type of the `failed_row` field: the input row.
    pub(super) fn failed_type(&self) -> FieldType {
        self.schema
            .as_ref()
            .map_or_else(FieldType::bytes, |schema| {
                FieldType::row((**schema).clone())
            })
    }

    /// The `failed_row` value of `element`: the input row.
    pub(super) fn failed_value(&self, element: RowElement) -> Result<FieldValue, CodecError> {
        match element {
            RowElement::Row(row) => Ok(FieldValue::Row(row)),
            encoded => self
                .row(&encoded)
                .map(|row| FieldValue::Row(row.into_owned())),
        }
    }
}
