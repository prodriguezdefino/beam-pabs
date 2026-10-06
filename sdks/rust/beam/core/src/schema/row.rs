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

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use crate::coders::{CoderError, RowCoder};

use super::error::SchemaError;
use super::types::Schema;

/// A typed value of a field in a [`Row`].
#[derive(Debug, Clone, PartialEq)]
pub enum FieldValue {
    Byte(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    Float(f32),
    Double(f64),
    String(String),
    Boolean(bool),
    Bytes(Vec<u8>),
    Array(Vec<Option<FieldValue>>),
    Map(Vec<(FieldValue, Option<FieldValue>)>),
    Row(Row),
}

impl fmt::Display for FieldValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Byte(v) => write!(f, "{v}"),
            Self::Int16(v) => write!(f, "{v}"),
            Self::Int32(v) => write!(f, "{v}"),
            Self::Int64(v) => write!(f, "{v}"),
            Self::Float(v) => write!(f, "{v}"),
            Self::Double(v) => write!(f, "{v}"),
            Self::String(v) => write!(f, "{v:?}"),
            Self::Boolean(v) => write!(f, "{v}"),
            Self::Bytes(v) => write!(f, "0x{}", hex_encode(v)),
            Self::Array(v) => write!(f, "{v:?}"),
            Self::Map(v) => write!(f, "{v:?}"),
            Self::Row(v) => write!(f, "{v:?}"),
        }
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// A structured row that follows its Beam [`Schema`].
#[derive(Debug, Clone, PartialEq)]
pub struct Row {
    schema: Arc<Schema>,
    values: Vec<Option<FieldValue>>,
}

impl Row {
    /// Returns [`SchemaError::ValueCountMismatch`] if the value and schema field counts differ.
    pub fn new(schema: Arc<Schema>, values: Vec<Option<FieldValue>>) -> Result<Self, SchemaError> {
        if values.len() != schema.num_fields() {
            return Err(SchemaError::ValueCountMismatch {
                expected: schema.num_fields(),
                actual: values.len(),
            });
        }
        Ok(Self { schema, values })
    }

    pub fn builder(schema: Arc<Schema>) -> RowBuilder {
        RowBuilder::new(schema)
    }

    pub fn schema(&self) -> &Arc<Schema> {
        &self.schema
    }

    pub fn values(&self) -> &[Option<FieldValue>] {
        &self.values
    }

    pub fn get_value_by_index(&self, index: usize) -> Option<&Option<FieldValue>> {
        self.values.get(index)
    }

    pub fn get_value(&self, name: &str) -> Option<&Option<FieldValue>> {
        let index = self.schema.field_index(name)?;
        self.get_value_by_index(index)
    }

    fn get_typed<'a, T>(
        &'a self,
        name: &str,
        expected: &str,
        extract: impl FnOnce(&'a FieldValue) -> Option<T>,
    ) -> Result<Option<T>, SchemaError> {
        match self.get_value(name) {
            Some(Some(val)) => extract(val)
                .map(Some)
                .ok_or_else(|| SchemaError::TypeMismatch {
                    field: name.to_string(),
                    expected: expected.to_string(),
                    actual: format!("{val:?}"),
                }),
            Some(None) => Ok(None),
            None => Err(SchemaError::FieldNotFound(name.to_string())),
        }
    }

    pub fn get_string(&self, name: &str) -> Result<Option<&str>, SchemaError> {
        self.get_typed(name, "STRING", |v| match v {
            FieldValue::String(s) => Some(s.as_str()),
            _ => None,
        })
    }

    pub fn get_i64(&self, name: &str) -> Result<Option<i64>, SchemaError> {
        self.get_typed(name, "INT64", |v| match v {
            FieldValue::Int64(n) => Some(*n),
            FieldValue::Int32(n) => Some(i64::from(*n)),
            _ => None,
        })
    }

    pub fn get_i32(&self, name: &str) -> Result<Option<i32>, SchemaError> {
        self.get_typed(name, "INT32", |v| match v {
            FieldValue::Int32(n) => Some(*n),
            _ => None,
        })
    }

    pub fn get_f64(&self, name: &str) -> Result<Option<f64>, SchemaError> {
        self.get_typed(name, "DOUBLE", |v| match v {
            FieldValue::Double(n) => Some(*n),
            FieldValue::Float(n) => Some(f64::from(*n)),
            _ => None,
        })
    }

    pub fn get_bool(&self, name: &str) -> Result<Option<bool>, SchemaError> {
        self.get_typed(name, "BOOLEAN", |v| match v {
            FieldValue::Boolean(b) => Some(*b),
            _ => None,
        })
    }

    pub fn get_bytes(&self, name: &str) -> Result<Option<&[u8]>, SchemaError> {
        self.get_typed(name, "BYTES", |v| match v {
            FieldValue::Bytes(b) => Some(b.as_slice()),
            _ => None,
        })
    }

    pub fn get_row(&self, name: &str) -> Result<Option<&Row>, SchemaError> {
        self.get_typed(name, "ROW", |v| match v {
            FieldValue::Row(r) => Some(r),
            _ => None,
        })
    }

    /// Encodes this row in the standard `beam:coder:row:v1` wire format.
    pub fn to_row_bytes(&self) -> Result<Vec<u8>, CoderError> {
        let mut buf = Vec::new();
        RowCoder::encode_row(self, &mut buf)?;
        Ok(buf)
    }

    /// Decodes a row from the standard `beam:coder:row:v1` wire format.
    pub fn from_row_bytes(schema: &Arc<Schema>, bytes: &[u8]) -> Result<Self, CoderError> {
        RowCoder::decode_row(schema, &mut { bytes })
    }
}

pub struct RowBuilder {
    schema: Arc<Schema>,
    values: Vec<Option<FieldValue>>,
    named_values: HashMap<String, Option<FieldValue>>,
}

impl RowBuilder {
    pub fn new(schema: Arc<Schema>) -> Self {
        Self {
            schema,
            values: Vec::new(),
            named_values: HashMap::new(),
        }
    }

    /// Appends a field value. Positional values follow the schema order.
    pub fn with_value(mut self, value: impl Into<FieldValue>) -> Self {
        self.values.push(Some(value.into()));
        self
    }

    /// Appends a null field value.
    pub fn with_null(mut self) -> Self {
        self.values.push(None);
        self
    }

    /// Sets a field by name. Then [`build`](Self::build) ignores all positional values.
    pub fn with_named(
        mut self,
        name: impl Into<String>,
        value: Option<impl Into<FieldValue>>,
    ) -> Self {
        self.named_values.insert(name.into(), value.map(Into::into));
        self
    }

    /// Builds the row. Fails if a named non-nullable field has no value, or if the positional
    /// value count is not the schema field count.
    pub fn build(mut self) -> Result<Row, SchemaError> {
        if !self.named_values.is_empty() {
            let values = self
                .schema
                .fields
                .iter()
                .map(|field| {
                    let val = self.named_values.remove(&field.name).flatten();
                    if val.is_none() && !field.field_type.nullable {
                        return Err(SchemaError::InvalidSchema(format!(
                            "Non-nullable field '{}' was not provided in row builder",
                            field.name
                        )));
                    }
                    Ok(val)
                })
                .collect::<Result<Vec<_>, _>>()?;
            Row::new(self.schema, values)
        } else {
            Row::new(self.schema, self.values)
        }
    }
}

impl From<String> for FieldValue {
    fn from(v: String) -> Self {
        Self::String(v)
    }
}

impl From<&str> for FieldValue {
    fn from(v: &str) -> Self {
        Self::String(v.to_string())
    }
}

impl From<i64> for FieldValue {
    fn from(v: i64) -> Self {
        Self::Int64(v)
    }
}

impl From<i32> for FieldValue {
    fn from(v: i32) -> Self {
        Self::Int32(v)
    }
}

impl From<i16> for FieldValue {
    fn from(v: i16) -> Self {
        Self::Int16(v)
    }
}

impl From<i8> for FieldValue {
    fn from(v: i8) -> Self {
        Self::Byte(v)
    }
}

impl From<f64> for FieldValue {
    fn from(v: f64) -> Self {
        Self::Double(v)
    }
}

impl From<f32> for FieldValue {
    fn from(v: f32) -> Self {
        Self::Float(v)
    }
}

impl From<bool> for FieldValue {
    fn from(v: bool) -> Self {
        Self::Boolean(v)
    }
}

impl From<Vec<u8>> for FieldValue {
    fn from(v: Vec<u8>) -> Self {
        Self::Bytes(v)
    }
}

impl From<Row> for FieldValue {
    fn from(v: Row) -> Self {
        Self::Row(v)
    }
}
