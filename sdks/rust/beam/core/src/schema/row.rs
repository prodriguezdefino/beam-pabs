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

/// A key and its value in a [`FieldValue::Map`]. A `None` value is null.
pub type MapEntry = (FieldValue, Option<FieldValue>);

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

/// A row as the input of a `DoFn`: the row coder bytes of the row, or the row.
///
/// An element from another worker or another SDK arrives as bytes. A step that converts rows
/// into another format, such as Arrow, reads the bytes and builds no [`Row`]. An element from
/// a fused Rust step stays a row, with no encode. Apply the `DoFn` to
/// `PCollection::as_row_elements`.
///
/// Between fused steps, the harness converts a [`Row`] into a `RowElement::Row` and back with
/// no encode. Only these two types convert this way. A generic conversion trait for each pair
/// of types, which the harness looks up by type, could cover more types, for example
/// `#[derive(BeamRow)]` types and Arrow batches. See `take_converted` in the harness handler.
#[derive(Debug, Clone, PartialEq)]
pub enum RowElement {
    /// The `beam:coder:row:v1` bytes of one row.
    Encoded(Vec<u8>),
    Row(Row),
}

impl From<Row> for RowElement {
    fn from(row: Row) -> Self {
        Self::Row(row)
    }
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

    pub(crate) fn into_values(self) -> Vec<Option<FieldValue>> {
        self.values
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

    /// Also reads an INT32 value, widened to `i64`.
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

    pub fn get_i16(&self, name: &str) -> Result<Option<i16>, SchemaError> {
        self.get_typed(name, "INT16", |v| match v {
            FieldValue::Int16(n) => Some(*n),
            _ => None,
        })
    }

    /// Reads a BYTE value.
    pub fn get_i8(&self, name: &str) -> Result<Option<i8>, SchemaError> {
        self.get_typed(name, "BYTE", |v| match v {
            FieldValue::Byte(n) => Some(*n),
            _ => None,
        })
    }

    /// Also reads a FLOAT value, widened to `f64`.
    pub fn get_f64(&self, name: &str) -> Result<Option<f64>, SchemaError> {
        self.get_typed(name, "DOUBLE", |v| match v {
            FieldValue::Double(n) => Some(*n),
            FieldValue::Float(n) => Some(f64::from(*n)),
            _ => None,
        })
    }

    pub fn get_f32(&self, name: &str) -> Result<Option<f32>, SchemaError> {
        self.get_typed(name, "FLOAT", |v| match v {
            FieldValue::Float(n) => Some(*n),
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

    /// Reads an ARRAY or ITERABLE value. A `None` element is a null element.
    pub fn get_array(&self, name: &str) -> Result<Option<&[Option<FieldValue>]>, SchemaError> {
        self.get_typed(name, "ARRAY", |v| match v {
            FieldValue::Array(items) => Some(items.as_slice()),
            _ => None,
        })
    }

    /// Reads the entries of a MAP value, in their order in the row.
    pub fn get_map(&self, name: &str) -> Result<Option<&[MapEntry]>, SchemaError> {
        self.get_typed(name, "MAP", |v| match v {
            FieldValue::Map(entries) => Some(entries.as_slice()),
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

/// Builds a [`Row`] from values in schema order, or from values by field name.
///
/// Use one form for each row: [`build`](Self::build) rejects a builder that mixes them.
pub struct RowBuilder {
    schema: Arc<Schema>,
    values: BuilderValues,
}

/// The values that a [`RowBuilder`] collects.
enum BuilderValues {
    /// Values in schema order. A new builder starts empty in this state.
    Positional(Vec<Option<FieldValue>>),
    Named(HashMap<String, Option<FieldValue>>),
    /// The builder got both positional and named values.
    Mixed,
}

impl RowBuilder {
    pub fn new(schema: Arc<Schema>) -> Self {
        Self {
            schema,
            values: BuilderValues::Positional(Vec::new()),
        }
    }

    fn push(mut self, value: Option<FieldValue>) -> Self {
        self.values = match self.values {
            BuilderValues::Positional(mut values) => {
                values.push(value);
                BuilderValues::Positional(values)
            }
            BuilderValues::Named(_) | BuilderValues::Mixed => BuilderValues::Mixed,
        };
        self
    }

    /// Appends a field value. Positional values follow the schema order.
    pub fn with_value(self, value: impl Into<FieldValue>) -> Self {
        self.push(Some(value.into()))
    }

    /// Appends a null field value.
    pub fn with_null(self) -> Self {
        self.push(None)
    }

    /// Sets a field by name. A field with no named value is null.
    pub fn with_named(
        mut self,
        name: impl Into<String>,
        value: Option<impl Into<FieldValue>>,
    ) -> Self {
        let mut named = match self.values {
            BuilderValues::Positional(values) if values.is_empty() => HashMap::new(),
            BuilderValues::Named(named) => named,
            BuilderValues::Positional(_) | BuilderValues::Mixed => {
                self.values = BuilderValues::Mixed;
                return self;
            }
        };
        named.insert(name.into(), value.map(Into::into));
        self.values = BuilderValues::Named(named);
        self
    }

    /// Builds the row.
    ///
    /// # Errors
    ///
    /// Returns an error if the builder mixes positional and named values, if a name is not a
    /// field of the schema, if a non-nullable field has no named value, or if the positional
    /// value count is not the schema field count.
    pub fn build(self) -> Result<Row, SchemaError> {
        let Self { schema, values } = self;
        match values {
            BuilderValues::Positional(values) => Row::new(schema, values),
            BuilderValues::Named(named) => {
                let values = named_values(&schema, named)?;
                Row::new(schema, values)
            }
            BuilderValues::Mixed => Err(SchemaError::InvalidSchema(
                "Row builder mixes positional and named values".to_string(),
            )),
        }
    }
}

/// Orders the named values by the schema fields.
fn named_values(
    schema: &Schema,
    mut named: HashMap<String, Option<FieldValue>>,
) -> Result<Vec<Option<FieldValue>>, SchemaError> {
    let values = schema
        .fields
        .iter()
        .map(|field| {
            let value = named.remove(&field.name).flatten();
            match (&value, field.field_type.nullable) {
                (None, false) => Err(SchemaError::InvalidSchema(format!(
                    "Non-nullable field '{}' was not provided in row builder",
                    field.name
                ))),
                _ => Ok(value),
            }
        })
        .collect::<Result<Vec<_>, _>>()?;
    // The smallest unknown name makes the error the same for each run.
    match named.into_keys().min() {
        Some(unknown) => Err(SchemaError::FieldNotFound(unknown)),
        None => Ok(values),
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
