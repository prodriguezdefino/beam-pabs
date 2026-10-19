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

//! How the value of a Kafka record maps to a Beam row.
//!
//! The format names, the rows and the error rows are those of the Kafka SchemaTransforms of
//! the Managed API, so a caller can send the same configuration to either expansion service:
//!
//! | Format | Read rows | Written value |
//! |---|---|---|
//! | `RAW` | `(payload: BYTES)` | The one `BYTES` field of the row |
//! | `STRING` | `(payload: STRING)` | Read only |
//! | `JSON` | The fields of the JSON Schema | The row as a JSON object |

use std::fmt;
use std::str::FromStr;
use std::sync::Arc;

use beam::schema::{AtomicType, Field, FieldType, FieldValue, Row, Schema, TypeInfo};
use json_io::{JsonReader, schema_from_json_schema, write_json};

/// The format of the records that a read decodes.
#[derive(Clone, Debug, PartialEq)]
pub enum ReadFormat {
    /// The value as it is: rows `(payload: BYTES)`.
    Raw,
    /// The value as UTF-8 text: rows `(payload: STRING)`.
    String,
    /// A JSON object with the fields of a schema. Build it with [`ReadFormat::json`].
    Json(Arc<JsonReader>),
}

impl ReadFormat {
    /// The JSON format of the objects that the JSON Schema `json_schema` describes.
    pub fn json(json_schema: &str) -> beam::Result<Self> {
        schema_from_json_schema(json_schema)
            .and_then(|schema| JsonReader::new(Arc::new(schema)))
            .map(|reader| Self::Json(Arc::new(reader)))
            .map_err(beam::Error::msg)
    }

    /// Parses a format name in any case. `JSON` needs `json_schema`.
    pub fn parse(name: &str, json_schema: Option<&str>) -> beam::Result<Self> {
        match (name.to_ascii_uppercase().as_str(), json_schema) {
            ("RAW", _) => Ok(Self::Raw),
            ("STRING", _) => Ok(Self::String),
            ("JSON", Some(schema)) => Self::json(schema),
            ("JSON", None) => Err(beam::Error::msg("The JSON format needs a JSON Schema")),
            ("AVRO" | "PROTO", _) => Err(beam::Error::msg(format!(
                "The native Kafka read does not support the {name} format yet"
            ))),
            _ => Err(beam::Error::msg(format!(
                "Unknown format '{name}': expected RAW, STRING or JSON"
            ))),
        }
    }

    /// The schema of the rows of this format.
    pub fn row_schema(&self) -> Arc<Schema> {
        match self {
            Self::Raw => payload_schema(FieldType::bytes()),
            Self::String => payload_schema(FieldType::string()),
            Self::Json(reader) => Arc::clone(reader.schema()),
        }
    }

    /// The row of the record value `payload`. `schema` is [`row_schema`](Self::row_schema),
    /// which the caller computes once.
    pub fn decode(&self, schema: &Arc<Schema>, payload: &[u8]) -> Result<Row, String> {
        let value = match self {
            Self::Raw => FieldValue::Bytes(payload.to_vec()),
            Self::String => std::str::from_utf8(payload)
                .map(|text| FieldValue::String(text.to_string()))
                .map_err(|e| format!("The value is not UTF-8: {e}"))?,
            Self::Json(reader) => return reader.read(payload),
        };
        Row::new(Arc::clone(schema), vec![Some(value)]).map_err(|e| e.to_string())
    }
}

fn payload_schema(field_type: FieldType) -> Arc<Schema> {
    Arc::new(Schema::new(vec![Field::new("payload", field_type)]))
}

/// The schema of the error output of a read: the record value and the reason.
pub fn read_error_schema() -> Schema {
    Schema::new(vec![
        Field::new("failed_row", FieldType::bytes()),
        Field::new("error_message", FieldType::string()),
    ])
}

/// The format of the records that a write produces.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum WriteFormat {
    /// The one `BYTES` field of each row, as it is.
    #[default]
    Raw,
    /// Each row as a JSON object.
    Json,
}

impl fmt::Display for WriteFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Raw => "RAW",
            Self::Json => "JSON",
        })
    }
}

impl FromStr for WriteFormat {
    type Err = beam::Error;

    /// Parses a format name in any case.
    fn from_str(name: &str) -> beam::Result<Self> {
        match name.to_ascii_uppercase().as_str() {
            "RAW" => Ok(Self::Raw),
            "JSON" => Ok(Self::Json),
            "AVRO" | "PROTO" => Err(beam::Error::msg(format!(
                "The native Kafka write does not support the {name} format yet"
            ))),
            _ => Err(beam::Error::msg(format!(
                "Unknown format '{name}': expected RAW or JSON"
            ))),
        }
    }
}

/// Converts rows of one schema into record values.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Encoder {
    /// The `BYTES` field at this index, the only field of the schema.
    Raw(usize),
    Json,
}

impl Encoder {
    /// The encoder of `format` for rows of `schema`. `RAW` needs exactly one field, of type
    /// `BYTES`.
    pub fn new(format: WriteFormat, schema: &Schema) -> beam::Result<Self> {
        match (format, schema.fields.as_slice()) {
            (WriteFormat::Json, _) => Ok(Self::Json),
            (WriteFormat::Raw, [field]) if is_bytes(&field.field_type) => Ok(Self::Raw(0)),
            (WriteFormat::Raw, [field]) => Err(beam::Error::msg(format!(
                "The RAW format needs one BYTES field, but '{}' is {}",
                field.name, field.field_type
            ))),
            (WriteFormat::Raw, fields) => Err(beam::Error::msg(format!(
                "The RAW format needs exactly one BYTES field, found {} fields",
                fields.len()
            ))),
        }
    }

    /// The record value of `row`. `RAW` borrows the value from the row, and `JSON` writes it
    /// into `buffer`, which the caller reuses for each row.
    pub fn encode<'a>(&self, row: &'a Row, buffer: &'a mut Vec<u8>) -> Result<&'a [u8], String> {
        match self {
            Self::Raw(index) => match row.get_value_by_index(*index) {
                Some(Some(FieldValue::Bytes(bytes))) => Ok(bytes),
                Some(None) => Err("The payload is null".to_string()),
                _ => Err("The row has no BYTES payload".to_string()),
            },
            Self::Json => write_json(row, buffer).map(|()| buffer.as_slice()),
        }
    }
}

fn is_bytes(field_type: &FieldType) -> bool {
    matches!(field_type.type_info, TypeInfo::Atomic(AtomicType::Bytes))
}
