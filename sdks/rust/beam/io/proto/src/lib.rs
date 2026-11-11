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

//! Protocol Buffers and Beam rows: Beam schemas from message descriptors, and rows to and from
//! the wire format.
//!
//! Connectors that read or write Protocol Buffers records use this crate, for example the
//! Kafka connector with the `PROTO` format. A pipeline step can also use it, through
//! `beam::io::proto` with the `proto` feature of `beam`.
//!
//! [`ProtoSchema`] takes one message of a file descriptor set, as `protoc --include_imports
//! --descriptor_set_out` writes it. It decodes and encodes the wire format with the dynamic
//! messages of `prost-reflect`, and maps them to and from `Row`s:
//!
//! | Protocol Buffers | Beam |
//! |---|---|
//! | `double`, `float`, `bool`, `string`, `bytes` | `DOUBLE`, `FLOAT`, `BOOLEAN`, `STRING`, `BYTES` |
//! | `int32`, `sint32`, `sfixed32` | `INT32` |
//! | `int64`, `sint64`, `sfixed64`, `uint32`, `fixed32` | `INT64` |
//! | `uint64`, `fixed64` | `INT64`: the same 64 bits, so a value above `i64::MAX` is negative |
//! | `enum` | `STRING`: the name of the value |
//! | message, group | `ROW` |
//! | `repeated T` | `ARRAY<T>` |
//! | `map<K, V>` | `MAP<K, V>`, with the key types above |
//!
//! A field with presence is nullable: a message field, a member of a `oneof` (also the
//! `oneof` of a proto3 `optional` field), and a proto2 `optional` field. The decode gives null
//! for such a field that the message does not have. The other fields are not nullable, and the
//! decode gives their default value. A `oneof` is not a Beam type: each member is a field of
//! its own, and the encode does not check that only one member is set.
//!
//! Not supported: recursive messages and extensions. Well-known types such as
//! `google.protobuf.Timestamp` and `google.protobuf.Any` are rows of their fields, not Beam
//! logical types. The decode fails on an enum number that the enum does not have.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use beam::schema::{Field, FieldType, FieldValue, Row, Schema};
use prost::Message;
use prost::bytes::Bytes;
use prost_reflect::{
    Cardinality, DescriptorPool, DynamicMessage, EnumDescriptor, FieldDescriptor, Kind, MapKey,
    MessageDescriptor, Value,
};

/// One Protocol Buffers message type, with the Beam schema of its rows.
#[derive(Clone, Debug)]
pub struct ProtoSchema {
    message: Arc<MessageShape>,
}

impl ProtoSchema {
    /// The message `message_name` of `descriptor_set`, a serialized
    /// `google.protobuf.FileDescriptorSet`. The name is the full name, with the package.
    ///
    /// # Errors
    ///
    /// Fails if `descriptor_set` does not decode, does not have the message, or the message
    /// is recursive.
    pub fn new(descriptor_set: &[u8], message_name: &str) -> Result<Self, String> {
        let pool = DescriptorPool::decode(descriptor_set)
            .map_err(|e| format!("The file descriptor set does not decode: {e}"))?;
        let name = message_name.trim_start_matches('.');
        let descriptor = pool
            .get_message_by_name(name)
            .ok_or_else(|| format!("The descriptor set has no message '{name}'"))?;
        let message = message_shape(&descriptor, &mut Vec::new(), &mut HashMap::new())?;
        Ok(Self { message })
    }

    /// The message `message_name` of the file descriptor set in the file `path`.
    ///
    /// # Errors
    ///
    /// Fails if the file does not read, or as [`ProtoSchema::new`] fails.
    pub fn from_file(path: impl AsRef<Path>, message_name: &str) -> Result<Self, String> {
        let path = path.as_ref();
        let bytes = std::fs::read(path).map_err(|e| {
            format!(
                "Cannot read the file descriptor set {}: {e}",
                path.display()
            )
        })?;
        Self::new(&bytes, message_name)
    }

    /// The Beam schema of the rows.
    pub fn schema(&self) -> &Arc<Schema> {
        &self.message.schema
    }

    /// The full name of the message.
    pub fn message_name(&self) -> &str {
        self.message.descriptor.full_name()
    }

    /// The row of the message `bytes`.
    ///
    /// # Errors
    ///
    /// Fails if `bytes` is not a message of this type.
    pub fn decode(&self, bytes: &[u8]) -> Result<Row, String> {
        let message = DynamicMessage::decode(self.message.descriptor.clone(), bytes)
            .map_err(|e| format!("The message does not decode: {e}"))?;
        message_row(&self.message, message)
    }

    /// Writes the message of `row` at the end of `buffer`.
    ///
    /// # Errors
    ///
    /// Fails if a value of `row` does not fit its field.
    pub fn encode(&self, row: &Row, buffer: &mut Vec<u8>) -> Result<(), String> {
        row_message(&self.message, row)?
            .encode(buffer)
            .map_err(|e| format!("The message does not encode: {e}"))
    }

    /// Fails if `schema` does not have the field names of the message, in their order.
    ///
    /// # Errors
    ///
    /// Fails with the names of both.
    pub fn check_fields(&self, schema: &Schema) -> Result<(), String> {
        let names = schema.fields.iter().map(|f| f.name.as_str());
        let expected = self.message.fields.iter().map(|f| f.descriptor.name());
        if names.eq(expected) {
            Ok(())
        } else {
            Err(format!(
                "The rows have the fields {:?}, but the message '{}' has {:?}",
                schema.fields.iter().map(|f| &f.name).collect::<Vec<_>>(),
                self.message_name(),
                self.message
                    .fields
                    .iter()
                    .map(|f| f.descriptor.name())
                    .collect::<Vec<_>>(),
            ))
        }
    }
}

/// A message type and the Beam schema of its rows.
#[derive(Debug)]
struct MessageShape {
    descriptor: MessageDescriptor,
    /// The fields in the order of the descriptor, which is the order of the Beam fields.
    fields: Vec<FieldShape>,
    schema: Arc<Schema>,
}

#[derive(Debug)]
struct FieldShape {
    descriptor: FieldDescriptor,
    value: ValueShape,
    nullable: bool,
}

/// The Beam side of the values of a field.
#[derive(Debug)]
enum ValueShape {
    Int32,
    Int64,
    Uint32,
    Uint64,
    Float,
    Double,
    Bool,
    String,
    Bytes,
    Enum(EnumDescriptor),
    Message(Arc<MessageShape>),
    List(Box<ValueShape>),
    Map(Box<ValueShape>, Box<ValueShape>),
}

impl ValueShape {
    fn beam_type(&self) -> FieldType {
        match self {
            Self::Int32 => FieldType::int32(),
            Self::Int64 | Self::Uint32 | Self::Uint64 => FieldType::int64(),
            Self::Float => FieldType::float(),
            Self::Double => FieldType::double(),
            Self::Bool => FieldType::boolean(),
            Self::String | Self::Enum(_) => FieldType::string(),
            Self::Bytes => FieldType::bytes(),
            Self::Message(message) => FieldType::row(Arc::clone(&message.schema)),
            Self::List(items) => FieldType::array(items.beam_type()),
            Self::Map(key, value) => FieldType::map(key.beam_type(), value.beam_type()),
        }
    }
}

// Shapes.

/// The shape of `descriptor`. `open` holds the messages that contain it, to find recursive
/// messages, and `done` the shapes made so far.
fn message_shape(
    descriptor: &MessageDescriptor,
    open: &mut Vec<String>,
    done: &mut HashMap<String, Arc<MessageShape>>,
) -> Result<Arc<MessageShape>, String> {
    let name = descriptor.full_name();
    if let Some(shape) = done.get(name) {
        return Ok(Arc::clone(shape));
    }
    if open.iter().any(|outer| outer == name) {
        return Err(format!(
            "The message '{name}' contains itself: recursive messages are not supported"
        ));
    }
    open.push(name.to_string());
    let fields = descriptor
        .fields()
        .map(|field| {
            let value = field_shape(&field, open, done)
                .map_err(|e| format!("Field '{}' of '{name}': {e}", field.name()))?;
            let nullable =
                field.supports_presence() && field.cardinality() != Cardinality::Required;
            Ok(FieldShape {
                descriptor: field,
                value,
                nullable,
            })
        })
        .collect::<Result<Vec<_>, String>>()?;
    open.pop();
    let schema = Schema::new(
        fields
            .iter()
            .map(|field| {
                let field_type = field.value.beam_type().with_nullable(field.nullable);
                Field::new(field.descriptor.name(), field_type)
            })
            .collect(),
    );
    let shape = Arc::new(MessageShape {
        descriptor: descriptor.clone(),
        fields,
        schema: Arc::new(schema),
    });
    done.insert(name.to_string(), Arc::clone(&shape));
    Ok(shape)
}

fn field_shape(
    field: &FieldDescriptor,
    open: &mut Vec<String>,
    done: &mut HashMap<String, Arc<MessageShape>>,
) -> Result<ValueShape, String> {
    match (field.is_map(), field.is_list(), field.kind()) {
        (true, _, Kind::Message(entry)) => Ok(ValueShape::Map(
            Box::new(kind_shape(entry.map_entry_key_field().kind(), open, done)?),
            Box::new(kind_shape(
                entry.map_entry_value_field().kind(),
                open,
                done,
            )?),
        )),
        (false, true, kind) => Ok(ValueShape::List(Box::new(kind_shape(kind, open, done)?))),
        (_, _, kind) => kind_shape(kind, open, done),
    }
}

fn kind_shape(
    kind: Kind,
    open: &mut Vec<String>,
    done: &mut HashMap<String, Arc<MessageShape>>,
) -> Result<ValueShape, String> {
    Ok(match kind {
        Kind::Int32 | Kind::Sint32 | Kind::Sfixed32 => ValueShape::Int32,
        Kind::Int64 | Kind::Sint64 | Kind::Sfixed64 => ValueShape::Int64,
        Kind::Uint32 | Kind::Fixed32 => ValueShape::Uint32,
        Kind::Uint64 | Kind::Fixed64 => ValueShape::Uint64,
        Kind::Float => ValueShape::Float,
        Kind::Double => ValueShape::Double,
        Kind::Bool => ValueShape::Bool,
        Kind::String => ValueShape::String,
        Kind::Bytes => ValueShape::Bytes,
        Kind::Enum(descriptor) => ValueShape::Enum(descriptor),
        Kind::Message(descriptor) => ValueShape::Message(message_shape(&descriptor, open, done)?),
    })
}

// Decoding: a dynamic message to a row. The values move out of the message.

fn message_row(shape: &MessageShape, mut message: DynamicMessage) -> Result<Row, String> {
    let values = shape
        .fields
        .iter()
        .map(|field| {
            let value = match (message.take_field(&field.descriptor), field.nullable) {
                (Some(value), _) => value,
                (None, true) => return Ok(None),
                (None, false) => field.descriptor.default_value(),
            };
            field_value(&field.value, value)
                .map(Some)
                .map_err(|e| format!("Field '{}': {e}", field.descriptor.name()))
        })
        .collect::<Result<Vec<_>, String>>()?;
    Row::new(Arc::clone(&shape.schema), values).map_err(|e| e.to_string())
}

fn field_value(shape: &ValueShape, value: Value) -> Result<FieldValue, String> {
    // The casts reinterpret the bits, as the type table of the crate says.
    Ok(match (shape, value) {
        (ValueShape::Int32, Value::I32(v)) => FieldValue::Int32(v),
        (ValueShape::Int64, Value::I64(v)) => FieldValue::Int64(v),
        (ValueShape::Uint32, Value::U32(v)) => FieldValue::Int64(i64::from(v)),
        (ValueShape::Uint64, Value::U64(v)) => FieldValue::Int64(v as i64),
        (ValueShape::Float, Value::F32(v)) => FieldValue::Float(v),
        (ValueShape::Double, Value::F64(v)) => FieldValue::Double(v),
        (ValueShape::Bool, Value::Bool(v)) => FieldValue::Boolean(v),
        (ValueShape::String, Value::String(v)) => FieldValue::String(v),
        (ValueShape::Bytes, Value::Bytes(v)) => FieldValue::Bytes(Vec::from(v)),
        (ValueShape::Enum(descriptor), Value::EnumNumber(number)) => {
            let value = descriptor.get_value(number).ok_or_else(|| {
                format!(
                    "The enum '{}' has no value {number}",
                    descriptor.full_name()
                )
            })?;
            FieldValue::String(value.name().to_string())
        }
        (ValueShape::Message(message), Value::Message(value)) => {
            FieldValue::Row(message_row(message, value)?)
        }
        (ValueShape::List(items), Value::List(values)) => FieldValue::Array(
            values
                .into_iter()
                .map(|value| field_value(items, value).map(Some))
                .collect::<Result<_, _>>()?,
        ),
        (ValueShape::Map(key, value), Value::Map(entries)) => {
            let mut entries: Vec<(MapKey, Value)> = entries.into_iter().collect();
            // A map has no order. Sort it, so that one message gives one row.
            entries.sort_unstable_by(|a, b| a.0.cmp(&b.0));
            FieldValue::Map(
                entries
                    .into_iter()
                    .map(|(k, v)| {
                        Ok((
                            field_value(key, key_value(k))?,
                            Some(field_value(value, v)?),
                        ))
                    })
                    .collect::<Result<_, String>>()?,
            )
        }
        (shape, value) => return Err(format!("The value {value:?} does not fit {shape:?}")),
    })
}

fn key_value(key: MapKey) -> Value {
    match key {
        MapKey::Bool(v) => Value::Bool(v),
        MapKey::I32(v) => Value::I32(v),
        MapKey::I64(v) => Value::I64(v),
        MapKey::U32(v) => Value::U32(v),
        MapKey::U64(v) => Value::U64(v),
        MapKey::String(v) => Value::String(v),
    }
}

// Encoding: a row to a dynamic message. The values are copied, because the row is borrowed.

fn row_message(shape: &MessageShape, row: &Row) -> Result<DynamicMessage, String> {
    let values = row.values();
    if values.len() != shape.fields.len() {
        return Err(format!(
            "The row has {} fields, the message '{}' {}",
            values.len(),
            shape.descriptor.full_name(),
            shape.fields.len()
        ));
    }
    let mut message = DynamicMessage::new(shape.descriptor.clone());
    shape
        .fields
        .iter()
        .zip(values)
        .filter_map(|(field, value)| value.as_ref().map(|value| (field, value)))
        .try_for_each(|(field, value)| {
            let value = proto_value(&field.value, value)
                .map_err(|e| format!("Field '{}': {e}", field.descriptor.name()))?;
            // A field with no presence leaves out its default value, as protobuf encoders do.
            let implicit =
                !field.nullable && field.descriptor.cardinality() != Cardinality::Required;
            if implicit && value.is_default_for_field(&field.descriptor) {
                return Ok(());
            }
            message
                .try_set_field(&field.descriptor, value)
                .map_err(|e| format!("Field '{}': {e}", field.descriptor.name()))
        })?;
    Ok(message)
}

fn proto_value(shape: &ValueShape, value: &FieldValue) -> Result<Value, String> {
    let mismatch = || format!("The value {value} does not fit {shape:?}");
    let integer = || match value {
        FieldValue::Byte(v) => Some(i64::from(*v)),
        FieldValue::Int16(v) => Some(i64::from(*v)),
        FieldValue::Int32(v) => Some(i64::from(*v)),
        FieldValue::Int64(v) => Some(*v),
        _ => None,
    };
    // The casts reinterpret the bits, as the type table of the crate says.
    Ok(match (shape, value) {
        (ValueShape::Int32, _) => Value::I32(
            integer()
                .and_then(|v| i32::try_from(v).ok())
                .ok_or_else(mismatch)?,
        ),
        (ValueShape::Int64, _) => Value::I64(integer().ok_or_else(mismatch)?),
        (ValueShape::Uint32, _) => Value::U32(
            integer()
                .and_then(|v| u32::try_from(v).ok())
                .ok_or_else(mismatch)?,
        ),
        (ValueShape::Uint64, _) => Value::U64(integer().ok_or_else(mismatch)? as u64),
        (ValueShape::Float, FieldValue::Float(v)) => Value::F32(*v),
        (ValueShape::Double, FieldValue::Double(v)) => Value::F64(*v),
        (ValueShape::Bool, FieldValue::Boolean(v)) => Value::Bool(*v),
        (ValueShape::String, FieldValue::String(v)) => Value::String(v.clone()),
        (ValueShape::Bytes, FieldValue::Bytes(v)) => Value::Bytes(Bytes::copy_from_slice(v)),
        (ValueShape::Enum(descriptor), FieldValue::String(name)) => {
            let value = descriptor.get_value_by_name(name).ok_or_else(|| {
                format!(
                    "'{name}' is not a value of the enum '{}'",
                    descriptor.full_name()
                )
            })?;
            Value::EnumNumber(value.number())
        }
        (ValueShape::Message(message), FieldValue::Row(row)) => {
            Value::Message(row_message(message, row)?)
        }
        (ValueShape::List(items), FieldValue::Array(values)) => Value::List(
            values
                .iter()
                .map(|value| {
                    value
                        .as_ref()
                        .ok_or_else(|| "A repeated field has a null item".to_string())
                        .and_then(|value| proto_value(items, value))
                })
                .collect::<Result<_, _>>()?,
        ),
        (ValueShape::Map(key, value), FieldValue::Map(entries)) => Value::Map(
            entries
                .iter()
                .map(|(k, v)| {
                    let v = v
                        .as_ref()
                        .ok_or_else(|| "A map field has a null value".to_string())?;
                    Ok((map_key(proto_value(key, k)?)?, proto_value(value, v)?))
                })
                .collect::<Result<_, String>>()?,
        ),
        (
            ValueShape::Float
            | ValueShape::Double
            | ValueShape::Bool
            | ValueShape::String
            | ValueShape::Bytes
            | ValueShape::Enum(_)
            | ValueShape::Message(_)
            | ValueShape::List(_)
            | ValueShape::Map(..),
            _,
        ) => return Err(mismatch()),
    })
}

fn map_key(value: Value) -> Result<MapKey, String> {
    match value {
        Value::Bool(v) => Ok(MapKey::Bool(v)),
        Value::I32(v) => Ok(MapKey::I32(v)),
        Value::I64(v) => Ok(MapKey::I64(v)),
        Value::U32(v) => Ok(MapKey::U32(v)),
        Value::U64(v) => Ok(MapKey::U64(v)),
        Value::String(v) => Ok(MapKey::String(v)),
        other => Err(format!("{other:?} is not a map key")),
    }
}
