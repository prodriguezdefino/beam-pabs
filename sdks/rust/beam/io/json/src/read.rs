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

//! JSON objects to rows, with no intermediate JSON tree.
//!
//! The reader of a schema walks the tokens of the object once. The name of each member selects
//! the reader of its field, and the reader converts the value into the field value directly.
//! A member that the schema does not have is skipped.
//!
//! serde gives each kind of token to its own visitor method. Every method of [`ValueSeed`]
//! sends its token to one `match` on the expected type and the token, so the conversion rules
//! are in one place: [`ValueSeed::token`] for scalars, `visit_seq` and `visit_map` for
//! containers.

use std::borrow::Cow;
use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use beam::schema::{AtomicType, FieldType, FieldValue, Row, Schema, TypeInfo};
use serde::Deserializer;
use serde::de::{self, DeserializeSeed, IgnoredAny, MapAccess, SeqAccess, Visitor};

/// A schema with more fields than this finds the field of a member with a hash map. A smaller
/// schema compares the names one by one, which is faster for a few short names.
const MAX_SCANNED_FIELDS: usize = 16;

/// Reads JSON objects into rows of one schema. Build it once for each schema: the rows of a
/// nested row field share the schema that the reader holds.
#[derive(Debug, PartialEq)]
pub struct JsonReader {
    schema: Arc<Schema>,
    fields: Vec<FieldReader>,
    /// The field of each member name, for a schema with more than [`MAX_SCANNED_FIELDS`]
    /// fields.
    positions: Option<HashMap<String, usize>>,
}

/// Reads the member of one field.
#[derive(Debug, PartialEq)]
struct FieldReader {
    name: String,
    value: Nullable,
}

/// Reads a value that can be null when `nullable` is set.
#[derive(Debug, PartialEq)]
struct Nullable {
    nullable: bool,
    reader: ValueReader,
}

/// Reads a value of one type. A logical type reads as its representation.
#[derive(Debug, PartialEq)]
enum ValueReader {
    Atomic(AtomicType),
    List(Box<Nullable>),
    Map(AtomicType, Box<Nullable>),
    Row(JsonReader),
}

impl JsonReader {
    /// Builds a reader for the given Beam [`Schema`].
    ///
    /// Fails for a map whose keys are not of an atomic type.
    pub fn new(schema: Arc<Schema>) -> Result<Self, String> {
        let fields: Vec<FieldReader> = schema
            .fields
            .iter()
            .map(|field| {
                let value = Nullable::new(&field.field_type)
                    .map_err(|e| format!("field '{}': {e}", field.name))?;
                Ok(FieldReader {
                    name: field.name.clone(),
                    value,
                })
            })
            .collect::<Result<_, String>>()?;
        let positions = (fields.len() > MAX_SCANNED_FIELDS).then(|| {
            fields
                .iter()
                .enumerate()
                .map(|(i, field)| (field.name.clone(), i))
                .collect()
        });
        Ok(Self {
            schema,
            fields,
            positions,
        })
    }

    pub fn schema(&self) -> &Arc<Schema> {
        &self.schema
    }

    /// The row of the JSON object `bytes`. Members that the schema does not have are ignored,
    /// and a missing member is null. When a member occurs more than once, the last one counts.
    ///
    /// The reader stops at the first error. Bytes that are not JSON give an error that starts
    /// with `not JSON`. A value that does not match its field gives an error that names the
    /// path of the field, for example `field 'a': field 'b': expected INT64, found a string`.
    pub fn read(&self, bytes: &[u8]) -> Result<Row, String> {
        let errors = Errors::default();
        let mut deserializer = serde_json::Deserializer::from_slice(bytes);
        // The root may be null for the visitor, so that null gives the same error as any
        // other value that is not an object.
        let root = ValueSeed {
            expected: Expected::Row(self),
            nullable: true,
            errors: &errors,
        };
        let value = root
            .deserialize(&mut deserializer)
            .and_then(|value| deserializer.end().map(|()| value))
            .map_err(|e| errors.take().unwrap_or_else(|| format!("not JSON: {e}")))?;
        match value {
            Some(FieldValue::Row(row)) => Ok(row),
            None => Err(format!("expected {}, found null", Expected::Row(self))),
            Some(other) => Err(format!("expected {}, found {other}", Expected::Row(self))),
        }
    }

    /// The field of the member `name`, or `None` when the schema has no such field.
    fn position(&self, name: &str) -> Option<usize> {
        match &self.positions {
            Some(positions) => positions.get(name).copied(),
            None => self.fields.iter().position(|field| field.name == name),
        }
    }

    /// Reads the members of an object into a row of this schema.
    fn read_members<'de, A: MapAccess<'de>>(
        &self,
        mut map: A,
        errors: &Errors,
    ) -> Result<Row, A::Error> {
        let mut values: Vec<Option<FieldValue>> = self.fields.iter().map(|_| None).collect();
        while let Some(position) = map.next_key_seed(MemberSeed(self))? {
            let Some(i) = position else {
                map.next_value::<IgnoredAny>()?;
                continue;
            };
            let field = &self.fields[i];
            values[i] = map
                .next_value_seed(ValueSeed::of(&field.value, errors))
                .inspect_err(|_| errors.in_field(&field.name))?;
        }
        // A member that is present and null failed above, so a value that is still missing
        // here belongs to a member that is not in the object.
        let missing = self
            .fields
            .iter()
            .zip(&values)
            .find(|(field, value)| value.is_none() && !field.value.nullable);
        if let Some((field, _)) = missing {
            return Err(errors.fail(format!(
                "field '{}': null in a field that is not nullable",
                field.name
            )));
        }
        Row::new(Arc::clone(&self.schema), values).map_err(|e| errors.fail(e.to_string()))
    }
}

impl Nullable {
    fn new(field_type: &FieldType) -> Result<Self, String> {
        Ok(Self {
            nullable: field_type.nullable,
            reader: ValueReader::new(field_type)?,
        })
    }
}

impl ValueReader {
    fn new(field_type: &FieldType) -> Result<Self, String> {
        match &field_type.type_info {
            TypeInfo::Atomic(atomic) => Ok(Self::Atomic(*atomic)),
            TypeInfo::Array(element) | TypeInfo::Iterable(element) => {
                Nullable::new(element).map(|element| Self::List(Box::new(element)))
            }
            TypeInfo::Map(key, value) => match &key.type_info {
                TypeInfo::Atomic(key) => Ok(Self::Map(*key, Box::new(Nullable::new(value)?))),
                _ => Err(format!("the map key type {key} is not atomic")),
            },
            TypeInfo::Row(schema) => JsonReader::new(Arc::clone(schema)).map(Self::Row),
            TypeInfo::Logical { representation, .. } => Self::new(representation),
        }
    }
}

/// The first error of a read that is not a JSON syntax error.
///
/// serde_json adds the position to the message of an error that a visitor returns. The reader
/// keeps its own message here instead, so the message stays the same for any position, and
/// adds the path of the field as the error goes up through the rows.
#[derive(Default)]
struct Errors(RefCell<Option<String>>);

impl Errors {
    /// Keeps `message` and returns an error that stops the read.
    fn fail<E: de::Error>(&self, message: String) -> E {
        self.0.replace(Some(message));
        E::custom("the value does not match the schema")
    }

    /// Puts the field `name` in front of the kept message, if there is one. A JSON syntax
    /// error keeps no message, so it gets no field.
    fn in_field(&self, name: &str) {
        if let Some(message) = self.0.borrow_mut().as_mut() {
            message.insert_str(0, &format!("field '{name}': "));
        }
    }

    fn take(&self) -> Option<String> {
        self.0.take()
    }
}

/// The type that the reader expects at a position of the JSON text.
#[derive(Clone, Copy)]
enum Expected<'r> {
    Row(&'r JsonReader),
    Atomic(AtomicType),
    List(&'r Nullable),
    Map(AtomicType, &'r Nullable),
}

impl fmt::Display for Expected<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Row(_) => f.write_str("a JSON object"),
            Self::Atomic(atomic) => write!(f, "{atomic}"),
            Self::List(_) => f.write_str("an array"),
            Self::Map(..) => f.write_str("an object"),
        }
    }
}

/// A JSON token that is not an array or an object.
enum Token<'a> {
    Null,
    Bool(bool),
    I64(i64),
    U64(u64),
    F64(f64),
    Str(Cow<'a, str>),
}

impl Token<'_> {
    fn kind(&self) -> &'static str {
        match self {
            Self::Null => "null",
            Self::Bool(_) => "a boolean",
            Self::I64(_) | Self::U64(_) | Self::F64(_) => "a number",
            Self::Str(_) => "a string",
        }
    }
}

/// Reads one JSON value of the expected type. Null gives `None` when the value is nullable.
struct ValueSeed<'r> {
    expected: Expected<'r>,
    nullable: bool,
    errors: &'r Errors,
}

impl<'r> ValueSeed<'r> {
    fn of(value: &'r Nullable, errors: &'r Errors) -> Self {
        let expected = match &value.reader {
            ValueReader::Atomic(atomic) => Expected::Atomic(*atomic),
            ValueReader::List(element) => Expected::List(element),
            ValueReader::Map(key, value) => Expected::Map(*key, value),
            ValueReader::Row(reader) => Expected::Row(reader),
        };
        Self {
            expected,
            nullable: value.nullable,
            errors,
        }
    }

    fn mismatch<E: de::Error>(&self, kind: &str) -> E {
        self.errors
            .fail(format!("expected {}, found {kind}", self.expected))
    }

    /// Converts a scalar token. This is the only place that matches scalars to types.
    fn token<E: de::Error>(self, token: Token<'_>) -> Result<Option<FieldValue>, E> {
        match (self.expected, token) {
            (_, Token::Null) if self.nullable => Ok(None),
            (_, Token::Null) => Err(self
                .errors
                .fail("null in a field that is not nullable".to_string())),
            (Expected::Atomic(atomic), token) => atomic_value(atomic, token)
                .map(Some)
                .map_err(|message| self.errors.fail(message)),
            (_, token) => Err(self.mismatch(token.kind())),
        }
    }
}

impl<'de> DeserializeSeed<'de> for ValueSeed<'_> {
    type Value = Option<FieldValue>;

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<Self::Value, D::Error> {
        deserializer.deserialize_any(self)
    }
}

impl<'de> Visitor<'de> for ValueSeed<'_> {
    type Value = Option<FieldValue>;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.expected)
    }

    fn visit_unit<E: de::Error>(self) -> Result<Self::Value, E> {
        self.token(Token::Null)
    }

    fn visit_bool<E: de::Error>(self, b: bool) -> Result<Self::Value, E> {
        self.token(Token::Bool(b))
    }

    fn visit_i64<E: de::Error>(self, n: i64) -> Result<Self::Value, E> {
        self.token(Token::I64(n))
    }

    fn visit_u64<E: de::Error>(self, n: u64) -> Result<Self::Value, E> {
        self.token(Token::U64(n))
    }

    fn visit_f64<E: de::Error>(self, n: f64) -> Result<Self::Value, E> {
        self.token(Token::F64(n))
    }

    fn visit_str<E: de::Error>(self, text: &str) -> Result<Self::Value, E> {
        self.token(Token::Str(Cow::Borrowed(text)))
    }

    fn visit_string<E: de::Error>(self, text: String) -> Result<Self::Value, E> {
        self.token(Token::Str(Cow::Owned(text)))
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
        let Expected::List(element) = self.expected else {
            return Err(self.mismatch("an array"));
        };
        let mut items = Vec::with_capacity(seq.size_hint().unwrap_or(0));
        while let Some(item) = seq.next_element_seed(ValueSeed::of(element, self.errors))? {
            items.push(item);
        }
        Ok(Some(FieldValue::Array(items)))
    }

    fn visit_map<A: MapAccess<'de>>(self, map: A) -> Result<Self::Value, A::Error> {
        match self.expected {
            Expected::Row(reader) => reader
                .read_members(map, self.errors)
                .map(|row| Some(FieldValue::Row(row))),
            Expected::Map(key, value) => read_map(map, key, value, self.errors).map(Some),
            Expected::Atomic(_) | Expected::List(_) => Err(self.mismatch("an object")),
        }
    }
}

/// The value of a scalar token in a field of type `atomic`.
fn atomic_value(atomic: AtomicType, token: Token<'_>) -> Result<FieldValue, String> {
    match (atomic, token) {
        (AtomicType::Boolean, Token::Bool(b)) => Ok(FieldValue::Boolean(b)),
        (AtomicType::String, Token::Str(text)) => Ok(FieldValue::String(text.into_owned())),
        (AtomicType::Bytes, Token::Str(text)) => BASE64
            .decode(text.as_bytes())
            .map(FieldValue::Bytes)
            .map_err(|e| format!("not base64: {e}")),
        #[allow(
            clippy::cast_possible_truncation,
            reason = "a JSON number is a double, so a FLOAT keeps the nearest value"
        )]
        (AtomicType::Float, Token::F64(n)) => Ok(FieldValue::Float(n as f32)),
        (AtomicType::Double, Token::F64(n)) => Ok(FieldValue::Double(n)),
        (_, Token::I64(n)) => integer_value(atomic, n),
        (_, Token::U64(n)) => match (i64::try_from(n), atomic) {
            (Ok(n), _) => integer_value(atomic, n),
            // A number above i64::MAX is still a number for the floating point types.
            #[allow(
                clippy::cast_precision_loss,
                reason = "a JSON number is a double, so a large integer keeps the nearest value"
            )]
            (Err(_), AtomicType::Float | AtomicType::Double) => {
                atomic_value(atomic, Token::F64(n as f64))
            }
            (Err(_), _) => Err(format!("expected {atomic}, found a number")),
        },
        (_, token) => Err(format!("expected {atomic}, found {}", token.kind())),
    }
}

/// The value of the integer `n` in a field of type `atomic`.
fn integer_value(atomic: AtomicType, n: i64) -> Result<FieldValue, String> {
    let narrow = |_| format!("{n} is out of range for {atomic}");
    match atomic {
        AtomicType::Byte => i8::try_from(n).map(FieldValue::Byte).map_err(narrow),
        AtomicType::Int16 => i16::try_from(n).map(FieldValue::Int16).map_err(narrow),
        AtomicType::Int32 => i32::try_from(n).map(FieldValue::Int32).map_err(narrow),
        AtomicType::Int64 => Ok(FieldValue::Int64(n)),
        #[allow(
            clippy::cast_possible_truncation,
            clippy::cast_precision_loss,
            reason = "a JSON number is a double, so a FLOAT keeps the nearest value"
        )]
        AtomicType::Float => Ok(FieldValue::Float(n as f64 as f32)),
        #[allow(
            clippy::cast_precision_loss,
            reason = "a JSON number is a double, so a large integer keeps the nearest value"
        )]
        AtomicType::Double => Ok(FieldValue::Double(n as f64)),
        AtomicType::Boolean | AtomicType::String | AtomicType::Bytes => {
            Err(format!("expected {atomic}, found a number"))
        }
    }
}

/// Reads the members of an object into a `MAP` value. The entries are in key order, and a key
/// that occurs more than once keeps its last value, as with a JSON tree.
fn read_map<'de, A: MapAccess<'de>>(
    mut map: A,
    key: AtomicType,
    value: &Nullable,
    errors: &Errors,
) -> Result<FieldValue, A::Error> {
    let mut entries: Vec<(String, Option<FieldValue>)> =
        Vec::with_capacity(map.size_hint().unwrap_or(0));
    while let Some(name) = map.next_key::<String>()? {
        entries.push((name, map.next_value_seed(ValueSeed::of(value, errors))?));
    }
    // Reverse, then sort stably: the last member with a name comes first, and the
    // deduplication keeps the first of each name.
    entries.reverse();
    entries.sort_by(|(a, _), (b, _)| a.cmp(b));
    entries.dedup_by(|(a, _), (b, _)| a == b);
    entries
        .into_iter()
        .map(|(name, value)| Ok((map_key(key, name)?, value)))
        .collect::<Result<Vec<_>, String>>()
        .map(FieldValue::Map)
        .map_err(|message| errors.fail(message))
}

/// A map key from the name of a JSON member: the name, or the integer that it holds.
fn map_key(key: AtomicType, name: String) -> Result<FieldValue, String> {
    match key {
        AtomicType::String => Ok(FieldValue::String(name)),
        _ => name
            .parse::<i64>()
            .map_err(|_| format!("the map key '{name}' is not an integer"))
            .and_then(|n| integer_value(key, n)),
    }
}

/// Reads the name of a member as the position of its field, with no copy of the name.
struct MemberSeed<'r>(&'r JsonReader);

impl<'de> DeserializeSeed<'de> for MemberSeed<'_> {
    type Value = Option<usize>;

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<Self::Value, D::Error> {
        deserializer.deserialize_str(self)
    }
}

impl Visitor<'_> for MemberSeed<'_> {
    type Value = Option<usize>;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a member name")
    }

    fn visit_str<E: de::Error>(self, name: &str) -> Result<Self::Value, E> {
        Ok(self.0.position(name))
    }
}
