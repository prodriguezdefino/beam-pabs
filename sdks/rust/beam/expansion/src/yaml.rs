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

//! Converts the YAML config of a Managed request to a row of the provider schema.

use std::fmt;
use std::sync::Arc;

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use beam::schema::{AtomicType, FieldType, FieldValue, Row, Schema, TypeInfo};
use serde::de::{Deserialize, Deserializer, Error, MapAccess, SeqAccess, Visitor};

/// A YAML node. A `!!binary` scalar of PyYAML arrives as bytes or as base64 text.
#[derive(Debug)]
enum Yaml {
    Null,
    Bool(bool),
    Int(i128),
    Float(f64),
    Str(String),
    Bytes(Vec<u8>),
    Seq(Vec<Yaml>),
    Map(Vec<(Yaml, Yaml)>),
}

impl Yaml {
    fn kind(&self) -> &'static str {
        match self {
            Self::Null => "null",
            Self::Bool(_) => "boolean",
            Self::Int(_) => "integer",
            Self::Float(_) => "float",
            Self::Str(_) => "string",
            Self::Bytes(_) => "binary",
            Self::Seq(_) => "sequence",
            Self::Map(_) => "mapping",
        }
    }
}

/// Parses `text` and converts its mapping to a row of `schema`. `aliases` renames the keys of
/// the top-level mapping, as `(managed key, schema key)`. An empty document gives a row of
/// nulls. A key that is not in the schema is an error.
pub(crate) fn to_row(text: &str, schema: Schema, aliases: &[(&str, &str)]) -> Result<Row, String> {
    let yaml = if text.trim().is_empty() {
        Yaml::Null
    } else {
        // Only `true` and `false` are booleans, as in PyYAML and JSON. YAML 1.1 also reads
        // `y`, `n`, `on` and `off` as booleans. A `!!binary` scalar stays base64 text, the
        // form of a BYTES field.
        let options = serde_saphyr::options! {
            strict_booleans: true,
            ignore_binary_tag_for_string: true,
        };
        serde_saphyr::from_str_with_options::<Option<Yaml>>(text, options)
            .map_err(|e| format!("invalid YAML: {e}"))?
            .unwrap_or(Yaml::Null)
    };
    let entries = match yaml {
        Yaml::Null => Vec::new(),
        Yaml::Map(entries) => entries,
        other => {
            return Err(format!(
                "the config is a {}, expected a mapping",
                other.kind()
            ));
        }
    };
    let entries = string_keys("", entries)?
        .into_iter()
        .map(
            |(key, value)| match aliases.iter().find(|(from, _)| *from == key) {
                Some((_, to)) => ((*to).to_string(), value),
                None => (key, value),
            },
        )
        .collect();
    to_row_value("", &Arc::new(schema), entries)
}

fn string_keys(path: &str, entries: Vec<(Yaml, Yaml)>) -> Result<Vec<(String, Yaml)>, String> {
    entries
        .into_iter()
        .map(|(key, value)| match key {
            Yaml::Str(key) => Ok((key, value)),
            other => Err(format!(
                "a key of '{}' is a {}, expected a string",
                if path.is_empty() { "the config" } else { path },
                other.kind()
            )),
        })
        .collect()
}

fn join(path: &str, key: &str) -> String {
    if path.is_empty() {
        key.to_string()
    } else {
        format!("{path}.{key}")
    }
}

fn to_row_value(
    path: &str,
    schema: &Arc<Schema>,
    entries: Vec<(String, Yaml)>,
) -> Result<Row, String> {
    let mut values = vec![None; schema.num_fields()];
    let mut seen = vec![false; schema.num_fields()];
    for (key, value) in entries {
        let index = schema.field_index(&key).ok_or_else(|| {
            let names: Vec<&str> = schema.fields.iter().map(|f| f.name.as_str()).collect();
            format!(
                "unknown key '{}', expected one of: {}",
                join(path, &key),
                names.join(", ")
            )
        })?;
        if seen[index] {
            return Err(format!("duplicate key '{}'", join(path, &key)));
        }
        seen[index] = true;
        values[index] = to_value(&join(path, &key), &schema.fields[index].field_type, value)?;
    }
    if let Some(field) = schema
        .fields
        .iter()
        .zip(&values)
        .find_map(|(field, value)| (value.is_none() && !field.field_type.nullable).then_some(field))
    {
        return Err(format!(
            "missing required key '{}'",
            join(path, &field.name)
        ));
    }
    Row::new(Arc::clone(schema), values).map_err(|e| e.to_string())
}

/// Converts `value` to a value of `field_type`. A null gives `None`.
fn to_value(path: &str, field_type: &FieldType, value: Yaml) -> Result<Option<FieldValue>, String> {
    let mismatch =
        |value: &Yaml| format!("key '{path}' is a {}, expected {field_type}", value.kind());
    match (&field_type.type_info, value) {
        (_, Yaml::Null) => Ok(None),
        (TypeInfo::Atomic(atomic), value) => to_atomic(path, *atomic, value).map(Some),
        (TypeInfo::Array(element) | TypeInfo::Iterable(element), Yaml::Seq(items)) => items
            .into_iter()
            .map(|item| match to_value(path, element, item)? {
                None if !element.nullable => Err(format!("key '{path}' has a null element")),
                item => Ok(item),
            })
            .collect::<Result<_, _>>()
            .map(|items| Some(FieldValue::Array(items))),
        (TypeInfo::Map(key_type, value_type), Yaml::Map(entries)) => entries
            .into_iter()
            .map(|(key, value)| {
                let key = to_value(path, key_type, key)?
                    .ok_or_else(|| format!("key '{path}' has a null map key"))?;
                let value = match to_value(path, value_type, value)? {
                    None if !value_type.nullable => {
                        return Err(format!("key '{path}' has a null map value"));
                    }
                    value => value,
                };
                Ok((key, value))
            })
            .collect::<Result<_, String>>()
            .map(|entries| Some(FieldValue::Map(entries))),
        (TypeInfo::Row(schema), Yaml::Map(entries)) => {
            to_row_value(path, schema, string_keys(path, entries)?)
                .map(|row| Some(FieldValue::Row(row)))
        }
        (TypeInfo::Logical { representation, .. }, value) => to_value(path, representation, value),
        (
            TypeInfo::Array(_) | TypeInfo::Iterable(_) | TypeInfo::Map(..) | TypeInfo::Row(_),
            value,
        ) => Err(mismatch(&value)),
    }
}

/// Converts a scalar. Numbers and booleans also come from strings, and a string field takes
/// any scalar, as the Java provider does.
fn to_atomic(path: &str, atomic: AtomicType, value: Yaml) -> Result<FieldValue, String> {
    let mismatch = |value: &Yaml| format!("key '{path}' is a {}, expected {atomic}", value.kind());
    let parsed = |text: &str| -> Result<f64, String> {
        text.trim()
            .parse()
            .map_err(|_| format!("key '{path}' is '{text}', expected {atomic}"))
    };
    let integer = |value: Yaml| -> Result<i128, String> {
        match value {
            Yaml::Int(v) => Ok(v),
            Yaml::Str(text) => text
                .trim()
                .parse()
                .map_err(|_| format!("key '{path}' is '{text}', expected {atomic}")),
            other => Err(mismatch(&other)),
        }
    };
    let float = |value: Yaml| -> Result<f64, String> {
        match value {
            Yaml::Float(v) => Ok(v),
            Yaml::Int(v) => Ok(v as f64),
            Yaml::Str(text) => parsed(&text),
            other => Err(mismatch(&other)),
        }
    };
    match atomic {
        AtomicType::Byte => fit(path, atomic, integer(value)?, FieldValue::Byte),
        AtomicType::Int16 => fit(path, atomic, integer(value)?, FieldValue::Int16),
        AtomicType::Int32 => fit(path, atomic, integer(value)?, FieldValue::Int32),
        AtomicType::Int64 => fit(path, atomic, integer(value)?, FieldValue::Int64),
        AtomicType::Float => float(value).map(|v| FieldValue::Float(v as f32)),
        AtomicType::Double => float(value).map(FieldValue::Double),
        AtomicType::Boolean => match value {
            Yaml::Bool(v) => Ok(FieldValue::Boolean(v)),
            Yaml::Str(text) if text == "true" || text == "false" => {
                Ok(FieldValue::Boolean(text == "true"))
            }
            other => Err(mismatch(&other)),
        },
        AtomicType::String => match value {
            Yaml::Str(text) => Ok(FieldValue::String(text)),
            Yaml::Int(v) => Ok(FieldValue::String(v.to_string())),
            Yaml::Float(v) => Ok(FieldValue::String(v.to_string())),
            Yaml::Bool(v) => Ok(FieldValue::String(v.to_string())),
            other => Err(mismatch(&other)),
        },
        AtomicType::Bytes => match value {
            Yaml::Bytes(bytes) => Ok(FieldValue::Bytes(bytes)),
            Yaml::Str(text) => {
                let text: String = text.chars().filter(|c| !c.is_ascii_whitespace()).collect();
                STANDARD
                    .decode(text)
                    .map(FieldValue::Bytes)
                    .map_err(|e| format!("key '{path}' is not base64: {e}"))
            }
            other => Err(mismatch(&other)),
        },
    }
}

fn fit<T: TryFrom<i128>>(
    path: &str,
    atomic: AtomicType,
    value: i128,
    wrap: fn(T) -> FieldValue,
) -> Result<FieldValue, String> {
    T::try_from(value)
        .map(wrap)
        .map_err(|_| format!("key '{path}' value {value} is out of range for {atomic}"))
}

impl<'de> Deserialize<'de> for Yaml {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_any(YamlVisitor)
    }
}

struct YamlVisitor;

impl<'de> Visitor<'de> for YamlVisitor {
    type Value = Yaml;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a YAML value")
    }

    fn visit_unit<E>(self) -> Result<Yaml, E> {
        Ok(Yaml::Null)
    }

    fn visit_none<E>(self) -> Result<Yaml, E> {
        Ok(Yaml::Null)
    }

    fn visit_some<D: Deserializer<'de>>(self, deserializer: D) -> Result<Yaml, D::Error> {
        Yaml::deserialize(deserializer)
    }

    fn visit_bool<E>(self, v: bool) -> Result<Yaml, E> {
        Ok(Yaml::Bool(v))
    }

    fn visit_i64<E>(self, v: i64) -> Result<Yaml, E> {
        Ok(Yaml::Int(i128::from(v)))
    }

    fn visit_u64<E>(self, v: u64) -> Result<Yaml, E> {
        Ok(Yaml::Int(i128::from(v)))
    }

    fn visit_i128<E>(self, v: i128) -> Result<Yaml, E> {
        Ok(Yaml::Int(v))
    }

    fn visit_u128<E: Error>(self, v: u128) -> Result<Yaml, E> {
        i128::try_from(v)
            .map(Yaml::Int)
            .map_err(|_| E::custom(format!("integer {v} is out of range")))
    }

    fn visit_f64<E>(self, v: f64) -> Result<Yaml, E> {
        Ok(Yaml::Float(v))
    }

    fn visit_str<E>(self, v: &str) -> Result<Yaml, E> {
        Ok(Yaml::Str(v.to_string()))
    }

    fn visit_string<E>(self, v: String) -> Result<Yaml, E> {
        Ok(Yaml::Str(v))
    }

    fn visit_bytes<E>(self, v: &[u8]) -> Result<Yaml, E> {
        Ok(Yaml::Bytes(v.to_vec()))
    }

    fn visit_byte_buf<E>(self, v: Vec<u8>) -> Result<Yaml, E> {
        Ok(Yaml::Bytes(v))
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Yaml, A::Error> {
        std::iter::from_fn(|| seq.next_element::<Yaml>().transpose())
            .collect::<Result<_, _>>()
            .map(Yaml::Seq)
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Yaml, A::Error> {
        std::iter::from_fn(|| map.next_entry::<Yaml, Yaml>().transpose())
            .collect::<Result<_, _>>()
            .map(Yaml::Map)
    }
}
