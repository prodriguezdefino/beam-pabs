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

//! Beam schemas from JSON Schemas.

use beam::schema::{Field, FieldType, Schema};
use serde_json::Value;

/// The Beam schema of the objects that the JSON Schema `text` describes.
pub fn schema_from_json_schema(text: &str) -> Result<Schema, String> {
    let root: Value =
        serde_json::from_str(text).map_err(|e| format!("The JSON Schema is not JSON: {e}"))?;
    object_schema(&root, "the JSON Schema")
}

fn object_schema(schema: &Value, path: &str) -> Result<Schema, String> {
    let properties = schema
        .get("properties")
        .and_then(Value::as_object)
        .ok_or_else(|| format!("{path} is not an object schema with properties"))?;
    let required: Vec<&str> = schema
        .get("required")
        .and_then(Value::as_array)
        .map(|names| names.iter().filter_map(Value::as_str).collect())
        .unwrap_or_default();
    let required_fields = required.iter().map(|name| {
        let property = properties
            .get(*name)
            .ok_or_else(|| format!("{path} requires '{name}', which is not a property"))?;
        let field_type = field_type(property, &format!("{path}.{name}"))?;
        Ok(Field::new(
            *name,
            field_type.with_nullable(declares_null(property)),
        ))
    });
    // serde_json keeps object members in name order.
    let optional_fields = properties
        .iter()
        .filter(|(name, _)| !required.contains(&name.as_str()))
        .map(|(name, property)| {
            let field_type = field_type(property, &format!("{path}.{name}"))?;
            Ok(Field::nullable(name.as_str(), field_type))
        });
    required_fields
        .chain(optional_fields)
        .collect::<Result<Vec<_>, String>>()
        .map(Schema::new)
}

/// Whether a property schema allows null: `"nullable": true`, or `null` in its type list.
fn declares_null(schema: &Value) -> bool {
    schema.get("nullable").and_then(Value::as_bool) == Some(true)
        || type_names(schema).contains(&"null")
}

fn type_names(schema: &Value) -> Vec<&str> {
    match schema.get("type") {
        Some(Value::String(name)) => vec![name.as_str()],
        Some(Value::Array(names)) => names.iter().filter_map(Value::as_str).collect(),
        _ => Vec::new(),
    }
}

fn field_type(schema: &Value, path: &str) -> Result<FieldType, String> {
    if schema.get("$ref").is_some() {
        return Err(format!("{path}: $ref is not supported"));
    }
    let names: Vec<&str> = type_names(schema)
        .into_iter()
        .filter(|name| *name != "null")
        .collect();
    let name = match (names.as_slice(), schema.get("properties")) {
        ([name], _) => *name,
        ([], Some(_)) => "object",
        _ => return Err(format!("{path}: expected one type, found {names:?}")),
    };
    match name {
        "object" => object_schema(schema, path).map(FieldType::row),
        "integer" => Ok(FieldType::int64()),
        "number" => Ok(FieldType::double()),
        "string" => Ok(FieldType::string()),
        "boolean" => Ok(FieldType::boolean()),
        "array" => match schema.get("items") {
            Some(items @ Value::Object(_)) => {
                let element = field_type(items, &format!("{path}[]"))?;
                Ok(FieldType::array(
                    element.with_nullable(declares_null(items)),
                ))
            }
            _ => Err(format!(
                "{path}: an array needs one items schema; tuple arrays are not supported"
            )),
        },
        other => Err(format!("{path}: the type '{other}' is not supported")),
    }
}
