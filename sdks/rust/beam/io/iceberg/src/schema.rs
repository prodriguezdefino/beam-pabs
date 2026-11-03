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

//! Beam schemas as Iceberg schemas: the schema of a new table, its partition spec, and the
//! check of an input schema against an existing table.
//!
//! | Beam | Iceberg |
//! |---|---|
//! | `BOOLEAN`, `INT32`, `INT64`, `FLOAT`, `DOUBLE`, `STRING`, `BYTES` | `boolean`, `int`, `long`, `float`, `double`, `string`, `binary` |
//! | `micros_instant`, `millis_instant` | `timestamptz` |
//! | `date` | `date` |
//! | `ROW`, `ARRAY` or `ITERABLE`, `MAP` | `struct`, `list`, `map` |
//! | nullable | optional |
//!
//! The write rejects other types, `BYTE`, `INT16` and `decimal` included.

use beam::schema::{
    AtomicType, Field, FieldType, Schema, TypeInfo, URN_DATE, URN_MICROS_INSTANT,
    URN_MILLIS_INSTANT,
};
use iceberg::spec::{
    ListType, MapType, NestedField, NestedFieldRef, PrimitiveType, Schema as TableSchema,
    StructType, Type, UnboundPartitionField, UnboundPartitionSpec,
};

use crate::options::{PartitionField, Transform};

/// The Iceberg form of a Beam field type, one level deep.
enum Shape<'a> {
    Primitive(PrimitiveType),
    Struct(&'a [Field]),
    List(&'a FieldType),
    Map(&'a FieldType, &'a FieldType),
}

/// Reads the shape of the field at `path`, or fails if the write does not support its type.
fn shape<'a>(path: &str, field_type: &'a FieldType) -> beam::Result<Shape<'a>> {
    let unsupported = || {
        Err(beam::Error::msg(format!(
            "The field '{path}' has the type {field_type}, which an Iceberg write does not support"
        )))
    };
    match &field_type.type_info {
        TypeInfo::Atomic(atomic) => match atomic {
            AtomicType::Boolean => Ok(Shape::Primitive(PrimitiveType::Boolean)),
            AtomicType::Int32 => Ok(Shape::Primitive(PrimitiveType::Int)),
            AtomicType::Int64 => Ok(Shape::Primitive(PrimitiveType::Long)),
            AtomicType::Float => Ok(Shape::Primitive(PrimitiveType::Float)),
            AtomicType::Double => Ok(Shape::Primitive(PrimitiveType::Double)),
            AtomicType::String => Ok(Shape::Primitive(PrimitiveType::String)),
            AtomicType::Bytes => Ok(Shape::Primitive(PrimitiveType::Binary)),
            AtomicType::Byte | AtomicType::Int16 => unsupported(),
        },
        TypeInfo::Logical { urn, .. } => match urn.as_str() {
            URN_MICROS_INSTANT | URN_MILLIS_INSTANT => {
                Ok(Shape::Primitive(PrimitiveType::Timestamptz))
            }
            URN_DATE => Ok(Shape::Primitive(PrimitiveType::Date)),
            _ => unsupported(),
        },
        TypeInfo::Row(schema) => Ok(Shape::Struct(&schema.fields)),
        TypeInfo::Array(element) | TypeInfo::Iterable(element) => Ok(Shape::List(element)),
        TypeInfo::Map(key, value) => Ok(Shape::Map(key, value)),
    }
}

fn child(path: &str, name: &str) -> String {
    match path {
        "" => name.to_string(),
        _ => format!("{path}.{name}"),
    }
}

/// The schema of a new table for rows of `schema`.
///
/// The fields of a struct take consecutive ids, and their nested fields take the ids after
/// them. The catalog can give new ids when it creates the table.
///
/// # Errors
///
/// Fails if a field has a type that the write does not support.
pub fn iceberg_schema(schema: &Schema) -> beam::Result<TableSchema> {
    let (fields, _) = struct_fields("", &schema.fields, 1)?;
    Ok(TableSchema::builder().with_fields(fields).build()?)
}

/// The fields of a struct, with ids from `first_id`, and the next free id.
fn struct_fields(
    path: &str,
    fields: &[Field],
    first_id: i32,
) -> beam::Result<(Vec<NestedFieldRef>, i32)> {
    let count = i32::try_from(fields.len()).map_err(beam::Error::msg)?;
    fields.iter().zip(first_id..).try_fold(
        (Vec::with_capacity(fields.len()), first_id + count),
        |(mut out, next_id), (field, id)| {
            let (field_type, next_id) =
                iceberg_type(&child(path, &field.name), &field.field_type, next_id)?;
            out.push(
                NestedField::new(id, &field.name, field_type, !field.field_type.nullable).into(),
            );
            Ok((out, next_id))
        },
    )
}

/// The Iceberg type of `field_type`, with nested ids from `next_id`, and the next free id.
fn iceberg_type(path: &str, field_type: &FieldType, next_id: i32) -> beam::Result<(Type, i32)> {
    match shape(path, field_type)? {
        Shape::Primitive(primitive) => Ok((Type::Primitive(primitive), next_id)),
        Shape::Struct(fields) => {
            let (fields, next_id) = struct_fields(path, fields, next_id)?;
            Ok((Type::Struct(StructType::new(fields)), next_id))
        }
        Shape::List(element) => {
            let (element_type, after) =
                iceberg_type(&child(path, "element"), element, next_id + 1)?;
            let element = NestedField::list_element(next_id, element_type, !element.nullable);
            Ok((Type::List(ListType::new(element.into())), after))
        }
        Shape::Map(key, value) => {
            let (key_type, after_key) = iceberg_type(&child(path, "key"), key, next_id + 2)?;
            let (value_type, after) = iceberg_type(&child(path, "value"), value, after_key)?;
            let key = NestedField::map_key_element(next_id, key_type);
            let value = NestedField::map_value_element(next_id + 1, value_type, !value.nullable);
            Ok((Type::Map(MapType::new(key.into(), value.into())), after))
        }
    }
}

/// The id of the first partition field of a spec. Some REST catalogs reject a new spec whose
/// fields have no id.
const FIRST_PARTITION_FIELD_ID: i32 = 1000;

/// The partition spec of a new table with the schema `schema`: spec id 0, and field ids from
/// 1000.
///
/// # Errors
///
/// Fails if a source field is not a top-level field of `schema`, if the transform does not
/// accept its type, or if a bucket count or a truncate width is 0.
pub fn partition_spec(
    fields: &[PartitionField],
    schema: &TableSchema,
) -> beam::Result<UnboundPartitionSpec> {
    fields
        .iter()
        .zip(FIRST_PARTITION_FIELD_ID..)
        .try_fold(
            UnboundPartitionSpec::builder().with_spec_id(0),
            |spec, (field, field_id)| {
                let PartitionField { source, transform } = field;
                let source_field = schema.as_struct().field_by_name(source).ok_or_else(|| {
                    beam::Error::msg(format!("The partition source '{source}' is not a field"))
                })?;
                let name = match transform {
                    Transform::Identity => source.clone(),
                    Transform::Year => format!("{source}_year"),
                    Transform::Month => format!("{source}_month"),
                    Transform::Day => format!("{source}_day"),
                    Transform::Hour => format!("{source}_hour"),
                    Transform::Bucket(0) | Transform::Truncate(0) => {
                        return Err(beam::Error::msg(format!(
                            "The partition transform of '{source}' needs a value greater than 0"
                        )));
                    }
                    Transform::Bucket(_) => format!("{source}_bucket"),
                    Transform::Truncate(_) => format!("{source}_trunc"),
                };
                let transform = iceberg::spec::Transform::from(*transform);
                transform
                    .result_type(&source_field.field_type)
                    .map_err(|e| {
                        beam::Error::from(e).context(format!("Partition field '{name}'"))
                    })?;
                Ok(spec.add_partition_fields([UnboundPartitionField {
                    source_id: source_field.id,
                    field_id: Some(field_id),
                    name,
                    transform,
                }])?)
            },
        )
        .map(|spec| spec.build())
}

/// Checks that the table can take rows of `input`.
///
/// Each input field must be in the table with the same type. A required table field must be
/// in the input and must not be nullable there. Fields match by name.
///
/// # Errors
///
/// Fails with each field that does not match.
pub fn check_compatible(input: &Schema, table: &TableSchema) -> beam::Result {
    let errors = struct_errors("", &input.fields, table.as_struct());
    match errors.as_slice() {
        [] => Ok(()),
        _ => Err(beam::Error::msg(format!(
            "The table schema does not match the input schema: {}",
            errors.join("; ")
        ))),
    }
}

fn struct_errors(path: &str, input: &[Field], table: &StructType) -> Vec<String> {
    let missing = table
        .fields()
        .iter()
        .filter(|field| field.required && !input.iter().any(|f| f.name == field.name))
        .map(|field| {
            format!(
                "the required table field '{}' is not in the input",
                child(path, &field.name)
            )
        });
    let mismatched = input.iter().flat_map(|field| {
        let path = child(path, &field.name);
        match table.field_by_name(&field.name) {
            None => vec![format!("the input field '{path}' is not in the table")],
            Some(table_field) => field_errors(
                &path,
                &field.field_type,
                &table_field.field_type,
                table_field.required,
            ),
        }
    });
    missing.chain(mismatched).collect()
}

fn field_errors(path: &str, input: &FieldType, table: &Type, required: bool) -> Vec<String> {
    let nullability = (required && input.nullable)
        .then(|| format!("the input field '{path}' is nullable, but the table field is required"));
    let types = match (shape(path, input), table) {
        (Err(e), _) => vec![e.to_string()],
        (Ok(Shape::Primitive(primitive)), Type::Primitive(table_primitive))
            if primitive == *table_primitive =>
        {
            Vec::new()
        }
        (Ok(Shape::Struct(fields)), Type::Struct(table_struct)) => {
            struct_errors(path, fields, table_struct)
        }
        (Ok(Shape::List(element)), Type::List(list)) => field_errors(
            &child(path, "element"),
            element,
            &list.element_field.field_type,
            list.element_field.required,
        ),
        (Ok(Shape::Map(key, value)), Type::Map(map)) => field_errors(
            &child(path, "key"),
            key,
            &map.key_field.field_type,
            map.key_field.required,
        )
        .into_iter()
        .chain(field_errors(
            &child(path, "value"),
            value,
            &map.value_field.field_type,
            map.value_field.required,
        ))
        .collect(),
        (Ok(_), _) => vec![format!(
            "the input field '{path}' has the type {input}, but the table field has the type {table}"
        )],
    };
    nullability.into_iter().chain(types).collect()
}
