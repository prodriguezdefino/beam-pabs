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

//! The Iceberg schema of a new table, its partition spec, and the check of an input schema
//! against a table.

use std::sync::Arc;

use beam::schema::{Field, FieldType, Schema};
use iceberg::spec::{
    ListType, MapType, NestedField, PrimitiveType, Schema as TableSchema, StructType,
    Transform as IcebergTransform, Type,
};
use iceberg_io::schema::{check_compatible, iceberg_schema, partition_spec};
use iceberg_io::{PartitionField, Transform};

use crate::common::{date, device_metric, millis_instant};

fn primitive(primitive: PrimitiveType) -> Type {
    Type::Primitive(primitive)
}

fn table(fields: Vec<NestedField>) -> TableSchema {
    TableSchema::builder()
        .with_fields(fields.into_iter().map(Arc::new))
        .build()
        .expect("a valid table schema")
}

#[test]
fn the_target_job_row_maps_to_top_level_fields_with_ids_from_1() {
    let schema = iceberg_schema(&device_metric()).expect("a supported schema");
    let expected = table(vec![
        NestedField::required(1, "id", primitive(PrimitiveType::Long)),
        NestedField::required(2, "timestamp_ms", primitive(PrimitiveType::Timestamptz)),
        NestedField::required(3, "device_id", primitive(PrimitiveType::String)),
        NestedField::required(4, "payload", primitive(PrimitiveType::Double)),
        NestedField::optional(5, "location", primitive(PrimitiveType::String)),
    ]);
    assert_eq!(schema.as_struct(), expected.as_struct());
}

#[test]
fn each_supported_type_maps_to_its_iceberg_type() {
    let cases = [
        (FieldType::boolean(), PrimitiveType::Boolean),
        (FieldType::int32(), PrimitiveType::Int),
        (FieldType::int64(), PrimitiveType::Long),
        (FieldType::float(), PrimitiveType::Float),
        (FieldType::double(), PrimitiveType::Double),
        (FieldType::string(), PrimitiveType::String),
        (FieldType::bytes(), PrimitiveType::Binary),
        (millis_instant(), PrimitiveType::Timestamptz),
        (
            FieldType::logical(
                beam::schema::URN_MICROS_INSTANT,
                Vec::new(),
                FieldType::int64(),
            ),
            PrimitiveType::Timestamptz,
        ),
        (date(), PrimitiveType::Date),
    ];
    for (field_type, expected) in cases {
        let schema = Schema::new(vec![Field::new("value", field_type.clone())]);
        let table_schema = iceberg_schema(&schema).expect("a supported type");
        let field = table_schema.field_by_id(1).expect("the field has id 1");
        assert_eq!(*field.field_type, primitive(expected), "{field_type}");
    }
}

#[test]
fn nested_fields_take_ids_after_the_fields_of_their_struct() {
    let inner = Schema::new(vec![
        Field::new("x", FieldType::int32()),
        Field::nullable("y", FieldType::string()),
    ]);
    let schema = Schema::new(vec![
        Field::new("point", FieldType::row(inner)),
        Field::new(
            "tags",
            FieldType::array(FieldType::string().with_nullable(true)),
        ),
        Field::new(
            "counts",
            FieldType::map(FieldType::string(), FieldType::int64()),
        ),
    ]);
    let expected = table(vec![
        NestedField::required(
            1,
            "point",
            Type::Struct(StructType::new(vec![
                NestedField::required(4, "x", primitive(PrimitiveType::Int)).into(),
                NestedField::optional(5, "y", primitive(PrimitiveType::String)).into(),
            ])),
        ),
        NestedField::required(
            2,
            "tags",
            Type::List(ListType::new(
                NestedField::list_element(6, primitive(PrimitiveType::String), false).into(),
            )),
        ),
        NestedField::required(
            3,
            "counts",
            Type::Map(MapType::new(
                NestedField::map_key_element(7, primitive(PrimitiveType::String)).into(),
                NestedField::map_value_element(8, primitive(PrimitiveType::Long), true).into(),
            )),
        ),
    ]);
    let schema = iceberg_schema(&schema).expect("a supported schema");
    assert_eq!(schema.as_struct(), expected.as_struct());
}

#[test]
fn an_unsupported_nested_type_names_its_path() {
    let inner = Schema::new(vec![Field::new("small", FieldType::int16())]);
    let schema = Schema::new(vec![Field::new("outer", FieldType::row(inner))]);
    let error = iceberg_schema(&schema).expect_err("INT16 is not supported");
    assert!(error.to_string().contains("'outer.small'"), "{error}");
}

#[test]
fn each_transform_gives_its_partition_field() {
    let schema = Schema::new(vec![
        Field::new("hourly", millis_instant()),
        Field::new("daily", millis_instant()),
        Field::new("monthly", date()),
        Field::new("yearly", date()),
        Field::new("name", FieldType::string()),
        Field::new("n", FieldType::int64()),
    ]);
    let table_schema = iceberg_schema(&schema).expect("a supported schema");
    let fields = [
        PartitionField::new("hourly", Transform::Hour),
        PartitionField::new("daily", Transform::Day),
        PartitionField::new("monthly", Transform::Month),
        PartitionField::new("yearly", Transform::Year),
        PartitionField::new("name", Transform::Truncate(4)),
        PartitionField::new("n", Transform::Bucket(16)),
        PartitionField::new("name", Transform::Identity),
    ];
    let spec = partition_spec(&fields, &table_schema).expect("a valid spec");
    let actual: Vec<_> = spec
        .fields()
        .iter()
        .map(|field| {
            (
                field.source_id,
                field.field_id,
                field.name.as_str(),
                field.transform,
            )
        })
        .collect();
    assert_eq!(spec.spec_id(), Some(0));
    assert_eq!(
        actual,
        vec![
            (1, Some(1000), "hourly_hour", IcebergTransform::Hour),
            (2, Some(1001), "daily_day", IcebergTransform::Day),
            (3, Some(1002), "monthly_month", IcebergTransform::Month),
            (4, Some(1003), "yearly_year", IcebergTransform::Year),
            (5, Some(1004), "name_trunc", IcebergTransform::Truncate(4)),
            (6, Some(1005), "n_bucket", IcebergTransform::Bucket(16)),
            (5, Some(1006), "name", IcebergTransform::Identity),
        ]
    );
}

#[test]
fn two_time_transforms_of_one_source_are_invalid() {
    let table_schema = iceberg_schema(&device_metric()).expect("a supported schema");
    let fields = [
        PartitionField::new("timestamp_ms", Transform::Hour),
        PartitionField::new("timestamp_ms", Transform::Day),
    ];
    partition_spec(&fields, &table_schema).expect_err("the day partition is redundant");
}

#[test]
fn the_schema_of_the_input_is_compatible_with_its_own_table() {
    let table_schema = iceberg_schema(&device_metric()).expect("a supported schema");
    check_compatible(&device_metric(), &table_schema).expect("the schemas match");
}

#[test]
fn an_optional_table_field_can_be_missing_from_the_input() {
    let table_schema = table(vec![
        NestedField::required(1, "id", primitive(PrimitiveType::Long)),
        NestedField::optional(2, "extra", primitive(PrimitiveType::String)),
    ]);
    let input = Schema::new(vec![Field::new("id", FieldType::int64())]);
    check_compatible(&input, &table_schema).expect("the optional field can be null");
}

#[test]
fn each_mismatch_is_reported() {
    let table_schema = table(vec![
        NestedField::required(1, "id", primitive(PrimitiveType::Long)),
        NestedField::required(2, "name", primitive(PrimitiveType::String)),
        NestedField::required(3, "needed", primitive(PrimitiveType::Int)),
    ]);
    let input = Schema::new(vec![
        Field::new("id", FieldType::int32()),
        Field::nullable("name", FieldType::string()),
        Field::new("unknown", FieldType::string()),
    ]);
    let error = check_compatible(&input, &table_schema)
        .expect_err("the schemas do not match")
        .to_string();
    for expected in [
        "the required table field 'needed' is not in the input",
        "the input field 'id' has the type INT32, but the table field has the type long",
        "the input field 'name' is nullable, but the table field is required",
        "the input field 'unknown' is not in the table",
    ] {
        assert!(error.contains(expected), "missing '{expected}' in: {error}");
    }
}

#[test]
fn a_nested_mismatch_names_its_path() {
    let table_schema = table(vec![NestedField::required(
        1,
        "tags",
        Type::List(ListType::new(
            NestedField::list_element(2, primitive(PrimitiveType::String), true).into(),
        )),
    )]);
    let input = Schema::new(vec![Field::new(
        "tags",
        FieldType::array(FieldType::string().with_nullable(true)),
    )]);
    let error = check_compatible(&input, &table_schema).expect_err("the element is nullable");
    assert!(
        error.to_string().contains("'tags.element' is nullable"),
        "{error}"
    );
}
