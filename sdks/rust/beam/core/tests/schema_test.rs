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

use std::sync::Arc;

use beam::schema::{AtomicType, FieldType, FieldValue, Row, Schema, SchemaError, TypeInfo};

#[test]
fn test_schema_builder_and_accessors() {
    let schema = Schema::builder()
        .field("id", FieldType::int64())
        .field("name", FieldType::string())
        .nullable_field("age", FieldType::int32())
        .id("test-schema")
        .build();

    assert_eq!(schema.num_fields(), 3);
    assert_eq!(schema.field_index("id"), Some(0));
    assert_eq!(schema.field_index("name"), Some(1));
    assert_eq!(schema.field_index("age"), Some(2));
    assert_eq!(schema.field_index("unknown"), None);

    assert!(!schema.fields[0].field_type.nullable);
    assert!(schema.fields[2].field_type.nullable);
}

#[test]
fn test_row_builder_and_getters() {
    let schema = Arc::new(
        Schema::builder()
            .field("id", FieldType::int64())
            .field("name", FieldType::string())
            .nullable_field("score", FieldType::double())
            .field("active", FieldType::boolean())
            .field("avatar", FieldType::bytes())
            .build(),
    );

    let row = Row::builder(schema)
        .with_named("id", Some(42i64))
        .with_named("name", Some("Alice"))
        .with_named("score", Some(98.5f64))
        .with_named("active", Some(true))
        .with_named("avatar", Some(FieldValue::Bytes(vec![1, 2, 3])))
        .build()
        .unwrap();

    assert_eq!(row.get_i64("id").unwrap(), Some(42));
    assert_eq!(row.get_string("name").unwrap(), Some("Alice"));
    assert_eq!(row.get_f64("score").unwrap(), Some(98.5));
    assert_eq!(row.get_bool("active").unwrap(), Some(true));
    assert_eq!(row.get_bytes("avatar").unwrap(), Some(&[1u8, 2, 3][..]));
}

#[test]
fn test_row_builder_sequential() {
    let schema = Arc::new(
        Schema::builder()
            .field("a", FieldType::int32())
            .field("b", FieldType::string())
            .nullable_field("c", FieldType::boolean())
            .build(),
    );

    let row = Row::builder(schema)
        .with_value(123i32)
        .with_value("hello")
        .with_null()
        .build()
        .unwrap();

    assert_eq!(row.get_i32("a").unwrap(), Some(123));
    assert_eq!(row.get_string("b").unwrap(), Some("hello"));
    assert_eq!(row.get_bool("c").unwrap(), None);
}

#[test]
fn test_row_validation_errors() {
    let schema = Arc::new(
        Schema::builder()
            .field("req_int", FieldType::int64())
            .field("req_str", FieldType::string())
            .build(),
    );

    // Omission of non-nullable field causes validation error.
    let err = Row::builder(schema.clone())
        .with_named("req_int", Some(10i64))
        .build();
    assert_eq!(
        err,
        Err(SchemaError::InvalidSchema(
            "Non-nullable field 'req_str' was not provided in row builder".to_string()
        ))
    );

    // Value count mismatch returns error.
    let err_count = Row::new(schema.clone(), vec![Some(FieldValue::Int64(1))]);
    assert!(matches!(
        err_count,
        Err(SchemaError::ValueCountMismatch { .. })
    ));

    // Field lookup and type mismatch errors.
    let row = Row::builder(schema)
        .with_value(99i64)
        .with_value("text")
        .build()
        .unwrap();

    assert!(matches!(
        row.get_string("non_existent"),
        Err(SchemaError::FieldNotFound(_))
    ));
    assert!(matches!(
        row.get_i64("req_str"),
        Err(SchemaError::TypeMismatch { .. })
    ));
}

#[test]
fn test_schema_display() {
    assert_eq!(AtomicType::Int64.to_string(), "INT64");
    assert_eq!(FieldType::string().to_string(), "STRING");
    assert_eq!(
        FieldType::nullable_atomic(AtomicType::Float).to_string(),
        "FLOAT?"
    );
    assert_eq!(
        TypeInfo::Array(Box::new(FieldType::int32())).to_string(),
        "ARRAY<INT32>"
    );
}
