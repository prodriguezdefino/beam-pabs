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

fn id_and_name() -> Arc<Schema> {
    Arc::new(
        Schema::builder()
            .field("id", FieldType::int64())
            .nullable_field("name", FieldType::string())
            .build(),
    )
}

#[test]
fn a_row_builder_rejects_mixed_positional_and_named_values() {
    let mixed =
        SchemaError::InvalidSchema("Row builder mixes positional and named values".to_string());
    let positional_then_named = Row::builder(id_and_name())
        .with_value(1i64)
        .with_named("name", Some("a"))
        .build();
    assert_eq!(positional_then_named, Err(mixed.clone()));

    let named_then_positional = Row::builder(id_and_name())
        .with_named("id", Some(1i64))
        .with_null()
        .build();
    assert_eq!(named_then_positional, Err(mixed));
}

#[test]
fn a_row_builder_rejects_an_unknown_field_name() {
    let unknown = Row::builder(id_and_name())
        .with_named("id", Some(1i64))
        .with_named("zeta", Some("z"))
        .with_named("nmae", Some("typo"))
        .build();
    assert_eq!(unknown, Err(SchemaError::FieldNotFound("nmae".to_string())));
}

#[test]
fn a_named_row_builder_orders_values_by_the_schema_and_nulls_missing_fields() {
    let row = Row::builder(id_and_name())
        .with_named("id", Some(7i64))
        .build()
        .unwrap();
    assert_eq!(row.values(), &[Some(FieldValue::Int64(7)), None]);
}

#[test]
fn typed_getters_read_their_own_type_and_reject_others() {
    let schema = Arc::new(
        Schema::builder()
            .field("i16", FieldType::int16())
            .field("i8", FieldType::byte())
            .field("f32", FieldType::float())
            .field("i32", FieldType::int32())
            .field(
                "array",
                FieldType::array(FieldType::int32().with_nullable(true)),
            )
            .field(
                "map",
                FieldType::map(FieldType::string(), FieldType::int64()),
            )
            .nullable_field("null", FieldType::int16())
            .build(),
    );
    let row = Row::builder(schema)
        .with_value(-3i16)
        .with_value(-4i8)
        .with_value(1.5f32)
        .with_value(9i32)
        .with_value(FieldValue::Array(vec![Some(FieldValue::Int32(1)), None]))
        .with_value(FieldValue::Map(vec![(
            FieldValue::from("k"),
            Some(FieldValue::Int64(2)),
        )]))
        .with_null()
        .build()
        .unwrap();

    assert_eq!(row.get_i16("i16"), Ok(Some(-3)));
    assert_eq!(row.get_i8("i8"), Ok(Some(-4)));
    assert_eq!(row.get_f32("f32"), Ok(Some(1.5)));
    assert_eq!(
        row.get_array("array"),
        Ok(Some(&[Some(FieldValue::Int32(1)), None][..]))
    );
    assert_eq!(
        row.get_map("map"),
        Ok(Some(
            &[(FieldValue::from("k"), Some(FieldValue::Int64(2)))][..]
        ))
    );
    assert_eq!(row.get_i16("null"), Ok(None));

    // `get_i64` widens INT32 and `get_f64` widens FLOAT. The narrow getters do not widen.
    assert_eq!(row.get_i64("i32"), Ok(Some(9)));
    assert_eq!(row.get_f64("f32"), Ok(Some(1.5)));
    assert!(matches!(
        row.get_i16("i8"),
        Err(SchemaError::TypeMismatch { .. })
    ));
    assert!(matches!(
        row.get_i8("i16"),
        Err(SchemaError::TypeMismatch { .. })
    ));
    assert!(matches!(
        row.get_f32("i32"),
        Err(SchemaError::TypeMismatch { .. })
    ));
    assert!(matches!(
        row.get_array("map"),
        Err(SchemaError::TypeMismatch { .. })
    ));
    assert!(matches!(
        row.get_map("array"),
        Err(SchemaError::TypeMismatch { .. })
    ));
}
