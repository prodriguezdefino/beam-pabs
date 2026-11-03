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

//! Batches of `arrow_io` take the names, field ids and types of the table.

use std::sync::Arc;

use arrow_io::arrow_array::cast::AsArray;
use arrow_io::arrow_array::types::TimestampMicrosecondType;
use arrow_io::arrow_array::{Array, RecordBatch};
use arrow_io::arrow_schema::{DataType, TimeUnit};
use arrow_io::{beam_to_arrow_schema, micros_instant_type, rows_to_record_batch};
use beam::schema::{Field, FieldType, FieldValue, Row, Schema};
use iceberg_io::arrow::{Adapter, table_arrow_schema};

use crate::common::{device_metric, metric, table};

fn adapter(input: &Schema, table_schema: &Schema) -> Adapter {
    let target = table_arrow_schema(&table(table_schema)).expect("an Arrow schema");
    Adapter::new(
        &beam_to_arrow_schema(input).expect("an input schema"),
        target,
    )
    .expect("the types change into the table types")
}

fn batch(schema: &Arc<Schema>, rows: &[Row]) -> RecordBatch {
    rows_to_record_batch(schema, rows).expect("an input batch")
}

#[test]
fn millis_become_micros_in_utc_with_the_field_ids_of_the_table() {
    let schema = device_metric();
    let adapter = adapter(&schema, &schema);
    let input = batch(&schema, &[metric(&schema, 1, 1_700_000_000_123, None)]);

    let adapted = adapter.adapt(&input).expect("the batch adapts");

    assert_eq!(adapted.schema(), *adapter.target());
    let field = adapted
        .schema()
        .field_with_name("timestamp_ms")
        .cloned()
        .expect("the field");
    assert_eq!(
        field.data_type(),
        &DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into()))
    );
    assert_eq!(
        field.metadata().get("PARQUET:field_id").map(String::as_str),
        Some("2")
    );
    let micros = adapted.column(1).as_primitive::<TimestampMicrosecondType>();
    assert_eq!(micros.value(0), 1_700_000_000_123_000);
}

#[test]
fn micros_keep_their_buffer() {
    let schema = Arc::new(Schema::new(vec![Field::new("ts", micros_instant_type())]));
    let adapter = adapter(&schema, &schema);
    let instant = |seconds: i64, micros: i64| {
        let beam::schema::TypeInfo::Logical { representation, .. } =
            &schema.fields[0].field_type.type_info
        else {
            panic!("micros_instant is logical");
        };
        let beam::schema::TypeInfo::Row(representation) = &representation.type_info else {
            panic!("micros_instant is a row");
        };
        let value = Row::new(
            Arc::clone(representation),
            vec![
                Some(FieldValue::Int64(seconds)),
                Some(FieldValue::Int64(micros)),
            ],
        )
        .expect("a micros_instant value");
        Row::new(Arc::clone(&schema), vec![Some(FieldValue::Row(value))]).expect("a row")
    };
    let input = batch(&schema, &[instant(10, 5)]);

    let adapted = adapter.adapt(&input).expect("the batch adapts");

    let before = input.column(0).as_primitive::<TimestampMicrosecondType>();
    let after = adapted.column(0).as_primitive::<TimestampMicrosecondType>();
    assert_eq!(after.value(0), 10_000_005);
    assert_eq!(before.values().as_ptr(), after.values().as_ptr());
}

#[test]
fn nested_types_take_the_names_and_field_ids_of_the_table() {
    let inner = Arc::new(Schema::new(vec![
        Field::new("x", FieldType::int32()),
        Field::nullable("y", FieldType::string()),
    ]));
    let schema = Arc::new(Schema::new(vec![
        Field::new("point", FieldType::row(Arc::clone(&inner))),
        Field::new("tags", FieldType::array(FieldType::string())),
        Field::new(
            "counts",
            FieldType::map(FieldType::string(), FieldType::int64()),
        ),
    ]));
    let adapter = adapter(&schema, &schema);
    let point =
        Row::new(Arc::clone(&inner), vec![Some(FieldValue::Int32(3)), None]).expect("a point");
    let row = Row::new(
        Arc::clone(&schema),
        vec![
            Some(FieldValue::Row(point)),
            Some(FieldValue::Array(vec![Some(FieldValue::String(
                "a".into(),
            ))])),
            Some(FieldValue::Map(vec![(
                FieldValue::String("k".into()),
                Some(FieldValue::Int64(7)),
            )])),
        ],
    )
    .expect("a row");

    let adapted = adapter
        .adapt(&batch(&schema, &[row]))
        .expect("the batch adapts");

    assert_eq!(adapted.schema(), *adapter.target());
    let DataType::Map(entries, _) = adapted.schema().field(2).data_type().clone() else {
        panic!("counts is a map");
    };
    assert_eq!(entries.name(), "key_value");
    assert_eq!(adapted.column(2).len(), 1);
}

#[test]
fn a_missing_optional_column_is_null() {
    let table_schema = device_metric();
    let input = Arc::new(Schema::new(table_schema.fields[..4].to_vec()));
    let adapter = adapter(&input, &table_schema);
    let row = Row::new(
        Arc::clone(&input),
        vec![
            Some(FieldValue::Int64(1)),
            Some(FieldValue::Int64(0)),
            Some(FieldValue::String("d".into())),
            Some(FieldValue::Double(1.0)),
        ],
    )
    .expect("a row");

    let adapted = adapter
        .adapt(&batch(&input, &[row]))
        .expect("the batch adapts");

    assert_eq!(adapted.num_columns(), 5);
    assert!(adapted.column(4).is_null(0));
}

#[test]
fn a_missing_required_column_is_rejected() {
    let table_schema = device_metric();
    let input = Schema::new(table_schema.fields[1..].to_vec());
    let target = table_arrow_schema(&table(&table_schema)).expect("an Arrow schema");
    let error = Adapter::new(&beam_to_arrow_schema(&input).expect("a schema"), target)
        .expect_err("id is required");
    assert!(error.to_string().contains("'id'"), "{error}");
}

#[test]
fn a_timestamp_out_of_the_microsecond_range_fails() {
    let schema = device_metric();
    let adapter = adapter(&schema, &schema);
    let input = batch(&schema, &[metric(&schema, 1, i64::MAX, None)]);
    let error = adapter.adapt(&input).expect_err("the value overflows");
    assert!(error.to_string().contains("out of the range"), "{error}");
}
