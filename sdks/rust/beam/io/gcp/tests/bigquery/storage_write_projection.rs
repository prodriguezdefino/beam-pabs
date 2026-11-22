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

//! The fields of the input rows that the Storage Write connector writes.

use std::sync::Arc;

use beam::coders::RowCoder;
use beam::schema::{FieldType, FieldValue, Row, Schema, TypeInfo};
use beam_io_arrow::FieldSelection;
use beam_io_arrow::Projection;
use beam_io_arrow::arrow_schema::{DataType, Field as ArrowField, Fields};
use beam_io_gcp::bigquery::write::projection::Selection;
use beam_io_gcp::bigquery::write_failure_schema;

fn input() -> Arc<Schema> {
    let payload = Schema::builder()
        .field("id", FieldType::int64())
        .nullable_field("name", FieldType::string())
        .build();
    Arc::new(
        Schema::builder()
            .field("id", FieldType::int64())
            .nullable_field("name", FieldType::string())
            .nullable_field("extra", FieldType::string())
            .nullable_field("payload", FieldType::row(payload))
            .build(),
    )
}

fn table(names: &[&str]) -> Fields {
    names
        .iter()
        .map(|name| ArrowField::new(*name, DataType::Int64, true))
        .collect()
}

fn names(schema: &Schema) -> Vec<&str> {
    schema.fields.iter().map(|f| f.name.as_str()).collect()
}

fn select(selection: &FieldSelection, ignore: bool) -> beam::Result<Selection> {
    Selection::new(input(), selection, ignore)
}

#[test]
fn keep_drop_and_only_choose_the_written_fields() {
    let keep = select(
        &FieldSelection::Keep(vec!["name".into(), "id".into()]),
        false,
    );
    assert_eq!(names(keep.unwrap().written()), ["id", "name"]);
    let drop = select(
        &FieldSelection::Drop(vec!["extra".into(), "payload".into()]),
        false,
    );
    assert_eq!(names(drop.unwrap().written()), ["id", "name"]);
    let only = select(&FieldSelection::Only("payload".into()), false);
    assert_eq!(names(only.unwrap().written()), ["id", "name"]);
    let all = select(&FieldSelection::All, false).unwrap();
    assert_eq!(names(all.written()), ["id", "name", "extra", "payload"]);
}

#[test]
fn a_name_that_the_input_lacks_is_an_error_that_names_it() {
    for (selection, message) in [
        (
            FieldSelection::Keep(vec!["nope".into()]),
            "keep: the input has no field 'nope'",
        ),
        (
            FieldSelection::Drop(vec!["nope".into()]),
            "drop: the input has no field 'nope'",
        ),
        (
            FieldSelection::Only("nope".into()),
            "only: the input has no field 'nope'",
        ),
        (
            FieldSelection::Only("id".into()),
            "only: the field 'id' is not a row",
        ),
    ] {
        let err = select(&selection, false).unwrap_err().to_string();
        assert!(err.contains(message), "{err}");
    }
}

#[test]
fn a_field_that_the_table_lacks_is_an_error_unless_ignored() {
    let keep = FieldSelection::Keep(vec!["id".into(), "extra".into()]);
    let err = select(&keep, false)
        .unwrap()
        .for_table(&table(&["id"]))
        .err()
        .unwrap()
        .to_string();
    assert!(err.contains("'extra'"), "{err}");

    let selection = select(&keep, true)
        .unwrap()
        .for_table(&table(&["ID"]))
        .unwrap();
    assert_eq!(selection.projection, Projection::Fields(vec![0]));
    assert_eq!(names(&selection.written), ["id"]);
}

#[test]
fn all_fields_in_the_table_write_the_input_unchanged() {
    let selection = select(&FieldSelection::All, false).unwrap();
    let table = selection
        .for_table(&table(&["id", "name", "extra", "payload"]))
        .unwrap();
    assert!(Arc::ptr_eq(&table.written, selection.input()));
    assert!(table.restore.is_none());
}

#[test]
fn only_unnests_and_a_written_row_goes_back_into_the_only_field() {
    let selection = select(&FieldSelection::Only("payload".into()), true).unwrap();
    let table = selection.for_table(&table(&["name"])).unwrap();
    assert_eq!(
        table.projection,
        Projection::Unnest {
            field: 3,
            fields: vec![1]
        }
    );
    let written = Row::new(
        Arc::clone(&table.written),
        vec![Some(FieldValue::String("a".into()))],
    )
    .unwrap();
    let row = table.input_row(written).unwrap();
    assert_eq!(row.values().len(), 4);
    assert_eq!(row.values()[..3], [None, None, None]);
    let Some(Some(FieldValue::Row(payload))) = row.values().get(3) else {
        panic!("payload is not a row: {row:?}");
    };
    assert_eq!(
        payload.values(),
        [None, Some(FieldValue::String("a".into()))]
    );
}

fn assert_none_fields_nullable(row: &Row) {
    for (field, value) in row.schema().fields.iter().zip(row.values()) {
        match value {
            None => assert!(
                field.field_type.nullable,
                "field '{}' is None but not nullable",
                field.name
            ),
            Some(FieldValue::Row(nested)) => assert_none_fields_nullable(nested),
            Some(_) => {}
        }
    }
}

#[test]
fn error_input_marks_omitted_fields_nullable_and_round_trips_through_row_coder() {
    let all = select(&FieldSelection::All, false).unwrap();
    assert!(Arc::ptr_eq(all.error_input(), all.input()));

    let all_ignore = select(&FieldSelection::All, true).unwrap();
    assert!(
        all_ignore
            .error_input()
            .fields
            .iter()
            .all(|f| f.field_type.nullable)
    );

    // Keep only `name`, so non-nullable `id` is omitted and becomes nullable in error rows.
    let keep = select(&FieldSelection::Keep(vec!["name".into()]), false).unwrap();
    assert!(keep.error_input().field("id").unwrap().field_type.nullable);
    let keep_table = keep.for_table(&table(&["name"])).unwrap();
    let keep_written = Row::new(
        Arc::clone(&keep_table.written),
        vec![Some(FieldValue::String("ok".into()))],
    )
    .unwrap();
    let keep_restored = keep_table.input_row(keep_written).unwrap();
    let keep_err_schema = Arc::new(write_failure_schema(keep.error_input()));
    let keep_err = Row::new(
        Arc::clone(&keep_err_schema),
        vec![
            Some(FieldValue::Row(keep_restored)),
            Some(FieldValue::String("rejected".into())),
        ],
    )
    .unwrap();
    let mut keep_bytes = Vec::new();
    RowCoder::encode_row(&keep_err, &mut keep_bytes).unwrap();
    let keep_decoded = RowCoder::decode_row(&keep_err_schema, &mut keep_bytes.as_slice()).unwrap();
    assert_eq!(keep_decoded, keep_err);
    assert_none_fields_nullable(&keep_decoded);

    // `Only("payload")` with `ignore_unknown_values` drops both outer `id` and inner `id`.
    let only_strict = select(&FieldSelection::Only("payload".into()), false).unwrap();
    assert!(
        only_strict
            .error_input()
            .field("id")
            .unwrap()
            .field_type
            .nullable
    );
    let TypeInfo::Row(strict_payload) = &only_strict
        .error_input()
        .field("payload")
        .unwrap()
        .field_type
        .type_info
    else {
        panic!("payload field is not a row");
    };
    assert!(!strict_payload.field("id").unwrap().field_type.nullable);

    let only_ignore = select(&FieldSelection::Only("payload".into()), true).unwrap();
    let only_table = only_ignore.for_table(&table(&["name"])).unwrap();
    let only_written = Row::new(
        Arc::clone(&only_table.written),
        vec![Some(FieldValue::String("a".into()))],
    )
    .unwrap();
    let only_restored = only_table.input_row(only_written).unwrap();
    let only_err_schema = Arc::new(write_failure_schema(only_ignore.error_input()));
    let only_err = Row::new(
        Arc::clone(&only_err_schema),
        vec![
            Some(FieldValue::Row(only_restored)),
            Some(FieldValue::String("rejected".into())),
        ],
    )
    .unwrap();
    let mut only_bytes = Vec::new();
    RowCoder::encode_row(&only_err, &mut only_bytes).unwrap();
    let only_decoded = RowCoder::decode_row(&only_err_schema, &mut only_bytes.as_slice()).unwrap();
    assert_eq!(only_decoded, only_err);
    assert_none_fields_nullable(&only_decoded);
}
