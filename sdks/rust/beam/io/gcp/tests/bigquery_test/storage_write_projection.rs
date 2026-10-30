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

//! The fields of the input rows that the native Storage Write connector writes.

use std::sync::Arc;

use arrow_io::Projection;
use arrow_io::arrow_schema::{DataType, Field as ArrowField, Fields};
use beam::schema::{FieldType, FieldValue, Row, Schema};
use gcp::bigquery::storage::write::projection::{FieldSelection, Selection};

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

fn select(selection: FieldSelection, ignore: bool) -> beam::Result<Selection> {
    Selection::new(input(), &selection, ignore)
}

#[test]
fn keep_drop_and_only_choose_the_written_fields() {
    let keep = select(
        FieldSelection::Keep(vec!["name".into(), "id".into()]),
        false,
    );
    assert_eq!(names(keep.unwrap().written()), ["id", "name"]);
    let drop = select(
        FieldSelection::Drop(vec!["extra".into(), "payload".into()]),
        false,
    );
    assert_eq!(names(drop.unwrap().written()), ["id", "name"]);
    let only = select(FieldSelection::Only("payload".into()), false);
    assert_eq!(names(only.unwrap().written()), ["id", "name"]);
    let all = select(FieldSelection::All, false).unwrap();
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
        let err = select(selection, false).unwrap_err().to_string();
        assert!(err.contains(message), "{err}");
    }
}

#[test]
fn a_field_that_the_table_lacks_is_an_error_unless_ignored() {
    let keep = FieldSelection::Keep(vec!["id".into(), "extra".into()]);
    let err = select(keep.clone(), false)
        .unwrap()
        .for_table(&table(&["id"]))
        .err()
        .unwrap()
        .to_string();
    assert!(err.contains("'extra'"), "{err}");

    let selection = select(keep, true)
        .unwrap()
        .for_table(&table(&["ID"]))
        .unwrap();
    assert_eq!(selection.projection, Projection::Fields(vec![0]));
    assert_eq!(names(&selection.written), ["id"]);
}

#[test]
fn all_fields_in_the_table_write_the_input_unchanged() {
    let selection = select(FieldSelection::All, false).unwrap();
    let table = selection
        .for_table(&table(&["id", "name", "extra", "payload"]))
        .unwrap();
    assert!(Arc::ptr_eq(&table.written, selection.input()));
    assert!(table.restore.is_none());
}

#[test]
fn only_unnests_and_a_written_row_goes_back_into_the_only_field() {
    let selection = select(FieldSelection::Only("payload".into()), true).unwrap();
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
