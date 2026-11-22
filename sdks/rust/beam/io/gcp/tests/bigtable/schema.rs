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

//! Element schemas of Cloud Bigtable rows and mutations.

use std::sync::Arc;

use beam::prelude::*;
use beam::schema::FieldValue;
use beam_io_gcp::bigtable::{
    BigtableCell, BigtableColumn, BigtableMutation, batched_mutation_schema, cell_schema,
    flattened_row_schema, mutation_schema, nested_row_schema,
};
use beam_test_utils::xlang::{describe, fixture, string_value, values};

const CELL: &str = "ROW<value: BYTES, timestamp_micros: INT64>";

#[test]
fn test_bigtable_element_schemas_match_java() {
    assert_eq!(
        describe(&cell_schema()),
        fixture(&[("value", "BYTES"), ("timestamp_micros", "INT64")])
    );
    assert_eq!(
        describe(&flattened_row_schema()),
        fixture(&[
            ("key", "BYTES"),
            ("family_name", "STRING"),
            ("column_qualifier", "BYTES"),
            ("cells", &format!("ARRAY<{CELL}>")),
        ])
    );
    assert_eq!(
        describe(&nested_row_schema()),
        fixture(&[
            ("key", "BYTES"),
            (
                "column_families",
                &format!("MAP<STRING, MAP<STRING, ARRAY<{CELL}>>>"),
            ),
        ])
    );
    assert_eq!(
        describe(&mutation_schema()),
        fixture(&[
            ("key", "BYTES"),
            ("type", "STRING"),
            ("value", "BYTES?"),
            ("column_qualifier", "BYTES?"),
            ("family_name", "STRING?"),
            ("timestamp_micros", "INT64?"),
            ("start_timestamp_micros", "INT64?"),
            ("end_timestamp_micros", "INT64?"),
        ])
    );
    assert_eq!(
        describe(&batched_mutation_schema()),
        fixture(&[("key", "BYTES"), ("mutations", "ARRAY<MAP<STRING, BYTES>>")])
    );
}

#[test]
fn test_mutation_rows() {
    let b = |v: &[u8]| Some(FieldValue::Bytes(v.to_vec()));
    let i = |v: i64| Some(FieldValue::Int64(v));
    let row = |vals: [Option<FieldValue>; 8]| -> Vec<(String, Option<FieldValue>)> {
        [
            "key",
            "type",
            "value",
            "column_qualifier",
            "family_name",
            "timestamp_micros",
            "start_timestamp_micros",
            "end_timestamp_micros",
        ]
        .into_iter()
        .map(String::from)
        .zip(vals)
        .collect()
    };

    let set = Row::from(BigtableMutation::set_cell("k", "cf", "q", "v"));
    assert_eq!(
        values(&set),
        row([
            b(b"k"),
            string_value("SetCell"),
            b(b"v"),
            b(b"q"),
            string_value("cf"),
            None,
            None,
            None
        ])
    );
    let set_ts = Row::from(BigtableMutation::SetCell {
        key: b"k".to_vec(),
        family_name: "cf".into(),
        column_qualifier: b"q".to_vec(),
        value: b"v".to_vec(),
        timestamp_micros: Some(7),
    });
    assert_eq!(
        values(&set_ts),
        row([
            b(b"k"),
            string_value("SetCell"),
            b(b"v"),
            b(b"q"),
            string_value("cf"),
            i(7),
            None,
            None
        ])
    );

    let del_col = Row::from(BigtableMutation::DeleteFromColumn {
        key: b"k".to_vec(),
        family_name: "cf".into(),
        column_qualifier: b"q".to_vec(),
        start_timestamp_micros: Some(10),
        end_timestamp_micros: Some(20),
    });
    assert_eq!(
        values(&del_col),
        row([
            b(b"k"),
            string_value("DeleteFromColumn"),
            None,
            b(b"q"),
            string_value("cf"),
            None,
            i(10),
            i(20)
        ])
    );

    let del_fam = Row::from(BigtableMutation::DeleteFromFamily {
        key: b"k".to_vec(),
        family_name: "cf".into(),
    });
    assert_eq!(
        values(&del_fam),
        row([
            b(b"k"),
            string_value("DeleteFromFamily"),
            None,
            None,
            string_value("cf"),
            None,
            None,
            None
        ])
    );

    let del_row = Row::from(BigtableMutation::DeleteFromRow { key: b"k".to_vec() });
    assert_eq!(
        values(&del_row),
        row([
            b(b"k"),
            string_value("DeleteFromRow"),
            None,
            None,
            None,
            None,
            None,
            None
        ])
    );

    for r in [set, set_ts, del_col, del_fam, del_row] {
        assert_eq!(r.schema(), &mutation_schema());
        let bytes = r.to_row_bytes().unwrap();
        assert_eq!(Row::from_row_bytes(&mutation_schema(), &bytes).unwrap(), r);
    }
}

#[test]
fn test_column_from_flattened_row() {
    let cell = |v: &[u8], ts: i64| {
        Some(FieldValue::Row(
            Row::new(
                cell_schema(),
                vec![
                    Some(FieldValue::Bytes(v.to_vec())),
                    Some(FieldValue::Int64(ts)),
                ],
            )
            .unwrap(),
        ))
    };
    let row = Row::new(
        flattened_row_schema(),
        vec![
            Some(FieldValue::Bytes(b"k".to_vec())),
            Some(FieldValue::String("cf".into())),
            Some(FieldValue::Bytes(b"q".to_vec())),
            Some(FieldValue::Array(vec![cell(b"new", 2), cell(b"old", 1)])),
        ],
    )
    .unwrap();

    let row = Row::from_row_bytes(&flattened_row_schema(), &row.to_row_bytes().unwrap()).unwrap();

    let column = BigtableColumn::try_from(&row).expect("decodes");
    assert_eq!(column.key, b"k");
    assert_eq!(column.family_name, "cf");
    assert_eq!(column.column_qualifier, b"q");
    assert_eq!(
        column.cells,
        vec![
            BigtableCell {
                value: b"new".to_vec(),
                timestamp_micros: 2
            },
            BigtableCell {
                value: b"old".to_vec(),
                timestamp_micros: 1
            },
        ]
    );
    assert_eq!(column.latest_value(), Some(&b"new"[..]));
}

#[test]
fn test_column_try_from_errors() {
    use beam::schema::{Field, FieldType, SchemaError};

    let bad_row = Row::new(
        Arc::new(Schema::new(vec![Field::new(
            "family_name",
            FieldType::string(),
        )])),
        vec![Some(FieldValue::String("cf".into()))],
    )
    .unwrap();
    assert!(matches!(
        BigtableColumn::try_from(&bad_row),
        Err(SchemaError::FieldNotFound(name)) if name == "key"
    ));

    let null_key_row = Row::new(
        Arc::new(Schema::new(vec![
            Field::new("key", FieldType::bytes()),
            Field::new("family_name", FieldType::string()),
            Field::new("column_qualifier", FieldType::bytes()),
            Field::new(
                "cells",
                FieldType::array(FieldType::row((*cell_schema()).clone())),
            ),
        ])),
        vec![
            None,
            Some(FieldValue::String("cf".into())),
            Some(FieldValue::Bytes(b"q".to_vec())),
            Some(FieldValue::Array(vec![])),
        ],
    )
    .unwrap();
    assert!(matches!(
        BigtableColumn::try_from(&null_key_row),
        Err(SchemaError::UnexpectedNull { .. })
    ));
}
