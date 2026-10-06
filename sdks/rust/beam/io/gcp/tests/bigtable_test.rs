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

mod common;

use std::sync::Arc;

use beam::prelude::*;
use beam::schema::FieldValue;
use common::{Role, assert_spliced, decode_payload, describe, fixture, s, start_mock, values};
use external::{ExpansionError, URN_EXPANSION_SCHEMA_TRANSFORM};
use gcp::bigtable::{
    BigtableCell, BigtableColumn, BigtableMutation, BigtableRead, BigtableWrite,
    DEFAULT_EXPANSION_SERVICE, URN_BIGTABLE_READ, URN_BIGTABLE_WRITE, cell_schema,
    flattened_row_schema, mutation_schema, nested_row_schema,
};

// Configuration schema that Java derives for `BigtableReadSchemaTransformConfiguration`
// in `BigtableReadSchemaTransformProvider.java`.
//
// Java derives the names with `AutoValueSchema(getters).sorted().toSnakeCase()`
// (`getInstanceId` becomes `instance_id`). Only `getFlatten()` is `@Nullable`.
const BIGTABLE_READ_CONFIG: &[(&str, &str)] = &[
    ("flatten", "BOOLEAN?"),
    ("instance_id", "STRING"),
    ("project_id", "STRING"),
    ("table_id", "STRING"),
];

// Configuration schema that Java derives for `BigtableWriteSchemaTransformConfiguration`
// in `BigtableWriteSchemaTransformProvider.java`.
const BIGTABLE_WRITE_CONFIG: &[(&str, &str)] = &[
    ("instance_id", "STRING"),
    ("project_id", "STRING"),
    ("table_id", "STRING"),
];

const CELL: &str = "ROW<value: BYTES, timestamp_micros: INT64>";

fn config_values(flatten: Option<bool>) -> Vec<(String, Option<FieldValue>)> {
    flatten
        .map(|f| ("flatten".to_string(), Some(FieldValue::Boolean(f))))
        .into_iter()
        .chain([
            ("instance_id".to_string(), s("i")),
            ("project_id".to_string(), s("p")),
            ("table_id".to_string(), s("t")),
        ])
        .collect()
}

// Element schemas must equal the Java constants
// `BigtableReadSchemaTransformProvider.{CELL_SCHEMA, FLATTENED_ROW_SCHEMA, ROW_SCHEMA}`.
// The mutation schema must pass the field checks in
// `BigtableWriteSchemaTransformProvider.BigtableWriteSchemaTransform.expand`: key BYTES,
// type STRING, value BYTES, column_qualifier BYTES, family_name STRING and
// *_timestamp_micros INT64.
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
}

#[test]
fn test_bigtable_read_default_config_row() {
    // `flatten` defaults to true. The transform always sends it explicitly.
    let read = BigtableRead::new("BigtableRead", "p", "i", "t");
    assert_eq!(read.output_schema(), flattened_row_schema());
    let row = read.build_config_row().unwrap();
    assert_eq!(describe(row.schema()), fixture(BIGTABLE_READ_CONFIG));
    assert_eq!(values(&row), config_values(Some(true)));

    let source = read.build().unwrap();
    assert_eq!(source.transform().endpoint, DEFAULT_EXPANSION_SERVICE);
    assert_eq!(source.transform().name, "BigtableRead");
    assert_eq!(source.main_output_tag(), "output");
}

#[test]
fn test_bigtable_read_config_row() {
    let read = BigtableRead::new("BigtableRead", "p", "i", "t")
        .with_flatten(false)
        .with_expansion_service("host:1234");
    assert_eq!(read.output_schema(), nested_row_schema());

    let source = read.build().unwrap();
    assert_eq!(source.transform().endpoint, "host:1234");
    assert_eq!(source.transform().urn, URN_EXPANSION_SCHEMA_TRANSFORM);
    let (identifier, row) = decode_payload(&source.transform().payload);
    assert_eq!(identifier, URN_BIGTABLE_READ);
    assert_eq!(describe(row.schema()), fixture(BIGTABLE_READ_CONFIG));
    assert_eq!(values(&row), config_values(Some(false)));
}

#[test]
fn test_bigtable_read_validation() {
    let err = |read: BigtableRead| read.build().expect_err("invalid read");
    let msg = |what: &str| {
        ExpansionError::InvalidResponse(format!("BigtableRead requires a non-empty {what}"))
    };
    assert_eq!(
        err(BigtableRead::new("BigtableRead", "", "", "")),
        msg("project_id")
    );
    assert_eq!(
        err(BigtableRead::new("BigtableRead", "p", "i", "")),
        msg("table_id")
    );
    assert_eq!(
        err(BigtableRead::new("BigtableRead", "p", "", "t")),
        msg("instance_id")
    );
}

#[test]
fn test_bigtable_write_validation() {
    let msg = |what: &str| {
        ExpansionError::InvalidResponse(format!("BigtableWrite requires a non-empty {what}"))
    };
    assert_eq!(
        BigtableWrite::new("BigtableWrite", "", "", "")
            .build()
            .err(),
        Some(msg("project_id"))
    );
    assert_eq!(
        BigtableWrite::new("BigtableWrite", "p", "", "t")
            .build()
            .err(),
        Some(msg("instance_id"))
    );
    let sink = BigtableWrite::new("BigtableWrite", "p", "i", "t")
        .build()
        .unwrap();
    assert_eq!(sink.input_tag(), "input");
    assert_eq!(sink.transform().name, "BigtableWrite");
    assert_eq!(sink.transform().endpoint, DEFAULT_EXPANSION_SERVICE);
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

    let set = BigtableMutation::set_cell("k", "cf", "q", "v").to_row();
    assert_eq!(
        values(&set),
        row([
            b(b"k"),
            s("SetCell"),
            b(b"v"),
            b(b"q"),
            s("cf"),
            None,
            None,
            None
        ])
    );
    let set_ts = BigtableMutation::SetCell {
        key: b"k".to_vec(),
        family_name: "cf".into(),
        column_qualifier: b"q".to_vec(),
        value: b"v".to_vec(),
        timestamp_micros: Some(7),
    }
    .to_row();
    assert_eq!(
        values(&set_ts),
        row([
            b(b"k"),
            s("SetCell"),
            b(b"v"),
            b(b"q"),
            s("cf"),
            i(7),
            None,
            None
        ])
    );

    let del_col = BigtableMutation::DeleteFromColumn {
        key: b"k".to_vec(),
        family_name: "cf".into(),
        column_qualifier: b"q".to_vec(),
        start_timestamp_micros: Some(10),
        end_timestamp_micros: Some(20),
    }
    .to_row();
    assert_eq!(
        values(&del_col),
        row([
            b(b"k"),
            s("DeleteFromColumn"),
            None,
            b(b"q"),
            s("cf"),
            None,
            i(10),
            i(20)
        ])
    );

    let del_fam = BigtableMutation::DeleteFromFamily {
        key: b"k".to_vec(),
        family_name: "cf".into(),
    }
    .to_row();
    assert_eq!(
        values(&del_fam),
        row([
            b(b"k"),
            s("DeleteFromFamily"),
            None,
            None,
            s("cf"),
            None,
            None,
            None
        ])
    );

    let del_row = BigtableMutation::DeleteFromRow { key: b"k".to_vec() }.to_row();
    assert_eq!(
        values(&del_row),
        row([
            b(b"k"),
            s("DeleteFromRow"),
            None,
            None,
            None,
            None,
            None,
            None
        ])
    );

    // Every mutation row must round-trip through the portable row coder.
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
                Arc::new(cell_schema()),
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

    // Round-trip through the row coder, as the Java harness delivers the row.
    let row = Row::from_row_bytes(&flattened_row_schema(), &row.to_row_bytes().unwrap()).unwrap();

    let column = BigtableColumn::from_row(&row).expect("decodes");
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
fn test_bigtable_pipeline_integration() {
    let mock = start_mock(&[
        (URN_BIGTABLE_READ, Role::Source),
        (URN_BIGTABLE_WRITE, Role::Sink),
    ]);
    let p = Pipeline::new();

    let columns = p.apply(
        BigtableRead::new("BigtableRead", "p", "i", "t").with_expansion_service(&mock.endpoint),
    );
    let mutations = p
        .apply(Create::new(
            "Mutations",
            vec![
                BigtableMutation::set_cell("k1", "cf", "q", "v").to_row(),
                BigtableMutation::DeleteFromRow {
                    key: b"k2".to_vec(),
                }
                .to_row(),
            ],
        ))
        .with_row_schema(&mutation_schema());
    mutations.apply(
        BigtableWrite::new("BigtableWrite", "p", "i", "t").with_expansion_service(&mock.endpoint),
    );

    let seen = mock.seen();
    assert_eq!(seen.len(), 2, "{seen:?}");
    let (read, write) = (&seen[0], &seen[1]);

    assert_eq!(read.unique_name, "BigtableRead");
    assert_eq!(read.spec_urn, URN_EXPANSION_SCHEMA_TRANSFORM);
    assert_eq!(read.identifier, URN_BIGTABLE_READ);
    assert_eq!(
        describe(read.config.schema()),
        fixture(BIGTABLE_READ_CONFIG)
    );
    assert_eq!(values(&read.config), config_values(Some(true)));

    assert_eq!(write.unique_name, "BigtableWrite");
    assert_eq!(write.identifier, URN_BIGTABLE_WRITE);
    assert_eq!(
        describe(write.config.schema()),
        fixture(BIGTABLE_WRITE_CONFIG)
    );
    assert_eq!(values(&write.config), config_values(None));
    assert_eq!(write.inputs["input"], mutations.id());

    let read_out = format!("{}/output", read.namespace);
    assert_eq!(columns.id(), read_out);
    assert_eq!(columns.coder_id(), format!("{}/row_coder", read.namespace));
    assert_spliced(&p, read, &[], &[("output", &read_out)]);
    assert_spliced(&p, write, &[("input", mutations.id())], &[]);
}
