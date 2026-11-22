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

//! Pipeline construction, validation, and display data tests for Bigtable read and write
//! transforms.

use std::collections::HashMap;
use std::sync::Arc;

use beam::prelude::*;
use beam::schema::{BeamRow, Field, FieldType, FieldValue, Schema};
use beam::transforms::display_data::DisplayDataItem;
use beam_io_gcp::bigtable::{
    BigtableMutation, BigtableReadShape, Read, ReadOptions, ReadRows, Write, WriteOptions,
    WriteRows, flattened_row_schema, mutation_schema, read_failure_schema, write_failure_schema,
};
use bytes::Bytes;

fn mutations(p: &Pipeline) -> PCollection<Row> {
    p.apply(
        "Create",
        Create::new(vec![Row::from(BigtableMutation::set_cell(
            "k", "cf", "q", "v",
        ))]),
    )
    .with_row_schema(&mutation_schema())
}

#[derive(Clone, Debug, PartialEq, Eq, BeamRow)]
struct TypedCell {
    family_name: String,
}

#[derive(Clone, Debug, PartialEq, Eq, BeamRow)]
struct CellValue {
    value: Bytes,
    timestamp_micros: i64,
}

#[derive(Clone, Debug, PartialEq, Eq, BeamRow)]
struct FullFlattenedRow {
    key: Bytes,
    family_name: String,
    column_qualifier: Bytes,
    cells: Vec<CellValue>,
}

#[derive(Clone, Debug, PartialEq, Eq, BeamRow)]
struct MissingFieldRow {
    unknown_field: String,
}

#[derive(Clone, Debug, PartialEq, Eq, BeamRow)]
struct BatchedMutationRow {
    key: Bytes,
    mutations: Vec<HashMap<String, Bytes>>,
}

struct NonClone;

fn assert_clone<C: Clone>(_: &C) {}

#[test]
fn typed_transforms_clone_without_t_clone() {
    let read = Read::<NonClone>::new(ReadOptions::from_table("p", "i", "t"));
    assert_clone(&read);
    let read_failures = read.with_failures();
    assert_clone(&read_failures);

    let write = Write::<NonClone>::new(WriteOptions::from_table("p", "i", "t"));
    assert_clone(&write);
}

#[test]
fn read_rows_with_invalid_options_fails_validate() {
    let p = Pipeline::new();
    let options = ReadOptions::from_table("project", "instance", "table")
        .with_key_range(b"z".to_vec(), b"a".to_vec());
    let rows: PCollection<Row> = p.apply("ReadBigtable", ReadRows::new(options.clone()));
    // Later transforms still attach to the output of the failed read.
    let _ = rows.apply("Reshuffle", Reshuffle::new());

    let err = p
        .validate()
        .expect_err("an inverted key range fails validate");
    let message = err.to_string();
    assert!(message.contains("ReadBigtable"), "{message}");
    assert!(message.contains("key range"), "{message}");

    let p2 = Pipeline::new();
    let try_err = p2
        .try_apply("ReadBigtable", ReadRows::new(options))
        .expect_err("try_apply rejects an inverted key range");
    assert!(try_err.to_string().contains("key range"), "{try_err}");
}

#[test]
fn read_rows_with_valid_options_passes_validate() {
    let p = Pipeline::new();
    let _rows: PCollection<Row> = p.apply(
        "ReadBigtable",
        ReadRows::new(ReadOptions::from_table("project", "instance", "table")),
    );
    p.validate().expect("a valid read passes validate");
}

#[test]
fn read_typed_expands_for_projected_and_exact_schemas_and_validates_errors() {
    assert!(FullFlattenedRow::matches_schema(&flattened_row_schema()));

    let p = Pipeline::new();
    let projected: PCollection<TypedCell> = p.apply(
        "ReadProjected",
        Read::new(ReadOptions::from_table("project", "instance", "table")),
    );
    let _ = projected.apply("ReshuffleProjected", Reshuffle::new());

    let exact: PCollection<FullFlattenedRow> = p
        .try_apply(
            "ReadExact",
            Read::new(ReadOptions::from_table("project", "instance", "table")),
        )
        .expect("valid typed read");
    let _ = exact.apply("ReshuffleExact", Reshuffle::new());
    p.validate().expect("typed reads pass validate");

    // Invalid options fail both `apply` and `try_apply`.
    let bad_opts = ReadOptions::from_table("project", "instance", "table")
        .with_key_range(b"z".to_vec(), b"a".to_vec());
    let p_bad_opts = Pipeline::new();
    let _: PCollection<TypedCell> = p_bad_opts.apply("ReadBadOpts", Read::new(bad_opts.clone()));
    let err = p_bad_opts.validate().expect_err("invalid options fail");
    assert!(err.to_string().contains("ReadBadOpts"), "{err}");

    let p_try_bad_opts = Pipeline::new();
    assert!(
        p_try_bad_opts
            .try_apply("ReadBadOpts", Read::<TypedCell>::new(bad_opts))
            .is_err()
    );

    // A missing required field fails both `apply` and `try_apply`.
    let valid_opts = ReadOptions::from_table("project", "instance", "table");
    let p_bad_type = Pipeline::new();
    let _: PCollection<MissingFieldRow> =
        p_bad_type.apply("ReadBadType", Read::new(valid_opts.clone()));
    let err = p_bad_type.validate().expect_err("incompatible type fails");
    assert!(err.to_string().contains("unknown_field"), "{err}");

    let p_try_bad_type = Pipeline::new();
    let try_err = p_try_bad_type
        .try_apply("ReadBadType", Read::<MissingFieldRow>::new(valid_opts))
        .expect_err("try_apply rejects incompatible type");
    assert!(try_err.to_string().contains("unknown_field"), "{try_err}");
}

#[test]
fn read_with_failures_expands_and_preserves_failure_schema() {
    let p = Pipeline::new();
    let applied = p.apply(
        "ReadFailures",
        Read::<TypedCell>::new(ReadOptions::from_table("project", "instance", "table"))
            .with_failures(),
    );
    assert_eq!(
        applied.failures.row_schema().unwrap(),
        *read_failure_schema()
    );

    let tried = p
        .try_apply(
            "TryReadFailures",
            Read::<FullFlattenedRow>::new(ReadOptions::from_table("project", "instance", "table"))
                .with_failures(),
        )
        .expect("valid typed read with failures");
    assert_eq!(tried.failures.row_schema().unwrap(), *read_failure_schema());
    p.validate()
        .expect("typed reads with failures pass validate");

    let bad_opts = ReadOptions::from_table("project", "instance", "");
    let p_bad = Pipeline::new();
    let invalid = p_bad.apply(
        "ReadInvalid",
        Read::<TypedCell>::new(bad_opts.clone()).with_failures(),
    );
    assert_eq!(
        invalid.failures.row_schema().unwrap(),
        *read_failure_schema()
    );
    let err = p_bad.validate().expect_err("empty table id fails");
    assert!(err.to_string().contains("table_id"), "{err}");

    let p_try_bad = Pipeline::new();
    assert!(
        p_try_bad
            .try_apply(
                "ReadInvalid",
                Read::<TypedCell>::new(bad_opts).with_failures(),
            )
            .is_err()
    );
}

#[test]
fn write_rows_with_invalid_options_fails_validate() {
    let p = Pipeline::new();
    let written = mutations(&p).apply(
        "WriteBigtable",
        WriteRows::new(WriteOptions::from_table("project", "instance", "")),
    );
    assert_eq!(
        written.failures.row_schema().unwrap(),
        Arc::new(write_failure_schema(&mutation_schema()))
    );

    let err = p.validate().expect_err("an empty table id fails validate");
    let message = err.to_string();
    assert!(message.contains("WriteBigtable"), "{message}");
    assert!(message.contains("table_id"), "{message}");
}

#[test]
fn write_rows_with_unsupported_input_schema_fails_validate() {
    let p = Pipeline::new();
    let schema = Arc::new(Schema::new(vec![Field::new("name", FieldType::string())]));
    let row = Row::new(
        Arc::clone(&schema),
        vec![Some(FieldValue::String("a".to_string()))],
    )
    .unwrap();
    let input = p
        .apply("Create", Create::new(vec![row]))
        .with_row_schema(&schema);
    let _ = input.apply(
        "WriteBigtable",
        WriteRows::new(WriteOptions::from_table("project", "instance", "table")),
    );

    let err = p
        .validate()
        .expect_err("an unsupported input schema fails validate");
    assert!(
        err.to_string()
            .contains("Unsupported Bigtable write input schema")
    );
}

#[test]
fn write_typed_expands_and_validates_options_and_schema() {
    let p = Pipeline::new();
    let seed = p.apply(
        "SeedBatched",
        Create::new(vec![BatchedMutationRow {
            key: Bytes::from_static(b"k1"),
            mutations: Vec::new(),
        }]),
    );
    let opts = WriteOptions::from_table("project", "instance", "table");
    let written = seed.apply("WriteApplied", Write::new(opts.clone()));
    assert_eq!(
        written.failures.row_schema().unwrap(),
        Arc::new(write_failure_schema(&BatchedMutationRow::beam_schema()))
    );

    let tried = seed
        .try_apply("WriteTried", Write::new(opts.clone()))
        .expect("valid typed write");
    assert_eq!(
        tried.failures.row_schema().unwrap(),
        Arc::new(write_failure_schema(&BatchedMutationRow::beam_schema()))
    );
    p.validate().expect("typed writes pass validate");

    // Invalid options fail both `apply` and `try_apply`.
    let bad_opts = WriteOptions::from_table("project", "instance", "");
    let p_bad_opts = Pipeline::new();
    let seed_bad = p_bad_opts.apply(
        "Seed",
        Create::new(vec![BatchedMutationRow {
            key: Bytes::from_static(b"k1"),
            mutations: Vec::new(),
        }]),
    );
    let bad_written = seed_bad.apply("WriteBadOpts", Write::new(bad_opts.clone()));
    assert_eq!(
        bad_written.failures.row_schema().unwrap(),
        Arc::new(write_failure_schema(&BatchedMutationRow::beam_schema()))
    );
    let err = p_bad_opts.validate().expect_err("empty table id fails");
    assert!(err.to_string().contains("WriteBadOpts"), "{err}");
    assert!(
        seed_bad
            .try_apply("TryWriteBadOpts", Write::new(bad_opts))
            .is_err()
    );

    // An unsupported `BeamRow` schema fails both `apply` and `try_apply`.
    let p_bad_schema = Pipeline::new();
    let non_mutations = p_bad_schema.apply(
        "SeedBad",
        Create::new(vec![TypedCell {
            family_name: "cf".to_string(),
        }]),
    );
    let _ = non_mutations.apply("WriteBadSchema", Write::new(opts.clone()));
    let schema_err = p_bad_schema
        .validate()
        .expect_err("unsupported BeamRow schema fails");
    assert!(
        schema_err
            .to_string()
            .contains("Unsupported Bigtable write input schema"),
        "{schema_err}"
    );
    let try_schema_err = non_mutations
        .try_apply("TryWriteBadSchema", Write::new(opts))
        .expect_err("try_apply rejects unsupported BeamRow schema");
    assert!(
        try_schema_err
            .to_string()
            .contains("Unsupported Bigtable write input schema"),
        "{try_schema_err}"
    );
}

#[test]
fn transforms_record_display_data_in_pipeline_proto() {
    let p = Pipeline::new();
    let _ = p.apply(
        "ReadTable",
        ReadRows::new(
            ReadOptions::from_table("proj", "inst", "tbl")
                .with_app_profile_id("read-prof")
                .with_shape(BigtableReadShape::Nested)
                .with_max_initial_bundles(32),
        ),
    );
    let _ = p.apply(
        "ReadMvFailures",
        ReadRows::new(ReadOptions::from_materialized_view("proj", "inst", "mv")).with_failures(),
    );
    let _: PCollection<TypedCell> = p.apply(
        "ReadTyped",
        Read::new(ReadOptions::from_table("proj", "inst", "typed")),
    );
    let _ = p.apply(
        "ReadTypedFailures",
        Read::<TypedCell>::new(ReadOptions::from_table("proj", "inst", "typed-fail"))
            .with_failures(),
    );
    let _ = mutations(&p).apply(
        "WriteTable",
        WriteRows::new(
            WriteOptions::from_table("proj", "inst", "sink")
                .with_app_profile_id("write-prof")
                .with_max_elements_per_batch(50)
                .with_max_bytes_per_batch(4096)
                .with_flow_control(true),
        ),
    );
    let _ = p
        .apply(
            "SeedTypedWrite",
            Create::new(vec![BatchedMutationRow {
                key: Bytes::from_static(b"k"),
                mutations: Vec::new(),
            }]),
        )
        .apply(
            "WriteTyped",
            Write::new(WriteOptions::from_table("proj", "inst", "typed-sink")),
        );

    let components = p.to_proto().components.unwrap();
    let display_map = |name: &str| -> HashMap<String, String> {
        let transform = components
            .transforms
            .values()
            .find(|t| t.unique_name == name)
            .unwrap();
        transform
            .display_data
            .iter()
            .map(|d| {
                let item = DisplayDataItem::from_proto(d).unwrap();
                (item.key, item.value.to_string())
            })
            .collect()
    };

    let read_items = display_map("ReadTable");
    assert_eq!(
        read_items.get("target").map(String::as_str),
        Some("projects/proj/instances/inst/tables/tbl")
    );
    assert_eq!(
        read_items.get("app_profile_id").map(String::as_str),
        Some("read-prof")
    );
    assert_eq!(read_items.get("shape").map(String::as_str), Some("NESTED"));
    assert_eq!(
        read_items.get("max_initial_bundles").map(String::as_str),
        Some("32")
    );

    let mv_items = display_map("ReadMvFailures");
    assert_eq!(
        mv_items.get("target").map(String::as_str),
        Some("projects/proj/instances/inst/materializedViews/mv")
    );
    assert_eq!(mv_items.get("shape").map(String::as_str), Some("FLATTENED"));
    assert!(!mv_items.contains_key("app_profile_id"));

    let typed_items = display_map("ReadTyped");
    assert_eq!(
        typed_items.get("target").map(String::as_str),
        Some("projects/proj/instances/inst/tables/typed")
    );

    let typed_fail_items = display_map("ReadTypedFailures");
    assert_eq!(
        typed_fail_items.get("target").map(String::as_str),
        Some("projects/proj/instances/inst/tables/typed-fail")
    );

    let write_items = display_map("WriteTable");
    assert_eq!(
        write_items.get("target").map(String::as_str),
        Some("projects/proj/instances/inst/tables/sink")
    );
    assert_eq!(
        write_items.get("app_profile_id").map(String::as_str),
        Some("write-prof")
    );
    assert_eq!(
        write_items
            .get("max_elements_per_batch")
            .map(String::as_str),
        Some("50")
    );
    assert_eq!(
        write_items.get("max_bytes_per_batch").map(String::as_str),
        Some("4096")
    );
    assert_eq!(
        write_items.get("flow_control").map(String::as_str),
        Some("true")
    );

    let write_typed_items = display_map("WriteTyped");
    assert_eq!(
        write_typed_items.get("target").map(String::as_str),
        Some("projects/proj/instances/inst/tables/typed-sink")
    );
}
