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

//! Tests that an invalid read or write configuration makes `Pipeline::validate` fail.

use std::sync::Arc;

use beam::prelude::*;
use beam::schema::{Field, FieldType, FieldValue, Schema};
use beam_io_gcp::bigtable::read::{ReadOptions, ReadRows};
use beam_io_gcp::bigtable::write::{WriteMutations, WriteOptions};
use beam_io_gcp::bigtable::{BigtableMutation, mutation_schema};

fn mutations(p: &Pipeline) -> PCollection<Row> {
    p.apply(
        "Create",
        Create::new(vec![Row::from(BigtableMutation::set_cell(
            "k", "cf", "q", "v",
        ))]),
    )
    .with_row_schema(&mutation_schema())
}

#[test]
fn read_rows_with_invalid_options_fails_validate() {
    let p = Pipeline::new();
    let options = ReadOptions::from_table("project", "instance", "table")
        .with_key_range(b"z".to_vec(), b"a".to_vec());
    let rows: PCollection<Row> = p.apply("ReadBigtable", ReadRows::new(options));
    // Later transforms still attach to the output of the failed read.
    let _ = rows.apply("Reshuffle", Reshuffle::new());

    let err = p
        .validate()
        .expect_err("an inverted key range fails validate");
    let message = err.to_string();
    assert!(message.contains("ReadBigtable"), "{message}");
    assert!(message.contains("key range"), "{message}");
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
fn write_mutations_with_invalid_options_fails_validate() {
    let p = Pipeline::new();
    let rejected = mutations(&p).apply(
        "WriteBigtable",
        WriteMutations::new(WriteOptions::from_table("project", "instance", "")),
    );
    assert!(rejected.row_schema().is_some());

    let err = p.validate().expect_err("an empty table id fails validate");
    let message = err.to_string();
    assert!(message.contains("WriteBigtable"), "{message}");
    assert!(message.contains("table_id"), "{message}");
}

#[test]
fn write_mutations_with_unsupported_input_schema_fails_validate() {
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
        WriteMutations::new(WriteOptions::from_table("project", "instance", "table")),
    );

    let err = p
        .validate()
        .expect_err("an unsupported input schema fails validate");
    assert!(
        err.to_string()
            .contains("Unsupported Bigtable write input schema")
    );
}

#[derive(Clone, Debug, PartialEq, Eq, beam::schema::BeamRow)]
struct TypedCell {
    family_name: String,
}

#[test]
fn transforms_record_display_data_in_pipeline_proto() {
    use std::collections::HashMap;

    use beam::transforms::display_data::DisplayDataItem;
    use beam_io_gcp::bigtable::BigtableReadShape;

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
    let _ = p.apply(
        "ReadTyped",
        ReadRows::new(ReadOptions::from_table("proj", "inst", "typed")).typed::<TypedCell>(),
    );
    let _ = mutations(&p).apply(
        "WriteTable",
        WriteMutations::new(
            WriteOptions::from_table("proj", "inst", "sink")
                .with_app_profile_id("write-prof")
                .with_max_elements_per_batch(50)
                .with_max_bytes_per_batch(4096)
                .with_flow_control(true),
        ),
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
}
