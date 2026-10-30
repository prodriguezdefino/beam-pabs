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

//! Tests that an invalid native read or write configuration makes `Pipeline::validate` fail.

use std::sync::Arc;

use beam::prelude::*;
use beam::schema::{Field, FieldType, FieldValue, Schema};
use gcp::bigtable::read::{ReadOptions, ReadRows};
use gcp::bigtable::write::{WriteMutations, WriteOptions};
use gcp::bigtable::{BigtableMutation, mutation_schema};

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
