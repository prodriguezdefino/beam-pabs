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

//! Graph tests of the write transforms, with no runner or catalog.

use beam::pipeline::{Pipeline, PipelineError};
use beam::schema::Row;
use beam::transforms::{Create, Map};
use beam_io_iceberg::{Catalog, SnapshotInfo, WriteOptions, WriteRows};

#[test]
fn a_write_of_rows_without_a_schema_records_an_error_and_returns_outputs() {
    let pipeline = Pipeline::new();
    // An empty `Create` of rows has a row coder with no schema.
    let result = pipeline
        .apply("Create", Create::new(Vec::<Row>::new()))
        .apply(
            "Write",
            WriteRows::new(WriteOptions::new(Catalog::rest("http://unused"), "ns.t")),
        );
    result
        .errors
        .apply("Errors", Map::new(|row: Row| row.values().len() as i64));
    result
        .snapshots
        .apply("Snapshots", Map::new(|_: SnapshotInfo| 1i64));
    assert!(result.errors.row_schema().is_some());
    assert_eq!(
        pipeline.validate(),
        Err(PipelineError::InvalidConstruction {
            transform_id: "Write".to_string(),
            reason: "The input of the write has no row schema".to_string(),
        })
    );
}
