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

//! Schemas and row domain types (`BigtableCell`, `BigtableColumn`, `BigtableMutation`,
//! `BigtableReadShape`) shared by native and cross-language Cloud Bigtable transforms.

mod types;

use std::sync::{Arc, LazyLock};

use beam::schema::{Field, FieldType, Schema};
use serde::{Deserialize, Serialize};

pub use self::types::{BigtableCell, BigtableColumn, BigtableMutation};

/// Mutation type for cell updates.
pub const MUTATION_SET_CELL: &str = "SetCell";

/// Mutation type for column cell deletions.
pub const MUTATION_DELETE_FROM_COLUMN: &str = "DeleteFromColumn";

/// Mutation type for column family deletions.
pub const MUTATION_DELETE_FROM_FAMILY: &str = "DeleteFromFamily";

/// Mutation type for row deletions.
pub const MUTATION_DELETE_FROM_ROW: &str = "DeleteFromRow";

static CELL_SCHEMA: LazyLock<Arc<Schema>> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("value", FieldType::bytes()),
        Field::new("timestamp_micros", FieldType::int64()),
    ]))
});

static FLATTENED_ROW_SCHEMA: LazyLock<Arc<Schema>> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("key", FieldType::bytes()),
        Field::new("family_name", FieldType::string()),
        Field::new("column_qualifier", FieldType::bytes()),
        Field::new(
            "cells",
            FieldType::array(FieldType::row((*cell_schema()).clone())),
        ),
    ]))
});

static NESTED_ROW_SCHEMA: LazyLock<Arc<Schema>> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("key", FieldType::bytes()),
        Field::new(
            "column_families",
            FieldType::map(
                FieldType::string(),
                FieldType::map(
                    FieldType::string(),
                    FieldType::array(FieldType::row((*cell_schema()).clone())),
                ),
            ),
        ),
    ]))
});

static MUTATION_SCHEMA: LazyLock<Arc<Schema>> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("key", FieldType::bytes()),
        Field::new("type", FieldType::string()),
        Field::nullable("value", FieldType::bytes()),
        Field::nullable("column_qualifier", FieldType::bytes()),
        Field::nullable("family_name", FieldType::string()),
        Field::nullable("timestamp_micros", FieldType::int64()),
        Field::nullable("start_timestamp_micros", FieldType::int64()),
        Field::nullable("end_timestamp_micros", FieldType::int64()),
    ]))
});

static BATCHED_MUTATION_SCHEMA: LazyLock<Arc<Schema>> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("key", FieldType::bytes()),
        Field::new(
            "mutations",
            FieldType::array(FieldType::map(FieldType::string(), FieldType::bytes())),
        ),
    ]))
});

/// `(value: BYTES, timestamp_micros: INT64)`, matching `CELL_SCHEMA` of
/// `BigtableReadSchemaTransformProvider`.
pub fn cell_schema() -> Arc<Schema> {
    Arc::clone(&CELL_SCHEMA)
}

/// One column per element (the default read shape), matching `FLATTENED_ROW_SCHEMA`:
/// `(key: BYTES, family_name: STRING, column_qualifier: BYTES, cells: ARRAY<cell>)`.
pub fn flattened_row_schema() -> Arc<Schema> {
    Arc::clone(&FLATTENED_ROW_SCHEMA)
}

/// One row per element (flattening off), matching `ROW_SCHEMA`:
/// `(key: BYTES, column_families: MAP<family, MAP<qualifier, ARRAY<cell>>>)`.
pub fn nested_row_schema() -> Arc<Schema> {
    Arc::clone(&NESTED_ROW_SCHEMA)
}

/// Flat write element schema. `key` and `type` are required; other fields depend on `type`.
pub fn mutation_schema() -> Arc<Schema> {
    Arc::clone(&MUTATION_SCHEMA)
}

/// Batched write element schema (`key: BYTES, mutations: ARRAY<MAP<STRING, BYTES>>`), matching
/// `BigtableWriteSchemaTransformProvider`.
pub fn batched_mutation_schema() -> Arc<Schema> {
    Arc::clone(&BATCHED_MUTATION_SCHEMA)
}

/// Output schema shape for Bigtable reads.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum BigtableReadShape {
    /// Emits [`flattened_row_schema`] columns.
    #[default]
    Flattened,
    /// Emits [`nested_row_schema`] rows.
    Nested,
}
