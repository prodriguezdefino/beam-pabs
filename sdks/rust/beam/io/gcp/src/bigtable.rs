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

//! Google Cloud Bigtable I/O through the SchemaTransform providers `bigtable_read:v1`
//! and `bigtable_write:v1`.
//!
//! # Reading from Bigtable
//! By default each element is one *column* with [`flattened_row_schema`]; decode it with
//! [`BigtableColumn::from_row`].
//! ```no_run
//! use beam::prelude::*;
//! use gcp::bigtable::BigtableRead;
//!
//! let p = Pipeline::new();
//! let columns: PCollection<Row> = p.apply(
//!     BigtableRead::new("BigtableRead", "my-project", "my-instance", "my-table"),
//! );
//! ```
//!
//! # Writing to Bigtable
//! Input rows have [`mutation_schema`], as produced by [`BigtableMutation::to_row`].
//! ```no_run
//! use beam::prelude::*;
//! use gcp::bigtable::{BigtableMutation, BigtableWrite, mutation_schema};
//!
//! # let p = Pipeline::new();
//! let mutations = p
//!     .apply(Create::new("Create", vec![
//!         BigtableMutation::set_cell("row-1", "cf", "greeting", "hello").to_row(),
//!     ]))
//!     .with_row_schema(&mutation_schema());
//! mutations.apply(
//!     BigtableWrite::new("BigtableWrite", "my-project", "my-instance", "my-table"),
//! );
//! ```

use std::sync::{Arc, LazyLock};

use beam::prelude::*;
use beam::schema::{Field, FieldType, FieldValue};
use external::{ExpansionError, ExternalSink, ExternalSource, ExternalTransform};

/// Standard SchemaTransform URN for Bigtable Read.
pub const URN_BIGTABLE_READ: &str = "beam:schematransform:org.apache.beam:bigtable_read:v1";

/// Standard SchemaTransform URN for Bigtable Write.
pub const URN_BIGTABLE_WRITE: &str = "beam:schematransform:org.apache.beam:bigtable_write:v1";

/// Default expansion service target for GCP SchemaTransforms.
pub const DEFAULT_EXPANSION_SERVICE: &str =
    "autojava::sdks:java:io:google-cloud-platform:expansion-service:runExpansionService";

/// `MUTATION_*` are the `type` values of `BigtableWriteSchemaTransformProvider`.
pub const MUTATION_SET_CELL: &str = "SetCell";
pub const MUTATION_DELETE_FROM_COLUMN: &str = "DeleteFromColumn";
pub const MUTATION_DELETE_FROM_FAMILY: &str = "DeleteFromFamily";
pub const MUTATION_DELETE_FROM_ROW: &str = "DeleteFromRow";

static CELL_SCHEMA: LazyLock<Schema> = LazyLock::new(|| {
    Schema::new(vec![
        Field::new("value", FieldType::bytes()),
        Field::new("timestamp_micros", FieldType::int64()),
    ])
});

static FLATTENED_ROW_SCHEMA: LazyLock<Arc<Schema>> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("key", FieldType::bytes()),
        Field::new("family_name", FieldType::string()),
        Field::new("column_qualifier", FieldType::bytes()),
        Field::new("cells", FieldType::array(FieldType::row(cell_schema()))),
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
                    FieldType::array(FieldType::row(cell_schema())),
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

static BIGTABLE_READ_CONFIG_SCHEMA: LazyLock<Arc<Schema>> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::nullable("flatten", FieldType::boolean()),
        Field::new("instance_id", FieldType::string()),
        Field::new("project_id", FieldType::string()),
        Field::new("table_id", FieldType::string()),
    ]))
});

static BIGTABLE_WRITE_CONFIG_SCHEMA: LazyLock<Arc<Schema>> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("instance_id", FieldType::string()),
        Field::new("project_id", FieldType::string()),
        Field::new("table_id", FieldType::string()),
    ]))
});

/// `(value: BYTES, timestamp_micros: INT64)`, matching `CELL_SCHEMA` of
/// `BigtableReadSchemaTransformProvider`.
pub fn cell_schema() -> Schema {
    CELL_SCHEMA.clone()
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

/// Write element schema. `key` and `type` are required; other fields depend on `type`.
/// The sink groups rows by key and sends one `MutateRow` per key.
pub fn mutation_schema() -> Arc<Schema> {
    Arc::clone(&MUTATION_SCHEMA)
}

/// A single timestamped Bigtable cell value.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BigtableCell {
    pub value: Vec<u8>,
    pub timestamp_micros: i64,
}

impl BigtableCell {
    fn from_field(value: &FieldValue) -> Option<Self> {
        let FieldValue::Row(cell) = value else {
            return None;
        };
        Some(Self {
            value: cell.get_bytes("value").ok().flatten()?.to_vec(),
            timestamp_micros: cell.get_i64("timestamp_micros").ok().flatten()?,
        })
    }
}

/// One column of a Bigtable row, as produced by a flattened [`BigtableRead`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BigtableColumn {
    pub key: Vec<u8>,
    pub family_name: String,
    pub column_qualifier: Vec<u8>,
    /// Cell versions, newest first (the Bigtable order).
    pub cells: Vec<BigtableCell>,
}

impl BigtableColumn {
    /// Decodes a [`flattened_row_schema`] element, or `None` if a field is missing or mistyped.
    pub fn from_row(row: &Row) -> Option<Self> {
        let key = row.get_bytes("key").ok().flatten()?.to_vec();
        let family_name = row.get_string("family_name").ok().flatten()?.to_string();
        let column_qualifier = row.get_bytes("column_qualifier").ok().flatten()?.to_vec();
        let cells = match row.get_value("cells")? {
            Some(FieldValue::Array(items)) => items
                .iter()
                .map(|c| c.as_ref().and_then(BigtableCell::from_field))
                .collect::<Option<Vec<_>>>()?,
            Some(_) => return None,
            None => Vec::new(),
        };
        Some(Self {
            key,
            family_name,
            column_qualifier,
            cells,
        })
    }

    /// Newest cell value.
    pub fn latest_value(&self) -> Option<&[u8]> {
        self.cells.first().map(|c| c.value.as_slice())
    }
}

/// A single Bigtable mutation convertible to a row with [`mutation_schema`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BigtableMutation {
    /// Writes `value` to `family_name:column_qualifier`; a `None` timestamp is server-assigned.
    SetCell {
        key: Vec<u8>,
        family_name: String,
        column_qualifier: Vec<u8>,
        value: Vec<u8>,
        timestamp_micros: Option<i64>,
    },
    /// Deletes cells of one column, optionally restricted to `[start, end)` micros.
    DeleteFromColumn {
        key: Vec<u8>,
        family_name: String,
        column_qualifier: Vec<u8>,
        start_timestamp_micros: Option<i64>,
        end_timestamp_micros: Option<i64>,
    },
    /// Deletes every cell of a column family.
    DeleteFromFamily {
        key: Vec<u8>,
        family_name: String,
    },
    DeleteFromRow {
        key: Vec<u8>,
    },
}

impl BigtableMutation {
    /// Creates a [`BigtableMutation::SetCell`] with a server timestamp.
    pub fn set_cell(
        key: impl Into<Vec<u8>>,
        family_name: impl Into<String>,
        column_qualifier: impl Into<Vec<u8>>,
        value: impl Into<Vec<u8>>,
    ) -> Self {
        Self::SetCell {
            key: key.into(),
            family_name: family_name.into(),
            column_qualifier: column_qualifier.into(),
            value: value.into(),
            timestamp_micros: None,
        }
    }

    pub fn key(&self) -> &[u8] {
        match self {
            Self::SetCell { key, .. }
            | Self::DeleteFromColumn { key, .. }
            | Self::DeleteFromFamily { key, .. }
            | Self::DeleteFromRow { key } => key,
        }
    }

    /// Returns the `type` string that the SchemaTransform provider dispatches on.
    pub fn type_name(&self) -> &'static str {
        match self {
            Self::SetCell { .. } => MUTATION_SET_CELL,
            Self::DeleteFromColumn { .. } => MUTATION_DELETE_FROM_COLUMN,
            Self::DeleteFromFamily { .. } => MUTATION_DELETE_FROM_FAMILY,
            Self::DeleteFromRow { .. } => MUTATION_DELETE_FROM_ROW,
        }
    }

    pub fn to_row(&self) -> Row {
        let (value, qualifier, family, ts, start, end) = match self {
            Self::SetCell {
                family_name,
                column_qualifier,
                value,
                timestamp_micros,
                ..
            } => (
                Some(FieldValue::Bytes(value.clone())),
                Some(FieldValue::Bytes(column_qualifier.clone())),
                Some(FieldValue::String(family_name.clone())),
                timestamp_micros.map(FieldValue::Int64),
                None,
                None,
            ),
            Self::DeleteFromColumn {
                family_name,
                column_qualifier,
                start_timestamp_micros,
                end_timestamp_micros,
                ..
            } => (
                None,
                Some(FieldValue::Bytes(column_qualifier.clone())),
                Some(FieldValue::String(family_name.clone())),
                None,
                start_timestamp_micros.map(FieldValue::Int64),
                end_timestamp_micros.map(FieldValue::Int64),
            ),
            Self::DeleteFromFamily { family_name, .. } => (
                None,
                None,
                Some(FieldValue::String(family_name.clone())),
                None,
                None,
                None,
            ),
            Self::DeleteFromRow { .. } => (None, None, None, None, None, None),
        };
        Row::new(
            mutation_schema(),
            vec![
                Some(FieldValue::Bytes(self.key().to_vec())),
                Some(FieldValue::String(self.type_name().to_string())),
                value,
                qualifier,
                family,
                ts,
                start,
                end,
            ],
        )
        .expect("mutation row matches mutation_schema")
    }
}

fn require<'a>(value: &'a str, what: &str, op: &str) -> Result<&'a str, ExpansionError> {
    match value {
        v if !v.is_empty() => Ok(v),
        _ => Err(ExpansionError::InvalidResponse(format!(
            "{op} requires a non-empty {what}"
        ))),
    }
}

/// Transform for reading from Bigtable.
#[derive(Clone, Debug)]
pub struct BigtableRead {
    name: String,
    project_id: String,
    instance_id: String,
    table_id: String,
    flatten: bool,
    expansion_service: String,
}

impl BigtableRead {
    pub fn new(
        name: impl Into<String>,
        project_id: impl Into<String>,
        instance_id: impl Into<String>,
        table_id: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            project_id: project_id.into(),
            instance_id: instance_id.into(),
            table_id: table_id.into(),
            flatten: true,
            expansion_service: DEFAULT_EXPANSION_SERVICE.to_string(),
        }
    }

    /// `true` (default) emits [`flattened_row_schema`] columns, `false` [`nested_row_schema`] rows.
    pub fn with_flatten(mut self, flatten: bool) -> Self {
        self.flatten = flatten;
        self
    }

    /// Defaults to [`DEFAULT_EXPANSION_SERVICE`].
    pub fn with_expansion_service(mut self, endpoint: impl Into<String>) -> Self {
        self.expansion_service = endpoint.into();
        self
    }

    pub fn project_id(&self) -> &str {
        &self.project_id
    }

    pub fn instance_id(&self) -> &str {
        &self.instance_id
    }

    pub fn table_id(&self) -> &str {
        &self.table_id
    }

    pub fn flatten(&self) -> bool {
        self.flatten
    }

    pub fn expansion_service(&self) -> &str {
        &self.expansion_service
    }

    pub fn output_schema(&self) -> Arc<Schema> {
        if self.flatten {
            flattened_row_schema()
        } else {
            nested_row_schema()
        }
    }

    /// Builds the `BigtableReadSchemaTransformConfiguration` [`Row`] (`snake_case` fields
    /// sorted by name). Fails if an ID is empty.
    pub fn build_config_row(&self) -> Result<Row, ExpansionError> {
        let op = "BigtableRead";
        let project_id = require(&self.project_id, "project_id", op)?;
        let instance_id = require(&self.instance_id, "instance_id", op)?;
        let table_id = require(&self.table_id, "table_id", op)?;

        Row::builder(Arc::clone(&BIGTABLE_READ_CONFIG_SCHEMA))
            .with_named("flatten", Some(self.flatten))
            .with_named("instance_id", Some(instance_id))
            .with_named("project_id", Some(project_id))
            .with_named("table_id", Some(table_id))
            .build()
            .map_err(|e| {
                ExpansionError::Encoding(format!("Failed to build BigtableRead config: {e}"))
            })
    }

    pub fn build(&self) -> Result<ExternalSource, ExpansionError> {
        let config_row = self.build_config_row()?;
        let transform = ExternalTransform::schema_transform(
            self.name.clone(),
            URN_BIGTABLE_READ,
            &self.expansion_service,
            &config_row,
        )?;
        Ok(ExternalSource::new(transform).with_output_tag("output"))
    }

    /// Fallible form of [`PTransform::expand`].
    pub fn try_expand(&self, input: &PBegin) -> Result<PCollection<Row>, ExpansionError> {
        self.build()?.try_expand(input)
    }
}

impl PTransform<PBegin> for BigtableRead {
    type Output = PCollection<Row>;

    fn expand(&self, input: &PBegin) -> Self::Output {
        self.try_expand(input)
            .unwrap_or_else(|e| panic!("{} could not be expanded: {e}", self.name))
    }
}

/// Transform for writing [`mutation_schema`] rows to Bigtable.
#[derive(Clone, Debug)]
pub struct BigtableWrite {
    name: String,
    project_id: String,
    instance_id: String,
    table_id: String,
    expansion_service: String,
}

impl BigtableWrite {
    pub fn new(
        name: impl Into<String>,
        project_id: impl Into<String>,
        instance_id: impl Into<String>,
        table_id: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            project_id: project_id.into(),
            instance_id: instance_id.into(),
            table_id: table_id.into(),
            expansion_service: DEFAULT_EXPANSION_SERVICE.to_string(),
        }
    }

    /// Defaults to [`DEFAULT_EXPANSION_SERVICE`].
    pub fn with_expansion_service(mut self, endpoint: impl Into<String>) -> Self {
        self.expansion_service = endpoint.into();
        self
    }

    pub fn project_id(&self) -> &str {
        &self.project_id
    }

    pub fn instance_id(&self) -> &str {
        &self.instance_id
    }

    pub fn table_id(&self) -> &str {
        &self.table_id
    }

    pub fn expansion_service(&self) -> &str {
        &self.expansion_service
    }

    /// Builds the `BigtableWriteSchemaTransformConfiguration` [`Row`] (`snake_case` fields
    /// sorted by name). Fails if an ID is empty.
    pub fn build_config_row(&self) -> Result<Row, ExpansionError> {
        let op = "BigtableWrite";
        let project_id = require(&self.project_id, "project_id", op)?;
        let instance_id = require(&self.instance_id, "instance_id", op)?;
        let table_id = require(&self.table_id, "table_id", op)?;

        Row::builder(Arc::clone(&BIGTABLE_WRITE_CONFIG_SCHEMA))
            .with_named("instance_id", Some(instance_id))
            .with_named("project_id", Some(project_id))
            .with_named("table_id", Some(table_id))
            .build()
            .map_err(|e| {
                ExpansionError::Encoding(format!("Failed to build BigtableWrite config: {e}"))
            })
    }

    pub fn build(&self) -> Result<ExternalSink, ExpansionError> {
        let config_row = self.build_config_row()?;
        let transform = ExternalTransform::schema_transform(
            self.name.clone(),
            URN_BIGTABLE_WRITE,
            &self.expansion_service,
            &config_row,
        )?;
        Ok(ExternalSink::new(transform).with_input_tag("input"))
    }

    /// Fallible form of [`PTransform::expand`].
    pub fn try_expand(&self, input: &PCollection<Row>) -> Result<PDone, ExpansionError> {
        self.build()?.try_expand(input)
    }
}

impl PTransform<PCollection<Row>> for BigtableWrite {
    type Output = PDone;

    fn expand(&self, input: &PCollection<Row>) -> Self::Output {
        self.try_expand(input)
            .unwrap_or_else(|e| panic!("{} could not be expanded: {e}", self.name))
    }
}
