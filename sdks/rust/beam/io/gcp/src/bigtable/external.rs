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

//! Cross-language SchemaTransform wrappers ([`BigtableRead`], [`BigtableWrite`]) for
//! `beam:schematransform:org.apache.beam:bigtable_read:v1` and
//! `beam:schematransform:org.apache.beam:bigtable_write:v1`.

use std::sync::{Arc, LazyLock};

use beam::prelude::*;
use beam::schema::{Field, FieldType};
use external::{ExpansionError, ExternalSink, ExternalSource, ExternalTransform};

use super::schema::{BigtableReadShape, flattened_row_schema, nested_row_schema};

/// Standard SchemaTransform URN for Bigtable Read.
pub const URN_BIGTABLE_READ: &str = "beam:schematransform:org.apache.beam:bigtable_read:v1";

/// Standard SchemaTransform URN for Bigtable Write.
pub const URN_BIGTABLE_WRITE: &str = "beam:schematransform:org.apache.beam:bigtable_write:v1";

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

fn require<'a>(value: &'a str, what: &str, op: &str) -> Result<&'a str, ExpansionError> {
    if value.is_empty() {
        Err(ExpansionError::InvalidConfig(format!(
            "{op} requires a non-empty {what}"
        )))
    } else {
        Ok(value)
    }
}

/// Cross-language SchemaTransform for reading from Bigtable.
#[derive(Clone, Debug)]
pub struct BigtableRead {
    project_id: String,
    instance_id: String,
    table_id: String,
    shape: BigtableReadShape,
    expansion_service: String,
}

impl BigtableRead {
    pub fn new(
        project_id: impl Into<String>,
        instance_id: impl Into<String>,
        table_id: impl Into<String>,
    ) -> Self {
        Self {
            project_id: project_id.into(),
            instance_id: instance_id.into(),
            table_id: table_id.into(),
            shape: BigtableReadShape::Flattened,
            expansion_service: crate::DEFAULT_EXPANSION_SERVICE.to_string(),
        }
    }

    /// Selects output schema shape. Defaults to [`BigtableReadShape::Flattened`].
    pub fn with_shape(mut self, shape: BigtableReadShape) -> Self {
        self.shape = shape;
        self
    }

    /// Defaults to [`crate::DEFAULT_EXPANSION_SERVICE`].
    pub fn with_expansion_service(mut self, endpoint: impl Into<String>) -> Self {
        self.expansion_service = endpoint.into();
        self
    }

    /// Google Cloud project ID.
    #[doc(hidden)]
    pub fn project_id(&self) -> &str {
        &self.project_id
    }

    /// Bigtable instance ID.
    #[doc(hidden)]
    pub fn instance_id(&self) -> &str {
        &self.instance_id
    }

    /// Bigtable table ID.
    #[doc(hidden)]
    pub fn table_id(&self) -> &str {
        &self.table_id
    }

    /// Configured output schema shape.
    #[doc(hidden)]
    pub fn shape(&self) -> BigtableReadShape {
        self.shape
    }

    /// Expansion service address.
    #[doc(hidden)]
    pub fn expansion_service(&self) -> &str {
        &self.expansion_service
    }

    /// Schema of emitted PCollection rows for this read transform.
    pub fn output_schema(&self) -> Arc<Schema> {
        match self.shape {
            BigtableReadShape::Flattened => flattened_row_schema(),
            BigtableReadShape::Nested => nested_row_schema(),
        }
    }

    /// Builds the `BigtableReadSchemaTransformConfiguration` [`Row`] (`snake_case` fields
    /// sorted by name). Fails if an ID is empty.
    pub fn build_config_row(&self) -> Result<Row, ExpansionError> {
        let op = "BigtableRead";
        let project_id = require(&self.project_id, "project_id", op)?;
        let instance_id = require(&self.instance_id, "instance_id", op)?;
        let table_id = require(&self.table_id, "table_id", op)?;
        let flatten = matches!(self.shape, BigtableReadShape::Flattened);

        Row::builder(Arc::clone(&BIGTABLE_READ_CONFIG_SCHEMA))
            .with_named("flatten", Some(flatten))
            .with_named("instance_id", Some(instance_id))
            .with_named("project_id", Some(project_id))
            .with_named("table_id", Some(table_id))
            .build()
            .map_err(|e| {
                ExpansionError::Encoding(format!("Failed to build BigtableRead config: {e}"))
            })
    }

    /// Builds the underlying [`ExternalSource`] transform.
    pub fn build(&self) -> Result<ExternalSource, ExpansionError> {
        let config_row = self.build_config_row()?;
        let transform = ExternalTransform::schema_transform(
            URN_BIGTABLE_READ,
            &self.expansion_service,
            &config_row,
        )?;
        Ok(ExternalSource::new(transform).with_output_tag("output"))
    }
}

impl TryPTransform<PBegin> for BigtableRead {
    type Error = ExpansionError;

    fn try_expand(&self, input: &PBegin) -> Result<PCollection<Row>, ExpansionError> {
        self.build()?.try_expand(input)
    }
}

impl PTransform<PBegin> for BigtableRead {
    type Output = PCollection<Row>;

    /// # Panics
    ///
    /// Panics if the expansion fails. Use `try_apply` to get the error.
    fn expand(&self, input: &PBegin) -> Self::Output {
        self.try_expand(input)
            .unwrap_or_else(|e| panic!("the transform could not be expanded: {e}"))
    }
}

/// Cross-language SchemaTransform for writing [`mutation_schema`](super::mutation_schema) rows
/// to Bigtable.
#[derive(Clone, Debug)]
pub struct BigtableWrite {
    project_id: String,
    instance_id: String,
    table_id: String,
    expansion_service: String,
}

impl BigtableWrite {
    pub fn new(
        project_id: impl Into<String>,
        instance_id: impl Into<String>,
        table_id: impl Into<String>,
    ) -> Self {
        Self {
            project_id: project_id.into(),
            instance_id: instance_id.into(),
            table_id: table_id.into(),
            expansion_service: crate::DEFAULT_EXPANSION_SERVICE.to_string(),
        }
    }

    /// Defaults to [`crate::DEFAULT_EXPANSION_SERVICE`].
    pub fn with_expansion_service(mut self, endpoint: impl Into<String>) -> Self {
        self.expansion_service = endpoint.into();
        self
    }

    /// Google Cloud project ID.
    #[doc(hidden)]
    pub fn project_id(&self) -> &str {
        &self.project_id
    }

    /// Bigtable instance ID.
    #[doc(hidden)]
    pub fn instance_id(&self) -> &str {
        &self.instance_id
    }

    /// Bigtable table ID.
    #[doc(hidden)]
    pub fn table_id(&self) -> &str {
        &self.table_id
    }

    /// Expansion service address.
    #[doc(hidden)]
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

    /// Builds the underlying [`ExternalSink`] transform.
    pub fn build(&self) -> Result<ExternalSink, ExpansionError> {
        let config_row = self.build_config_row()?;
        let transform = ExternalTransform::schema_transform(
            URN_BIGTABLE_WRITE,
            &self.expansion_service,
            &config_row,
        )?;
        Ok(ExternalSink::new(transform).with_input_tag("input"))
    }
}

impl TryPTransform<PCollection<Row>> for BigtableWrite {
    type Error = ExpansionError;

    fn try_expand(&self, input: &PCollection<Row>) -> Result<PDone, ExpansionError> {
        self.build()?.try_expand(input)
    }
}

impl PTransform<PCollection<Row>> for BigtableWrite {
    type Output = PDone;

    /// # Panics
    ///
    /// Panics if the expansion fails. Use `try_apply` to get the error.
    fn expand(&self, input: &PCollection<Row>) -> Self::Output {
        self.try_expand(input)
            .unwrap_or_else(|e| panic!("the transform could not be expanded: {e}"))
    }
}
