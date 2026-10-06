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

//! Google Cloud BigQuery I/O through the SchemaTransform providers for Storage Read,
//! Storage Write and File Loads.
//!
//! Reads and [`WriteMethod::Auto`] or at-least-once writes expand through Managed I/O
//! (`beam:transform:managed:v1`, [`managed_io`]), so runners such as Dataflow can upgrade
//! them. Other write methods call their SchemaTransform directly.
//!
//! # Reading from BigQuery
//! ```no_run
//! use beam::prelude::*;
//! use gcp::bigquery::BigQueryRead;
//!
//! let p = Pipeline::new();
//! let rows = p.apply(
//!     BigQueryRead::new("BigQueryRead")
//!         .with_table("bigquery-public-data:samples.wikipedia")
//!         .with_selected_fields(["title", "id"])
//!         .with_row_restriction("wp_namespace = 0")
//! );
//! ```
//!
//! # Writing to BigQuery
//! ```no_run
//! use beam::prelude::*;
//! use gcp::bigquery::{BigQueryWrite, CreateDisposition, WriteDisposition, WriteMethod};
//!
//! # let p = Pipeline::new();
//! # let rows: PCollection<Row> = unimplemented!();
//! rows.apply(
//!     BigQueryWrite::new("BigQueryWrite", "my-project:my_dataset.my_table")
//!         .with_create_disposition(CreateDisposition::CreateIfNeeded)
//!         .with_write_disposition(WriteDisposition::WriteAppend)
//!         .with_method(WriteMethod::StorageWriteApi)
//! );
//! ```

use std::sync::{Arc, LazyLock};

use beam::prelude::*;
use beam::schema::{Field, FieldType, FieldValue};
use external::{ExpansionError, ExternalSink, ExternalSource, ExternalTransform};
use managed_io::{ManagedRead, ManagedWrite};

/// Standard SchemaTransform URN for BigQuery Storage Read.
pub const URN_BIGQUERY_STORAGE_READ: &str = managed_io::urns::BIGQUERY_READ;

/// Standard SchemaTransform URN for BigQuery Write.
pub const URN_BIGQUERY_WRITE: &str = managed_io::urns::BIGQUERY_WRITE;

/// Standard SchemaTransform URN for BigQuery File Loads write.
pub const URN_BIGQUERY_FILELOADS: &str =
    "beam:schematransform:org.apache.beam:bigquery_fileloads:v1";

/// Standard SchemaTransform URN for BigQuery Storage Write API v2.
pub const URN_BIGQUERY_STORAGE_WRITE: &str =
    "beam:schematransform:org.apache.beam:bigquery_storage_write:v2";

/// Default expansion service for GCP transforms.
pub const DEFAULT_EXPANSION_SERVICE: &str = managed_io::GCP_EXPANSION_SERVICE;

/// Specifies whether writing to BigQuery may create new tables.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum CreateDisposition {
    /// Creates the table if it does not already exist.
    #[default]
    CreateIfNeeded,
    /// Fails the job if the table does not exist.
    CreateNever,
}

impl CreateDisposition {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::CreateIfNeeded => "CREATE_IF_NEEDED",
            Self::CreateNever => "CREATE_NEVER",
        }
    }
}

/// Specifies the action to take when the destination table already exists.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum WriteDisposition {
    /// Appends data to the existing table.
    #[default]
    WriteAppend,
    /// Overwrites existing table contents.
    WriteTruncate,
    /// Fails the job if the table is not empty.
    WriteEmpty,
}

impl WriteDisposition {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::WriteAppend => "WRITE_APPEND",
            Self::WriteTruncate => "WRITE_TRUNCATE",
            Self::WriteEmpty => "WRITE_EMPTY",
        }
    }
}

/// Method used to write data into BigQuery.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum WriteMethod {
    /// Storage Write API or File Loads, chosen from input boundedness.
    #[default]
    Auto,
    /// Storage Write API, exactly-once.
    StorageWriteApi,
    /// Batch load jobs staged through GCS files.
    FileLoads,
    /// Storage Write API, at-least-once with lower latency.
    StorageApiAtLeastOnce,
}

/// Transform for reading from BigQuery.
#[derive(Clone, Debug)]
pub struct BigQueryRead {
    name: String,
    table: Option<String>,
    query: Option<String>,
    selected_fields: Option<Vec<String>>,
    row_restriction: Option<String>,
    kms_key: Option<String>,
    expansion_service: String,
}

static BIGQUERY_READ_CONFIG_SCHEMA: LazyLock<Arc<Schema>> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::nullable("table_spec", FieldType::string()),
        Field::nullable("query", FieldType::string()),
        Field::nullable("row_restriction", FieldType::string()),
        Field::nullable("selected_fields", FieldType::array(FieldType::string())),
        Field::nullable("kms_key", FieldType::string()),
    ]))
});

impl BigQueryRead {
    /// Set the source with [`with_table`](Self::with_table) or [`with_query`](Self::with_query).
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            table: None,
            query: None,
            selected_fields: None,
            row_restriction: None,
            kms_key: None,
            expansion_service: DEFAULT_EXPANSION_SERVICE.to_string(),
        }
    }

    /// Reads the table `[${PROJECT}:]${DATASET}.${TABLE}`.
    pub fn with_table(mut self, table: impl Into<String>) -> Self {
        self.table = Some(table.into());
        self
    }

    /// Reads the result of a GoogleSQL query.
    pub fn with_query(mut self, query: impl Into<String>) -> Self {
        self.query = Some(query.into());
        self
    }

    /// Limits the read to the specified columns.
    pub fn with_selected_fields(
        mut self,
        fields: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.selected_fields = Some(fields.into_iter().map(Into::into).collect());
        self
    }

    /// Filters rows at the storage layer with a SQL predicate.
    pub fn with_row_restriction(mut self, restriction: impl Into<String>) -> Self {
        self.row_restriction = Some(restriction.into());
        self
    }

    /// Cloud KMS key that decrypts table data.
    pub fn with_kms_key(mut self, kms_key: impl Into<String>) -> Self {
        self.kms_key = Some(kms_key.into());
        self
    }

    /// Defaults to [`DEFAULT_EXPANSION_SERVICE`].
    pub fn with_expansion_service(mut self, endpoint: impl Into<String>) -> Self {
        self.expansion_service = endpoint.into();
        self
    }

    pub fn table(&self) -> Option<&str> {
        self.table.as_deref()
    }

    pub fn query(&self) -> Option<&str> {
        self.query.as_deref()
    }

    pub fn selected_fields(&self) -> Option<&[String]> {
        self.selected_fields.as_deref()
    }

    pub fn row_restriction(&self) -> Option<&str> {
        self.row_restriction.as_deref()
    }

    pub fn expansion_service(&self) -> &str {
        &self.expansion_service
    }

    /// Builds the config [`Row`] of `BigQueryDirectReadSchemaTransformConfiguration`.
    pub fn build_config_row(&self) -> Result<Row, ExpansionError> {
        Row::builder(Arc::clone(&BIGQUERY_READ_CONFIG_SCHEMA))
            .with_named("table_spec", self.table.as_deref())
            .with_named("query", self.query.as_deref())
            .with_named("row_restriction", self.row_restriction.as_deref())
            .with_named(
                "selected_fields",
                self.selected_fields.as_ref().map(|fields| {
                    FieldValue::Array(
                        fields
                            .iter()
                            .map(|f| Some(FieldValue::String(f.clone())))
                            .collect(),
                    )
                }),
            )
            .with_named("kms_key", self.kms_key.as_deref())
            .build()
            .map_err(|e| {
                ExpansionError::Encoding(format!("Failed to build BigQueryRead config: {e}"))
            })
    }

    /// Returns the `bigquery_storage_read:v1` [`ManagedRead`]. Fails if neither table nor
    /// query is set.
    pub fn to_managed(&self) -> Result<ManagedRead, ExpansionError> {
        if self.table.is_none() && self.query.is_none() {
            return Err(ExpansionError::InvalidResponse(
                "BigQueryRead requires either table or query".to_string(),
            ));
        }

        let config_row = self.build_config_row()?;
        Ok(
            ManagedRead::new(self.name.clone(), URN_BIGQUERY_STORAGE_READ)
                .with_config_row(&config_row)
                .with_expansion_service(&self.expansion_service),
        )
    }

    /// Builds the [`ExternalSource`] that expands through Managed I/O.
    pub fn build(&self) -> Result<ExternalSource, ExpansionError> {
        self.to_managed()?.build()
    }
}

impl BigQueryRead {
    /// Fallible form of [`PTransform::expand`].
    pub fn try_expand(&self, input: &PBegin) -> Result<PCollection<Row>, ExpansionError> {
        self.build()?.try_expand(input)
    }
}

impl PTransform<PBegin> for BigQueryRead {
    type Output = PCollection<Row>;

    fn expand(&self, input: &PBegin) -> Self::Output {
        self.try_expand(input)
            .unwrap_or_else(|e| panic!("{} could not be expanded: {e}", self.name))
    }
}

/// Transform for writing to BigQuery.
#[derive(Clone, Debug)]
pub struct BigQueryWrite {
    name: String,
    table: String,
    create_disposition: Option<CreateDisposition>,
    write_disposition: Option<WriteDisposition>,
    method: WriteMethod,
    triggering_frequency_seconds: Option<i64>,
    use_at_least_once_semantics: Option<bool>,
    auto_sharding: Option<bool>,
    num_streams: Option<i32>,
    kms_key: Option<String>,
    error_output: Option<String>,
    expansion_service: String,
}

static ERROR_HANDLING_SCHEMA: LazyLock<Arc<Schema>> =
    LazyLock::new(|| Arc::new(Schema::new(vec![Field::new("output", FieldType::string())])));

static BIGQUERY_WRITE_CONFIG_SCHEMA: LazyLock<Arc<Schema>> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::nullable("auto_sharding", FieldType::boolean()),
        Field::nullable(
            "big_lake_configuration",
            FieldType::map(FieldType::string(), FieldType::string()),
        ),
        Field::nullable("clustering_fields", FieldType::array(FieldType::string())),
        Field::nullable("create_disposition", FieldType::string()),
        Field::nullable("drop", FieldType::array(FieldType::string())),
        Field::nullable(
            "error_handling",
            FieldType::row((**ERROR_HANDLING_SCHEMA).clone()),
        ),
        Field::nullable("keep", FieldType::array(FieldType::string())),
        Field::nullable("kms_key", FieldType::string()),
        Field::nullable("num_streams", FieldType::int32()),
        Field::nullable("only", FieldType::string()),
        Field::nullable("primary_key", FieldType::array(FieldType::string())),
        Field::new("table", FieldType::string()),
        Field::nullable("triggering_frequency_seconds", FieldType::int64()),
        Field::nullable("use_at_least_once_semantics", FieldType::boolean()),
        Field::nullable("use_cdc_writes", FieldType::boolean()),
        Field::nullable("write_disposition", FieldType::string()),
    ]))
});

impl BigQueryWrite {
    /// `table` has the form `[${PROJECT}:]${DATASET}.${TABLE}`.
    pub fn new(name: impl Into<String>, table: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            table: table.into(),
            create_disposition: Some(CreateDisposition::CreateIfNeeded),
            write_disposition: Some(WriteDisposition::WriteAppend),
            method: WriteMethod::Auto,
            triggering_frequency_seconds: None,
            use_at_least_once_semantics: None,
            auto_sharding: None,
            num_streams: None,
            kms_key: None,
            error_output: None,
            expansion_service: DEFAULT_EXPANSION_SERVICE.to_string(),
        }
    }

    /// Specifies whether table creation is permitted.
    pub fn with_create_disposition(mut self, disposition: CreateDisposition) -> Self {
        self.create_disposition = Some(disposition);
        self
    }

    /// Action when the destination table already exists.
    pub fn with_write_disposition(mut self, disposition: WriteDisposition) -> Self {
        self.write_disposition = Some(disposition);
        self
    }

    pub fn with_method(mut self, method: WriteMethod) -> Self {
        self.method = method;
        self
    }

    /// Interval in seconds between commits of streaming data.
    pub fn with_triggering_frequency_seconds(mut self, secs: i64) -> Self {
        self.triggering_frequency_seconds = Some(secs);
        self
    }

    /// Enables lower-latency at-least-once writes.
    pub fn with_use_at_least_once_semantics(mut self, at_least_once: bool) -> Self {
        self.use_at_least_once_semantics = Some(at_least_once);
        self
    }

    /// Lets the service choose the number of Storage Write API streams.
    pub fn with_auto_sharding(mut self, auto_sharding: bool) -> Self {
        self.auto_sharding = Some(auto_sharding);
        self
    }

    /// Number of Storage Write API streams.
    pub fn with_num_streams(mut self, streams: i32) -> Self {
        self.num_streams = Some(streams);
        self
    }

    /// Cloud KMS key that encrypts table data.
    pub fn with_kms_key(mut self, kms_key: impl Into<String>) -> Self {
        self.kms_key = Some(kms_key.into());
        self
    }

    /// Sends rejected rows `(failed_row: ROW, error_message: STRING)` to the output tag
    /// `output` instead of failing. Needs [`WriteMethod::Auto`] or
    /// [`WriteMethod::StorageApiAtLeastOnce`]; the provider then always uses the Storage Write API.
    /// The transform returns [`PDone`], so to read errors expand `to_managed()?.with_outputs()`.
    pub fn with_error_handling(mut self, output: impl Into<String>) -> Self {
        self.error_output = Some(output.into());
        self
    }

    pub fn error_output(&self) -> Option<&str> {
        self.error_output.as_deref()
    }

    /// Defaults to [`DEFAULT_EXPANSION_SERVICE`].
    pub fn with_expansion_service(mut self, endpoint: impl Into<String>) -> Self {
        self.expansion_service = endpoint.into();
        self
    }

    pub fn table(&self) -> &str {
        &self.table
    }

    pub fn method(&self) -> WriteMethod {
        self.method
    }

    pub fn create_disposition(&self) -> Option<CreateDisposition> {
        self.create_disposition
    }

    pub fn write_disposition(&self) -> Option<WriteDisposition> {
        self.write_disposition
    }

    pub fn expansion_service(&self) -> &str {
        &self.expansion_service
    }

    /// Builds the config [`Row`] of `BigQueryWriteConfiguration`. Fails if the table is empty.
    pub fn build_config_row(&self) -> Result<Row, ExpansionError> {
        if self.table.is_empty() {
            return Err(ExpansionError::InvalidResponse(
                "BigQueryWrite requires destination table".to_string(),
            ));
        }
        let table = self.table.as_str();

        let use_at_least_once = match self.method {
            WriteMethod::StorageApiAtLeastOnce => Some(true),
            _ => self.use_at_least_once_semantics,
        };

        let error_handling = self
            .error_output
            .as_deref()
            .map(|tag| {
                Row::new(
                    Arc::clone(&ERROR_HANDLING_SCHEMA),
                    vec![Some(FieldValue::String(tag.to_string()))],
                )
                .map(FieldValue::Row)
                .map_err(|e| ExpansionError::Encoding(format!("BigQuery error_handling: {e}")))
            })
            .transpose()?;

        Row::builder(Arc::clone(&BIGQUERY_WRITE_CONFIG_SCHEMA))
            .with_named("auto_sharding", self.auto_sharding)
            .with_named("big_lake_configuration", None::<FieldValue>)
            .with_named("clustering_fields", None::<FieldValue>)
            .with_named(
                "create_disposition",
                self.create_disposition.map(|d| d.as_str()),
            )
            .with_named("drop", None::<FieldValue>)
            .with_named("error_handling", error_handling)
            .with_named("keep", None::<FieldValue>)
            .with_named("kms_key", self.kms_key.as_deref())
            .with_named("num_streams", self.num_streams)
            .with_named("only", None::<FieldValue>)
            .with_named("primary_key", None::<FieldValue>)
            .with_named("table", Some(table))
            .with_named(
                "triggering_frequency_seconds",
                self.triggering_frequency_seconds,
            )
            .with_named("use_at_least_once_semantics", use_at_least_once)
            .with_named("use_cdc_writes", None::<FieldValue>)
            .with_named(
                "write_disposition",
                self.write_disposition.map(|d| d.as_str()),
            )
            .build()
            .map_err(|e| {
                ExpansionError::Encoding(format!("Failed to build BigQueryWrite config: {e}"))
            })
    }

    /// Returns the `bigquery_write:v1` [`ManagedWrite`] for [`WriteMethod::Auto`] and
    /// [`WriteMethod::StorageApiAtLeastOnce`], or `None` for methods with no Managed equivalent.
    pub fn to_managed(&self) -> Result<Option<ManagedWrite>, ExpansionError> {
        let config_row = self.build_config_row()?;
        Ok(match self.method {
            WriteMethod::Auto | WriteMethod::StorageApiAtLeastOnce => Some(
                ManagedWrite::new(self.name.clone(), URN_BIGQUERY_WRITE)
                    .with_config_row(&config_row)
                    .with_expansion_service(&self.expansion_service),
            ),
            WriteMethod::StorageWriteApi | WriteMethod::FileLoads => None,
        })
    }

    /// Builds the [`ExternalSink`] through Managed I/O if possible, else the SchemaTransform
    /// directly. Fails if error handling is set on a method that does not support it.
    pub fn build(&self) -> Result<ExternalSink, ExpansionError> {
        if let Some(managed) = self.to_managed()? {
            return managed.build();
        }
        // Nothing can consume the error output of a direct transform, so rows would be lost.
        if let Some(tag) = &self.error_output {
            return Err(ExpansionError::InvalidResponse(format!(
                "BigQueryWrite error handling ('{tag}') needs WriteMethod::Auto or \
                 WriteMethod::StorageApiAtLeastOnce, not {:?}",
                self.method
            )));
        }

        let urn = match self.method {
            WriteMethod::FileLoads => URN_BIGQUERY_FILELOADS,
            _ => URN_BIGQUERY_STORAGE_WRITE,
        };
        let transform = ExternalTransform::schema_transform(
            self.name.clone(),
            urn,
            &self.expansion_service,
            &self.build_config_row()?,
        )?;

        Ok(ExternalSink::new(transform).with_input_tag("input"))
    }
}

impl BigQueryWrite {
    /// Fallible form of [`PTransform::expand`].
    pub fn try_expand(&self, input: &PCollection<Row>) -> Result<PDone, ExpansionError> {
        self.build()?.try_expand(input)
    }
}

impl PTransform<PCollection<Row>> for BigQueryWrite {
    type Output = PDone;

    fn expand(&self, input: &PCollection<Row>) -> Self::Output {
        self.try_expand(input)
            .unwrap_or_else(|e| panic!("{} could not be expanded: {e}", self.name))
    }
}
