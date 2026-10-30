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

//! Google Cloud BigQuery I/O.
//!
//! This module holds the transforms that expand through the Java SchemaTransform providers
//! for Storage Read, Storage Write and File Loads. [`storage`] holds the native Storage Read
//! and Storage Write connectors, with the `bigquery-storage` feature.
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
//!     "BigQueryRead",
//!     BigQueryRead::new()
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
//!     "BigQueryWrite",
//!     BigQueryWrite::new("my-project:my_dataset.my_table")
//!         .with_create_disposition(CreateDisposition::CreateIfNeeded)
//!         .with_write_disposition(WriteDisposition::WriteAppend)
//!         .with_method(WriteMethod::StorageWriteApi)
//! );
//! ```

#[cfg(feature = "bigquery-storage")]
pub mod storage;
pub mod table;

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
    /// Returns the BigQuery configuration string for this disposition (e.g. `"CREATE_IF_NEEDED"`).
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::CreateIfNeeded => "CREATE_IF_NEEDED",
            Self::CreateNever => "CREATE_NEVER",
        }
    }
}

impl AsRef<str> for CreateDisposition {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl std::fmt::Display for CreateDisposition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl From<CreateDisposition> for &'static str {
    fn from(disposition: CreateDisposition) -> Self {
        disposition.as_str()
    }
}

impl std::str::FromStr for CreateDisposition {
    type Err = String;

    /// Parses a name of [`as_str`](Self::as_str), in any case.
    fn from_str(name: &str) -> Result<Self, String> {
        match name.to_ascii_uppercase().as_str() {
            "CREATE_IF_NEEDED" => Ok(Self::CreateIfNeeded),
            "CREATE_NEVER" => Ok(Self::CreateNever),
            _ => Err(format!(
                "Unknown create disposition '{name}': expected CREATE_IF_NEEDED or CREATE_NEVER"
            )),
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
    /// Returns the BigQuery configuration string for this disposition (e.g. `"WRITE_APPEND"`).
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::WriteAppend => "WRITE_APPEND",
            Self::WriteTruncate => "WRITE_TRUNCATE",
            Self::WriteEmpty => "WRITE_EMPTY",
        }
    }
}

impl AsRef<str> for WriteDisposition {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl std::fmt::Display for WriteDisposition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl From<WriteDisposition> for &'static str {
    fn from(disposition: WriteDisposition) -> Self {
        disposition.as_str()
    }
}

impl std::str::FromStr for WriteDisposition {
    type Err = String;

    /// Parses a name of [`as_str`](Self::as_str), in any case.
    fn from_str(name: &str) -> Result<Self, String> {
        match name.to_ascii_uppercase().as_str() {
            "WRITE_APPEND" => Ok(Self::WriteAppend),
            "WRITE_TRUNCATE" => Ok(Self::WriteTruncate),
            "WRITE_EMPTY" => Ok(Self::WriteEmpty),
            _ => Err(format!(
                "Unknown write disposition '{name}': expected WRITE_APPEND, WRITE_TRUNCATE or WRITE_EMPTY"
            )),
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

impl WriteMethod {
    /// Each method with its name in Java's `BigQueryIO.Write.Method`.
    const NAMES: [(Self, &'static str); 4] = [
        (Self::Auto, "DEFAULT"),
        (Self::StorageWriteApi, "STORAGE_WRITE_API"),
        (Self::FileLoads, "FILE_LOADS"),
        (Self::StorageApiAtLeastOnce, "STORAGE_API_AT_LEAST_ONCE"),
    ];
}

/// The name of the method in Java's `BigQueryIO.Write.Method`, for example `FILE_LOADS`.
impl std::fmt::Display for WriteMethod {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let (_, name) = Self::NAMES
            .iter()
            .find(|(method, _)| method == self)
            .expect("NAMES has every method");
        f.write_str(name)
    }
}

/// Reads a name of Java's `BigQueryIO.Write.Method`, in any case: `DEFAULT` is
/// [`WriteMethod::Auto`].
impl std::str::FromStr for WriteMethod {
    type Err = UnknownWriteMethod;

    fn from_str(name: &str) -> Result<Self, Self::Err> {
        Self::NAMES
            .iter()
            .find(|(_, known)| known.eq_ignore_ascii_case(name))
            .map(|(method, _)| *method)
            .ok_or_else(|| UnknownWriteMethod(name.to_string()))
    }
}

/// A name that is not a [`WriteMethod`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UnknownWriteMethod(pub String);

impl std::fmt::Display for UnknownWriteMethod {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let names: Vec<&str> = WriteMethod::NAMES.iter().map(|(_, name)| *name).collect();
        write!(
            f,
            "Unknown BigQuery write method '{}'; expected one of {}",
            self.0,
            names.join(", ")
        )
    }
}

impl std::error::Error for UnknownWriteMethod {}

#[derive(Clone, Debug, PartialEq, Eq)]
enum ReadSource {
    Table(String),
    Query(String),
}

/// Transform for reading from BigQuery.
#[derive(Clone, Debug)]
pub struct BigQueryRead {
    source: Option<ReadSource>,
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

impl Default for BigQueryRead {
    fn default() -> Self {
        Self::new()
    }
}

impl BigQueryRead {
    /// Set the source with [`with_table`](Self::with_table) or [`with_query`](Self::with_query).
    pub fn new() -> Self {
        Self {
            source: None,
            selected_fields: None,
            row_restriction: None,
            kms_key: None,
            expansion_service: crate::DEFAULT_EXPANSION_SERVICE.to_string(),
        }
    }

    /// Reads the table `[${PROJECT}:]${DATASET}.${TABLE}`.
    pub fn with_table(mut self, table: impl Into<String>) -> Self {
        self.source = Some(ReadSource::Table(table.into()));
        self
    }

    /// Reads the result of a GoogleSQL query.
    pub fn with_query(mut self, query: impl Into<String>) -> Self {
        self.source = Some(ReadSource::Query(query.into()));
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

    /// Defaults to [`crate::DEFAULT_EXPANSION_SERVICE`].
    pub fn with_expansion_service(mut self, endpoint: impl Into<String>) -> Self {
        self.expansion_service = endpoint.into();
        self
    }

    /// Destination BigQuery table name if configured.
    #[doc(hidden)]
    pub fn table(&self) -> Option<&str> {
        match self.source.as_ref() {
            Some(ReadSource::Table(table)) => Some(table.as_str()),
            Some(ReadSource::Query(_)) | None => None,
        }
    }

    /// GoogleSQL query string if configured.
    #[doc(hidden)]
    pub fn query(&self) -> Option<&str> {
        match self.source.as_ref() {
            Some(ReadSource::Query(query)) => Some(query.as_str()),
            Some(ReadSource::Table(_)) | None => None,
        }
    }

    /// Selected table column projections.
    #[doc(hidden)]
    pub fn selected_fields(&self) -> Option<&[String]> {
        self.selected_fields.as_deref()
    }

    /// Row restriction SQL predicate.
    #[doc(hidden)]
    pub fn row_restriction(&self) -> Option<&str> {
        self.row_restriction.as_deref()
    }

    /// Expansion service address.
    #[doc(hidden)]
    pub fn expansion_service(&self) -> &str {
        &self.expansion_service
    }

    /// Builds the config [`Row`] of `BigQueryDirectReadSchemaTransformConfiguration`.
    pub fn build_config_row(&self) -> Result<Row, ExpansionError> {
        let (table_spec, query) = match self.source.as_ref() {
            Some(ReadSource::Table(table)) => (Some(table.as_str()), None),
            Some(ReadSource::Query(query)) => (None, Some(query.as_str())),
            None => (None, None),
        };

        Row::builder(Arc::clone(&BIGQUERY_READ_CONFIG_SCHEMA))
            .with_named("table_spec", table_spec)
            .with_named("query", query)
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
        if self.source.is_none() {
            return Err(ExpansionError::InvalidConfig(
                "BigQueryRead requires either table or query".to_string(),
            ));
        }

        let config_row = self.build_config_row()?;
        Ok(ManagedRead::new(URN_BIGQUERY_STORAGE_READ)
            .with_config_row(&config_row)
            .with_expansion_service(&self.expansion_service))
    }

    /// Builds the [`ExternalSource`] that expands through Managed I/O.
    pub fn build(&self) -> Result<ExternalSource, ExpansionError> {
        self.to_managed()?.build()
    }
}

impl TryPTransform<PBegin> for BigQueryRead {
    type Error = ExpansionError;

    fn try_expand(&self, input: &PBegin) -> Result<PCollection<Row>, ExpansionError> {
        self.build()?.try_expand(input)
    }
}

impl PTransform<PBegin> for BigQueryRead {
    type Output = PCollection<Row>;

    /// # Panics
    ///
    /// Panics if the expansion fails. Use `try_apply` to get the error.
    fn expand(&self, input: &PBegin) -> Self::Output {
        self.try_expand(input)
            .unwrap_or_else(|e| panic!("the transform could not be expanded: {e}"))
    }
}

/// Transform for writing to BigQuery.
#[derive(Clone, Debug)]
pub struct BigQueryWrite {
    table: String,
    create_disposition: CreateDisposition,
    write_disposition: WriteDisposition,
    method: WriteMethod,
    triggering_frequency_seconds: Option<i64>,
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
            FieldType::row(Arc::clone(&ERROR_HANDLING_SCHEMA)),
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
    pub fn new(table: impl Into<String>) -> Self {
        Self {
            table: table.into(),
            create_disposition: CreateDisposition::CreateIfNeeded,
            write_disposition: WriteDisposition::WriteAppend,
            method: WriteMethod::Auto,
            triggering_frequency_seconds: None,
            auto_sharding: None,
            num_streams: None,
            kms_key: None,
            error_output: None,
            expansion_service: crate::DEFAULT_EXPANSION_SERVICE.to_string(),
        }
    }

    /// Specifies whether table creation is permitted.
    pub fn with_create_disposition(mut self, disposition: CreateDisposition) -> Self {
        self.create_disposition = disposition;
        self
    }

    /// Action when the destination table already exists.
    pub fn with_write_disposition(mut self, disposition: WriteDisposition) -> Self {
        self.write_disposition = disposition;
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

    /// Routes rejected rows `(failed_row: ROW, error_message: STRING)` to the output tag
    /// `output` when a write fails. Requires [`WriteMethod::Auto`] or
    /// [`WriteMethod::StorageApiAtLeastOnce`]. The provider then always uses the
    /// Storage Write API. The transform returns [`PDone`], so expand
    /// `to_managed()?.with_outputs()` to read errors.
    pub fn with_error_handling(mut self, output: impl Into<String>) -> Self {
        self.error_output = Some(output.into());
        self
    }

    /// Output tag for rejected rows.
    #[doc(hidden)]
    pub fn error_output(&self) -> Option<&str> {
        self.error_output.as_deref()
    }

    /// Defaults to [`crate::DEFAULT_EXPANSION_SERVICE`].
    pub fn with_expansion_service(mut self, endpoint: impl Into<String>) -> Self {
        self.expansion_service = endpoint.into();
        self
    }

    /// Destination table in `[${PROJECT}:]${DATASET}.${TABLE}` format.
    #[doc(hidden)]
    pub fn table(&self) -> &str {
        &self.table
    }

    /// Write method used to insert rows.
    #[doc(hidden)]
    pub fn method(&self) -> WriteMethod {
        self.method
    }

    /// Table creation disposition.
    #[doc(hidden)]
    pub fn create_disposition(&self) -> CreateDisposition {
        self.create_disposition
    }

    /// Write disposition when the destination table already exists.
    #[doc(hidden)]
    pub fn write_disposition(&self) -> WriteDisposition {
        self.write_disposition
    }

    /// Expansion service address.
    #[doc(hidden)]
    pub fn expansion_service(&self) -> &str {
        &self.expansion_service
    }

    /// Builds the config [`Row`] of `BigQueryWriteConfiguration`. Fails if the table is empty.
    pub fn build_config_row(&self) -> Result<Row, ExpansionError> {
        if self.table.is_empty() {
            return Err(ExpansionError::InvalidConfig(
                "BigQueryWrite requires destination table".to_string(),
            ));
        }
        let table = self.table.as_str();

        let use_at_least_once = match self.method {
            WriteMethod::StorageApiAtLeastOnce => Some(true),
            WriteMethod::Auto | WriteMethod::StorageWriteApi | WriteMethod::FileLoads => None,
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
            .with_named("create_disposition", Some(self.create_disposition.as_str()))
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
            .with_named("write_disposition", Some(self.write_disposition.as_str()))
            .build()
            .map_err(|e| {
                ExpansionError::Encoding(format!("Failed to build BigQueryWrite config: {e}"))
            })
    }

    /// Returns the `bigquery_write:v1` [`ManagedWrite`] for [`WriteMethod::Auto`] and
    /// [`WriteMethod::StorageApiAtLeastOnce`], or `None` for methods with no Managed equivalent.
    pub fn to_managed(&self) -> Result<Option<ManagedWrite>, ExpansionError> {
        match self.method {
            WriteMethod::Auto | WriteMethod::StorageApiAtLeastOnce => {
                let config_row = self.build_config_row()?;
                Ok(Some(
                    ManagedWrite::new(URN_BIGQUERY_WRITE)
                        .with_config_row(&config_row)
                        .with_expansion_service(&self.expansion_service),
                ))
            }
            WriteMethod::StorageWriteApi | WriteMethod::FileLoads => Ok(None),
        }
    }

    /// Builds the [`ExternalSink`] through Managed I/O if possible, else the SchemaTransform
    /// directly. Fails if error handling is set on a method that does not support it.
    pub fn build(&self) -> Result<ExternalSink, ExpansionError> {
        if let Some(managed) = self.to_managed()? {
            return managed.build();
        }

        // Nothing can consume the error output of a direct transform, so rows would be lost.
        if let Some(tag) = &self.error_output {
            return Err(ExpansionError::InvalidConfig(format!(
                "BigQueryWrite error handling ('{tag}') needs WriteMethod::Auto or \
                 WriteMethod::StorageApiAtLeastOnce, not {:?}",
                self.method
            )));
        }

        let config_row = self.build_config_row()?;
        let urn = match self.method {
            WriteMethod::FileLoads => URN_BIGQUERY_FILELOADS,
            WriteMethod::StorageWriteApi => URN_BIGQUERY_STORAGE_WRITE,
            WriteMethod::Auto | WriteMethod::StorageApiAtLeastOnce => unreachable!(),
        };
        let transform =
            ExternalTransform::schema_transform(urn, &self.expansion_service, &config_row)?;

        Ok(ExternalSink::new(transform).with_input_tag("input"))
    }
}

impl TryPTransform<PCollection<Row>> for BigQueryWrite {
    type Error = ExpansionError;

    fn try_expand(&self, input: &PCollection<Row>) -> Result<PDone, ExpansionError> {
        self.build()?.try_expand(input)
    }
}

impl PTransform<PCollection<Row>> for BigQueryWrite {
    type Output = PDone;

    /// # Panics
    ///
    /// Panics if the expansion fails. Use `try_apply` to get the error.
    fn expand(&self, input: &PCollection<Row>) -> Self::Output {
        self.try_expand(input)
            .unwrap_or_else(|e| panic!("the transform could not be expanded: {e}"))
    }
}
