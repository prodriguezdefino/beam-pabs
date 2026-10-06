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

//! Managed I/O for the Apache Beam Rust SDK.
//!
//! Managed transforms are connectors that the runner can manage, for example upgrade on
//! Dataflow without pipeline changes. [`ManagedRead`] and [`ManagedWrite`] expand the
//! `beam:transform:managed:v1` SchemaTransform, which looks up the connector by identifier
//! and parses the configuration against its schema. Connectors and configuration keys are
//! listed on the [Managed I/O page](https://beam.apache.org/documentation/io/managed-io/).
//!
//! # Reading
//! ```no_run
//! use std::collections::BTreeMap;
//!
//! use beam::prelude::*;
//! use managed_io::{self as managed, ManagedRead};
//!
//! let p = Pipeline::new();
//! let rows: PCollection<Row> = p.apply(
//!     ManagedRead::new("Managed Read(ICEBERG)", managed::ICEBERG)
//!         .with_config_entry("table", "db.events")
//!         .with_config_entry("catalog_name", "local")
//!         .with_config_entry(
//!             "catalog_properties",
//!             BTreeMap::from([("type", "hadoop"), ("warehouse", "file:///tmp/warehouse")]),
//!         ),
//! );
//! ```
//!
//! # Writing
//! Configuration can be any [`serde::Serialize`] value that serializes to a map, a raw
//! YAML string, or the location of a YAML file:
//! ```no_run
//! use beam::prelude::*;
//! use managed_io::{self as managed, ManagedWrite};
//!
//! #[derive(serde::Serialize)]
//! struct BigQueryConfig {
//!     table: String,
//! }
//!
//! # let rows: PCollection<Row> = unimplemented!();
//! rows.apply(ManagedWrite::new("Managed Write(BIGQUERY)", managed::BIGQUERY).with_config(BigQueryConfig {
//!     table: "my-project.dataset.table".into(),
//! }));
//! # rows.apply(ManagedWrite::new("Managed Write(KAFKA)", managed::KAFKA).with_config_url("gs://bucket/kafka.yaml"));
//! ```
//!
//! Inputs and outputs are `PCollection<Row>` with a schema.
//!
//! # Extra outputs and error handling
//! An Iceberg write also emits the [`SNAPSHOTS`] it committed, and connectors that accept
//! [`ERROR_HANDLING`] emit failed records. `with_all_outputs` (reads) and `with_outputs`
//! (writes) return all outputs as [`ExternalOutputs`]:
//! ```no_run
//! use beam::prelude::*;
//! use managed_io::{self as managed, ManagedRead, ManagedWrite};
//!
//! # fn run(p: &Pipeline, rows: PCollection<Row>) -> Result<(), external::ExpansionError> {
//! let written = rows.apply(
//!     ManagedWrite::new("Managed Write(ICEBERG)", managed::ICEBERG)
//!         .with_config_entry("table", "db.events")
//!         .with_outputs(),
//! );
//! let snapshots = written.expect(managed::SNAPSHOTS)?;
//!
//! let read = p.apply(
//!     ManagedRead::new("Managed Read(KAFKA)", managed::KAFKA)
//!         .with_config_entry("topic", "events")
//!         .with_error_handling("errors")
//!         .with_all_outputs(),
//! );
//! let (records, errors) = (read.expect(managed::OUTPUT)?, read.expect("errors")?);
//! # Ok(()) }
//! ```
//! Output tags are declared up front (see [`known_outputs`]). Expansion fails if the
//! expansion service does not produce a declared tag.
//!
//! # Configuration encoding
//! Structured configuration is serialized as JSON, which the Managed YAML loader reads
//! unchanged. Every string is quoted, so YAML 1.1 coercions cannot turn `off`, `yes` or
//! `0123` into booleans or numbers.

use std::sync::{Arc, LazyLock};

use beam::prelude::*;
use beam::schema::{Field, FieldType, FieldValue};
/// Every output of a multi-output Managed transform, keyed by tag.
pub use external::ExternalOutputs;
use external::{ExpansionError, ExternalSink, ExternalSource, ExternalTransform};
use serde::Serialize;
use serde_json::{Map, Value};

/// Identifier of `ManagedSchemaTransformProvider`.
pub const URN_MANAGED: &str = "beam:transform:managed:v1";

/// Apache Iceberg (read and write).
pub const ICEBERG: &str = "iceberg";
/// Apache Iceberg change data capture (read only). Hidden until integration tests vet it.
#[doc(hidden)]
pub const ICEBERG_CDC: &str = "iceberg_cdc";
/// Apache Kafka (read and write).
pub const KAFKA: &str = "kafka";
/// Google BigQuery (Storage Read API for reads; `bigquery_write` for writes).
pub const BIGQUERY: &str = "bigquery";
/// PostgreSQL over JDBC (read and write).
pub const POSTGRES: &str = "postgres";
/// MySQL over JDBC (read and write).
pub const MYSQL: &str = "mysql";
/// Microsoft SQL Server over JDBC (read and write).
pub const SQL_SERVER: &str = "sqlserver";
/// Delta Lake (read only).
pub const DELTA: &str = "delta";

/// Underlying SchemaTransform URNs (`ManagedTransforms.Urns` in `external_transforms.proto`).
pub mod urns {
    pub const ICEBERG_READ: &str = "beam:schematransform:org.apache.beam:iceberg_read:v1";
    pub const ICEBERG_WRITE: &str = "beam:schematransform:org.apache.beam:iceberg_write:v1";
    pub const ICEBERG_CDC_READ: &str = "beam:schematransform:org.apache.beam:iceberg_cdc_read:v1";
    pub const KAFKA_READ: &str = "beam:schematransform:org.apache.beam:kafka_read:v1";
    pub const KAFKA_WRITE: &str = "beam:schematransform:org.apache.beam:kafka_write:v1";
    pub const BIGQUERY_READ: &str = "beam:schematransform:org.apache.beam:bigquery_storage_read:v1";
    pub const BIGQUERY_WRITE: &str = "beam:schematransform:org.apache.beam:bigquery_write:v1";
    pub const POSTGRES_READ: &str = "beam:schematransform:org.apache.beam:postgres_read:v1";
    pub const POSTGRES_WRITE: &str = "beam:schematransform:org.apache.beam:postgres_write:v1";
    pub const MYSQL_READ: &str = "beam:schematransform:org.apache.beam:mysql_read:v1";
    pub const MYSQL_WRITE: &str = "beam:schematransform:org.apache.beam:mysql_write:v1";
    pub const SQL_SERVER_READ: &str = "beam:schematransform:org.apache.beam:sql_server_read:v1";
    pub const SQL_SERVER_WRITE: &str = "beam:schematransform:org.apache.beam:sql_server_write:v1";
    pub const DELTA_LAKE_READ: &str = "beam:schematransform:org.apache.beam:delta_lake_read:v1";
}

/// The Java I/O expansion service: Iceberg, Kafka and Delta Lake.
pub const IO_EXPANSION_SERVICE: &str =
    "autojava::sdks:java:io:expansion-service:runExpansionService";

/// The Java GCP I/O expansion service: BigQuery and the JDBC databases.
pub const GCP_EXPANSION_SERVICE: &str =
    "autojava::sdks:java:io:google-cloud-platform:expansion-service:runExpansionService";

const READ_TRANSFORMS: &[(&str, &str)] = &[
    (ICEBERG, urns::ICEBERG_READ),
    (ICEBERG_CDC, urns::ICEBERG_CDC_READ),
    (KAFKA, urns::KAFKA_READ),
    (BIGQUERY, urns::BIGQUERY_READ),
    (POSTGRES, urns::POSTGRES_READ),
    (MYSQL, urns::MYSQL_READ),
    (SQL_SERVER, urns::SQL_SERVER_READ),
    (DELTA, urns::DELTA_LAKE_READ),
];

const WRITE_TRANSFORMS: &[(&str, &str)] = &[
    (ICEBERG, urns::ICEBERG_WRITE),
    (KAFKA, urns::KAFKA_WRITE),
    (BIGQUERY, urns::BIGQUERY_WRITE),
    (POSTGRES, urns::POSTGRES_WRITE),
    (MYSQL, urns::MYSQL_WRITE),
    (SQL_SERVER, urns::SQL_SERVER_WRITE),
];

fn lookup(table: &[(&str, &'static str)], name: &str) -> Option<&'static str> {
    table
        .iter()
        .find(|(n, _)| n.eq_ignore_ascii_case(name))
        .map(|(_, urn)| *urn)
}

fn public_names(table: &[(&str, &str)]) -> Vec<&'static str> {
    // `ICEBERG_CDC` is hidden, so it is not in this list.
    [ICEBERG, KAFKA, BIGQUERY, POSTGRES, MYSQL, SQL_SERVER, DELTA]
        .into_iter()
        .filter(|n| table.iter().any(|(t, _)| t == n))
        .collect()
}

/// URN of the managed read for `source` (case-insensitive), e.g. [`ICEBERG`].
pub fn read_urn(source: &str) -> Option<&'static str> {
    lookup(READ_TRANSFORMS, source)
}

/// URN of the managed write for `sink` (case-insensitive), e.g. [`BIGQUERY`].
pub fn write_urn(sink: &str) -> Option<&'static str> {
    lookup(WRITE_TRANSFORMS, sink)
}

/// Returns the expansion service that bundles the transform `urn`.
pub fn default_expansion_service(urn: &str) -> Option<&'static str> {
    use urns::*;
    match urn {
        ICEBERG_READ | ICEBERG_WRITE | ICEBERG_CDC_READ | KAFKA_READ | KAFKA_WRITE
        | DELTA_LAKE_READ => Some(IO_EXPANSION_SERVICE),
        BIGQUERY_READ | BIGQUERY_WRITE | POSTGRES_READ | POSTGRES_WRITE | MYSQL_READ
        | MYSQL_WRITE | SQL_SERVER_READ | SQL_SERVER_WRITE => Some(GCP_EXPANSION_SERVICE),
        _ => None,
    }
}

/// Main output of every Managed read.
pub const OUTPUT: &str = "output";
/// Committed snapshots of an Iceberg write, one row per commit: `table` plus Iceberg's
/// snapshot fields (`snapshot_id`, `operation`, `summary`, `timestamp_millis`, ...).
pub const SNAPSHOTS: &str = "snapshots";

/// Configuration key `error_handling: {output: <tag>}`. Set it with `with_error_handling`.
pub const ERROR_HANDLING: &str = "error_handling";

/// Output tags the transform `urn` always produces besides [`OUTPUT`], declared up front
/// so a worker can rebuild the pipeline without an expansion service. Config-dependent
/// outputs are not listed: `with_error_handling` declares the error output, and BigQuery's
/// `post_write` exists only when the Storage Write API is selected.
pub fn known_outputs(urn: &str) -> &'static [&'static str] {
    match urn {
        urns::ICEBERG_WRITE => &[SNAPSHOTS],
        _ => &[],
    }
}

/// Looks up a connector name in `table`. A value that contains `:` is used as a URN.
fn resolve_spec(
    name: String,
    connector: &str,
    table: &[(&str, &'static str)],
    kind: &str,
) -> ManagedSpec {
    if connector.contains(':') {
        return ManagedSpec::new(connector, name);
    }
    match lookup(table, connector) {
        Some(urn) => ManagedSpec::new(urn, name),
        None => ManagedSpec::invalid(
            name,
            format!(
                "An unsupported {kind} was specified: '{connector}'. Please specify one of the \
                 following {kind}s: {:?}",
                public_names(table)
            ),
        ),
    }
}

/// State shared by [`ManagedRead`] and [`ManagedWrite`].
#[derive(Clone, Debug)]
struct ManagedSpec {
    /// Underlying SchemaTransform URN; `None` if construction failed.
    transform_identifier: Option<String>,
    name: String,
    config: Map<String, Value>,
    yaml_config: Option<String>,
    config_url: Option<String>,
    expansion_service: Option<String>,
    /// First error recorded by a builder method, reported on build.
    error: Option<String>,
}

impl ManagedSpec {
    fn new(urn: &str, name: String) -> Self {
        Self {
            transform_identifier: Some(urn.to_string()),
            name,
            config: Map::new(),
            yaml_config: None,
            config_url: None,
            expansion_service: None,
            error: None,
        }
    }

    fn invalid(name: String, error: String) -> Self {
        Self {
            transform_identifier: None,
            name,
            config: Map::new(),
            yaml_config: None,
            config_url: None,
            expansion_service: None,
            error: Some(error),
        }
    }

    fn fail(&mut self, error: String) {
        self.error.get_or_insert(error);
    }

    fn merge_config(&mut self, config: impl Serialize) {
        match serde_json::to_value(config) {
            Ok(Value::Object(map)) => self.config.extend(map),
            Ok(Value::Null) => {}
            Ok(other) => self.fail(format!(
                "Managed configuration must serialize to a map, got: {other}"
            )),
            Err(e) => self.fail(format!(
                "Managed configuration could not be serialized: {e}"
            )),
        }
    }

    fn set_entry(&mut self, key: String, value: impl Serialize) {
        match serde_json::to_value(value) {
            Ok(v) => {
                self.config.insert(key, v);
            }
            Err(e) => self.fail(format!("Managed configuration key '{key}': {e}")),
        }
    }

    fn merge_row(&mut self, row: &Row) {
        match row_to_config(row) {
            Ok(map) => self.config.extend(map),
            Err(e) => self.fail(e),
        }
    }

    fn transform_identifier(&self) -> Result<&str, ExpansionError> {
        if let Some(e) = &self.error {
            return Err(invalid(e.clone()));
        }
        self.transform_identifier
            .as_deref()
            .ok_or_else(|| invalid("Managed transform has no identifier"))
    }

    fn expansion_service(&self) -> Result<&str, ExpansionError> {
        let urn = self.transform_identifier()?;
        match &self.expansion_service {
            Some(endpoint) => Ok(endpoint),
            None => default_expansion_service(urn).ok_or_else(|| {
                invalid(format!(
                    "No expansion service was specified and could not find a default \
                     expansion service for '{urn}'"
                ))
            }),
        }
    }

    /// The config string sent to the expansion service, or `None` if only a URL is used.
    fn config_string(&self) -> Result<Option<String>, ExpansionError> {
        let structured = !self.config.is_empty();
        match (&self.yaml_config, &self.config_url, structured) {
            (Some(_), _, true) => Err(invalid(
                "Managed transform takes either a YAML config string or structured config \
                 entries, not both",
            )),
            (Some(_), Some(_), _) | (None, Some(_), true) => Err(invalid(
                "Please specify a config or a config URL, but not both",
            )),
            (Some(yaml), None, false) => Ok(Some(yaml.clone())),
            (None, Some(_), false) => Ok(None),
            // An empty mapping is still a config: the provider rejects neither-config-nor-URL,
            // and some transforms have no required fields.
            (None, None, _) => serde_json::to_string(&self.config)
                .map(Some)
                .map_err(|e| ExpansionError::Encoding(format!("Managed config: {e}"))),
        }
    }

    fn build_config_row(&self) -> Result<Row, ExpansionError> {
        let identifier = self.transform_identifier()?;
        let config = self.config_string()?;
        Row::builder(managed_config_schema())
            .with_named("config", config.as_deref())
            .with_named("config_url", self.config_url.as_deref())
            .with_named("transform_identifier", Some(identifier))
            .build()
            .map_err(|e| ExpansionError::Encoding(format!("Failed to build Managed config: {e}")))
    }

    fn error_output(&self) -> Option<&str> {
        self.config.get(ERROR_HANDLING)?.get("output")?.as_str()
    }

    /// Known outputs plus the error output. Derived from the config, so an `error_handling`
    /// set through `with_config` is declared too, and submitter and worker agree on tags.
    fn output_tags(&self) -> Vec<String> {
        self.transform_identifier
            .as_deref()
            .map(known_outputs)
            .unwrap_or_default()
            .iter()
            .copied()
            .chain(self.error_output())
            .map(str::to_string)
            .collect()
    }

    fn build_transform(&self) -> Result<ExternalTransform, ExpansionError> {
        let config_row = self.build_config_row()?;
        ExternalTransform::schema_transform(
            self.name.clone(),
            URN_MANAGED,
            self.expansion_service()?,
            &config_row,
        )
        .map(|t| t.with_output_tags(self.output_tags()))
    }
}

static MANAGED_CONFIG_SCHEMA: LazyLock<Arc<Schema>> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::nullable("config", FieldType::string()),
        Field::nullable("config_url", FieldType::string()),
        Field::new("transform_identifier", FieldType::string()),
    ]))
});

/// Schema of `ManagedSchemaTransformProvider.ManagedConfig` (sorted, snake_case).
pub fn managed_config_schema() -> Arc<Schema> {
    Arc::clone(&MANAGED_CONFIG_SCHEMA)
}

fn invalid(msg: impl Into<String>) -> ExpansionError {
    ExpansionError::InvalidResponse(msg.into())
}

/// Converts a SchemaTransform configuration [`Row`] to a Managed config mapping, so a typed
/// connector can expand through Managed. Null fields are omitted so the provider applies defaults.
pub fn row_to_config(row: &Row) -> Result<Map<String, Value>, String> {
    row.schema()
        .fields
        .iter()
        .zip(row.values())
        .filter_map(|(field, value)| value.as_ref().map(|v| (field, v)))
        .map(|(field, value)| {
            field_value_to_json(value)
                .map(|v| (field.name.clone(), v))
                .map_err(|e| format!("config field '{}': {e}", field.name))
        })
        .collect()
}

fn field_value_to_json(value: &FieldValue) -> Result<Value, String> {
    Ok(match value {
        FieldValue::Byte(v) => Value::from(*v),
        FieldValue::Int16(v) => Value::from(*v),
        FieldValue::Int32(v) => Value::from(*v),
        FieldValue::Int64(v) => Value::from(*v),
        FieldValue::Float(v) => Value::from(f64::from(*v)),
        FieldValue::Double(v) => Value::from(*v),
        FieldValue::String(v) => Value::from(v.as_str()),
        FieldValue::Boolean(v) => Value::from(*v),
        // The Managed YAML loader decodes BYTES fields from base64.
        FieldValue::Bytes(v) => Value::from(base64_encode(v)),
        FieldValue::Array(items) => Value::Array(
            items
                .iter()
                .map(|item| match item {
                    Some(v) => field_value_to_json(v),
                    None => Err("null array elements are not supported".to_string()),
                })
                .collect::<Result<_, _>>()?,
        ),
        FieldValue::Map(entries) => Value::Object(
            entries
                .iter()
                .map(|(k, v)| {
                    let FieldValue::String(key) = k else {
                        return Err(format!("map keys must be strings, got {k}"));
                    };
                    let value = match v {
                        Some(v) => field_value_to_json(v)?,
                        None => Value::Null,
                    };
                    Ok((key.clone(), value))
                })
                .collect::<Result<_, _>>()?,
        ),
        FieldValue::Row(nested) => Value::Object(row_to_config(nested)?),
    })
}

fn base64_encode(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    bytes
        .chunks(3)
        .flat_map(|chunk| {
            let b0 = u32::from(chunk[0]);
            let b1 = u32::from(chunk.get(1).copied().unwrap_or(0));
            let b2 = u32::from(chunk.get(2).copied().unwrap_or(0));
            let n = (b0 << 16) | (b1 << 8) | b2;
            let len = chunk.len();
            (0..4).map(move |i| {
                if i <= len {
                    char::from(ALPHABET[((n >> (18 - 6 * i)) & 0x3f) as usize])
                } else {
                    '='
                }
            })
        })
        .collect()
}

// Builder methods shared by read and write.
macro_rules! managed_builder_methods {
    () => {
        /// Merges `config`, which must serialize to a map (a `#[derive(Serialize)]` struct,
        /// `BTreeMap` or JSON object). Keys are the connector's names in the Managed I/O docs.
        pub fn with_config(mut self, config: impl Serialize) -> Self {
            self.spec.merge_config(config);
            self
        }

        /// Sets one configuration key.
        pub fn with_config_entry(mut self, key: impl Into<String>, value: impl Serialize) -> Self {
            self.spec.set_entry(key.into(), value);
            self
        }

        /// Merges the non-null fields of a SchemaTransform configuration [`Row`].
        pub fn with_config_row(mut self, row: &Row) -> Self {
            self.spec.merge_row(row);
            self
        }

        /// Uses a raw YAML configuration string instead of structured entries.
        pub fn with_yaml_config(mut self, yaml: impl Into<String>) -> Self {
            self.spec.yaml_config = Some(yaml.into());
            self
        }

        /// Reads the configuration from a YAML file at `url`.
        pub fn with_config_url(mut self, url: impl Into<String>) -> Self {
            self.spec.config_url = Some(url.into());
            self
        }

        /// Overrides the default expansion service, [`default_expansion_service`].
        pub fn with_expansion_service(mut self, endpoint: impl Into<String>) -> Self {
            self.spec.expansion_service = Some(endpoint.into());
            self
        }

        /// Routes records the connector fails to process to an extra output `output`
        /// instead of failing the bundle, by setting `error_handling: {output: <output>}`.
        /// Read the rows (typically `error_message` plus `failed_row`) through
        /// `with_all_outputs` or `with_outputs`. Only connectors that accept `error_handling`
        /// support this (Kafka, BigQuery writes); for BigQuery it also selects the Storage
        /// Write API.
        pub fn with_error_handling(self, output: impl Into<String>) -> Self {
            let output: String = output.into();
            self.with_config_entry(ERROR_HANDLING, serde_json::json!({ "output": output }))
        }

        /// The error output tag configured with [`Self::with_error_handling`], if any.
        pub fn error_output(&self) -> Option<&str> {
            self.spec.error_output()
        }

        /// Output tags besides the main output: [`known_outputs`] plus the error output.
        pub fn declared_outputs(&self) -> Vec<String> {
            self.spec.output_tags()
        }

        /// Underlying SchemaTransform URN, or `None` if the connector name was invalid.
        pub fn transform_identifier(&self) -> Option<&str> {
            self.spec.transform_identifier.as_deref()
        }

        /// Structured configuration entries set so far.
        pub fn config(&self) -> &Map<String, Value> {
            &self.spec.config
        }

        pub fn name(&self) -> &str {
            &self.spec.name
        }

        pub fn expansion_service(&self) -> Result<&str, ExpansionError> {
            self.spec.expansion_service()
        }

        /// Constructs the `beam:transform:managed:v1` configuration [`Row`]:
        /// `config` (JSON/YAML), `config_url` and `transform_identifier`.
        pub fn build_config_row(&self) -> Result<Row, ExpansionError> {
            self.spec.build_config_row()
        }
    };
}

/// A Managed read, producing a `PCollection<Row>`.
#[derive(Clone, Debug)]
pub struct ManagedRead {
    spec: ManagedSpec,
}

impl ManagedRead {
    /// Creates a Managed read from `source`: [`ICEBERG`], [`KAFKA`], [`BIGQUERY`],
    /// [`POSTGRES`], [`MYSQL`], [`SQL_SERVER`] or [`DELTA`] (case-insensitive), or any
    /// SchemaTransform URN. Runners manage only the URNs in [`urns`], and only those have a
    /// default expansion service. An unknown name fails at build or expansion.
    pub fn new(name: impl Into<String>, source: &str) -> Self {
        Self {
            spec: resolve_spec(name.into(), source, READ_TRANSFORMS, "source"),
        }
    }

    managed_builder_methods!();

    /// Builds the underlying [`ExternalSource`].
    pub fn build(&self) -> Result<ExternalSource, ExpansionError> {
        Ok(ExternalSource::new(self.spec.build_transform()?).with_output_tag("output"))
    }

    /// Builds and expands this read: the fallible form of [`PTransform::expand`].
    pub fn try_expand(&self, input: &PBegin) -> Result<PCollection<Row>, ExpansionError> {
        self.build()?.try_expand(input)
    }

    /// This read as a transform that returns every output, keyed by tag: [`OUTPUT`] plus
    /// the tag set by [`with_error_handling`](Self::with_error_handling).
    pub fn with_all_outputs(self) -> ManagedReadOutputs {
        ManagedReadOutputs(self)
    }
}

/// A [`ManagedRead`] that returns every output. Created by [`ManagedRead::with_all_outputs`].
#[derive(Clone, Debug)]
pub struct ManagedReadOutputs(ManagedRead);

impl ManagedReadOutputs {
    /// Builds and expands the read, returning its outputs keyed by tag.
    pub fn try_expand(&self, input: &PBegin) -> Result<ExternalOutputs, ExpansionError> {
        self.0.build()?.try_expand_all(input)
    }
}

impl PTransform<PBegin> for ManagedReadOutputs {
    type Output = ExternalOutputs;

    fn expand(&self, input: &PBegin) -> Self::Output {
        self.try_expand(input)
            .unwrap_or_else(|e| panic!("{} could not be expanded: {e}", self.0.spec.name))
    }
}

impl PTransform<PBegin> for ManagedRead {
    type Output = PCollection<Row>;

    fn expand(&self, input: &PBegin) -> Self::Output {
        self.try_expand(input)
            .unwrap_or_else(|e| panic!("{} could not be expanded: {e}", self.spec.name))
    }
}

/// A Managed write, consuming a `PCollection<Row>`.
#[derive(Clone, Debug)]
pub struct ManagedWrite {
    spec: ManagedSpec,
}

impl ManagedWrite {
    /// Creates a Managed write to `sink`: [`ICEBERG`], [`KAFKA`], [`BIGQUERY`],
    /// [`POSTGRES`], [`MYSQL`] or [`SQL_SERVER`] (case-insensitive), or any SchemaTransform
    /// URN. Runners manage only the URNs in [`urns`], and only those have a default
    /// expansion service. An unknown name fails at build or expansion.
    pub fn new(name: impl Into<String>, sink: &str) -> Self {
        Self {
            spec: resolve_spec(name.into(), sink, WRITE_TRANSFORMS, "sink"),
        }
    }

    managed_builder_methods!();

    /// Builds the underlying [`ExternalSink`].
    pub fn build(&self) -> Result<ExternalSink, ExpansionError> {
        Ok(ExternalSink::new(self.spec.build_transform()?).with_input_tag("input"))
    }

    /// Builds and expands this write: the fallible form of [`PTransform::expand`].
    pub fn try_expand(&self, input: &PCollection<Row>) -> Result<PDone, ExpansionError> {
        self.build()?.try_expand(input)
    }

    /// This write as a transform that returns its outputs instead of [`PDone`], such as
    /// [`SNAPSHOTS`] or the [`with_error_handling`](Self::with_error_handling) tag.
    pub fn with_outputs(self) -> ManagedWriteOutputs {
        ManagedWriteOutputs(self)
    }
}

/// A [`ManagedWrite`] that returns every output. Created by [`ManagedWrite::with_outputs`].
#[derive(Clone, Debug)]
pub struct ManagedWriteOutputs(ManagedWrite);

impl ManagedWriteOutputs {
    /// Builds and expands the write, returning its outputs keyed by tag.
    pub fn try_expand(&self, input: &PCollection<Row>) -> Result<ExternalOutputs, ExpansionError> {
        self.0.build()?.try_expand_all(input)
    }
}

impl PTransform<PCollection<Row>> for ManagedWriteOutputs {
    type Output = ExternalOutputs;

    fn expand(&self, input: &PCollection<Row>) -> Self::Output {
        self.try_expand(input)
            .unwrap_or_else(|e| panic!("{} could not be expanded: {e}", self.0.spec.name))
    }
}

impl PTransform<PCollection<Row>> for ManagedWrite {
    type Output = PDone;

    fn expand(&self, input: &PCollection<Row>) -> Self::Output {
        self.try_expand(input)
            .unwrap_or_else(|e| panic!("{} could not be expanded: {e}", self.spec.name))
    }
}
