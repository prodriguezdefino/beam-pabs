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

//! SchemaTransform providers for the native BigQuery Storage Read and Write connectors.

use std::collections::HashMap;
use std::time::Duration;

#[cfg(doc)]
use beam::io::gcp::bigquery::storage::read::error_schema;
use beam::io::gcp::bigquery::storage::read::{ReadOptions, ReadRows};
use beam::io::gcp::bigquery::storage::write::{Semantics, WriteOptions, WriteRows, is_unbounded};
use beam::io::gcp::bigquery::{CreateDisposition, WriteDisposition};
use beam::pipeline::Pipeline;
use beam::schema::{BeamRow, Row, Schema};
use beam::transforms::{DoFn, ParDo, ProcessContext};
use expansion::{
    PCollectionId, SchemaTransformProvider, TransformInputs, register_schema_transform,
};

use crate::ErrorHandling;

/// Identifier of [`BigQueryStorageReadProvider`].
pub const URN_BIGQUERY_STORAGE_READ: &str =
    "beam:schematransform:org.apache.beam:rust:bigquery_storage_read:v1";

/// The configuration of [`BigQueryStorageReadProvider`]. The fields from `query` to `kms_key`
/// are the fields of the Java `BigQueryDirectReadSchemaTransformProvider`. An empty string
/// counts as unset.
#[derive(Clone, Debug, Default, PartialEq, BeamRow)]
pub struct BigQueryStorageReadConfig {
    pub query: Option<String>,
    pub table_spec: Option<String>,
    pub row_restriction: Option<String>,
    pub selected_fields: Option<Vec<String>>,
    pub kms_key: Option<String>,
    pub billing_project: Option<String>,
    pub max_stream_count: Option<i64>,
    /// `NONE`, `LZ4_FRAME` or `ZSTD`.
    pub compression: Option<String>,
    /// `BATCH` or `INTERACTIVE`.
    pub query_priority: Option<String>,
    pub query_location: Option<String>,
    pub temp_dataset: Option<String>,
    pub use_legacy_sql: Option<bool>,
    pub flatten_results: Option<bool>,
    /// An RFC 3339 timestamp, such as `2024-05-01T12:30:00Z`. Table reads only.
    pub snapshot_time: Option<String>,
    pub preferred_min_stream_count: Option<i64>,
    /// The output for rows that do not convert to Beam rows. See
    /// [`BigQueryStorageReadProvider`].
    pub error_handling: Option<ErrorHandling>,
}

impl BigQueryStorageReadConfig {
    /// Returns the read options of this configuration, or why they are not valid.
    pub fn read_options(self) -> Result<ReadOptions, String> {
        let set = |value: Option<String>| value.filter(|s| !s.is_empty());
        let mut options = match (set(self.table_spec), set(self.query)) {
            (Some(table_spec), None) => ReadOptions::new(table_spec),
            (None, Some(query)) => ReadOptions::from_query(query),
            _ => return Err("Set one of table_spec and query".to_string()),
        };
        if let Some(filter) = set(self.row_restriction) {
            options = options.with_row_restriction(filter);
        }
        if let Some(fields) = self.selected_fields.filter(|f| !f.is_empty()) {
            options = options.with_selected_fields(fields);
        }
        if let Some(key) = set(self.kms_key) {
            options = options.with_kms_key(key);
        }
        if let Some(project) = set(self.billing_project) {
            options = options.with_billing_project(project);
        }
        if let Some(count) = self.max_stream_count {
            let count = i32::try_from(count)
                .map_err(|_| format!("max_stream_count {count} is too large"))?;
            options = options.with_max_stream_count(count);
        }
        if let Some(name) = set(self.compression) {
            options = options.with_compression(name.parse()?);
        }
        if let Some(name) = set(self.query_priority) {
            options = options.with_query_priority(name.parse()?);
        }
        if let Some(location) = set(self.query_location) {
            options = options.with_query_location(location);
        }
        if let Some(dataset) = set(self.temp_dataset) {
            options = options.with_temp_dataset(dataset);
        }
        if let Some(legacy) = self.use_legacy_sql {
            options = options.with_use_legacy_sql(legacy);
        }
        if let Some(flatten) = self.flatten_results {
            options = options.with_flatten_results(flatten);
        }
        if let Some(time) = set(self.snapshot_time) {
            options = options.with_snapshot_time(time);
        }
        if let Some(count) = self.preferred_min_stream_count {
            let count = i32::try_from(count)
                .ok()
                .filter(|count| *count >= 0)
                .ok_or_else(|| {
                    format!("preferred_min_stream_count {count} is negative or too large")
                })?;
            options = options.with_preferred_min_stream_count(count);
        }
        options.validate().map_err(beam::Error::msg)?;
        Ok(options)
    }
}

/// Reads a BigQuery table or query with the Storage Read API. See
/// [`BigQueryStorageReadConfig`] for the configuration.
///
/// The rows go to the output `output`. With `error_handling`, rows that do not convert go to
/// the output that it names, with the fields of [`error_schema`]. Without it, such a row
/// fails the pipeline.
#[derive(Default)]
pub struct BigQueryStorageReadProvider;

impl SchemaTransformProvider for BigQueryStorageReadProvider {
    fn identifier(&self) -> &'static str {
        URN_BIGQUERY_STORAGE_READ
    }

    fn description(&self) -> &'static str {
        "Reads a BigQuery table or query with the Storage Read API in Arrow format, and outputs Beam rows."
    }

    fn config_schema(&self) -> Schema {
        (**BigQueryStorageReadConfig::beam_schema()).clone()
    }

    fn input_tags(&self) -> Vec<String> {
        Vec::new()
    }

    /// Looks up the schema of the rows, so the expansion service needs credentials that can
    /// create a read session or run a dry-run job. A worker that replays the expansion looks
    /// the schema up again.
    fn build_transform(
        &self,
        config: Row,
        _inputs: TransformInputs,
        pipeline: &mut Pipeline,
    ) -> beam::Result<HashMap<String, PCollectionId>> {
        let mut config = BigQueryStorageReadConfig::from_row(&config).map_err(beam::Error::msg)?;
        let errors = config
            .error_handling
            .take()
            .map(|handling| handling.output)
            .filter(|output| !output.is_empty());
        // The rows go to another SDK, so they leave the reader as row coder bytes.
        let read =
            ReadRows::new(config.read_options().map_err(beam::Error::msg)?).with_encoded_rows();
        let begin = pipeline.begin();
        let outputs = match errors {
            Some(errors) => {
                let outputs = begin
                    .try_apply("ReadFromBigQuery", read.with_failures())
                    .map_err(beam::Error::msg)?;
                HashMap::from([
                    ("output".to_string(), outputs.output.id().into()),
                    (errors, outputs.failures.id().into()),
                ])
            }
            None => {
                let rows = begin
                    .try_apply("ReadFromBigQuery", read)
                    .map_err(beam::Error::msg)?;
                HashMap::from([("output".to_string(), rows.id().into())])
            }
        };
        Ok(outputs)
    }
}

register_schema_transform!(BigQueryStorageReadProvider);

/// Identifier of [`BigQueryStorageWriteProvider`].
pub const URN_BIGQUERY_STORAGE_WRITE: &str =
    "beam:schematransform:org.apache.beam:rust:bigquery_storage_write:v1";

/// The configuration of [`BigQueryStorageWriteProvider`]. The fields are fields of the Java
/// `BigQueryWriteConfiguration`. An empty string counts as unset.
#[derive(Clone, Debug, Default, PartialEq, BeamRow)]
pub struct BigQueryStorageWriteConfig {
    pub table: String,
    /// `CREATE_IF_NEEDED` or `CREATE_NEVER`.
    pub create_disposition: Option<String>,
    /// `WRITE_APPEND` or `WRITE_EMPTY`. The Storage Write API cannot replace rows, so
    /// `WRITE_TRUNCATE` fails.
    pub write_disposition: Option<String>,
    /// The flush interval of an exactly-once write of an unbounded input. As in the Java
    /// provider, a bounded input and an at-least-once write ignore it.
    pub triggering_frequency_seconds: Option<i64>,
    pub use_at_least_once_semantics: Option<bool>,
    /// Shards an unbounded exactly-once write automatically, one shard for each DoFn copy. As
    /// in the Java provider, it is on unless `num_streams` is set, and `false` needs
    /// `num_streams`.
    pub auto_sharding: Option<bool>,
    /// `INT32`, as in the Java configuration. The expansion service accepts an `INT64` from
    /// Python when the value fits.
    pub num_streams: Option<i32>,
    pub kms_key: Option<String>,
    pub clustering_fields: Option<Vec<String>>,
    /// Writes only these input fields. Set at most one of `keep`, `drop` and `only`.
    pub keep: Option<Vec<String>>,
    /// Writes all input fields except these.
    pub drop: Option<Vec<String>>,
    /// Writes the fields of this input row field as the columns of the table.
    pub only: Option<String>,
    /// Does not write the fields that the table has no column for. Without it, such a field
    /// fails the write.
    pub ignore_unknown_values: Option<bool>,
    pub error_handling: Option<ErrorHandling>,
}

impl BigQueryStorageWriteConfig {
    /// Returns the write options of this configuration for an input that is `unbounded` or
    /// not, or why they are not valid.
    pub fn write_options(self, unbounded: bool) -> Result<WriteOptions, String> {
        let set = |value: Option<String>| value.filter(|s| !s.is_empty());
        if self.table.is_empty() {
            return Err("Set table".to_string());
        }
        let num_streams = self.num_streams.unwrap_or(0);
        match (self.auto_sharding, num_streams) {
            (Some(true), n) if n > 0 => {
                return Err("Set at most one of auto_sharding and num_streams".to_string());
            }
            (Some(false), 0) => {
                return Err("auto_sharding false needs num_streams".to_string());
            }
            _ => {}
        }
        let mut options = WriteOptions::new(self.table);
        if let Some(name) = set(self.create_disposition) {
            options = options.with_create_disposition(name.parse::<CreateDisposition>()?);
        }
        if let Some(name) = set(self.write_disposition) {
            options = options.with_write_disposition(name.parse::<WriteDisposition>()?);
        }
        if let Some(seconds) = self.triggering_frequency_seconds.filter(|_| unbounded) {
            let seconds = u64::try_from(seconds)
                .ok()
                .filter(|s| *s > 0)
                .ok_or_else(|| format!("triggering_frequency_seconds {seconds} is not positive"))?;
            options = options.with_triggering_frequency(Duration::from_secs(seconds));
        }
        if self.use_at_least_once_semantics == Some(true) {
            options = options.with_semantics(Semantics::AtLeastOnce);
        }
        if let Some(count) = self.num_streams {
            let count =
                u32::try_from(count).map_err(|_| format!("num_streams {count} is negative"))?;
            options = options.with_num_streams(count);
        }
        if let Some(key) = set(self.kms_key) {
            options = options.with_kms_key(key);
        }
        if let Some(fields) = self.clustering_fields.filter(|f| !f.is_empty()) {
            options = options.with_clustering_fields(fields);
        }
        if let Some(fields) = self.keep.filter(|f| !f.is_empty()) {
            options = options.with_keep(fields);
        }
        if let Some(fields) = self.drop.filter(|f| !f.is_empty()) {
            options = options.with_drop(fields);
        }
        if let Some(field) = set(self.only) {
            options = options.with_only(field);
        }
        if self.ignore_unknown_values == Some(true) {
            options = options.with_ignore_unknown_values(true);
        }
        options.validate().map_err(beam::Error::msg)?;
        Ok(options)
    }
}

/// Writes rows to a BigQuery table with the Storage Write API. See
/// [`BigQueryStorageWriteConfig`] for the configuration.
///
/// The rejected rows go to the output that `error_handling` names, with the fields
/// `failed_row` and `error_message`. Without `error_handling`, a rejected row fails the
/// pipeline, and the output `post_write` is empty. Callers that need an output, such as the
/// Java `External` transform, get it either way.
#[derive(Default)]
pub struct BigQueryStorageWriteProvider;

impl SchemaTransformProvider for BigQueryStorageWriteProvider {
    fn identifier(&self) -> &'static str {
        URN_BIGQUERY_STORAGE_WRITE
    }

    fn description(&self) -> &'static str {
        "Writes Beam rows to a BigQuery table with the Storage Write API in Arrow format, at least once or exactly once."
    }

    fn config_schema(&self) -> Schema {
        (**BigQueryStorageWriteConfig::beam_schema()).clone()
    }

    fn input_tags(&self) -> Vec<String> {
        vec!["input".to_string()]
    }

    fn build_transform(
        &self,
        config: Row,
        inputs: TransformInputs,
        pipeline: &mut Pipeline,
    ) -> beam::Result<HashMap<String, PCollectionId>> {
        let config = BigQueryStorageWriteConfig::from_row(&config).map_err(beam::Error::msg)?;
        let output = config
            .error_handling
            .as_ref()
            .map(|handling| handling.output.clone())
            .filter(|output| !output.is_empty());
        let rows = inputs
            .get::<Row>(pipeline, "input")
            .map_err(beam::Error::msg)?;
        let options = config
            .write_options(is_unbounded(&rows))
            .map_err(beam::Error::msg)?;
        let errors = rows
            .try_apply("WriteToBigQuery", WriteRows::new(options))
            .map_err(beam::Error::msg)?;
        match output {
            Some(output) => Ok(HashMap::from([(output, errors.id().into())])),
            None => {
                let done = errors.apply("FailOnError", ParDo::new(FailOnErrorFn));
                Ok(HashMap::from([(
                    "post_write".to_string(),
                    done.id().into(),
                )]))
            }
        }
    }
}

/// Fails the bundle on the first rejected row, so its output has no elements.
#[derive(Clone)]
struct FailOnErrorFn;

impl DoFn for FailOnErrorFn {
    type In = Row;
    type Out = i32;

    fn process_element(&mut self, row: Row, _ctx: &mut ProcessContext<'_, i32>) -> beam::Result {
        let reason = row
            .get_string("error_message")
            .ok()
            .flatten()
            .unwrap_or_default();
        Err(beam::Error::msg(format!(
            "BigQuery rejected a row, and the write has no error_handling output: {reason}"
        )))
    }
}

register_schema_transform!(BigQueryStorageWriteProvider);
