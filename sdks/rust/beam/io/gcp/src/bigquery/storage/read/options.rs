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

//! The options of a read: what it reads, and the session and query settings.

use arrow_io::arrow_ipc::convert::try_schema_from_ipc_buffer;
use beam::schema::Schema;
use google_cloud_bigquery::client::Read;
use google_cloud_wkt::Timestamp;

use super::query::{QueryClients, QueryPriority, query_schema};
use super::session::{create_session, session_schema};
use crate::bigquery::storage::schema::beam_schema;
use crate::runtime::block_on_async;

/// The compression of the Arrow buffers in read responses. Compression costs CPU on both sides,
/// and sends fewer bytes from BigQuery to the workers.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ArrowCompression {
    /// No compression.
    #[default]
    None,
    /// LZ4 frames: fast, with a lower ratio. The native Python read uses it.
    Lz4Frame,
    /// Zstandard: a higher ratio, with more CPU.
    Zstd,
}

impl ArrowCompression {
    /// The name in configurations: `NONE`, `LZ4_FRAME` or `ZSTD`.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::None => "NONE",
            Self::Lz4Frame => "LZ4_FRAME",
            Self::Zstd => "ZSTD",
        }
    }
}

impl std::str::FromStr for ArrowCompression {
    type Err = String;

    /// Parses a name of [`as_str`](Self::as_str), in any case.
    fn from_str(name: &str) -> Result<Self, String> {
        match name.to_ascii_uppercase().as_str() {
            "NONE" | "" => Ok(Self::None),
            "LZ4_FRAME" | "LZ4" => Ok(Self::Lz4Frame),
            "ZSTD" => Ok(Self::Zstd),
            _ => Err(format!(
                "Unknown compression '{name}': expected NONE, LZ4_FRAME or ZSTD"
            )),
        }
    }
}

/// What a read reads.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum Source {
    /// A table spec: `[project:]dataset.table` or `project.dataset.table`.
    Table(String),
    /// The result of a query.
    Query(String),
}

/// What to read: a table or a query, with optional column, filter, query and session settings.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReadOptions {
    pub(super) source: Source,
    pub(super) row_restriction: Option<String>,
    pub(super) selected_fields: Vec<String>,
    pub(super) snapshot_time: Option<String>,
    pub(super) billing_project: Option<String>,
    pub(super) max_stream_count: i32,
    pub(super) preferred_min_stream_count: i32,
    pub(super) compression: ArrowCompression,
    pub(super) kms_key: Option<String>,
    pub(super) query_priority: QueryPriority,
    pub(super) query_location: Option<String>,
    pub(super) temp_dataset: Option<String>,
    pub(super) use_legacy_sql: bool,
    pub(super) flatten_results: bool,
}

impl ReadOptions {
    /// Reads the table `table_spec` (`[project:]dataset.table` or `project.dataset.table`).
    pub fn new(table_spec: impl Into<String>) -> Self {
        Self::with_source(Source::Table(table_spec.into()))
    }

    /// Reads the result of `query`. The read runs the query into a temporary table, reads that
    /// table, and then deletes it.
    pub fn from_query(query: impl Into<String>) -> Self {
        Self::with_source(Source::Query(query.into()))
    }

    fn with_source(source: Source) -> Self {
        Self {
            source,
            row_restriction: None,
            selected_fields: Vec::new(),
            snapshot_time: None,
            billing_project: None,
            max_stream_count: 0,
            preferred_min_stream_count: 0,
            compression: ArrowCompression::None,
            kms_key: None,
            query_priority: QueryPriority::Batch,
            query_location: None,
            temp_dataset: None,
            use_legacy_sql: false,
            flatten_results: true,
        }
    }

    /// Reads only the rows that match `filter`, a GoogleSQL boolean expression. Table reads
    /// only.
    pub fn with_row_restriction(mut self, filter: impl Into<String>) -> Self {
        self.row_restriction = Some(filter.into());
        self
    }

    /// Reads only these columns. The default reads all columns. Table reads only.
    pub fn with_selected_fields<I, S>(mut self, fields: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.selected_fields = fields.into_iter().map(Into::into).collect();
        self
    }

    /// Reads the table as it was at `time`, an RFC 3339 timestamp such as
    /// `2024-05-01T12:30:00Z`. The default reads the table when the session starts. Table reads
    /// only.
    pub fn with_snapshot_time(mut self, time: impl Into<String>) -> Self {
        self.snapshot_time = Some(time.into());
        self
    }

    /// The project that pays for the read sessions and query jobs. [`ReadRows`](super::ReadRows)
    /// uses the `--project` of the pipeline when this is not set, and then the project of the
    /// table. A public table or a query read needs one of the first two.
    pub fn with_billing_project(mut self, project: impl Into<String>) -> Self {
        self.billing_project = Some(project.into());
        self
    }

    /// The most streams for the session. The default, `0`, lets the server choose.
    pub fn with_max_stream_count(mut self, count: i32) -> Self {
        self.max_stream_count = count.max(0);
        self
    }

    /// The fewest streams that the server tries to give the session. The server can give fewer.
    /// The default, `0`, lets the server choose.
    pub fn with_preferred_min_stream_count(mut self, count: i32) -> Self {
        self.preferred_min_stream_count = count.max(0);
        self
    }

    /// Compresses the Arrow buffers of the responses. The default is no compression.
    pub fn with_compression(mut self, compression: ArrowCompression) -> Self {
        self.compression = compression;
        self
    }

    /// The Cloud KMS key that encrypts the temporary table of a query read.
    pub fn with_kms_key(mut self, kms_key: impl Into<String>) -> Self {
        self.kms_key = Some(kms_key.into());
        self
    }

    /// The priority of the query job. The default is [`QueryPriority::Batch`].
    pub fn with_query_priority(mut self, priority: QueryPriority) -> Self {
        self.query_priority = priority;
        self
    }

    /// The location where the query runs. The default is the location of the first table that
    /// the query reads.
    pub fn with_query_location(mut self, location: impl Into<String>) -> Self {
        self.query_location = Some(location.into());
        self
    }

    /// The dataset (`[project:]dataset` or `project.dataset`) of the temporary table of a query
    /// read. By default, the read creates a temporary dataset and deletes it at the end.
    pub fn with_temp_dataset(mut self, dataset: impl Into<String>) -> Self {
        self.temp_dataset = Some(dataset.into());
        self
    }

    /// Runs the query as legacy SQL. The default is GoogleSQL.
    pub fn with_use_legacy_sql(mut self, use_legacy_sql: bool) -> Self {
        self.use_legacy_sql = use_legacy_sql;
        self
    }

    /// Flattens the repeated fields of legacy SQL results. The default is `true`.
    pub fn with_flatten_results(mut self, flatten_results: bool) -> Self {
        self.flatten_results = flatten_results;
        self
    }

    /// These options, with `project` as the billing project if none is set.
    pub(super) fn with_default_billing_project(mut self, project: Option<String>) -> Self {
        if self.billing_project.is_none() {
            self.billing_project = project;
        }
        self
    }

    /// The parsed snapshot time, if one is set.
    #[doc(hidden)]
    pub fn snapshot_timestamp(&self) -> beam::Result<Option<Timestamp>> {
        self.snapshot_time
            .as_deref()
            .map(|time| {
                Timestamp::try_from(time).map_err(|e| {
                    beam::Error::msg(format!("snapshot_time '{time}' is not RFC 3339: {e}"))
                })
            })
            .transpose()
    }

    /// Checks the options without a call to BigQuery. A query read fails if the query is empty,
    /// or if it sets `row_restriction`, `selected_fields` or `snapshot_time`, which apply to
    /// table reads only. A `preferred_min_stream_count` above a set `max_stream_count` fails.
    pub fn validate(&self) -> beam::Result {
        let (preferred, max) = (self.preferred_min_stream_count, self.max_stream_count);
        if max > 0 && preferred > max {
            return Err(beam::Error::msg(format!(
                "preferred_min_stream_count {preferred} is greater than max_stream_count {max}"
            )));
        }
        self.snapshot_timestamp()?;
        let Source::Query(query) = &self.source else {
            return Ok(());
        };
        if query.is_empty() {
            return Err(beam::Error::msg("The query of a BigQuery read is empty"));
        }
        if self.row_restriction.is_some()
            || !self.selected_fields.is_empty()
            || self.snapshot_time.is_some()
        {
            return Err(beam::Error::msg(
                "row_restriction, selected_fields and snapshot_time apply to table reads only",
            ));
        }
        Ok(())
    }

    /// Returns the Beam schema of the rows that [`ReadRows`](super::ReadRows) reads with these
    /// options. A table read creates a read session with one stream. A query read runs a
    /// dry-run job.
    pub fn schema(&self) -> beam::Result<Schema> {
        self.schema_with(None, None)
    }

    pub(super) fn schema_with(
        &self,
        client: Option<Read>,
        query_clients: Option<QueryClients>,
    ) -> beam::Result<Schema> {
        self.validate()?;
        let table_spec = match &self.source {
            Source::Query(query) => return query_schema(self, query, query_clients),
            Source::Table(spec) => spec.clone(),
        };
        let options = Self {
            max_stream_count: 1,
            preferred_min_stream_count: 0,
            ..self.clone()
        };
        let session =
            block_on_async(async move { create_session(&table_spec, &options, client).await })
                .map_err(|e| {
                    beam::Error::from(e).context("Failed to create a BigQuery read session")
                })?;
        let arrow_schema = try_schema_from_ipc_buffer(&session_schema(&session)?)
            .map_err(|e| beam::Error::msg(format!("Invalid Arrow schema from BigQuery: {e}")))?;
        beam_schema(&arrow_schema)
    }
}
