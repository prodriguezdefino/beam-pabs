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

//! Native BigQuery reads through the Storage Read API.
//!
//! [`ReadRows`] reads a table or the result of a query into Beam rows. One DoFn creates a read
//! session, a reshuffle spreads the streams of the session across workers, and a splittable
//! DoFn reads each stream in Arrow format. The restriction of a stream is `[offset, i64::MAX)`,
//! because the server ends a stream only when its rows are read. A checkpoint keeps the rows
//! read so far, and the residual reads the same stream from the next offset. Other splits call
//! `SplitReadStream`: the reader continues in the primary stream, and the residual reads the
//! remainder stream.
//!
//! A query read first runs the query into a temporary table, and reads that table. When all
//! streams are read, a last DoFn deletes the temporary table.
//!
//! ```no_run
//! use beam::prelude::*;
//! use beam::schema::BeamRow;
//! use gcp::bigquery::storage::read::{ArrowCompression, ReadOptions, ReadRows};
//!
//! #[derive(Clone, Debug, BeamRow)]
//! struct WordCount {
//!     word: String,
//!     word_count: i64,
//! }
//!
//! let options = ReadOptions::new("bigquery-public-data.samples.shakespeare")
//!     .with_billing_project("my-project")
//!     .with_row_restriction("corpus = 'hamlet'")
//!     .with_compression(ArrowCompression::Lz4Frame);
//! let p = Pipeline::new();
//! // Rows, with the schema looked up when the transform expands.
//! let rows = p.apply(ReadRows::new("ReadRows", options.clone()));
//! // Values of a type, reading only its columns.
//! let counts = p.apply(ReadRows::new("ReadCounts", options).typed::<WordCount>());
//! // The result of a query.
//! let top = p.apply(ReadRows::new(
//!     "ReadTop",
//!     ReadOptions::from_query(
//!         "SELECT word, SUM(word_count) AS word_count \
//!          FROM `bigquery-public-data.samples.shakespeare` GROUP BY word",
//!     )
//!     .with_billing_project("my-project"),
//! ));
//! ```
//!
//! Column types map to Beam types as [`super::schema`] describes. `session` creates the read
//! session, `query` runs the query jobs, [`restriction`] tracks and splits one stream, and
//! `reader` reads it.

mod query;
mod reader;
pub mod restriction;
mod session;

use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::Arc;

use arrow_io::arrow_ipc::convert::try_schema_from_ipc_buffer;
use beam::Instant;
use beam::coders::DefaultCoder;
use beam::schema::{BeamRow, Row, RowElement, Schema};
use beam::transforms::sdf::SplittableParDo;
use beam::transforms::{DoFn, FinishBundleContext, PTransform, ParDo, ProcessContext, Reshuffle};
use beam::values::{PBegin, PCollection};
use beam::windowing::{BoundedWindow, GlobalWindow};
use google_cloud_bigquery::client::Read;
use google_cloud_bigquery_v2::client::{DatasetService, JobService, TableService};

pub use query::QueryPriority;
pub use restriction::{StreamRestriction, StreamRestrictionCoder, StreamTracker};

use crate::bigquery::storage::schema::beam_schema;
use crate::bigquery::storage::{add_composite, transform_ids};
use crate::options::GcpOptions;
use crate::runtime::block_on_async;
use query::{QueryClients, query_schema};
use reader::ReadStreamFn;
use session::{
    CreateQuerySessionFn, CreateSessionFn, DeleteTempTableFn, RunQueryFn, create_session,
    session_schema,
};

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
enum Source {
    /// A table spec: `[project:]dataset.table` or `project.dataset.table`.
    Table(String),
    /// The result of a query.
    Query(String),
}

/// What to read: a table or a query, with optional column, filter, query and session settings.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReadOptions {
    source: Source,
    row_restriction: Option<String>,
    selected_fields: Vec<String>,
    billing_project: Option<String>,
    max_stream_count: i32,
    compression: ArrowCompression,
    kms_key: Option<String>,
    query_priority: QueryPriority,
    query_location: Option<String>,
    temp_dataset: Option<String>,
    use_legacy_sql: bool,
    flatten_results: bool,
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
            billing_project: None,
            max_stream_count: 0,
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

    /// The project that pays for the read sessions and query jobs. [`ReadRows`] uses the
    /// `--project` of the pipeline when this is not set, and then the project of the table. A
    /// public table or a query read needs one of the first two.
    pub fn with_billing_project(mut self, project: impl Into<String>) -> Self {
        self.billing_project = Some(project.into());
        self
    }

    /// The most streams for the session. The default, `0`, lets the server choose.
    pub fn with_max_stream_count(mut self, count: i32) -> Self {
        self.max_stream_count = count.max(0);
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
    fn with_default_billing_project(mut self, project: Option<String>) -> Self {
        if self.billing_project.is_none() {
            self.billing_project = project;
        }
        self
    }

    /// Checks the options without a call to BigQuery. A query read fails if the query is empty,
    /// or if it sets `row_restriction` or `selected_fields`, which apply to table reads only.
    pub fn validate(&self) -> beam::Result {
        let Source::Query(query) = &self.source else {
            return Ok(());
        };
        if query.is_empty() {
            return Err(beam::Error::msg("The query of a BigQuery read is empty"));
        }
        if self.row_restriction.is_some() || !self.selected_fields.is_empty() {
            return Err(beam::Error::msg(
                "row_restriction and selected_fields apply to table reads only",
            ));
        }
        Ok(())
    }

    /// Returns the Beam schema of the rows that [`ReadRows`] reads with these options. A table
    /// read creates a read session with one stream. A query read runs a dry-run job.
    pub fn schema(&self) -> beam::Result<Schema> {
        self.schema_with(None, None)
    }

    fn schema_with(
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

/// Reads a BigQuery table or query through the Storage Read API into Beam rows.
///
/// The transform looks up the schema of the rows when it expands, unless
/// [`with_schema`](Self::with_schema) gives it. A worker builds the pipeline again, so it looks
/// the schema up too.
#[derive(Clone)]
pub struct ReadRows {
    name: String,
    options: ReadOptions,
    schema: Option<Arc<Schema>>,
    client: Option<Read>,
    query_clients: Option<QueryClients>,
    encoded: bool,
}

impl ReadRows {
    pub fn new(name: impl Into<String>, options: ReadOptions) -> Self {
        Self {
            name: name.into(),
            options,
            schema: None,
            client: None,
            query_clients: None,
            encoded: false,
        }
    }

    /// Converts each Arrow row into its row coder bytes, with no [`Row`] between. Use it when
    /// the rows go to another SDK, or to a step that reads the bytes, such as
    /// [`WriteRows`](crate::bigquery::storage::write::WriteRows). A Rust step that takes a
    /// [`Row`] decodes the bytes again.
    pub fn with_encoded_rows(mut self) -> Self {
        self.encoded = true;
        self
    }

    /// Reads rows of `schema`, usually from [`ReadOptions::schema`], and skips the lookup.
    pub fn with_schema(mut self, schema: impl Into<Arc<Schema>>) -> Self {
        self.schema = Some(schema.into());
        self
    }

    /// Reads with `client` instead of a client with Application Default Credentials, for
    /// example a client with other credentials or a test stub. A worker gets the client when
    /// it builds the pipeline again, so build it the same way on every machine.
    pub fn with_client(mut self, client: Read) -> Self {
        self.client = Some(client);
        self
    }

    /// Runs the jobs of a query read and manages its temporary dataset and table with these
    /// clients, instead of clients with Application Default Credentials. Build them the same
    /// way on every machine, as for [`with_client`](Self::with_client).
    pub fn with_query_clients(
        mut self,
        jobs: JobService,
        datasets: DatasetService,
        tables: TableService,
    ) -> Self {
        self.query_clients = Some(QueryClients {
            jobs,
            datasets,
            tables,
        });
        self
    }

    /// Reads values of a `#[derive(BeamRow)]` type instead of rows. The columns match the
    /// fields of `T` by name, and a table read selects only those columns unless
    /// [`ReadOptions::with_selected_fields`] chose others. No schema lookup is needed.
    pub fn typed<T: BeamRow + DefaultCoder>(mut self) -> ReadTyped<T> {
        let schema = T::beam_schema();
        let options = &mut self.options;
        if matches!(options.source, Source::Table(_)) && options.selected_fields.is_empty() {
            options.selected_fields = schema.fields.iter().map(|f| f.name.clone()).collect();
        }
        ReadTyped {
            rows: self.with_schema(Arc::clone(schema)),
            _type: PhantomData,
        }
    }

    /// Fallible form of [`PTransform::expand`]: fails if the options are invalid or the schema
    /// lookup fails.
    pub fn try_expand(&self, input: &PBegin) -> beam::Result<PCollection<Row>> {
        let pipeline = input.pipeline();
        let project = pipeline
            .options()
            .view_as::<GcpOptions>()
            .ok()
            .and_then(|gcp| gcp.project);
        let options = self.options.clone().with_default_billing_project(project);
        options
            .validate()
            .map_err(|e| e.context(self.name.clone()))?;
        let schema = match &self.schema {
            Some(schema) => Arc::clone(schema),
            None => Arc::new(
                options
                    .schema_with(self.client.clone(), self.query_clients.clone())
                    .map_err(|e| e.context(format!("{}: schema lookup failed", self.name)))?,
            ),
        };
        let name = pipeline.unique_transform_name(&self.name);
        let existing = transform_ids(pipeline);
        let (_, impulse) = pipeline.add_impulse(&format!("{name}/Impulse"));
        let client = self.client.clone();
        let (streams, temp_table) = match options.source.clone() {
            Source::Table(table_spec) => {
                let streams = impulse.apply(ParDo::new(
                    format!("{name}/CreateSession"),
                    CreateSessionFn {
                        table_spec,
                        options,
                        client,
                    },
                ));
                (streams, None)
            }
            Source::Query(query) => {
                let temp_table = impulse.apply(ParDo::new(
                    format!("{name}/RunQuery"),
                    RunQueryFn {
                        query,
                        options: options.clone(),
                        clients: self.query_clients.clone(),
                    },
                ));
                let streams = temp_table.apply(ParDo::new(
                    format!("{name}/CreateSession"),
                    CreateQuerySessionFn { options, client },
                ));
                (streams, Some(temp_table))
            }
        };
        let rows = streams
            .apply(Reshuffle::new(format!("{name}/Reshuffle")))
            .apply(SplittableParDo::new(
                format!("{name}/ReadStreams"),
                ReadStreamFn {
                    schema: Arc::clone(&schema),
                    client: self.client.clone(),
                    encoded: self.encoded,
                },
            ))
            .as_rows()
            .with_row_schema(&schema);
        if let Some(temp_table) = temp_table {
            let done = rows
                .as_row_elements()
                .apply(ParDo::new(format!("{name}/OnStreamEnd"), OnStreamEndFn))
                .as_iter();
            temp_table.apply(ParDo::new(
                format!("{name}/DeleteTempTable"),
                DeleteTempTableFn {
                    done,
                    clients: self.query_clients.clone(),
                },
            ));
        }
        add_composite(
            pipeline,
            &name,
            &existing,
            HashMap::from([("out".to_string(), rows.id().to_string())]),
        );
        Ok(rows)
    }
}

/// Emits one item when its bundle finishes, so a side input over its output closes only after
/// every stream is read.
#[derive(Clone)]
struct OnStreamEndFn;

impl DoFn for OnStreamEndFn {
    type In = RowElement;
    type Out = i32;

    fn process_element(
        &mut self,
        _row: RowElement,
        _ctx: &mut ProcessContext<'_, i32>,
    ) -> beam::Result {
        Ok(())
    }

    /// The streams are in the global window, so the item is too.
    fn finish_bundle(&mut self, ctx: &mut FinishBundleContext<'_, i32>) -> beam::Result {
        ctx.emit(
            0,
            BoundedWindow::Global(GlobalWindow),
            Instant::END_OF_GLOBAL_WINDOW,
        )
    }
}

impl PTransform<PBegin> for ReadRows {
    type Output = PCollection<Row>;

    fn expand(&self, input: &PBegin) -> PCollection<Row> {
        self.try_expand(input)
            .unwrap_or_else(|e| panic!("{} could not be expanded: {e}", self.name))
    }
}

/// Reads a BigQuery table or query into values of a `#[derive(BeamRow)]` type. See
/// [`ReadRows::typed`].
pub struct ReadTyped<T> {
    rows: ReadRows,
    _type: PhantomData<fn() -> T>,
}

impl<T> Clone for ReadTyped<T> {
    fn clone(&self) -> Self {
        Self {
            rows: self.rows.clone(),
            _type: PhantomData,
        }
    }
}

impl<T: BeamRow + DefaultCoder> PTransform<PBegin> for ReadTyped<T> {
    type Output = PCollection<T>;

    fn expand(&self, input: &PBegin) -> PCollection<T> {
        let pipeline = input.pipeline();
        let name = pipeline.unique_transform_name(&self.rows.name);
        let existing = transform_ids(pipeline);
        let rows = ReadRows {
            name: format!("{name}/ReadRows"),
            ..self.rows.clone()
        }
        .expand(input);
        let values = rows.apply(ParDo::new(
            format!("{name}/FromRows"),
            FromRowFn::<T>(PhantomData),
        ));
        add_composite(
            pipeline,
            &name,
            &existing,
            HashMap::from([("out".to_string(), values.id().to_string())]),
        );
        values
    }
}

/// Converts rows into values of `T`.
struct FromRowFn<T>(PhantomData<fn() -> T>);

impl<T> Clone for FromRowFn<T> {
    fn clone(&self) -> Self {
        Self(PhantomData)
    }
}

impl<T: BeamRow + DefaultCoder> DoFn for FromRowFn<T> {
    type In = Row;
    type Out = T;

    fn process_element(&mut self, row: Row, ctx: &mut ProcessContext<'_, T>) -> beam::Result {
        let value = T::from_row(&row).map_err(|e| {
            beam::Error::msg(format!("A BigQuery row does not convert to the type: {e}"))
        })?;
        ctx.emit(value)
    }
}
