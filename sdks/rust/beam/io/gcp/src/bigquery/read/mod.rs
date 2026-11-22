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

//! BigQuery reads through the Storage Read API.
//!
//! [`Read<T>`] reads a table or query into values of a `#[derive(BeamRow)]` type, and
//! [`ReadRows`] reads into Beam [`Row`]s. One DoFn creates a read session, a reshuffle spreads
//! the streams of the session across workers, and a splittable DoFn reads each stream in Arrow
//! format. The restriction of a stream is `[offset, i64::MAX)`, because the server ends a
//! stream only when its rows are read. A checkpoint keeps the rows read so far, and the
//! residual reads the same stream from the next offset. Other splits call `SplitReadStream`:
//! the reader continues in the primary stream, and the residual reads the remainder stream.
//!
//! A query read first runs the query into a temporary table, and reads that table. When all
//! streams are read, a last DoFn deletes the temporary table.
//!
//! ```no_run
//! use beam::prelude::*;
//! use beam::schema::BeamRow;
//! use beam_io_gcp::bigquery::{ArrowCompression, Read, ReadOptions, ReadRows};
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
//! let rows = p.apply("ReadRows", ReadRows::new(options.clone()));
//! // Values of a type, reading only its columns.
//! let counts = p.apply("ReadCounts", Read::<WordCount>::new(options));
//! // The result of a query.
//! let top = p.apply(
//!     "ReadTop",
//!     ReadRows::new(
//!         ReadOptions::from_query(
//!             "SELECT word, SUM(word_count) AS word_count \
//!              FROM `bigquery-public-data.samples.shakespeare` GROUP BY word",
//!         )
//!         .with_billing_project("my-project"),
//!     ),
//! );
//! ```
//!
//! Column types map to Beam types as [`super::schema`] describes. `options` holds what to read,
//! `session` creates the read session, `query` runs the query jobs, [`restriction`] tracks and
//! splits one stream, `reader` reads it, and `failures` builds the failure output of
//! [`Read::with_failures`] and [`ReadRows::with_failures`].

#[doc(hidden)]
pub mod failures;
mod options;
mod query;
mod reader;
pub mod restriction;
mod session;

use std::marker::PhantomData;
use std::sync::Arc;

use beam::Instant;
use beam::coders::DefaultCoder;
use beam::internals::PipelineInternals;
use beam::pipeline::Pipeline;
use beam::schema::{BeamRow, Row, Schema};
use beam::transforms::display_data::{DisplayDataBuilder, HasDisplayData};
use beam::transforms::sdf::SplittableParDo;
use beam::transforms::{
    DoFn, FinishBundleContext, Impulse, PTransform, ParDo, ProcessContext, Reshuffle,
    TryPTransform, WithFailures,
};
use beam::values::{IsBounded, PBegin, PCollection};
use beam::windowing::{BoundedWindow, GlobalWindow};
use beam_io_arrow::{BeamRowCodec, RowOutput, SchemaRowCodec};
use google_cloud_bigquery::client::Read as StorageReadClient;
use google_cloud_bigquery_v2::client::{DatasetService, JobService, TableService};

pub use failures::read_failure_schema;
pub use options::{ArrowCompression, ReadOptions};
pub use query::QueryPriority;
pub use restriction::{StreamRestriction, StreamRestrictionCoder, StreamTracker};

use crate::bigquery::table::TableRef;
use crate::options::GcpOptions;
use failures::WithErrors;
use options::Source;
use query::{QueryClients, TempTable};
use reader::ReadStreamFn;
use session::{CreateQuerySessionFn, CreateSessionFn, DeleteTempTableFn, RunQueryFn};

/// Reads a BigQuery table or query through the Storage Read API into Beam rows.
///
/// The transform looks up the schema of the rows when it expands, unless
/// [`with_schema`](Self::with_schema) gives it. A worker builds the pipeline again, so it looks
/// the schema up too.
#[derive(Clone)]
pub struct ReadRows {
    options: ReadOptions,
    schema: Option<Arc<Schema>>,
    client: Option<StorageReadClient>,
    query_clients: Option<QueryClients>,
    rows: RowOutput,
}

/// The steps of a read up to the reshuffled streams, and what the later steps need.
struct Streams {
    schema: Arc<Schema>,
    streams: PCollection<(StreamRestriction, Vec<u8>)>,
    temp_table: Option<PCollection<TempTable>>,
    lineage_table: Option<[String; 3]>,
}

impl ReadRows {
    /// Reads the table or query in `options`.
    pub fn new(options: ReadOptions) -> Self {
        Self {
            options,
            schema: None,
            client: None,
            query_clients: None,
            rows: RowOutput::Values,
        }
    }

    /// Converts each Arrow row into its row coder bytes, with no [`Row`] between. Use it when
    /// the rows go to another SDK, or to a step that reads the bytes, such as
    /// [`WriteRows`](crate::bigquery::WriteRows). A Rust step that takes a [`Row`] decodes the
    /// bytes again.
    pub fn with_encoded_rows(mut self) -> Self {
        self.rows = RowOutput::Encoded;
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
    pub fn with_client(mut self, client: StorageReadClient) -> Self {
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

    /// Sends rows that do not convert to [`failures`](WithFailures::failures) with
    /// [`read_failure_schema`], and continues reading. A batch with such a row converts again
    /// one row at a time.
    pub fn with_failures(self) -> ReadRowsWithFailures {
        ReadRowsWithFailures { read: self }
    }

    /// Checks the options, gets the schema, and adds the steps up to the reshuffled streams.
    fn expand_streams(&self, input: &PBegin) -> beam::Result<Streams> {
        expand_streams(
            input,
            &self.options,
            self.schema.as_ref(),
            self.client.as_ref(),
            self.query_clients.as_ref(),
        )
    }
}

/// Checks `raw_options`, resolves `known_schema` (or looks up the table or query schema), and
/// adds the steps up to the reshuffled streams.
fn expand_streams(
    input: &PBegin,
    raw_options: &ReadOptions,
    known_schema: Option<&Arc<Schema>>,
    client: Option<&StorageReadClient>,
    query_clients: Option<&QueryClients>,
) -> beam::Result<Streams> {
    let pipeline = input.pipeline();
    let step = pipeline.scope_name().unwrap_or_default();
    let project = pipeline
        .options()
        .view_as::<GcpOptions>()
        .ok()
        .and_then(|gcp| gcp.project);
    let options = raw_options.clone().with_default_billing_project(project);
    options.validate().map_err(|e| e.context(&step))?;
    let lineage_table = match &options.source {
        Source::Table(spec) => TableRef::parse(spec)
            .and_then(|t| t.table_reference(options.billing_project.as_deref()))
            .ok()
            .map(|r| [r.project_id, r.dataset_id, r.table_id]),
        Source::Query(_) => None,
    };
    let schema = match known_schema {
        Some(schema) => Arc::clone(schema),
        None => Arc::new(
            options
                .schema_with(client.cloned(), query_clients.cloned())
                .map_err(|e| e.context(format!("{step}: schema lookup failed")))?,
        ),
    };
    let impulse = pipeline.apply("Impulse", Impulse::new());
    let read_client = client.cloned();
    let (streams, temp_table) = match options.source.clone() {
        Source::Table(table_spec) => {
            let streams = impulse.apply(
                "CreateSession",
                ParDo::new(CreateSessionFn {
                    table_spec,
                    options,
                    client: read_client,
                }),
            );
            (streams, None)
        }
        Source::Query(query) => {
            let temp_table = impulse.apply(
                "RunQuery",
                ParDo::new(RunQueryFn {
                    query,
                    options: options.clone(),
                    clients: query_clients.cloned(),
                }),
            );
            let streams = temp_table.apply(
                "CreateSession",
                ParDo::new(CreateQuerySessionFn {
                    options,
                    client: read_client,
                }),
            );
            (streams, Some(temp_table))
        }
    };
    let streams = streams.apply("Reshuffle", Reshuffle::new());
    Ok(Streams {
        schema,
        streams,
        temp_table,
        lineage_table,
    })
}

/// Deletes the temporary table of a query read after all `output` elements are read.
fn finish<E: DefaultCoder>(
    temp_table: Option<PCollection<TempTable>>,
    query_clients: Option<QueryClients>,
    output: &PCollection<E>,
) {
    if let Some(temp_table) = temp_table {
        let done = output
            .apply("OnStreamEnd", ParDo::new(OnStreamEndFn::<E>(PhantomData)))
            .as_iter();
        temp_table.apply(
            "DeleteTempTable",
            ParDo::new(DeleteTempTableFn {
                done,
                clients: query_clients,
            }),
        );
    }
}

impl HasDisplayData for ReadRows {
    fn populate_display_data(&self, builder: &mut DisplayDataBuilder) {
        self.options.populate_display_data(builder);
    }
}

/// Emits one item when its bundle finishes, so a side input over its output closes only after
/// every stream is read.
struct OnStreamEndFn<E>(PhantomData<fn(E)>);

impl<E> Clone for OnStreamEndFn<E> {
    fn clone(&self) -> Self {
        Self(PhantomData)
    }
}

impl<E: DefaultCoder> DoFn for OnStreamEndFn<E> {
    type In = E;
    type Out = i32;

    fn process_element(&mut self, _element: E, _ctx: &mut ProcessContext<'_, i32>) -> beam::Result {
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

impl ReadRows {
    fn try_expand_ref(&self, input: &PBegin) -> beam::Result<PCollection<Row>> {
        input.pipeline().record_display_data(self);
        let streams = self.expand_streams(input)?;
        let codec = SchemaRowCodec::new(Arc::clone(&streams.schema)).with_output(self.rows);
        let elements = streams.streams.apply(
            "ReadStreams",
            SplittableParDo::new(ReadStreamFn::new(
                codec,
                self.client.clone(),
                streams.lineage_table,
            )),
        );
        finish(streams.temp_table, self.query_clients.clone(), &elements);
        Ok(elements.as_rows().with_row_schema(&streams.schema))
    }
}

impl TryPTransform<PBegin> for ReadRows {
    type Output = PCollection<Row>;
    type Error = beam::Error;

    /// Fails if the options are invalid or the schema lookup fails. The first row that does not
    /// convert fails the pipeline.
    fn try_expand(self, input: &PBegin) -> beam::Result<PCollection<Row>> {
        self.try_expand_ref(input)
    }
}

/// An expansion error is a construction error: [`Pipeline::validate`] returns it, so the
/// pipeline does not run. Use `try_apply` to get the error as a value.
///
/// [`Pipeline::validate`]: beam::pipeline::Pipeline::validate
impl PTransform<PBegin> for ReadRows {
    type Output = PCollection<Row>;

    fn expand(self, input: &PBegin) -> PCollection<Row> {
        self.try_expand_ref(input)
            .unwrap_or_else(|e| invalid_rows(input.pipeline(), e))
    }
}

/// Records `error` as a construction error and returns rows with no producer and no fields.
/// A failed expansion has no table schema.
fn invalid_rows(pipeline: &Pipeline, error: beam::Error) -> PCollection<Row> {
    let coder_id = pipeline.register_row_coder(&Schema::new(Vec::new()));
    pipeline.invalid_construction_output(error, &coder_id)
}

/// A [`ReadRows`] that sends the rows that do not convert to [`failures`](WithFailures::failures).
/// See [`ReadRows::with_failures`].
#[derive(Clone)]
pub struct ReadRowsWithFailures {
    read: ReadRows,
}

impl HasDisplayData for ReadRowsWithFailures {
    fn populate_display_data(&self, builder: &mut DisplayDataBuilder) {
        self.read.options.populate_display_data(builder);
    }
}

impl ReadRowsWithFailures {
    fn try_expand_ref(&self, input: &PBegin) -> beam::Result<WithFailures<Row, Row>> {
        input.pipeline().record_display_data(self);
        let read = &self.read;
        let streams = read.expand_streams(input)?;
        let codec = SchemaRowCodec::new(Arc::clone(&streams.schema)).with_output(read.rows);
        let WithFailures { output, failures } = streams.streams.apply(
            "ReadStreams",
            SplittableParDo::new(ReadStreamFn::new(
                WithErrors::new(codec),
                read.client.clone(),
                streams.lineage_table,
            )),
        );
        finish(streams.temp_table, read.query_clients.clone(), &output);
        let output = output.as_rows().with_row_schema(&streams.schema);
        let failures = failures.with_row_schema(read_failure_schema());
        Ok(WithFailures { output, failures })
    }
}

fn empty_failures(pipeline: &Pipeline) -> PCollection<Row> {
    pipeline.add_pcollection(
        &format!("{}.failures", pipeline.scope_name().unwrap_or_default()),
        &pipeline.register_row_coder(read_failure_schema()),
        IsBounded::Bounded,
    )
}

impl TryPTransform<PBegin> for ReadRowsWithFailures {
    type Output = WithFailures<Row, Row>;
    type Error = beam::Error;

    /// Fails if the options are invalid or the schema lookup fails.
    fn try_expand(self, input: &PBegin) -> beam::Result<WithFailures<Row, Row>> {
        self.try_expand_ref(input)
    }
}

/// An expansion error is a construction error: [`Pipeline::validate`] returns it, so the
/// pipeline does not run. Use `try_apply` to get the error as a value.
///
/// [`Pipeline::validate`]: beam::pipeline::Pipeline::validate
impl PTransform<PBegin> for ReadRowsWithFailures {
    type Output = WithFailures<Row, Row>;

    fn expand(self, input: &PBegin) -> WithFailures<Row, Row> {
        self.try_expand_ref(input).unwrap_or_else(|e| {
            let pipeline = input.pipeline();
            WithFailures {
                output: invalid_rows(pipeline, e),
                failures: empty_failures(pipeline),
            }
        })
    }
}

/// Selects the fields of `schema` on a table read when `options` names no selected fields.
fn select_type_fields(options: &mut ReadOptions, schema: &Schema) {
    if matches!(options.source, Source::Table(_)) && options.selected_fields.is_empty() {
        options.selected_fields = schema.fields.iter().map(|f| f.name.clone()).collect();
    }
}

/// Reads a BigQuery table or query through the Storage Read API into values of a
/// `#[derive(BeamRow)]` type.
///
/// Columns match the fields of `T` by name. A table read selects only those columns unless
/// [`ReadOptions::with_selected_fields`] chose others, and skips the schema lookup.
pub struct Read<T> {
    options: ReadOptions,
    client: Option<StorageReadClient>,
    query_clients: Option<QueryClients>,
    _type: PhantomData<fn() -> T>,
}

impl<T> Read<T> {
    /// Reads the table or query in `options`.
    pub fn new(options: ReadOptions) -> Self {
        Self {
            options,
            client: None,
            query_clients: None,
            _type: PhantomData,
        }
    }

    /// Reads with `client` instead of a client with Application Default Credentials, as
    /// [`ReadRows::with_client`] does.
    pub fn with_client(mut self, client: StorageReadClient) -> Self {
        self.client = Some(client);
        self
    }

    /// Runs the jobs of a query read and manages its temporary dataset and table with these
    /// clients, as [`ReadRows::with_query_clients`] does.
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

    /// Sends rows that do not convert to [`failures`](WithFailures::failures) with
    /// [`read_failure_schema`], and continues reading. See [`ReadRows::with_failures`].
    pub fn with_failures(self) -> ReadWithFailures<T> {
        ReadWithFailures { read: self }
    }
}

impl<T: BeamRow> Read<T> {
    fn expand_streams(&self, input: &PBegin) -> beam::Result<Streams> {
        let schema = T::beam_schema();
        let mut options = self.options.clone();
        select_type_fields(&mut options, &schema);
        expand_streams(
            input,
            &options,
            Some(&schema),
            self.client.as_ref(),
            self.query_clients.as_ref(),
        )
    }
}

impl<T: BeamRow + DefaultCoder> Read<T> {
    fn try_expand_ref(&self, input: &PBegin) -> beam::Result<PCollection<T>> {
        input.pipeline().record_display_data(self);
        let streams = self.expand_streams(input)?;
        let output = streams.streams.apply(
            "ReadStreams",
            SplittableParDo::new(ReadStreamFn::new(
                BeamRowCodec::<T>::new(),
                self.client.clone(),
                streams.lineage_table,
            )),
        );
        finish(streams.temp_table, self.query_clients.clone(), &output);
        Ok(output)
    }
}

impl<T> Clone for Read<T> {
    fn clone(&self) -> Self {
        Self {
            options: self.options.clone(),
            client: self.client.clone(),
            query_clients: self.query_clients.clone(),
            _type: PhantomData,
        }
    }
}

impl<T: BeamRow> HasDisplayData for Read<T> {
    fn populate_display_data(&self, builder: &mut DisplayDataBuilder) {
        let mut options = self.options.clone();
        select_type_fields(&mut options, &T::beam_schema());
        options.populate_display_data(builder);
    }
}

impl<T: BeamRow + DefaultCoder> TryPTransform<PBegin> for Read<T> {
    type Output = PCollection<T>;
    type Error = beam::Error;

    /// Fails if the options are invalid. The first row that does not convert fails the
    /// pipeline.
    fn try_expand(self, input: &PBegin) -> beam::Result<PCollection<T>> {
        self.try_expand_ref(input)
    }
}

/// An expansion error is a construction error: [`Pipeline::validate`] returns it, so the
/// pipeline does not run. Use `try_apply` to get the error as a value.
///
/// [`Pipeline::validate`]: beam::pipeline::Pipeline::validate
impl<T: BeamRow + DefaultCoder> PTransform<PBegin> for Read<T> {
    type Output = PCollection<T>;

    fn expand(self, input: &PBegin) -> PCollection<T> {
        self.try_expand_ref(input).unwrap_or_else(|e| {
            let pipeline = input.pipeline();
            pipeline.invalid_construction_output(e, &T::register_coder(pipeline))
        })
    }
}

/// A [`Read`] that sends rows that do not convert to [`failures`](WithFailures::failures).
/// See [`Read::with_failures`].
pub struct ReadWithFailures<T> {
    read: Read<T>,
}

impl<T> Clone for ReadWithFailures<T> {
    fn clone(&self) -> Self {
        Self {
            read: self.read.clone(),
        }
    }
}

impl<T: BeamRow> HasDisplayData for ReadWithFailures<T> {
    fn populate_display_data(&self, builder: &mut DisplayDataBuilder) {
        self.read.populate_display_data(builder);
    }
}

impl<T: BeamRow + DefaultCoder> ReadWithFailures<T> {
    fn try_expand_ref(&self, input: &PBegin) -> beam::Result<WithFailures<T, Row>> {
        input.pipeline().record_display_data(self);
        let streams = self.read.expand_streams(input)?;
        let WithFailures { output, failures } = streams.streams.apply(
            "ReadStreams",
            SplittableParDo::new(ReadStreamFn::new(
                WithErrors::new(BeamRowCodec::<T>::new()),
                self.read.client.clone(),
                streams.lineage_table,
            )),
        );
        finish(streams.temp_table, self.read.query_clients.clone(), &output);
        Ok(WithFailures {
            output,
            failures: failures.with_row_schema(read_failure_schema()),
        })
    }
}

impl<T: BeamRow + DefaultCoder> TryPTransform<PBegin> for ReadWithFailures<T> {
    type Output = WithFailures<T, Row>;
    type Error = beam::Error;

    /// Fails if the options are invalid.
    fn try_expand(self, input: &PBegin) -> beam::Result<WithFailures<T, Row>> {
        self.try_expand_ref(input)
    }
}

/// An expansion error is a construction error: [`Pipeline::validate`] returns it, so the
/// pipeline does not run. Use `try_apply` to get the error as a value.
///
/// [`Pipeline::validate`]: beam::pipeline::Pipeline::validate
impl<T: BeamRow + DefaultCoder> PTransform<PBegin> for ReadWithFailures<T> {
    type Output = WithFailures<T, Row>;

    fn expand(self, input: &PBegin) -> WithFailures<T, Row> {
        self.try_expand_ref(input).unwrap_or_else(|e| {
            let pipeline = input.pipeline();
            WithFailures {
                output: pipeline.invalid_construction_output(e, &T::register_coder(pipeline)),
                failures: empty_failures(pipeline),
            }
        })
    }
}
