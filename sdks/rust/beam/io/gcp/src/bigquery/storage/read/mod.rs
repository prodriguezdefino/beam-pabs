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
//! let rows = p.apply("ReadRows", ReadRows::new(options.clone()));
//! // Values of a type, reading only its columns.
//! let counts = p.apply("ReadCounts", ReadRows::new(options).typed::<WordCount>());
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
//! splits one stream, `reader` reads it, and `failures` makes the error output of
//! [`ReadRows::with_failures`].

#[doc(hidden)]
pub mod failures;
mod options;
mod query;
mod reader;
pub mod restriction;
mod session;

use arrow_io::RowOutput;
use std::marker::PhantomData;
use std::sync::Arc;

use beam::Instant;
use beam::coders::DefaultCoder;
use beam::internals::PipelineInternals;
use beam::schema::{BeamRow, Row, RowElement, Schema};
use beam::transforms::sdf::SplittableParDo;
use beam::transforms::{
    DoFn, FinishBundleContext, Impulse, PTransform, ParDo, ProcessContext, Reshuffle,
    TryPTransform, WithFailures,
};
use beam::values::{PBegin, PCollection};
use beam::windowing::{BoundedWindow, GlobalWindow};
use google_cloud_bigquery::client::Read;
use google_cloud_bigquery_v2::client::{DatasetService, JobService, TableService};

pub use failures::error_schema;
pub use options::{ArrowCompression, ReadOptions};
pub use query::QueryPriority;
pub use restriction::{StreamRestriction, StreamRestrictionCoder, StreamTracker};

use crate::options::GcpOptions;
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
    client: Option<Read>,
    query_clients: Option<QueryClients>,
    rows: RowOutput,
}

/// The steps of a read up to the reshuffled streams, and what the later steps need.
struct Streams {
    schema: Arc<Schema>,
    streams: PCollection<(StreamRestriction, Vec<u8>)>,
    temp_table: Option<PCollection<TempTable>>,
}

impl ReadRows {
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
    /// [`WriteRows`](crate::bigquery::storage::write::WriteRows). A Rust step that takes a
    /// [`Row`] decodes the bytes again.
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

    /// The same read, where a row that does not convert goes to
    /// [`failures`](WithFailures::failures) as a row of [`error_schema`], and the read
    /// continues. A batch with such a row converts again one row at a time.
    pub fn with_failures(self) -> ReadRowsWithFailures {
        ReadRowsWithFailures { read: self }
    }

    /// Checks the options, gets the schema, and adds the steps up to the reshuffled streams.
    fn expand_streams(&self, input: &PBegin) -> beam::Result<Streams> {
        let pipeline = input.pipeline();
        let step = pipeline.scope_name().unwrap_or_default();
        let project = pipeline
            .options()
            .view_as::<GcpOptions>()
            .ok()
            .and_then(|gcp| gcp.project);
        let options = self.options.clone().with_default_billing_project(project);
        options.validate().map_err(|e| e.context(&step))?;
        let schema = match &self.schema {
            Some(schema) => Arc::clone(schema),
            None => Arc::new(
                options
                    .schema_with(self.client.clone(), self.query_clients.clone())
                    .map_err(|e| e.context(format!("{step}: schema lookup failed")))?,
            ),
        };
        let impulse = pipeline.apply("Impulse", Impulse::new());
        let client = self.client.clone();
        let (streams, temp_table) = match options.source.clone() {
            Source::Table(table_spec) => {
                let streams = impulse.apply(
                    "CreateSession",
                    ParDo::new(CreateSessionFn {
                        table_spec,
                        options,
                        client,
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
                        clients: self.query_clients.clone(),
                    }),
                );
                let streams = temp_table.apply(
                    "CreateSession",
                    ParDo::new(CreateQuerySessionFn { options, client }),
                );
                (streams, Some(temp_table))
            }
        };
        let streams = streams.apply("Reshuffle", Reshuffle::new());
        Ok(Streams {
            schema,
            streams,
            temp_table,
        })
    }

    /// Deletes the temporary table of a query read after all `rows` are read.
    fn finish(&self, streams: Streams, rows: &PCollection<Row>) {
        if let Some(temp_table) = streams.temp_table {
            let done = rows
                .as_row_elements()
                .apply("OnStreamEnd", ParDo::new(OnStreamEndFn))
                .as_iter();
            temp_table.apply(
                "DeleteTempTable",
                ParDo::new(DeleteTempTableFn {
                    done,
                    clients: self.query_clients.clone(),
                }),
            );
        }
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

impl TryPTransform<PBegin> for ReadRows {
    type Error = beam::Error;

    /// Fails if the options are invalid or the schema lookup fails. The first row that does not
    /// convert fails the pipeline.
    fn try_expand(&self, input: &PBegin) -> beam::Result<PCollection<Row>> {
        let streams = self.expand_streams(input)?;
        let rows = streams
            .streams
            .apply(
                "ReadStreams",
                SplittableParDo::new(ReadStreamFn::<RowElement>::new(
                    Arc::clone(&streams.schema),
                    self.client.clone(),
                    self.rows,
                )),
            )
            .as_rows()
            .with_row_schema(&streams.schema);
        self.finish(streams, &rows);
        Ok(rows)
    }
}

impl PTransform<PBegin> for ReadRows {
    type Output = PCollection<Row>;

    fn expand(&self, input: &PBegin) -> PCollection<Row> {
        self.try_expand(input)
            .unwrap_or_else(|e| panic!("the transform could not be expanded: {e}"))
    }
}

/// A [`ReadRows`] that sends the rows that do not convert to [`failures`](WithFailures::failures).
/// See [`ReadRows::with_failures`].
#[derive(Clone)]
pub struct ReadRowsWithFailures {
    read: ReadRows,
}

impl TryPTransform<PBegin> for ReadRowsWithFailures {
    type Error = beam::Error;

    /// Fails if the options are invalid or the schema lookup fails.
    fn try_expand(&self, input: &PBegin) -> beam::Result<WithFailures<Row, Row>> {
        let read = &self.read;
        let streams = read.expand_streams(input)?;
        let WithFailures { output, failures } = streams.streams.apply(
            "ReadStreams",
            SplittableParDo::new(ReadStreamFn::<Result<RowElement, Row>>::new(
                Arc::clone(&streams.schema),
                read.client.clone(),
                read.rows,
            )),
        );
        let output = output.as_rows().with_row_schema(&streams.schema);
        let failures = failures.with_row_schema(error_schema());
        read.finish(streams, &output);
        Ok(WithFailures { output, failures })
    }
}

impl PTransform<PBegin> for ReadRowsWithFailures {
    type Output = WithFailures<Row, Row>;

    fn expand(&self, input: &PBegin) -> WithFailures<Row, Row> {
        self.try_expand(input)
            .unwrap_or_else(|e| panic!("the transform could not be expanded: {e}"))
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
        let rows = input.apply("ReadRows", self.rows.clone());
        rows.apply("FromRows", ParDo::new(FromRowFn::<T>(PhantomData)))
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
