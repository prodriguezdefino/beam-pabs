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

//! BigQuery writes through the Storage Write API.
//!
//! [`WriteRows`] writes Beam rows to one table in Arrow format. Before the first row, one step
//! creates or checks the table, and the writers read its column types as a side input. The
//! semantics and the boundedness of the input select one of three shapes:
//!
//! | Guarantee | Input | Shape |
//! |---|---|---|
//! | At least once | any | Each bundle converts its rows and appends them to the default stream of the table, with no shuffle. |
//! | Exactly once | bounded | Each bundle converts its rows and appends them to its own pending stream, with no shuffle, and finalizes the stream. A last step groups the streams by table and commits them in one call. |
//! | Exactly once | unbounded | Rows become Arrow batches with a shard key, then a shuffle. A stateful step keeps a buffered stream and its next offset for each shard, and appends at that offset. A timer flushes the rows after the bundle commits. |
//!
//! A retried bundle does not write its rows twice:
//!
//! - In a bounded write, the stream of a failed bundle is never committed, so BigQuery drops
//!   its rows. The retry converts the rows again and writes them to a new stream.
//! - In an unbounded write, the shuffle before the stateful step is a checkpoint: a retried
//!   bundle reads the same batches. It finds its offset written, flushes the rows of the
//!   bundles that committed, then writes to a new stream. The rows of the failed attempt are
//!   never flushed.
//!
//! Rows that do not convert to the column types, or that BigQuery rejects, go to the error
//! output with the reason. The other rows of the same request are appended again.
//!
//! ```no_run
//! use beam::prelude::*;
//! use beam::schema::Row;
//! use beam_io_gcp::bigquery::write::{WriteOptions, WriteRows};
//!
//! # fn rows() -> PCollection<Row> { unimplemented!() }
//! let rows: PCollection<Row> = rows();
//! let errors = rows.apply(
//!     "WriteRows",
//!     WriteRows::new(WriteOptions::new("my-project.my_dataset.my_table")),
//! );
//! ```

#[doc(hidden)]
pub mod append;
mod buffer;
#[doc(hidden)]
pub mod buffered;
mod client;
#[doc(hidden)]
pub mod commit;
mod default_stream;
mod disposition;
#[doc(hidden)]
pub mod encode;
mod encoded_batch;
mod options;
mod pending;
#[doc(hidden)]
pub mod projection;
#[doc(hidden)]
pub mod table;

use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;

use beam::Instant;
use beam::coders::DefaultCoder;
use beam::internals::{PipelineInternals, failed_row_schema};
use beam::pipeline::Pipeline;
use beam::schema::{BeamRow, Row, RowElement, Schema};
use beam::transforms::display_data::{DisplayDataBuilder, HasDisplayData};
use beam::transforms::{
    BundleWindows, FinishBundleContext, Flatten, GroupByKey, Impulse, Outputs, PTransform, ParDo,
    ToRows, TryPTransform, WithFailures,
};
use beam::values::{IsBounded, PCollection, PCollectionList, SingletonView};
use beam::windowing::{BoundedWindow, GlobalWindow, GlobalWindows, WindowInto};
use google_cloud_bigquery::client::Write;
use google_cloud_bigquery_v2::client::TableService;

use crate::bigquery::table::TableRef;
use crate::options::GcpOptions;
use beam_io_arrow::EncodedBatch;
use buffered::WriteBufferedFn;
use default_stream::WriteDefaultFn;
use encode::{EncodeFn, Sharding};
use pending::{CommitFn, WritePendingFn};
use projection::Selection;
use table::{Destination, PrepareTableFn};

pub use disposition::{CreateDisposition, WriteDisposition};
pub use options::{Semantics, WriteOptions};

/// The flush interval of an unbounded exactly-once write when the options set none.
const DEFAULT_TRIGGERING_FREQUENCY: Duration = Duration::from_secs(5);

/// The schema of the error output for rows of `schema`: the rejected row and the reason.
pub fn error_schema(schema: &Schema) -> Schema {
    failed_row_schema(schema)
}

/// Writes Beam rows to a BigQuery table through the Storage Write API, and returns the rows
/// that the write rejected, with the schema of [`error_schema`].
#[derive(Clone)]
pub struct WriteRows {
    options: WriteOptions,
    client: Option<Arc<Write>>,
    tables: Option<TableService>,
}

impl WriteRows {
    pub fn new(options: WriteOptions) -> Self {
        Self {
            options,
            client: None,
            tables: None,
        }
    }

    /// Writes with `client` instead of a client with Application Default Credentials. A
    /// worker gets the client when it builds the pipeline again, so build it the same way on
    /// every machine. The commit of a bounded exactly-once write always uses Application
    /// Default Credentials.
    pub fn with_client(mut self, client: Write) -> Self {
        self.client = Some(Arc::new(client));
        self
    }

    /// Creates and checks the table with `tables`, as for [`with_client`](Self::with_client).
    pub fn with_table_client(mut self, tables: TableService) -> Self {
        self.tables = Some(tables);
        self
    }

    /// Writes values of a `#[derive(BeamRow)]` type instead of [`Row`]s, using the schema of `T`.
    pub fn typed<T: BeamRow + DefaultCoder>(self) -> WriteTyped<T> {
        WriteTyped {
            rows: self,
            _type: PhantomData,
        }
    }

    /// Converts rows into Arrow batches keyed by `sharding`, and returns the
    /// batches and the rows that do not convert.
    fn encode(
        &self,
        rows: &PCollection<RowElement>,
        destination: &SingletonView<Destination>,
        (selection, errors): (&Arc<Selection>, &Arc<Schema>),
        sharding: Sharding,
    ) -> WithFailures<(i64, EncodedBatch), Row> {
        let encoder = EncodeFn::new(
            destination.clone(),
            Arc::clone(selection),
            Arc::clone(errors),
            sharding,
        );
        rows.apply("Encode", ParDo::new(encoder))
    }
}

impl HasDisplayData for WriteRows {
    fn populate_display_data(&self, builder: &mut DisplayDataBuilder) {
        self.options.populate_display_data(builder);
    }
}

impl WriteRows {
    fn try_expand_ref(&self, input: &PCollection<Row>) -> beam::Result<PCollection<Row>> {
        let pipeline = input.pipeline();
        pipeline.record_display_data(self);
        let step = pipeline.scope_name().unwrap_or_default();
        let schema = input
            .row_schema()
            .ok_or_else(|| beam::Error::msg(format!("{step}: the input has no row schema")))?;
        self.try_expand_elements(pipeline, input.is_bounded(), schema, || {
            input
                .apply("GlobalWindow", WindowInto::new(GlobalWindows))
                .as_row_elements()
        })
    }

    fn try_expand_elements(
        &self,
        pipeline: &Pipeline,
        bounded: IsBounded,
        schema: Arc<Schema>,
        windowed_rows: impl FnOnce() -> PCollection<RowElement>,
    ) -> beam::Result<PCollection<Row>> {
        let step = pipeline.scope_name().unwrap_or_default();
        let options = &self.options;
        options.validate().map_err(|e| e.context(&step))?;
        let unbounded = match (bounded, options.triggering_frequency) {
            (IsBounded::Unbounded, _) => true,
            (IsBounded::Bounded, None) => false,
            (IsBounded::Bounded, Some(_)) => {
                return Err(beam::Error::msg(
                    "A triggering frequency applies only to an unbounded input",
                )
                .context(&step));
            }
        };
        let project = pipeline
            .options()
            .view_as::<GcpOptions>()
            .ok()
            .and_then(|gcp| gcp.project);
        let (reference, resource) = TableRef::parse(&options.table_spec)
            .and_then(|table| {
                Ok((
                    table.table_reference(project.as_deref())?,
                    table.resource_name(project.as_deref())?,
                ))
            })
            .map_err(|e| beam::Error::from(e).context(&step))?;
        let errors = Arc::new(error_schema(&schema));
        let selection = Arc::new(
            Selection::new(
                schema,
                &options.field_selection()?,
                options.ignore_unknown_values,
            )
            .map_err(|e| e.context(&step))?,
        );

        let impulse = pipeline.apply("Impulse", Impulse::new());
        let destination = impulse
            .apply(
                "PrepareTable",
                ParDo::new(PrepareTableFn {
                    reference,
                    table: resource,
                    schema: Arc::clone(selection.written()),
                    options: options.clone(),
                    tables: self.tables.clone(),
                }),
            )
            .as_singleton();
        // The writers read the row coder bytes of each element, with no row between.
        let rows = windowed_rows();
        let failed = match (options.semantics, unbounded) {
            (Semantics::AtLeastOnce, _) => rows.apply(
                "WriteDefaultStream",
                ParDo::new(WriteDefaultFn::new(
                    destination,
                    Arc::clone(&selection),
                    Arc::clone(&errors),
                    self.client.clone(),
                    options.append_timeout,
                )),
            ),
            (Semantics::ExactlyOnce, false) => {
                let WithFailures {
                    output: streams,
                    failures,
                } = rows.apply(
                    "WritePendingStreams",
                    ParDo::new(WritePendingFn::new(
                        destination,
                        Arc::clone(&selection),
                        Arc::clone(&errors),
                        self.client.clone(),
                        options.append_timeout,
                    )),
                );
                streams
                    .apply("GroupStreams", GroupByKey::new())
                    .apply("CommitStreams", ParDo::new(CommitFn));
                failures
            }
            (Semantics::ExactlyOnce, true) => {
                let sharding = match options.num_streams {
                    0 => Sharding::Auto,
                    count => Sharding::Fixed(count),
                };
                let WithFailures {
                    output: batches,
                    failures: encode_errors,
                } = self.encode(&rows, &destination, (&selection, &errors), sharding);
                let writer = WriteBufferedFn::new(
                    destination,
                    Arc::clone(&selection),
                    Arc::clone(&errors),
                    self.client.clone(),
                    options
                        .triggering_frequency
                        .unwrap_or(DEFAULT_TRIGGERING_FREQUENCY),
                    options.append_timeout,
                );
                let write_errors = batches.apply("WriteBufferedStreams", ParDo::new(writer));
                PCollectionList::from_vec(pipeline.clone(), vec![encode_errors, write_errors])
                    .apply("Errors", Flatten::new())
            }
        }
        .with_row_schema(&errors);
        Ok(failed)
    }
}

impl TryPTransform<PCollection<Row>> for WriteRows {
    type Output = PCollection<Row>;
    type Error = beam::Error;

    /// Fails if the options are invalid, the input has no row schema, or the options set a
    /// triggering frequency for a bounded input.
    fn try_expand(self, input: &PCollection<Row>) -> beam::Result<PCollection<Row>> {
        self.try_expand_ref(input)
    }
}

/// The window of every element of the write: it rewindows its input into the global window.
const GLOBAL: BoundedWindow = BoundedWindow::Global(GlobalWindow);

/// The timestamp of the outputs of `finish_bundle`: the earliest timestamp of the bundle.
fn finish_timestamp(windows: &mut BundleWindows) -> Instant {
    windows
        .take(&GLOBAL)
        .unwrap_or(Instant::END_OF_GLOBAL_WINDOW)
}

/// Emits `output` from `finish_bundle` in the global window at `timestamp`.
fn emit_global<T: Outputs>(
    ctx: &mut FinishBundleContext<'_, T>,
    output: T,
    timestamp: Instant,
) -> beam::Result {
    ctx.emit(output, GLOBAL, timestamp)
}

/// An expansion error is a construction error: [`Pipeline::validate`] returns it, so the
/// pipeline does not run. Use `try_apply` to get the error as a value.
///
/// [`Pipeline::validate`]: beam::pipeline::Pipeline::validate
impl PTransform<PCollection<Row>> for WriteRows {
    type Output = PCollection<Row>;

    fn expand(self, input: &PCollection<Row>) -> PCollection<Row> {
        self.try_expand_ref(input).unwrap_or_else(|e| {
            let schema = input
                .row_schema()
                .unwrap_or_else(|| Arc::new(Schema::new(Vec::new())));
            let pipeline = input.pipeline();
            let coder_id = pipeline.register_row_coder(&error_schema(&schema));
            pipeline.invalid_construction_output(e, &coder_id)
        })
    }
}

/// Writes values of a `#[derive(BeamRow)]` type to a BigQuery table through the Storage Write API.
/// See [`WriteRows::typed`].
pub struct WriteTyped<T> {
    rows: WriteRows,
    _type: PhantomData<fn(T)>,
}

impl<T> Clone for WriteTyped<T> {
    fn clone(&self) -> Self {
        Self {
            rows: self.rows.clone(),
            _type: PhantomData,
        }
    }
}

impl<T> HasDisplayData for WriteTyped<T> {
    fn populate_display_data(&self, builder: &mut DisplayDataBuilder) {
        self.rows.populate_display_data(builder);
    }
}

impl<T: BeamRow + DefaultCoder> WriteTyped<T> {
    fn try_expand_ref(&self, input: &PCollection<T>) -> beam::Result<PCollection<Row>> {
        let pipeline = input.pipeline();
        pipeline.record_display_data(self);
        let schema = T::beam_schema();
        self.rows
            .try_expand_elements(pipeline, input.is_bounded(), Arc::clone(&schema), || {
                let windowed = input.apply("GlobalWindow", WindowInto::new(GlobalWindows));
                windowed
                    .as_row_elements()
                    .unwrap_or_else(|| windowed.apply("ToRows", ToRows::new()).as_row_elements())
            })
    }
}

impl<T: BeamRow + DefaultCoder> TryPTransform<PCollection<T>> for WriteTyped<T> {
    type Output = PCollection<Row>;
    type Error = beam::Error;

    /// Fails if the options are invalid or set a triggering frequency for a bounded input.
    fn try_expand(self, input: &PCollection<T>) -> beam::Result<PCollection<Row>> {
        self.try_expand_ref(input)
    }
}

impl<T: BeamRow + DefaultCoder> PTransform<PCollection<T>> for WriteTyped<T> {
    type Output = PCollection<Row>;

    fn expand(self, input: &PCollection<T>) -> PCollection<Row> {
        self.try_expand_ref(input).unwrap_or_else(|e| {
            let pipeline = input.pipeline();
            let coder_id = pipeline.register_row_coder(&error_schema(&T::beam_schema()));
            pipeline.invalid_construction_output(e, &coder_id)
        })
    }
}
