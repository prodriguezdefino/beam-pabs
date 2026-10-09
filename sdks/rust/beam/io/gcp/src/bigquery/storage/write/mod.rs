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

//! Native BigQuery writes through the Storage Write API.
//!
//! [`WriteRows`] writes Beam rows to one table in Arrow format. Before the first row, one step
//! creates or checks the table, and the writers read its column types as a side input. The
//! write has one of three shapes:
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
//! use gcp::bigquery::storage::write::{WriteOptions, WriteRows};
//!
//! # fn rows() -> PCollection<Row> { unimplemented!() }
//! let rows: PCollection<Row> = rows();
//! let errors = rows.apply(WriteRows::new(
//!     "WriteRows",
//!     WriteOptions::new("my-project.my_dataset.my_table"),
//! ));
//! ```

#[doc(hidden)]
pub mod append;
mod buffered;
mod client;
mod commit;
mod default_stream;
#[doc(hidden)]
pub mod encode;
mod pending;
mod table;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use beam::Instant;
use beam::schema::{Field, FieldType, Row, RowElement, Schema};
use beam::transforms::{
    FinishBundleContext, Flatten, GroupByKey, Outputs, PTransform, ParDo, WithFailures,
};
use beam::values::{PCollection, PCollectionList, SingletonView};
use beam::windowing::{BoundedWindow, GlobalWindow, GlobalWindows, WindowInto};
use google_cloud_bigquery::client::Write;
use google_cloud_bigquery_v2::client::TableService;

use crate::bigquery::storage::{add_composite, transform_ids};
use crate::bigquery::table::TableRef;
use crate::bigquery::{CreateDisposition, WriteDisposition};
use crate::options::GcpOptions;
use buffered::WriteBufferedFn;
use default_stream::WriteDefaultFn;
use encode::{EncodeFn, EncodedBatch, Sharding};
use pending::{CommitFn, WritePendingFn};
use table::{Destination, PrepareTableFn};

/// The output tag of the rows that a write step rejects.
const ERRORS_TAG: &str = "errors";

/// The flush interval of an unbounded exactly-once write when the options set none, as the
/// Java SDK default.
const DEFAULT_TRIGGERING_FREQUENCY: Duration = Duration::from_secs(5);

/// How long an append waits for its answer when the options set no time, as the Storage Write
/// client of the Java SDK waits for a request callback.
const DEFAULT_APPEND_TIMEOUT: Duration = Duration::from_secs(5 * 60);

/// How many times a row can arrive in the table.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Semantics {
    /// Each row arrives once. The write commits or flushes streams, and an unbounded write
    /// costs a shuffle.
    #[default]
    ExactlyOnce,
    /// A retried bundle can write its rows again. The write has no shuffle and the rows are
    /// visible at once.
    AtLeastOnce,
}

/// Where and how to write.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WriteOptions {
    table_spec: String,
    create_disposition: CreateDisposition,
    write_disposition: WriteDisposition,
    semantics: Semantics,
    triggering_frequency: Option<Duration>,
    num_streams: u32,
    append_timeout: Duration,
    kms_key: Option<String>,
    clustering_fields: Vec<String>,
}

impl WriteOptions {
    /// Writes to the table `table_spec` (`[project:]dataset.table` or `project.dataset.table`).
    /// A spec without a project uses the `--project` of the pipeline.
    pub fn new(table_spec: impl Into<String>) -> Self {
        Self {
            table_spec: table_spec.into(),
            create_disposition: CreateDisposition::CreateIfNeeded,
            write_disposition: WriteDisposition::WriteAppend,
            semantics: Semantics::ExactlyOnce,
            triggering_frequency: None,
            num_streams: 0,
            append_timeout: DEFAULT_APPEND_TIMEOUT,
            kms_key: None,
            clustering_fields: Vec::new(),
        }
    }

    /// The default is [`CreateDisposition::CreateIfNeeded`].
    pub fn with_create_disposition(mut self, disposition: CreateDisposition) -> Self {
        self.create_disposition = disposition;
        self
    }

    /// The default is [`WriteDisposition::WriteAppend`]. The Storage Write API cannot replace
    /// the rows of a table, so [`validate`](Self::validate) rejects
    /// [`WriteDisposition::WriteTruncate`].
    pub fn with_write_disposition(mut self, disposition: WriteDisposition) -> Self {
        self.write_disposition = disposition;
        self
    }

    /// The default is [`Semantics::ExactlyOnce`].
    pub fn with_semantics(mut self, semantics: Semantics) -> Self {
        self.semantics = semantics;
        self
    }

    /// How often an exactly-once write of an unbounded input makes its rows visible. Setting
    /// it also selects the unbounded shape for a bounded input, as in `BigQueryIO` of the Java
    /// SDK. The default is 5 seconds.
    pub fn with_triggering_frequency(mut self, frequency: Duration) -> Self {
        self.triggering_frequency = Some(frequency);
        self
    }

    /// The shards of an exactly-once write of an unbounded input, one buffered stream each.
    /// The default, `0`, shards automatically: one shard for each DoFn copy that converts rows,
    /// as the auto-sharding of the Java SDK gives one for each worker thread.
    pub fn with_num_streams(mut self, count: u32) -> Self {
        self.num_streams = count;
        self
    }

    /// How long an append waits for its answer. After this time, the writer opens a new
    /// connection and sends the append again. The default is 5 minutes.
    pub fn with_append_timeout(mut self, timeout: Duration) -> Self {
        self.append_timeout = timeout;
        self
    }

    /// The Cloud KMS key that encrypts a table that the write creates.
    pub fn with_kms_key(mut self, kms_key: impl Into<String>) -> Self {
        self.kms_key = Some(kms_key.into());
        self
    }

    /// The clustering columns of a table that the write creates.
    pub fn with_clustering_fields<I, S>(mut self, fields: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.clustering_fields = fields.into_iter().map(Into::into).collect();
        self
    }

    /// Checks the options without a call to BigQuery.
    pub fn validate(&self) -> beam::Result {
        TableRef::parse(&self.table_spec).map_err(beam::Error::from)?;
        match (self.write_disposition, self.triggering_frequency) {
            (WriteDisposition::WriteTruncate, _) => Err(beam::Error::msg(
                "The Storage Write API cannot replace the rows of a table: use WRITE_APPEND or WRITE_EMPTY",
            )),
            (_, Some(frequency)) if frequency.is_zero() => Err(beam::Error::msg(
                "The triggering frequency of a BigQuery write must be positive",
            )),
            _ if self.append_timeout.is_zero() => Err(beam::Error::msg(
                "The append timeout of a BigQuery write must be positive",
            )),
            _ => Ok(()),
        }
    }
}

/// The schema of the error output for rows of `schema`: the rejected row and the reason.
pub fn error_schema(schema: &Schema) -> Schema {
    Schema::new(vec![
        Field::new("failed_row", FieldType::row(schema.clone())),
        Field::new("error_message", FieldType::string()),
    ])
}

/// Writes Beam rows to a BigQuery table through the Storage Write API, and returns the rows
/// that the write rejected, with the schema of [`error_schema`].
#[derive(Clone)]
pub struct WriteRows {
    name: String,
    options: WriteOptions,
    client: Option<Arc<Write>>,
    tables: Option<TableService>,
}

impl WriteRows {
    pub fn new(name: impl Into<String>, options: WriteOptions) -> Self {
        Self {
            name: name.into(),
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

    /// Fallible form of [`PTransform::expand`]: fails if the options are invalid or the input
    /// has no row schema.
    pub fn try_expand(&self, input: &PCollection<Row>) -> beam::Result<PCollection<Row>> {
        let pipeline = input.pipeline();
        let options = &self.options;
        options
            .validate()
            .map_err(|e| e.context(self.name.clone()))?;
        let schema = input.row_schema().ok_or_else(|| {
            beam::Error::msg(format!("{}: the input has no row schema", self.name))
        })?;
        let project = pipeline
            .options()
            .view_as::<GcpOptions>()
            .ok()
            .and_then(|gcp| gcp.project);
        let table = TableRef::parse(&options.table_spec)
            .and_then(|table| {
                let resource = table.resource_name(project.as_deref())?;
                Ok((table, resource))
            })
            .map_err(|e| beam::Error::from(e).context(self.name.clone()))?;
        let errors = Arc::new(error_schema(&schema));
        let name = pipeline.unique_transform_name(&self.name);
        let existing = transform_ids(pipeline);

        let (_, impulse) = pipeline.add_impulse(&format!("{name}/Impulse"));
        let destination = impulse
            .apply(ParDo::new(
                format!("{name}/PrepareTable"),
                PrepareTableFn {
                    table: table.1,
                    schema: Arc::clone(&schema),
                    options: options.clone(),
                    tables: self.tables.clone(),
                },
            ))
            .as_singleton();
        // The writers read the row coder bytes of each element, with no row between.
        let rows = input
            .apply(WindowInto::new(
                format!("{name}/GlobalWindow"),
                GlobalWindows,
            ))
            .as_row_elements();
        let failed = match (options.semantics, self.unbounded(input)) {
            (Semantics::AtLeastOnce, _) => rows.apply(ParDo::new(
                format!("{name}/WriteDefaultStream"),
                WriteDefaultFn::new(
                    destination,
                    Arc::clone(&schema),
                    Arc::clone(&errors),
                    self.client.clone(),
                    options.append_timeout,
                ),
            )),
            (Semantics::ExactlyOnce, false) => {
                let WithFailures {
                    output: streams,
                    failures,
                } = rows.apply(ParDo::new(
                    format!("{name}/WritePendingStreams"),
                    WritePendingFn::new(
                        destination,
                        Arc::clone(&schema),
                        Arc::clone(&errors),
                        self.client.clone(),
                        options.append_timeout,
                    ),
                ));
                streams
                    .apply(GroupByKey::new(format!("{name}/GroupStreams")))
                    .apply(ParDo::new(format!("{name}/CommitStreams"), CommitFn));
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
                } = self.encode(&name, &rows, &destination, (&schema, &errors), sharding);
                let writer = WriteBufferedFn::new(
                    destination,
                    Arc::clone(&schema),
                    Arc::clone(&errors),
                    self.client.clone(),
                    options
                        .triggering_frequency
                        .unwrap_or(DEFAULT_TRIGGERING_FREQUENCY),
                    options.append_timeout,
                );
                let write_errors =
                    batches.apply(ParDo::new(format!("{name}/WriteBufferedStreams"), writer));
                PCollectionList::from_vec(pipeline.clone(), vec![encode_errors, write_errors])
                    .apply(Flatten::new(format!("{name}/Errors")))
            }
        }
        .with_row_schema(&errors);
        add_composite(
            pipeline,
            &name,
            &existing,
            HashMap::from([(ERRORS_TAG.to_string(), failed.id().to_string())]),
        );
        Ok(failed)
    }

    /// Whether the write takes the unbounded shape: the input is unbounded, the pipeline runs
    /// in streaming mode, or the options set a triggering frequency.
    fn unbounded(&self, input: &PCollection<Row>) -> bool {
        is_unbounded(input)
            || input.pipeline().options().streaming
            || self.options.triggering_frequency.is_some()
    }

    /// Converts rows into Arrow batches keyed by `sharding`, and returns the
    /// batches and the rows that do not convert.
    fn encode(
        &self,
        name: &str,
        rows: &PCollection<RowElement>,
        destination: &SingletonView<Destination>,
        (schema, errors): (&Arc<Schema>, &Arc<Schema>),
        sharding: Sharding,
    ) -> WithFailures<(i64, EncodedBatch), Row> {
        let encoder = EncodeFn::new(
            destination.clone(),
            Arc::clone(schema),
            Arc::clone(errors),
            sharding,
        );
        rows.apply(ParDo::new(format!("{name}/Encode"), encoder))
    }
}

/// Whether the graph marks `input` as unbounded. An expansion service gets this mark from the
/// caller.
pub fn is_unbounded<T: 'static>(input: &PCollection<T>) -> bool {
    input
        .pipeline()
        .lock()
        .components
        .pcollections
        .get(input.id())
        .is_some_and(|pcollection| {
            pcollection.is_bounded == model::pipeline::is_bounded::Enum::Unbounded as i32
        })
}

/// Emits `output` from `finish_bundle` at the end of the global window. The write rewindows
/// its input into the global window first.
fn emit_global<T: Outputs>(ctx: &mut FinishBundleContext<'_, T>, output: T) -> beam::Result {
    ctx.emit(
        output,
        BoundedWindow::Global(GlobalWindow),
        Instant::END_OF_GLOBAL_WINDOW,
    )
}

impl PTransform<PCollection<Row>> for WriteRows {
    type Output = PCollection<Row>;

    fn expand(&self, input: &PCollection<Row>) -> PCollection<Row> {
        self.try_expand(input)
            .unwrap_or_else(|e| panic!("{} could not be expanded: {e}", self.name))
    }
}
