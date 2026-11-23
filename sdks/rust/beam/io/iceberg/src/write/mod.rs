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

//! The public transforms of the write: [`Write`] for a `#[derive(BeamRow)]` struct and
//! [`WriteRows`] for rows with a row schema.
//!
//! A step prepares the table once. The rows convert to Arrow batches, a shuffle groups them
//! into shards, the writers write Parquet data files, and a last step commits the files.

#[doc(hidden)]
pub mod commit;
#[doc(hidden)]
pub mod encode;
#[doc(hidden)]
pub mod files;
#[doc(hidden)]
pub mod location;
pub(crate) mod options;
#[doc(hidden)]
pub mod parquet;
#[doc(hidden)]
pub mod schema;
#[doc(hidden)]
pub mod table;

use std::collections::HashMap;
use std::fmt::Display;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;

use beam::coders::{DefaultCoder, ShardedKey};
use beam::internals::PipelineInternals;
use beam::pipeline::Pipeline;
use beam::schema::{BeamRow, Row, RowElement, Schema};
use beam::transforms::display_data::{DisplayDataBuilder, HasDisplayData};
use beam::transforms::{
    DoFn, GroupIntoBatches, Impulse, PTransform, ParDo, ProcessContext, TryPTransform, WithFailures,
};
use beam::values::{IsBounded, PCollection, POutput};
use beam::windowing::{GlobalWindows, WindowInto};
use beam_io_arrow::{BeamRowCodec, EncodedBatch, RowCodec, SchemaRowCodec};
use bytes::Bytes;

use self::commit::{CommitFn, CommitTrigger, SnapshotInfo};
use self::encode::{DEFAULT_ENCODE_BATCH_BYTES, EncodeFn, write_failure_schema};
use self::files::WriteDataFilesFn;
use self::options::WriteOptions;
use self::parquet::DEFAULT_TARGET_FILE_BYTES;
use self::table::PrepareTableFn;
use crate::catalog::CatalogClient;

/// How often an unbounded write commits when the options set no triggering frequency.
pub const DEFAULT_TRIGGERING_FREQUENCY: Duration = Duration::from_secs(60);

/// The most Arrow IPC bytes in one group of a shard. The write step holds a full group in
/// memory, so this limits its peak memory. A file holds at most one group, so files are at
/// most this size before compression, also with a larger target file size.
const MAX_GROUP_BYTES: usize = 128 * 1024 * 1024;

/// The outputs of a write.
pub struct WriteResult {
    /// The elements that do not convert to the table schema, as rows
    /// `{failed_row, error_message}`.
    pub failures: PCollection<Row>,
    /// One element for each snapshot that the write commits.
    pub snapshots: PCollection<SnapshotInfo>,
}

impl POutput for WriteResult {
    fn pipeline(&self) -> &Pipeline {
        self.failures.pipeline()
    }

    fn pcollection_ids(&self) -> HashMap<String, String> {
        HashMap::from([
            ("failures".to_string(), self.failures.id().to_string()),
            ("snapshots".to_string(), self.snapshots.id().to_string()),
        ])
    }
}

/// Appends a `PCollection<T>` of a `#[derive(BeamRow)]` struct to an Iceberg table. The
/// table schema comes from the schema of `T`.
pub struct Write<T> {
    options: WriteOptions,
    client: Option<Arc<dyn iceberg::Catalog>>,
    _marker: PhantomData<fn(T)>,
}

impl<T> Clone for Write<T> {
    fn clone(&self) -> Self {
        Self {
            options: self.options.clone(),
            client: self.client.clone(),
            _marker: PhantomData,
        }
    }
}

impl<T> Write<T> {
    pub fn new(options: WriteOptions) -> Self {
        Self {
            options,
            client: None,
            _marker: PhantomData,
        }
    }

    /// Uses `client` for the catalog calls instead of the REST catalog of the options. The
    /// steps hold the client in memory, so use it only with a runner that runs the workers in
    /// this process, for example Prism in loopback mode.
    pub fn with_client(mut self, client: Arc<dyn iceberg::Catalog>) -> Self {
        self.client = Some(client);
        self
    }
}

impl<T> HasDisplayData for Write<T> {
    fn populate_display_data(&self, builder: &mut DisplayDataBuilder) {
        self.options.populate_display_data(builder);
    }
}

impl<T: BeamRow + DefaultCoder + Send + Sync + 'static> Write<T> {
    fn try_expand_ref(&self, input: &PCollection<T>) -> beam::Result<WriteResult> {
        input.pipeline().record_display_data(self);
        let codec: Arc<dyn RowCodec<T>> = Arc::new(BeamRowCodec::<T>::new());
        build(input, codec, &self.options, self.client.as_ref())
    }
}

impl<T: BeamRow + DefaultCoder + Send + Sync + 'static> TryPTransform<PCollection<T>> for Write<T> {
    type Output = WriteResult;
    type Error = beam::Error;

    fn try_expand(self, input: &PCollection<T>) -> beam::Result<WriteResult> {
        self.try_expand_ref(input)
    }
}

impl<T: BeamRow + DefaultCoder + Send + Sync + 'static> PTransform<PCollection<T>> for Write<T> {
    type Output = WriteResult;

    fn expand(self, input: &PCollection<T>) -> WriteResult {
        self.try_expand_ref(input)
            .unwrap_or_else(|error| invalid(input.pipeline(), &T::beam_schema(), error))
    }
}

/// Appends a `PCollection<Row>` with a row schema to an Iceberg table. The table schema comes
/// from the row schema. The rows go to Arrow from their coder bytes, with no `Row` decode.
#[derive(Clone)]
pub struct WriteRows {
    options: WriteOptions,
    client: Option<Arc<dyn iceberg::Catalog>>,
}

impl WriteRows {
    pub fn new(options: WriteOptions) -> Self {
        Self {
            options,
            client: None,
        }
    }

    /// Uses `client` for the catalog calls, as [`Write::with_client`] does.
    pub fn with_client(mut self, client: Arc<dyn iceberg::Catalog>) -> Self {
        self.client = Some(client);
        self
    }

    fn try_expand_ref(&self, input: &PCollection<Row>) -> beam::Result<WriteResult> {
        input.pipeline().record_display_data(self);
        let schema = input
            .row_schema()
            .ok_or_else(|| beam::Error::msg("The input of the write has no row schema"))?;
        let rows = input.as_row_elements();
        let codec: Arc<dyn RowCodec<RowElement>> = Arc::new(SchemaRowCodec::new(schema));
        build(&rows, codec, &self.options, self.client.as_ref())
    }
}

impl HasDisplayData for WriteRows {
    fn populate_display_data(&self, builder: &mut DisplayDataBuilder) {
        self.options.populate_display_data(builder);
    }
}

impl TryPTransform<PCollection<Row>> for WriteRows {
    type Output = WriteResult;
    type Error = beam::Error;

    fn try_expand(self, input: &PCollection<Row>) -> beam::Result<WriteResult> {
        self.try_expand_ref(input)
    }
}

impl PTransform<PCollection<Row>> for WriteRows {
    type Output = WriteResult;

    fn expand(self, input: &PCollection<Row>) -> WriteResult {
        self.try_expand_ref(input).unwrap_or_else(|error| {
            let schema = input
                .row_schema()
                .unwrap_or_else(|| Arc::new(Schema::new(Vec::new())));
            invalid(input.pipeline(), &schema, error)
        })
    }
}

fn build<T: DefaultCoder + Send + Sync + 'static>(
    input: &PCollection<T>,
    codec: Arc<dyn RowCodec<T>>,
    options: &WriteOptions,
    client: Option<&Arc<dyn iceberg::Catalog>>,
) -> beam::Result<WriteResult> {
    let client = match client {
        Some(client) => CatalogClient::Given(Arc::clone(client)),
        None => CatalogClient::Rest(options.catalog.clone()),
    };
    let schema = Arc::clone(codec.schema());
    options.validate(&schema)?;
    let written = Arc::new(options.written_schema(&schema)?);
    let unbounded = match input.is_bounded() {
        IsBounded::Unbounded => true,
        IsBounded::Bounded if options.triggering_frequency.is_some() => {
            return Err(beam::Error::msg(
                "A triggering frequency applies only to an unbounded input",
            ));
        }
        IsBounded::Bounded => false,
    };
    let frequency = options
        .triggering_frequency
        .unwrap_or(DEFAULT_TRIGGERING_FREQUENCY);
    let pipeline = input.pipeline();

    let table = pipeline
        .apply("Impulse", Impulse::new())
        .apply(
            "PrepareTable",
            ParDo::new(PrepareTableFn::new(
                client.clone(),
                options.clone(),
                written,
            )),
        )
        .as_singleton();
    let WithFailures {
        output: batches,
        failures,
    } = input
        .apply("GlobalWindow", WindowInto::new(GlobalWindows))
        .apply(
            "Encode",
            ParDo::new(EncodeFn::new(
                table.clone(),
                codec,
                options.fields.clone(),
                DEFAULT_ENCODE_BATCH_BYTES,
            )),
        );
    let group_bytes = options
        .target_file_bytes
        .unwrap_or(DEFAULT_TARGET_FILE_BYTES)
        .min(MAX_GROUP_BYTES);
    let grouping = GroupIntoBatches::new(usize::MAX)
        .with_byte_size_fn(group_bytes, |batch: &EncodedBatch| batch.bytes.len());
    let grouping = if unbounded {
        grouping.with_max_buffering_duration(frequency)
    } else {
        grouping
    };
    let grouped = match options.num_shards {
        Some(shards) => batches
            .apply("AssignShard", ParDo::new(AssignShardFn::new(shards)))
            .apply("GroupIntoBatches", grouping),
        None => batches.apply("GroupIntoBatches", grouping.with_sharded_key()),
    };
    let snapshots = grouped
        .apply(
            "WriteDataFiles",
            ParDo::new(WriteDataFilesFn::new(table.clone(), options)?),
        )
        .apply("CommitTrigger", CommitTrigger::new(frequency))
        .apply("Commit", ParDo::new(CommitFn::new(table, client, options)?));
    Ok(WriteResult {
        failures: failures.with_row_schema(&write_failure_schema(&schema)),
        snapshots,
    })
}

/// Records `error` as a construction error, and returns outputs with no producer, so that
/// the steps after the write still build. `Pipeline::validate` and `run` then fail.
fn invalid(pipeline: &Pipeline, schema: &Schema, error: impl Display) -> WriteResult {
    let snapshots =
        pipeline.invalid_construction_output(error, &SnapshotInfo::register_coder(pipeline));
    let failures_coder_id = pipeline.register_row_coder(&write_failure_schema(schema));
    let failures = pipeline.add_pcollection(
        &format!("{}.failures", pipeline.scope_name().unwrap_or_default()),
        &failures_coder_id,
        IsBounded::Bounded,
    );
    WriteResult {
        failures,
        snapshots,
    }
}

/// Puts the batches of each table on `shards` shard keys, round robin from a random start
/// for each copy, so the write has at most `shards` writers for each table.
#[derive(Clone)]
struct AssignShardFn {
    shards: usize,
    next: usize,
}

impl AssignShardFn {
    fn new(shards: usize) -> Self {
        Self { shards, next: 0 }
    }
}

impl DoFn for AssignShardFn {
    type In = (String, EncodedBatch);
    type Out = (ShardedKey<String>, EncodedBatch);

    fn setup(&mut self) -> beam::Result {
        let start = uuid::Uuid::new_v4().as_u128() % self.shards as u128;
        self.next = usize::try_from(start).unwrap_or_default();
        Ok(())
    }

    fn process_element(
        &mut self,
        (table, batch): Self::In,
        ctx: &mut ProcessContext<'_, Self::Out>,
    ) -> beam::Result {
        let shard = u32::try_from(self.next).unwrap_or(u32::MAX);
        self.next = (self.next + 1) % self.shards;
        let key = ShardedKey::new(table, Bytes::copy_from_slice(&shard.to_be_bytes()));
        ctx.emit((key, batch))
    }
}
