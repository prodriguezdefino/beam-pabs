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

use std::collections::HashMap;
use std::fmt::Display;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;

use beam_io_arrow::{BeamRowCodec, EncodedBatch, RowCodec, SchemaRowCodec};
use beam::coders::{DefaultCoder, ShardedKey};
use beam::internals::PipelineInternals;
use beam::pipeline::Pipeline;
use beam::schema::{BeamRow, Row, RowElement, Schema};
use beam::transforms::{
    DoFn, GroupIntoBatches, Impulse, PTransform, ParDo, ProcessContext, WithFailures,
};
use beam::values::{IsBounded, PCollection, POutput};
use beam::windowing::{GlobalWindows, WindowInto};
use bytes::Bytes;

use crate::catalog::CatalogClient;
use crate::commit::{CommitFn, CommitTrigger, SnapshotInfo};
use crate::encode::{DEFAULT_ENCODE_BATCH_BYTES, EncodeFn, error_schema};
use crate::options::WriteOptions;
use crate::parquet::DEFAULT_TARGET_FILE_BYTES;
use crate::table::PrepareTableFn;
use crate::write::WriteDataFilesFn;

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
    pub errors: PCollection<Row>,
    /// One element for each snapshot that the write commits.
    pub snapshots: PCollection<SnapshotInfo>,
}

impl POutput for WriteResult {
    fn pipeline(&self) -> &Pipeline {
        self.errors.pipeline()
    }

    fn pcollection_ids(&self) -> HashMap<String, String> {
        HashMap::from([
            ("errors".to_string(), self.errors.id().to_string()),
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
    pub fn with_catalog_client(mut self, client: Arc<dyn iceberg::Catalog>) -> Self {
        self.client = Some(client);
        self
    }
}

impl<T: BeamRow + DefaultCoder + Send + Sync + 'static> PTransform<PCollection<T>> for Write<T> {
    type Output = WriteResult;

    fn expand(self, input: &PCollection<T>) -> WriteResult {
        let codec: Arc<dyn RowCodec<T>> = Arc::new(BeamRowCodec::<T>::new());
        expand(input, codec, &self.options, self.client.as_ref())
    }
}

/// Appends a `PCollection<Row>` with a row schema to an Iceberg table. The table schema comes
/// from the row schema. The rows go to Arrow from their coder bytes, with no `Row` decode.
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

    /// Uses `client` for the catalog calls, as [`Write::with_catalog_client`] does.
    pub fn with_catalog_client(mut self, client: Arc<dyn iceberg::Catalog>) -> Self {
        self.client = Some(client);
        self
    }
}

impl PTransform<PCollection<Row>> for WriteRows {
    type Output = WriteResult;

    fn expand(self, input: &PCollection<Row>) -> WriteResult {
        let rows = input.as_row_elements();
        match input.row_schema() {
            Some(schema) => {
                let codec: Arc<dyn RowCodec<RowElement>> = Arc::new(SchemaRowCodec::new(schema));
                expand(&rows, codec, &self.options, self.client.as_ref())
            }
            None => invalid(
                input.pipeline(),
                &Schema::new(Vec::new()),
                "The input of the write has no row schema",
            ),
        }
    }
}

/// The write graph, or outputs with no elements and a construction error if the options are
/// not valid.
fn expand<T: DefaultCoder + Send + Sync + 'static>(
    input: &PCollection<T>,
    codec: Arc<dyn RowCodec<T>>,
    options: &WriteOptions,
    client: Option<&Arc<dyn iceberg::Catalog>>,
) -> WriteResult {
    let schema = Arc::clone(codec.schema());
    let client = match client {
        Some(client) => CatalogClient::Given(Arc::clone(client)),
        None => CatalogClient::Rest(options.catalog.clone()),
    };
    match build(input, codec, options, client) {
        Ok(result) => result,
        Err(error) => invalid(input.pipeline(), &schema, error),
    }
}

fn build<T: DefaultCoder + Send + Sync + 'static>(
    input: &PCollection<T>,
    codec: Arc<dyn RowCodec<T>>,
    options: &WriteOptions,
    client: CatalogClient,
) -> beam::Result<WriteResult> {
    let schema = Arc::clone(codec.schema());
    options.validate(&schema)?;
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
                Arc::clone(&schema),
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
                DEFAULT_ENCODE_BATCH_BYTES,
            )),
        );
    let group_bytes = options
        .target_file_bytes
        .unwrap_or(DEFAULT_TARGET_FILE_BYTES)
        .min(MAX_GROUP_BYTES);
    let grouping = GroupIntoBatches::new(usize::MAX)
        .with_byte_size_fn(group_bytes, |batch: &EncodedBatch| batch.bytes.len());
    let grouping = match unbounded {
        true => grouping.with_max_buffering_duration(frequency),
        false => grouping,
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
        errors: failures.with_row_schema(&error_schema(&schema)),
        snapshots,
    })
}

/// Records `error` as a construction error, and returns outputs with no producer, so that
/// the steps after the write still build. `Pipeline::validate` and `run` then fail.
fn invalid(pipeline: &Pipeline, schema: &Schema, error: impl Display) -> WriteResult {
    let snapshots =
        pipeline.invalid_construction_output(error, &SnapshotInfo::register_coder(pipeline));
    let errors_coder_id = pipeline.register_row_coder(&error_schema(schema));
    let errors = pipeline.add_pcollection(
        &format!("{}.errors", pipeline.scope_name().unwrap_or_default()),
        &errors_coder_id,
        IsBounded::Bounded,
    );
    WriteResult { errors, snapshots }
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
