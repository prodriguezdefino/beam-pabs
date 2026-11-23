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

//! The batch read: [`Read`] for a `#[derive(BeamRow)]` struct and [`ReadRows`] for rows with a
//! row schema.
//!
//! A planning step lists the data files of one snapshot after the impulse. A shuffle spreads
//! the files, and a splittable step reads the row groups of each file.

pub(crate) mod options;
#[doc(hidden)]
pub mod plan;
#[doc(hidden)]
pub mod reader;
mod row_group_cache;
mod task;

use std::marker::PhantomData;
use std::sync::Arc;

use beam::coders::DefaultCoder;
use beam::internals::PipelineInternals;
use beam::pipeline::Pipeline;
use beam::schema::{BeamRow, Row, RowElement, Schema};
use beam::transforms::display_data::{DisplayDataBuilder, HasDisplayData};
use beam::transforms::sdf::SplittableParDo;
use beam::transforms::{Impulse, PTransform, ParDo, Reshuffle, TryPTransform, WithFailures};
use beam::values::{IsBounded, PBegin, PCollection};
use beam_io_arrow::{BeamRowCodec, FieldSelection, RowCodec, RowOutput, SchemaRowCodec};

use self::options::ReadOptions;
use self::plan::{PlanTasksFn, row_schema};
use self::reader::{ReadTasksFn, WithErrors, read_failure_schema};
use crate::catalog::{CatalogClient, TableFileIo};

/// The catalog that a read plans with: the client that the program gives, or the REST
/// catalog of the options.
fn client(options: &ReadOptions, client: Option<&Arc<dyn iceberg::Catalog>>) -> CatalogClient {
    match client {
        Some(client) => CatalogClient::Given(Arc::clone(client)),
        None => CatalogClient::Rest(options.catalog.clone()),
    }
}

/// The steps up to the shuffled scan tasks, and the file IO of the reading step.
fn plan(
    input: &PBegin,
    client: CatalogClient,
    options: &ReadOptions,
    columns: FieldSelection,
) -> beam::Result<(PCollection<Vec<u8>>, TableFileIo)> {
    options.check()?;
    let file_io = TableFileIo::new(options.catalog.clone(), options.table_ident()?);
    let tasks = input
        .pipeline()
        .apply("Impulse", Impulse::new())
        .apply(
            "PlanTasks",
            ParDo::new(PlanTasksFn::new(client, options.clone(), columns)),
        )
        .apply("Reshuffle", Reshuffle::new());
    Ok((tasks, file_io))
}

/// The row schema of the read, from `schema` or else from the catalog.
fn read_rows_schema(
    client: &CatalogClient,
    options: &ReadOptions,
    schema: Option<&Arc<Schema>>,
) -> beam::Result<Arc<Schema>> {
    match schema {
        Some(schema) => Ok(Arc::clone(schema)),
        None => beam::internals::block_on_io(async {
            row_schema(client.catalog().await?.as_ref(), options, &options.columns).await
        })?
        .map(Arc::new),
    }
}

/// Records `error` as a construction error, and returns an output with no producer, so that
/// the steps after the read still build.
fn invalid_rows(pipeline: &Pipeline, error: beam::Error) -> PCollection<Row> {
    let coder_id = pipeline.register_row_coder(&Schema::new(Vec::new()));
    pipeline.invalid_construction_output(error, &coder_id)
}

fn empty_failures(pipeline: &Pipeline) -> PCollection<Row> {
    pipeline.add_pcollection(
        &format!("{}.failures", pipeline.scope_name().unwrap_or_default()),
        &pipeline.register_row_coder(read_failure_schema()),
        IsBounded::Bounded,
    )
}

/// Reads one snapshot of an Iceberg table into values of a `#[derive(BeamRow)]` struct. With
/// no column selection, the read takes the columns with the names of the fields of `T`.
pub struct Read<T> {
    options: ReadOptions,
    client: Option<Arc<dyn iceberg::Catalog>>,
    _marker: PhantomData<fn() -> T>,
}

impl<T> Clone for Read<T> {
    fn clone(&self) -> Self {
        Self {
            options: self.options.clone(),
            client: self.client.clone(),
            _marker: PhantomData,
        }
    }
}

impl<T> Read<T> {
    pub fn new(options: ReadOptions) -> Self {
        Self {
            options,
            client: None,
            _marker: PhantomData,
        }
    }

    /// Plans with `client` instead of the REST catalog of the options. The step holds the
    /// client in memory, so use it only with a runner that runs the workers in this process,
    /// for example Prism in loopback mode.
    pub fn with_client(mut self, client: Arc<dyn iceberg::Catalog>) -> Self {
        self.client = Some(client);
        self
    }

    /// Sends the rows that do not convert to a failure output with [`read_failure_schema`],
    /// instead of failing the bundle.
    pub fn with_failures(self) -> ReadWithFailures<T> {
        ReadWithFailures { read: self }
    }
}

impl<T: BeamRow> Read<T> {
    fn expand_tasks(&self, input: &PBegin) -> beam::Result<(PCollection<Vec<u8>>, TableFileIo)> {
        let columns = match &self.options.columns {
            FieldSelection::All => FieldSelection::Keep(
                T::beam_schema()
                    .fields
                    .iter()
                    .map(|f| f.name.clone())
                    .collect(),
            ),
            columns => columns.clone(),
        };
        let client = client(&self.options, self.client.as_ref());
        plan(input, client, &self.options, columns)
    }
}

impl<T> HasDisplayData for Read<T> {
    fn populate_display_data(&self, builder: &mut DisplayDataBuilder) {
        self.options.populate_display_data(builder);
    }
}

impl<T: BeamRow + DefaultCoder + Send + Sync + 'static> Read<T> {
    fn try_expand_ref(&self, input: &PBegin) -> beam::Result<PCollection<T>> {
        input.pipeline().record_display_data(self);
        let (tasks, file_io) = self.expand_tasks(input)?;
        let read = ReadTasksFn::new(file_io, self.options.batch_size, BeamRowCodec::<T>::new());
        Ok(tasks.apply("ReadTasks", SplittableParDo::new(read)))
    }
}

impl<T: BeamRow + DefaultCoder + Send + Sync + 'static> TryPTransform<PBegin> for Read<T> {
    type Output = PCollection<T>;
    type Error = beam::Error;

    fn try_expand(self, input: &PBegin) -> beam::Result<PCollection<T>> {
        self.try_expand_ref(input)
    }
}

impl<T: BeamRow + DefaultCoder + Send + Sync + 'static> PTransform<PBegin> for Read<T> {
    type Output = PCollection<T>;

    fn expand(self, input: &PBegin) -> PCollection<T> {
        self.try_expand_ref(input).unwrap_or_else(|error| {
            let pipeline = input.pipeline();
            pipeline.invalid_construction_output(error, &T::register_coder(pipeline))
        })
    }
}

/// A [`Read`] that sends the rows that do not convert to a failure output.
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

impl<T> HasDisplayData for ReadWithFailures<T> {
    fn populate_display_data(&self, builder: &mut DisplayDataBuilder) {
        self.read.options.populate_display_data(builder);
    }
}

impl<T: BeamRow + DefaultCoder + Send + Sync + 'static> ReadWithFailures<T> {
    fn try_expand_ref(&self, input: &PBegin) -> beam::Result<WithFailures<T, Row>> {
        input.pipeline().record_display_data(self);
        let (tasks, file_io) = self.read.expand_tasks(input)?;
        let read = ReadTasksFn::new(
            file_io,
            self.read.options.batch_size,
            WithErrors::<_, T>::new(BeamRowCodec::<T>::new()),
        );
        let WithFailures { output, failures } =
            tasks.apply("ReadTasks", SplittableParDo::new(read));
        Ok(WithFailures {
            output,
            failures: failures.with_row_schema(read_failure_schema()),
        })
    }
}

impl<T: BeamRow + DefaultCoder + Send + Sync + 'static> TryPTransform<PBegin>
    for ReadWithFailures<T>
{
    type Output = WithFailures<T, Row>;
    type Error = beam::Error;

    fn try_expand(self, input: &PBegin) -> beam::Result<WithFailures<T, Row>> {
        self.try_expand_ref(input)
    }
}

impl<T: BeamRow + DefaultCoder + Send + Sync + 'static> PTransform<PBegin> for ReadWithFailures<T> {
    type Output = WithFailures<T, Row>;

    fn expand(self, input: &PBegin) -> WithFailures<T, Row> {
        self.try_expand_ref(input).unwrap_or_else(|error| {
            let pipeline = input.pipeline();
            WithFailures {
                output: pipeline.invalid_construction_output(error, &T::register_coder(pipeline)),
                failures: empty_failures(pipeline),
            }
        })
    }
}

/// Reads one snapshot of an Iceberg table into rows. The row schema comes from the table when
/// the read expands, unless [`with_schema`](Self::with_schema) gives it.
#[derive(Clone)]
pub struct ReadRows {
    options: ReadOptions,
    schema: Option<Arc<Schema>>,
    client: Option<Arc<dyn iceberg::Catalog>>,
    rows: RowOutput,
}

impl ReadRows {
    pub fn new(options: ReadOptions) -> Self {
        Self {
            options,
            schema: None,
            client: None,
            rows: RowOutput::Values,
        }
    }

    /// Emits each row as its row coder bytes, with no `Row` between. Use it when the rows go
    /// to another SDK or across a fusion break.
    pub fn with_encoded_rows(mut self) -> Self {
        self.rows = RowOutput::Encoded;
        self
    }

    /// Reads rows of `schema`, with no catalog call when the read expands.
    pub fn with_schema(mut self, schema: impl Into<Arc<Schema>>) -> Self {
        self.schema = Some(schema.into());
        self
    }

    /// Plans with `client`, as [`Read::with_client`] does.
    pub fn with_client(mut self, client: Arc<dyn iceberg::Catalog>) -> Self {
        self.client = Some(client);
        self
    }

    /// Sends the rows that do not convert to a failure output with [`read_failure_schema`],
    /// instead of failing the bundle.
    pub fn with_failures(self) -> ReadRowsWithFailures {
        ReadRowsWithFailures { read: self }
    }

    /// The scan tasks, the file IO, and the codec of the rows.
    fn expand_tasks(
        &self,
        input: &PBegin,
    ) -> beam::Result<(PCollection<Vec<u8>>, TableFileIo, SchemaRowCodec)> {
        let client = client(&self.options, self.client.as_ref());
        let schema = read_rows_schema(&client, &self.options, self.schema.as_ref())?;
        let (tasks, file_io) = plan(input, client, &self.options, self.options.columns.clone())?;
        Ok((
            tasks,
            file_io,
            SchemaRowCodec::new(schema).with_output(self.rows),
        ))
    }

    fn try_expand_ref(&self, input: &PBegin) -> beam::Result<PCollection<Row>> {
        input.pipeline().record_display_data(self);
        let (tasks, file_io, codec) = self.expand_tasks(input)?;
        let schema = Arc::clone(codec.schema());
        let read = ReadTasksFn::new(file_io, self.options.batch_size, codec);
        Ok(tasks
            .apply("ReadTasks", SplittableParDo::new(read))
            .as_rows()
            .with_row_schema(&schema))
    }
}

impl HasDisplayData for ReadRows {
    fn populate_display_data(&self, builder: &mut DisplayDataBuilder) {
        self.options.populate_display_data(builder);
    }
}

impl TryPTransform<PBegin> for ReadRows {
    type Output = PCollection<Row>;
    type Error = beam::Error;

    fn try_expand(self, input: &PBegin) -> beam::Result<PCollection<Row>> {
        self.try_expand_ref(input)
    }
}

impl PTransform<PBegin> for ReadRows {
    type Output = PCollection<Row>;

    fn expand(self, input: &PBegin) -> PCollection<Row> {
        self.try_expand_ref(input)
            .unwrap_or_else(|error| invalid_rows(input.pipeline(), error))
    }
}

/// A [`ReadRows`] that sends the rows that do not convert to a failure output.
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
        let (tasks, file_io, codec) = self.read.expand_tasks(input)?;
        let schema = Arc::clone(codec.schema());
        let read = ReadTasksFn::new(
            file_io,
            self.read.options.batch_size,
            WithErrors::<_, RowElement>::new(codec),
        );
        let WithFailures { output, failures } =
            tasks.apply("ReadTasks", SplittableParDo::new(read));
        Ok(WithFailures {
            output: output.as_rows().with_row_schema(&schema),
            failures: failures.with_row_schema(read_failure_schema()),
        })
    }
}

impl TryPTransform<PBegin> for ReadRowsWithFailures {
    type Output = WithFailures<Row, Row>;
    type Error = beam::Error;

    fn try_expand(self, input: &PBegin) -> beam::Result<WithFailures<Row, Row>> {
        self.try_expand_ref(input)
    }
}

impl PTransform<PBegin> for ReadRowsWithFailures {
    type Output = WithFailures<Row, Row>;

    fn expand(self, input: &PBegin) -> WithFailures<Row, Row> {
        self.try_expand_ref(input).unwrap_or_else(|error| {
            let pipeline = input.pipeline();
            WithFailures {
                output: invalid_rows(pipeline, error),
                failures: empty_failures(pipeline),
            }
        })
    }
}
