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

//! Cloud Bigtable write connector (`MutateRows` batching, partial-failure retries, and
//! rejected-row failure output): [`Write`] for `#[derive(BeamRow)]` mutations and
//! [`WriteRows`] for [`Row`]s.

#[doc(hidden)]
pub mod batch;
#[doc(hidden)]
pub mod decode;
mod options;
#[doc(hidden)]
pub mod writer;

use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::Arc;

use beam::coders::DefaultCoder;
use beam::internals::{PipelineInternals, failed_row_schema};
use beam::pipeline::Pipeline;
use beam::schema::{BeamRow, Row, RowElement, Schema};
use beam::transforms::display_data::{DisplayDataBuilder, HasDisplayData};
use beam::transforms::{PTransform, ParDo, ToRows, TryPTransform};
use beam::values::{PCollection, POutput};
use beam::windowing::{GlobalWindows, WindowInto};

pub use self::batch::{
    DEFAULT_MAX_BYTES_PER_BATCH, DEFAULT_MAX_ELEMENTS_PER_BATCH, DEFAULT_MAX_IN_FLIGHT_BYTES,
    DEFAULT_MAX_IN_FLIGHT_REQUESTS,
};
pub use self::options::{DEFAULT_WRITE_ATTEMPT_TIMEOUT, WriteOptions};

use self::decode::MutationInputKind;
use self::writer::WriteMutationsFn;

use crate::bigtable::client::BigtableClient;

/// Returns the failure output [`Schema`] for rejected mutation rows of `input_schema`:
/// `(failed_row: ROW<input_schema>, error_message: STRING)`.
pub fn write_failure_schema(input_schema: &Schema) -> Schema {
    failed_row_schema(input_schema)
}

/// The outputs of a Bigtable write.
#[derive(Clone, Debug)]
pub struct WriteResult {
    /// Mutation rows that did not convert or that Bigtable rejected, with
    /// [`write_failure_schema`].
    pub failures: PCollection<Row>,
}

impl POutput for WriteResult {
    fn pipeline(&self) -> &Pipeline {
        self.failures.pipeline()
    }

    fn pcollection_ids(&self) -> HashMap<String, String> {
        HashMap::from([("failures".to_string(), self.failures.id().to_string())])
    }
}

fn expand_mutations(
    elements: &PCollection<RowElement>,
    options: &WriteOptions,
    client: Option<Arc<BigtableClient>>,
    kind: MutationInputKind,
    input_schema: Arc<Schema>,
) -> WriteResult {
    let err_schema = Arc::new(write_failure_schema(&input_schema));
    let failures = elements
        .apply(
            "WriteMutations",
            ParDo::new(WriteMutationsFn::new(
                Arc::new(options.clone()),
                kind,
                input_schema,
                Arc::clone(&err_schema),
                client,
            )),
        )
        .with_row_schema(&err_schema);
    WriteResult { failures }
}

/// Writes `#[derive(BeamRow)]` mutations (`mutation_schema` or `batched_mutation_schema`) to
/// Cloud Bigtable with `Bigtable.MutateRows` and returns [`WriteResult`].
///
/// A `SetCell` with a server-assigned timestamp (`timestamp_micros` unset or `-1`) is not
/// idempotent: a retry after an ambiguous failure can write a second cell version.
///
/// ```no_run
/// # use std::collections::HashMap;
/// # use beam::prelude::*;
/// # use beam::schema::BeamRow;
/// # use bytes::Bytes;
/// use beam_io_gcp::bigtable::{Write, WriteOptions};
///
/// #[derive(BeamRow, Clone, Debug)]
/// struct RowMutations {
///     key: Bytes,
///     mutations: Vec<HashMap<String, Bytes>>,
/// }
///
/// # fn f(mutations: PCollection<RowMutations>) {
/// let options = WriteOptions::from_table("my-project", "my-instance", "my-table");
/// let written = mutations.apply("WriteBigtable", Write::new(options));
/// // `written.failures` holds rejected mutation rows.
/// # }
/// ```
pub struct Write<T> {
    options: WriteOptions,
    client: Option<Arc<BigtableClient>>,
    _marker: PhantomData<fn(T)>,
}

impl<T> Write<T> {
    /// Creates a [`Write`] transform with `options`.
    pub fn new(options: WriteOptions) -> Self {
        Self {
            options,
            client: None,
            _marker: PhantomData,
        }
    }

    /// Uses `client` instead of opening a new connection on the worker.
    pub fn with_client(mut self, client: Arc<BigtableClient>) -> Self {
        self.client = Some(client);
        self
    }
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

impl<T> HasDisplayData for Write<T> {
    fn populate_display_data(&self, builder: &mut DisplayDataBuilder) {
        self.options.populate_display_data(builder);
    }
}

impl<T: BeamRow + DefaultCoder> Write<T> {
    fn try_expand_ref(&self, input: &PCollection<T>) -> beam::Result<WriteResult> {
        input.pipeline().record_display_data(self);
        self.options.validate()?;
        let input_schema = T::beam_schema();
        let kind = MutationInputKind::from_schema(&input_schema).map_err(beam::Error::msg)?;
        let windowed = input.apply("GlobalWindow", WindowInto::new(GlobalWindows));
        let elements = windowed
            .as_row_elements()
            .unwrap_or_else(|| windowed.apply("ToRows", ToRows::new()).as_row_elements());
        Ok(expand_mutations(
            &elements,
            &self.options,
            self.client.clone(),
            kind,
            input_schema,
        ))
    }
}

impl<T: BeamRow + DefaultCoder> TryPTransform<PCollection<T>> for Write<T> {
    type Output = WriteResult;
    type Error = beam::Error;

    fn try_expand(self, input: &PCollection<T>) -> beam::Result<WriteResult> {
        self.try_expand_ref(input)
    }
}

/// An invalid [`WriteOptions`] or input type `T` is a construction error:
/// [`Pipeline::validate`] returns it, so the pipeline does not run.
///
/// [`Pipeline::validate`]: beam::pipeline::Pipeline::validate
impl<T: BeamRow + DefaultCoder> PTransform<PCollection<T>> for Write<T> {
    type Output = WriteResult;

    fn expand(self, input: &PCollection<T>) -> WriteResult {
        self.try_expand_ref(input).unwrap_or_else(|e| {
            let pipeline = input.pipeline();
            let coder_id = pipeline.register_row_coder(&write_failure_schema(&T::beam_schema()));
            WriteResult {
                failures: pipeline.invalid_construction_output(e, &coder_id),
            }
        })
    }
}

/// Writes mutation [`Row`]s (`mutation_schema` or `batched_mutation_schema`) to Cloud
/// Bigtable with `Bigtable.MutateRows` and returns [`WriteResult`].
///
/// A `SetCell` with a server-assigned timestamp (`timestamp_micros` unset or `-1`) is not
/// idempotent: a retry after an ambiguous failure can write a second cell version.
#[derive(Clone)]
pub struct WriteRows {
    options: WriteOptions,
    client: Option<Arc<BigtableClient>>,
}

impl WriteRows {
    /// Creates a [`WriteRows`] transform with `options`.
    pub fn new(options: WriteOptions) -> Self {
        Self {
            options,
            client: None,
        }
    }

    /// Uses `client` instead of opening a new connection on the worker.
    pub fn with_client(mut self, client: Arc<BigtableClient>) -> Self {
        self.client = Some(client);
        self
    }
}

impl HasDisplayData for WriteRows {
    fn populate_display_data(&self, builder: &mut DisplayDataBuilder) {
        self.options.populate_display_data(builder);
    }
}

impl WriteRows {
    fn try_expand_ref(&self, input: &PCollection<Row>) -> beam::Result<WriteResult> {
        input.pipeline().record_display_data(self);
        self.options.validate()?;
        let step = input.pipeline().scope_name().unwrap_or_default();
        let input_schema = input
            .row_schema()
            .ok_or_else(|| beam::Error::msg(format!("{step}: the input has no row schema")))?;
        let kind = MutationInputKind::from_schema(&input_schema).map_err(beam::Error::msg)?;
        let elements = input
            .apply("GlobalWindow", WindowInto::new(GlobalWindows))
            .as_row_elements();
        Ok(expand_mutations(
            &elements,
            &self.options,
            self.client.clone(),
            kind,
            input_schema,
        ))
    }
}

impl TryPTransform<PCollection<Row>> for WriteRows {
    type Output = WriteResult;
    type Error = beam::Error;

    fn try_expand(self, input: &PCollection<Row>) -> beam::Result<WriteResult> {
        self.try_expand_ref(input)
    }
}

/// An invalid [`WriteOptions`] or input schema is a construction error:
/// [`Pipeline::validate`] returns it, so the pipeline does not run.
///
/// [`Pipeline::validate`]: beam::pipeline::Pipeline::validate
impl PTransform<PCollection<Row>> for WriteRows {
    type Output = WriteResult;

    fn expand(self, input: &PCollection<Row>) -> WriteResult {
        self.try_expand_ref(input).unwrap_or_else(|e| {
            // Without an input schema, the failure output carries an empty failed row schema.
            let input_schema = input
                .row_schema()
                .unwrap_or_else(|| Arc::new(Schema::new(Vec::new())));
            let pipeline = input.pipeline();
            let coder_id = pipeline.register_row_coder(&write_failure_schema(&input_schema));
            WriteResult {
                failures: pipeline.invalid_construction_output(e, &coder_id),
            }
        })
    }
}
