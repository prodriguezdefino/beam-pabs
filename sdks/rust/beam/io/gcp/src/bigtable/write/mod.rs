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

//! Cloud Bigtable write connector (`MutateRows` batching, partial-failure retries,
//! and rejected-row error output).

#[doc(hidden)]
pub mod batch;
#[doc(hidden)]
pub mod decode;
mod options;
#[doc(hidden)]
pub mod writer;

use std::sync::Arc;

use beam::internals::{PipelineInternals, failed_row_schema};
use beam::schema::{Row, Schema};
use beam::transforms::display_data::{DisplayDataBuilder, HasDisplayData};
use beam::transforms::{PTransform, ParDo, TryPTransform};
use beam::values::PCollection;
use beam::windowing::{GlobalWindows, WindowInto};

pub use self::batch::{
    DEFAULT_MAX_BYTES_PER_BATCH, DEFAULT_MAX_ELEMENTS_PER_BATCH, DEFAULT_MAX_IN_FLIGHT_BYTES,
    DEFAULT_MAX_IN_FLIGHT_REQUESTS,
};
pub use self::options::{DEFAULT_WRITE_ATTEMPT_TIMEOUT, WriteOptions};

use self::decode::MutationInputKind;
use self::writer::WriteMutationsFn;

use crate::bigtable::client::BigtableClient;

/// Returns the error output [`Schema`] for rejected mutation rows of `input_schema`:
/// `(failed_row: ROW<input_schema>, error_message: STRING)`.
pub fn error_schema(input_schema: &Schema) -> Schema {
    failed_row_schema(input_schema)
}

/// Cloud Bigtable `PTransform` that writes mutation [`Row`]s (`mutation_schema` or
/// `batched_mutation_schema`) using `Bigtable.MutateRows` and returns a [`PCollection<Row>`]
/// of rejected mutations with [`error_schema`].
///
/// A `SetCell` with a server-assigned timestamp (`timestamp_micros` unset or `-1`) is not
/// idempotent: a retry after an ambiguous failure can write a second cell version.
#[derive(Clone)]
pub struct WriteMutations {
    options: WriteOptions,
    client: Option<Arc<BigtableClient>>,
}

impl WriteMutations {
    /// Creates a new [`WriteMutations`] transform with `options`.
    pub fn new(options: WriteOptions) -> Self {
        Self {
            options,
            client: None,
        }
    }

    /// Uses a pre-configured [`BigtableClient`] (for example, in unit or integration tests).
    pub fn with_client(mut self, client: Arc<BigtableClient>) -> Self {
        self.client = Some(client);
        self
    }
}

impl HasDisplayData for WriteMutations {
    fn populate_display_data(&self, builder: &mut DisplayDataBuilder) {
        self.options.populate_display_data(builder);
    }
}

impl WriteMutations {
    fn try_expand_ref(&self, input: &PCollection<Row>) -> beam::Result<PCollection<Row>> {
        input.pipeline().record_display_data(self);
        self.options.validate()?;
        let step = input.pipeline().scope_name().unwrap_or_default();
        let input_schema = input
            .row_schema()
            .ok_or_else(|| beam::Error::msg(format!("{step}: the input has no row schema")))?;
        let kind = MutationInputKind::from_schema(&input_schema).map_err(beam::Error::msg)?;
        let err_schema = Arc::new(error_schema(&input_schema));

        let elements = input
            .apply("GlobalWindow", WindowInto::new(GlobalWindows))
            .as_row_elements();

        let failed = elements
            .apply(
                "WriteMutations",
                ParDo::new(WriteMutationsFn::new(
                    Arc::new(self.options.clone()),
                    kind,
                    Arc::clone(&input_schema),
                    Arc::clone(&err_schema),
                    self.client.clone(),
                )),
            )
            .with_row_schema(&err_schema);

        Ok(failed)
    }
}

impl TryPTransform<PCollection<Row>> for WriteMutations {
    type Output = PCollection<Row>;
    type Error = beam::Error;

    fn try_expand(self, input: &PCollection<Row>) -> beam::Result<PCollection<Row>> {
        self.try_expand_ref(input)
    }
}

/// An invalid [`WriteOptions`] or input schema is a construction error:
/// [`Pipeline::validate`] returns it, so the pipeline does not run.
///
/// [`Pipeline::validate`]: beam::pipeline::Pipeline::validate
impl PTransform<PCollection<Row>> for WriteMutations {
    type Output = PCollection<Row>;

    fn expand(self, input: &PCollection<Row>) -> PCollection<Row> {
        self.try_expand_ref(input).unwrap_or_else(|e| {
            // Without an input schema, the error output carries an empty failed row schema.
            let input_schema = input
                .row_schema()
                .unwrap_or_else(|| Arc::new(Schema::new(Vec::new())));
            let pipeline = input.pipeline();
            let coder_id = pipeline.register_row_coder(&error_schema(&input_schema));
            pipeline.invalid_construction_output(e, &coder_id)
        })
    }
}
