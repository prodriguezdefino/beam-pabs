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

//! Native Cloud Bigtable write connector (`MutateRows` batching, partial-failure retries,
//! and rejected-row error output).

pub mod batch;
pub mod decode;
pub mod options;
pub mod writer;

use std::sync::Arc;

use beam::internals::PipelineInternals;
use beam::schema::{Field, FieldType, Row, Schema};
use beam::transforms::{PTransform, ParDo, TryPTransform};
use beam::values::PCollection;
use beam::windowing::{GlobalWindows, WindowInto};

pub use self::batch::{
    BatchBuffer, BatchOutcome, BatchRequest, DEFAULT_MAX_BYTES_PER_BATCH,
    DEFAULT_MAX_ELEMENTS_PER_BATCH, DEFAULT_MAX_IN_FLIGHT_BYTES, DEFAULT_MAX_IN_FLIGHT_REQUESTS,
    InFlightLimit, partition_outcomes, rate_limit_backoff,
};
pub use self::decode::{MutationInputKind, PendingEntry, decode_mutation_row};
pub use self::options::{DEFAULT_WRITE_ATTEMPT_TIMEOUT, WriteOptions};
pub use self::writer::WriteMutationsFn;

use crate::bigtable::client::BigtableClient;
use crate::bigtable::construction_error_output;

/// Returns the error output [`Schema`] for rejected mutation rows of `input_schema`:
/// `(failed_row: ROW<input_schema>, error_message: STRING)`.
pub fn error_schema(input_schema: &Schema) -> Schema {
    Schema::new(vec![
        Field::new("failed_row", FieldType::row(input_schema.clone())),
        Field::new("error_message", FieldType::string()),
    ])
}

/// Native Cloud Bigtable `PTransform` that writes mutation [`Row`]s (`mutation_schema` or
/// `batched_mutation_schema`) using `Bigtable.MutateRows` and returns a [`PCollection<Row>`]
/// of rejected mutations with [`error_schema`].
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

impl TryPTransform<PCollection<Row>> for WriteMutations {
    type Error = beam::Error;

    fn try_expand(&self, input: &PCollection<Row>) -> beam::Result<PCollection<Row>> {
        self.options.validate().map_err(beam::Error::msg)?;
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
                    self.options.clone(),
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

/// An invalid [`WriteOptions`] or input schema is a construction error:
/// [`Pipeline::validate`] returns it, so the pipeline does not run.
///
/// [`Pipeline::validate`]: beam::pipeline::Pipeline::validate
impl PTransform<PCollection<Row>> for WriteMutations {
    type Output = PCollection<Row>;

    fn expand(&self, input: &PCollection<Row>) -> PCollection<Row> {
        self.try_expand(input).unwrap_or_else(|e| {
            // Without an input schema, the error output carries an empty failed row schema.
            let input_schema = input
                .row_schema()
                .unwrap_or_else(|| Arc::new(Schema::new(Vec::new())));
            let step = input.pipeline().scope_name().unwrap_or_default();
            construction_error_output(input.pipeline(), &step, e, &error_schema(&input_schema))
        })
    }
}
