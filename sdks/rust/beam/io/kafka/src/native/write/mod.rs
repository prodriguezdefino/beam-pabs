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

//! Writes rows to a Kafka topic, at least once.
//!
//! [`WriteRecords`] sends each row as the value of one record, with no key, in the
//! [`WriteFormat`] of its options. The rows need no shuffle: each bundle sends its rows and
//! waits until Kafka has all of them. A failed bundle is retried, so a record can be written
//! more than once.
//!
//! Rows that do not convert, and rows of records that Kafka rejects (for example a record
//! that is too large), go to the error output with the reason.
//!
//! ```no_run
//! use beam::prelude::*;
//! use beam::schema::Row;
//! use kafka_io::native::format::WriteFormat;
//! use kafka_io::native::write::{WriteOptions, WriteRecords};
//!
//! # fn rows() -> PCollection<Row> { unimplemented!() }
//! let errors = rows().apply(WriteRecords::new(
//!     "WriteEvents",
//!     WriteOptions::new("broker-1:9092", "events").with_format(WriteFormat::Json),
//! ));
//! ```

mod writer;

use std::collections::HashMap;
use std::sync::Arc;

use beam::Instant;
use beam::schema::{Field, FieldType, Row, Schema};
use beam::transforms::{FinishBundleContext, Outputs, PTransform, ParDo};
use beam::values::PCollection;
use beam::windowing::{BoundedWindow, GlobalWindow, GlobalWindows, WindowInto};

use super::client::{Auth, Connection};
use super::format::{Encoder, WriteFormat};
use super::{add_composite, transform_ids};
use writer::{Target, WriteFn};

/// The output tag of the rows that the write rejects.
const ERRORS_TAG: &str = "errors";

/// The schema of the error output for rows of `schema`: the rejected row and the reason.
pub fn error_schema(schema: &Schema) -> Schema {
    Schema::new(vec![
        Field::new("failed_row", FieldType::row(schema.clone())),
        Field::new("error_message", FieldType::string()),
    ])
}

/// The cluster, the topic and the record format of a write.
#[derive(Clone, Debug, PartialEq)]
pub struct WriteOptions {
    pub connection: Connection,
    pub topic: String,
    pub format: WriteFormat,
}

impl WriteOptions {
    pub fn new(bootstrap_servers: impl Into<String>, topic: impl Into<String>) -> Self {
        Self {
            connection: Connection::new(bootstrap_servers),
            topic: topic.into(),
            format: WriteFormat::default(),
        }
    }

    pub fn with_format(mut self, format: WriteFormat) -> Self {
        self.format = format;
        self
    }

    /// Sets the librdkafka producer properties. They replace the producer defaults
    /// `enable.idempotence=true` and `acks=all` when they set the same names.
    pub fn with_producer_properties(
        mut self,
        properties: impl IntoIterator<Item = (String, String)>,
    ) -> Self {
        self.connection.properties.extend(properties);
        self
    }

    /// Authenticates with Google Application Default Credentials, for Google Cloud Managed
    /// Service for Apache Kafka. See [`Auth::GoogleAdc`].
    pub fn with_google_auth(mut self) -> Self {
        self.connection.auth = Auth::GoogleAdc;
        self
    }

    /// Fails if the connection is not valid or the topic is empty.
    pub fn validate(&self) -> beam::Result {
        self.connection.validate()?;
        match self.topic.trim().is_empty() {
            true => Err(beam::Error::msg("Set the topic")),
            false => Ok(()),
        }
    }
}

/// Writes rows to a Kafka topic, and returns the rows that the write rejected, with the
/// schema of [`error_schema`].
#[derive(Clone, Debug)]
pub struct WriteRecords {
    name: String,
    options: WriteOptions,
}

impl WriteRecords {
    pub fn new(name: impl Into<String>, options: WriteOptions) -> Self {
        Self {
            name: name.into(),
            options,
        }
    }

    /// Fallible form of [`PTransform::expand`]: fails if the options are not valid, the input
    /// has no row schema, or the format cannot write rows of that schema.
    pub fn try_expand(&self, input: &PCollection<Row>) -> beam::Result<PCollection<Row>> {
        let context = |e: beam::Error| e.context(self.name.clone());
        self.options.validate().map_err(context)?;
        let schema = input.row_schema().ok_or_else(|| {
            beam::Error::msg(format!("{}: the input has no row schema", self.name))
        })?;
        let encoder = Encoder::new(self.options.format, &schema).map_err(context)?;
        let errors = Arc::new(error_schema(&schema));
        let target = Arc::new(Target {
            connection: self.options.connection.clone(),
            topic: self.options.topic.clone(),
            schema,
            errors: Arc::clone(&errors),
            encoder,
        });

        let pipeline = input.pipeline();
        let name = pipeline.unique_transform_name(&self.name);
        let existing = transform_ids(pipeline);
        // `finish_bundle` emits the rejected rows in the global window.
        let failed = input
            .apply(WindowInto::new(
                format!("{name}/GlobalWindow"),
                GlobalWindows,
            ))
            .as_row_elements()
            .apply(ParDo::new(format!("{name}/Write"), WriteFn::new(target)))
            .with_row_schema(&errors);
        add_composite(
            pipeline,
            &name,
            &existing,
            HashMap::from([(ERRORS_TAG.to_string(), failed.id().to_string())]),
        );
        Ok(failed)
    }
}

impl PTransform<PCollection<Row>> for WriteRecords {
    type Output = PCollection<Row>;

    fn expand(&self, input: &PCollection<Row>) -> PCollection<Row> {
        self.try_expand(input)
            .unwrap_or_else(|e| panic!("{} could not be expanded: {e}", self.name))
    }
}

/// Emits `output` from `finish_bundle` at the end of the global window. The write moves its
/// input into the global window first.
fn emit_global<T: Outputs>(ctx: &mut FinishBundleContext<'_, T>, output: T) -> beam::Result {
    ctx.emit(
        output,
        BoundedWindow::Global(GlobalWindow),
        Instant::END_OF_GLOBAL_WINDOW,
    )
}
