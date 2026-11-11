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

//! Writes the elements of a PCollection to a Kafka topic, at least once.
//!
//! [`Write`] sends each element as one record of its [`Encoder`]: by default a `Vec<u8>` as
//! the record value ([`Raw`]), or a codec of [`codec`](super::codec), or an encoder of its
//! own. The elements need no shuffle: each bundle sends its records and waits until Kafka has
//! all of them. A failed bundle is retried, so a record can be written more than once.
//!
//! Elements that do not convert, and elements of records that Kafka rejects (for example a
//! record that is too large), go to the failure output with the reason.
//!
//! ```no_run
//! use beam::prelude::*;
//! use beam::schema::Row;
//! use beam_io_kafka::codec::Json;
//! use beam_io_kafka::write::{Write, WriteOptions};
//!
//! # fn values() -> PCollection<Vec<u8>> { unimplemented!() }
//! # fn rows() -> PCollection<Row> { unimplemented!() }
//! let options = WriteOptions::new("broker-1:9092", "events");
//! let failed_values = values().apply("WriteValues", Write::new(options.clone()));
//! let failed_rows = rows().apply("WriteRows", Write::new(options).with_encoder(Json::new()));
//! ```

#[doc(hidden)]
pub mod delivery;
mod produce;
#[doc(hidden)]
pub mod writer;

use std::sync::Arc;

use beam::Instant;
use beam::internals::PipelineInternals;
use beam::schema::Row;
use beam::transforms::display_data::{DisplayDataBuilder, HasDisplayData};
use beam::transforms::{
    BundleWindows, FinishBundleContext, Outputs, PTransform, ParDo, TryPTransform,
};
use beam::values::PCollection;
use beam::windowing::{BoundedWindow, GlobalWindow, GlobalWindows, WindowInto};

use super::client::{Auth, Connection};
use super::codec::{Encoder, Raw, write_error_schema};
use writer::{Target, WriteFn};

/// The compression codec of the record batches of the producer (`compression.type`).
///
/// Kafka consumers read every codec from the header of the record batch.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum Compression {
    /// Record batches with no compression (`"none"`).
    #[default]
    None,
    /// Gzip compression (`"gzip"`).
    Gzip,
    /// Snappy compression (`"snappy"`).
    Snappy,
    /// LZ4 frame compression (`"lz4"`).
    Lz4,
    /// Zstandard compression (`"zstd"`).
    Zstd,
}

impl Compression {
    /// The value of the Kafka property `compression.type`.
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Gzip => "gzip",
            Self::Snappy => "snappy",
            Self::Lz4 => "lz4",
            Self::Zstd => "zstd",
        }
    }
}

/// The cluster and the topic of a write.
#[derive(Clone, Debug, PartialEq)]
pub struct WriteOptions {
    pub(crate) connection: Connection,
    pub(crate) topic: String,
}

impl WriteOptions {
    pub fn new(bootstrap_servers: impl Into<String>, topic: impl Into<String>) -> Self {
        Self {
            connection: Connection::new(bootstrap_servers),
            topic: topic.into(),
        }
    }

    /// Returns the cluster connection.
    pub fn connection(&self) -> &Connection {
        &self.connection
    }

    /// Returns the topic name.
    pub fn topic(&self) -> &str {
        &self.topic
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

    /// Sets the producer record batch compression codec (`compression.type`).
    pub fn with_compression(mut self, compression: Compression) -> Self {
        self.connection.properties.insert(
            "compression.type".to_string(),
            compression.as_str().to_string(),
        );
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
        if self.topic.trim().is_empty() {
            Err(beam::Error::msg("Set the topic"))
        } else {
            Ok(())
        }
    }
}

/// Writes the elements of a PCollection to a Kafka topic as the records of an [`Encoder`]
/// (by default [`Raw`]), and returns the elements that the write rejected, as rows of
/// [`write_error_schema`] of the type of the encoder.
#[derive(Clone, Debug)]
pub struct Write<E = Raw> {
    options: WriteOptions,
    encoder: E,
}

impl Write<Raw> {
    /// The write of `Vec<u8>` elements as record values.
    pub fn new(options: WriteOptions) -> Self {
        Self {
            options,
            encoder: Raw,
        }
    }
}

impl<E> HasDisplayData for Write<E> {
    fn populate_display_data(&self, builder: &mut DisplayDataBuilder) {
        builder.add_text(
            "bootstrap_servers",
            &self.options.connection.bootstrap_servers,
        );
        builder.add_text("topic", &self.options.topic);
    }
}

impl<E: Encoder> Write<E> {
    /// The same write, with the records of `encoder`.
    pub fn with_encoder<E2: Encoder>(self, encoder: E2) -> Write<E2> {
        Write {
            options: self.options,
            encoder,
        }
    }
}

impl<E: Encoder> Write<E> {
    fn try_expand_ref(&self, input: &PCollection<E::Input>) -> beam::Result<PCollection<Row>> {
        input.pipeline().record_display_data(self);
        let step = input.pipeline().scope_name().unwrap_or_default();
        let context = |e: beam::Error| e.context(&step);
        self.options.validate().map_err(context)?;
        // `finish_bundle` emits the rejected elements in the global window.
        let windowed = input.apply("GlobalWindow", WindowInto::new(GlobalWindows));
        let mut encoder = self.encoder.clone();
        let elements = encoder.elements(&windowed).map_err(context)?;
        let errors = Arc::new(write_error_schema(encoder.failed_type()));
        let target = Arc::new(Target {
            connection: self.options.connection.clone(),
            topic: self.options.topic.clone(),
            errors: Arc::clone(&errors),
        });
        let failed = elements
            .apply("Write", ParDo::new(WriteFn::new(target, Arc::new(encoder))))
            .with_row_schema(&errors);
        Ok(failed)
    }
}

impl<E: Encoder> TryPTransform<PCollection<E::Input>> for Write<E> {
    type Output = PCollection<Row>;
    type Error = beam::Error;

    /// Fails if the options are not valid or the encoder cannot write the elements of `input`.
    fn try_expand(self, input: &PCollection<E::Input>) -> beam::Result<PCollection<Row>> {
        self.try_expand_ref(input)
    }
}

/// An expansion error is a construction error: [`Pipeline::validate`] returns it, so the
/// pipeline does not run. Use `try_apply` to get the error as a value.
///
/// [`Pipeline::validate`]: beam::pipeline::Pipeline::validate
impl<E: Encoder> PTransform<PCollection<E::Input>> for Write<E> {
    type Output = PCollection<Row>;

    fn expand(self, input: &PCollection<E::Input>) -> PCollection<Row> {
        self.try_expand_ref(input).unwrap_or_else(|e| {
            let pipeline = input.pipeline();
            let errors = write_error_schema(self.encoder.failed_type());
            pipeline.invalid_construction_output(e, &pipeline.register_row_coder(&errors))
        })
    }
}

/// The window of every element of the write: it moves its input into the global window.
const GLOBAL: BoundedWindow = BoundedWindow::Global(GlobalWindow);

/// Emits `outputs` from `finish_bundle` in the global window, at the earliest timestamp of the
/// bundle in `windows`.
fn emit_global<T: Outputs>(
    ctx: &mut FinishBundleContext<'_, T>,
    windows: &mut BundleWindows,
    outputs: impl IntoIterator<Item = T>,
) -> beam::Result {
    let timestamp = windows
        .take(&GLOBAL)
        .unwrap_or(Instant::END_OF_GLOBAL_WINDOW);
    outputs
        .into_iter()
        .try_for_each(|output| ctx.emit(output, GLOBAL, timestamp))
}
