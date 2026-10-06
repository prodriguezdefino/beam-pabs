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

//! Google Cloud Pub/Sub I/O through the SchemaTransform providers `pubsub_read:v1` and
//! `pubsub_write:v1`.
//!
//! # Reading from Pub/Sub
//! ```no_run
//! use beam::prelude::*;
//! use gcp::pubsub::{PubsubFormat, PubsubRead};
//!
//! let p = Pipeline::new();
//! let messages = p.apply(
//!     "PubsubRead",
//!     PubsubRead::new()
//!         .with_subscription("projects/my-project/subscriptions/my-sub")
//!         .with_format(PubsubFormat::Raw)
//! );
//! ```
//!
//! # Writing to Pub/Sub
//! ```no_run
//! use beam::prelude::*;
//! use gcp::pubsub::{PubsubWrite, PubsubWriteFormat};
//!
//! # let p = Pipeline::new();
//! # let rows: PCollection<Row> = unimplemented!();
//! rows.apply(
//!     "PubsubWrite",
//!     PubsubWrite::new("projects/my-project/topics/my-topic")
//!         .with_format(PubsubWriteFormat::Raw)
//! );
//! ```

use std::sync::{Arc, LazyLock};

use beam::prelude::*;
use beam::schema::{Field, FieldType, FieldValue};
use external::{ExpansionError, ExternalSink, ExternalSource, ExternalTransform};

/// Standard SchemaTransform URN for Pub/Sub Read.
pub const URN_PUBSUB_READ: &str = "beam:schematransform:org.apache.beam:pubsub_read:v1";

/// Standard SchemaTransform URN for Pub/Sub Write.
pub const URN_PUBSUB_WRITE: &str = "beam:schematransform:org.apache.beam:pubsub_write:v1";

static RAW_BYTES_SCHEMA: LazyLock<Arc<Schema>> =
    LazyLock::new(|| Arc::new(Schema::new(vec![Field::new("payload", FieldType::bytes())])));

static RAW_STRING_SCHEMA: LazyLock<Arc<Schema>> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![Field::new(
        "payload",
        FieldType::string(),
    )]))
});

static PUBSUB_READ_CONFIG_SCHEMA: LazyLock<Arc<Schema>> = LazyLock::new(|| {
    let error_handling_schema = Schema::new(vec![Field::new("output", FieldType::string())]);
    let empty_row_schema = Schema::new(vec![]);
    Arc::new(Schema::new(vec![
        Field::nullable("attributes", FieldType::array(FieldType::string())),
        Field::nullable("attributes_map", FieldType::string()),
        Field::nullable("client_factory", FieldType::row(empty_row_schema.clone())),
        Field::nullable("clock", FieldType::row(empty_row_schema)),
        Field::nullable("error_handling", FieldType::row(error_handling_schema)),
        Field::new("format", FieldType::string()),
        Field::nullable("id_attribute", FieldType::string()),
        Field::new("schema", FieldType::string()),
        Field::nullable("subscription", FieldType::string()),
        Field::nullable("timestamp_attribute", FieldType::string()),
        Field::nullable("topic", FieldType::string()),
    ]))
});

static PUBSUB_WRITE_CONFIG_SCHEMA: LazyLock<Arc<Schema>> = LazyLock::new(|| {
    let error_handling_schema = Schema::new(vec![Field::new("output", FieldType::string())]);
    Arc::new(Schema::new(vec![
        Field::nullable("attributes", FieldType::array(FieldType::string())),
        Field::nullable("attributes_map", FieldType::string()),
        Field::nullable("error_handling", FieldType::row(error_handling_schema)),
        Field::new("format", FieldType::string()),
        Field::nullable("id_attribute", FieldType::string()),
        Field::nullable("timestamp_attribute", FieldType::string()),
        Field::new("topic", FieldType::string()),
    ]))
});

/// Pub/Sub message payload format.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub enum PubsubFormat {
    /// Unparsed bytes or strings.
    #[default]
    Raw,
    Json {
        /// JSON Schema definition.
        schema: String,
    },
    Avro {
        /// Avro schema definition.
        schema: String,
    },
}

impl PubsubFormat {
    /// Creates a JSON format with the given JSON Schema.
    pub fn json(schema: impl Into<String>) -> Self {
        Self::Json {
            schema: schema.into(),
        }
    }

    /// Creates an Avro format with the given Avro schema.
    pub fn avro(schema: impl Into<String>) -> Self {
        Self::Avro {
            schema: schema.into(),
        }
    }

    /// Format name expected by `PubsubReadSchemaTransformProvider` and
    /// `PubsubWriteSchemaTransformProvider`.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Raw => "RAW",
            Self::Json { .. } => "JSON",
            Self::Avro { .. } => "AVRO",
        }
    }

    /// Schema definition, or empty for [`PubsubFormat::Raw`].
    pub fn schema_str(&self) -> &str {
        match self {
            Self::Raw => "",
            Self::Json { schema } | Self::Avro { schema } => schema.as_str(),
        }
    }
}

/// Payload format for messages that [`PubsubWrite`] sends to Cloud Pub/Sub.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum PubsubWriteFormat {
    /// Unparsed raw bytes or strings.
    #[default]
    Raw,
    /// JSON formatted payload.
    Json,
    /// Avro formatted payload.
    Avro,
}

impl PubsubWriteFormat {
    /// Format name expected by Cloud Pub/Sub SchemaTransform provider.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Raw => "RAW",
            Self::Json => "JSON",
            Self::Avro => "AVRO",
        }
    }
}

impl AsRef<str> for PubsubWriteFormat {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl std::fmt::Display for PubsubWriteFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl From<PubsubWriteFormat> for &'static str {
    fn from(format: PubsubWriteFormat) -> Self {
        format.as_str()
    }
}

/// Single-field schema for raw byte payloads (`payload: BYTES`).
pub fn raw_bytes_schema() -> Arc<Schema> {
    Arc::clone(&RAW_BYTES_SCHEMA)
}

/// Single-field schema for raw string payloads (`payload: STRING`).
pub fn raw_string_schema() -> Arc<Schema> {
    Arc::clone(&RAW_STRING_SCHEMA)
}

/// Builds a [`raw_bytes_schema`] row with `payload`.
pub fn raw_bytes_row(payload: impl Into<Vec<u8>>) -> Row {
    Row::builder(raw_bytes_schema())
        .with_value(FieldValue::Bytes(payload.into()))
        .build()
        .expect("valid raw bytes row")
}

/// Builds a [`raw_string_schema`] row with `payload`.
pub fn raw_string_row(payload: impl Into<String>) -> Row {
    Row::builder(raw_string_schema())
        .with_value(FieldValue::String(payload.into()))
        .build()
        .expect("valid raw string row")
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum PubsubSource {
    Topic(String),
    Subscription(String),
}

/// Transform for reading from Cloud Pub/Sub.
#[derive(Clone, Debug)]
pub struct PubsubRead {
    source: Option<PubsubSource>,
    format: PubsubFormat,
    attributes: Option<Vec<String>>,
    attributes_map: Option<String>,
    id_attribute: Option<String>,
    timestamp_attribute: Option<String>,
    expansion_service: String,
}

impl Default for PubsubRead {
    fn default() -> Self {
        Self::new()
    }
}

impl PubsubRead {
    /// Set the source with [`with_topic`](Self::with_topic) or
    /// [`with_subscription`](Self::with_subscription).
    pub fn new() -> Self {
        Self {
            source: None,
            format: PubsubFormat::Raw,
            attributes: None,
            attributes_map: None,
            id_attribute: None,
            timestamp_attribute: None,
            expansion_service: crate::DEFAULT_EXPANSION_SERVICE.to_string(),
        }
    }

    /// Reads `projects/${PROJECT}/topics/${TOPIC}`.
    pub fn with_topic(mut self, topic: impl Into<String>) -> Self {
        self.source = Some(PubsubSource::Topic(topic.into()));
        self
    }

    /// Reads `projects/${PROJECT}/subscriptions/${SUBSCRIPTION}`.
    pub fn with_subscription(mut self, subscription: impl Into<String>) -> Self {
        self.source = Some(PubsubSource::Subscription(subscription.into()));
        self
    }

    /// Message payload format. Defaults to [`PubsubFormat::Raw`].
    pub fn with_format(mut self, format: PubsubFormat) -> Self {
        self.format = format;
        self
    }

    /// Attribute keys to emit as separate string fields.
    pub fn with_attributes(
        mut self,
        attributes: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.attributes = Some(attributes.into_iter().map(Into::into).collect());
        self
    }

    /// Output field that gets the map of all attributes.
    pub fn with_attributes_map(mut self, attributes_map: impl Into<String>) -> Self {
        self.attributes_map = Some(attributes_map.into());
        self
    }

    /// Attribute with the unique record ID used for deduplication.
    pub fn with_id_attribute(mut self, id_attribute: impl Into<String>) -> Self {
        self.id_attribute = Some(id_attribute.into());
        self
    }

    /// Attribute with the record timestamp.
    pub fn with_timestamp_attribute(mut self, timestamp_attribute: impl Into<String>) -> Self {
        self.timestamp_attribute = Some(timestamp_attribute.into());
        self
    }

    /// Defaults to [`crate::DEFAULT_EXPANSION_SERVICE`].
    pub fn with_expansion_service(mut self, endpoint: impl Into<String>) -> Self {
        self.expansion_service = endpoint.into();
        self
    }

    /// Source Pub/Sub topic name if configured.
    #[doc(hidden)]
    pub fn topic(&self) -> Option<&str> {
        match self.source.as_ref() {
            Some(PubsubSource::Topic(topic)) => Some(topic.as_str()),
            Some(PubsubSource::Subscription(_)) | None => None,
        }
    }

    /// Source Pub/Sub subscription name if configured.
    #[doc(hidden)]
    pub fn subscription(&self) -> Option<&str> {
        match self.source.as_ref() {
            Some(PubsubSource::Subscription(sub)) => Some(sub.as_str()),
            Some(PubsubSource::Topic(_)) | None => None,
        }
    }

    /// Configured payload format.
    #[doc(hidden)]
    pub fn format(&self) -> &PubsubFormat {
        &self.format
    }

    /// Attribute keys emitted as individual columns.
    #[doc(hidden)]
    pub fn attributes(&self) -> Option<&[String]> {
        self.attributes.as_deref()
    }

    /// Output field name for attribute map.
    #[doc(hidden)]
    pub fn attributes_map(&self) -> Option<&str> {
        self.attributes_map.as_deref()
    }

    /// Attribute key that holds the message ID.
    #[doc(hidden)]
    pub fn id_attribute(&self) -> Option<&str> {
        self.id_attribute.as_deref()
    }

    /// Attribute key that holds the publish timestamp.
    #[doc(hidden)]
    pub fn timestamp_attribute(&self) -> Option<&str> {
        self.timestamp_attribute.as_deref()
    }

    /// Expansion service address.
    #[doc(hidden)]
    pub fn expansion_service(&self) -> &str {
        &self.expansion_service
    }

    /// Builds the `PubsubReadSchemaTransformConfiguration` [`Row`]. Fails if neither topic
    /// nor subscription is configured.
    pub fn build_config_row(&self) -> Result<Row, ExpansionError> {
        let (topic, subscription) = match self.source.as_ref() {
            Some(PubsubSource::Topic(topic)) => (Some(topic.as_str()), None),
            Some(PubsubSource::Subscription(sub)) => (None, Some(sub.as_str())),
            None => {
                return Err(ExpansionError::InvalidConfig(
                    "PubsubRead requires either topic or subscription".to_string(),
                ));
            }
        };

        Row::builder(Arc::clone(&PUBSUB_READ_CONFIG_SCHEMA))
            .with_named(
                "attributes",
                self.attributes.as_ref().map(|attrs| {
                    FieldValue::Array(
                        attrs
                            .iter()
                            .map(|a| Some(FieldValue::String(a.clone())))
                            .collect(),
                    )
                }),
            )
            .with_named("attributes_map", self.attributes_map.as_deref())
            .with_named("client_factory", None::<FieldValue>)
            .with_named("clock", None::<FieldValue>)
            .with_named("error_handling", None::<FieldValue>)
            .with_named("format", Some(self.format.as_str()))
            .with_named("id_attribute", self.id_attribute.as_deref())
            .with_named("schema", Some(self.format.schema_str()))
            .with_named("subscription", subscription)
            .with_named("timestamp_attribute", self.timestamp_attribute.as_deref())
            .with_named("topic", topic)
            .build()
            .map_err(|e| {
                ExpansionError::Encoding(format!("Failed to build PubsubRead config: {e}"))
            })
    }

    /// Builds the underlying [`ExternalSource`] transform.
    pub fn build(&self) -> Result<ExternalSource, ExpansionError> {
        let config_row = self.build_config_row()?;
        let transform = ExternalTransform::schema_transform(
            URN_PUBSUB_READ,
            &self.expansion_service,
            &config_row,
        )?;

        Ok(ExternalSource::new(transform).with_output_tag("output"))
    }
}

impl TryPTransform<PBegin> for PubsubRead {
    type Error = ExpansionError;

    fn try_expand(&self, input: &PBegin) -> Result<PCollection<Row>, ExpansionError> {
        self.build()?.try_expand(input)
    }
}

impl PTransform<PBegin> for PubsubRead {
    type Output = PCollection<Row>;

    /// # Panics
    ///
    /// Panics if the expansion fails. Use `try_apply` to get the error.
    fn expand(&self, input: &PBegin) -> Self::Output {
        self.try_expand(input)
            .unwrap_or_else(|e| panic!("the transform could not be expanded: {e}"))
    }
}

/// Transform for writing to Cloud Pub/Sub.
#[derive(Clone, Debug)]
pub struct PubsubWrite {
    topic: String,
    format: PubsubWriteFormat,
    attributes: Option<Vec<String>>,
    attributes_map: Option<String>,
    id_attribute: Option<String>,
    timestamp_attribute: Option<String>,
    expansion_service: String,
}

impl PubsubWrite {
    /// `topic` has the form `projects/${PROJECT}/topics/${TOPIC}`.
    pub fn new(topic: impl Into<String>) -> Self {
        Self {
            topic: topic.into(),
            format: PubsubWriteFormat::Raw,
            attributes: None,
            attributes_map: None,
            id_attribute: None,
            timestamp_attribute: None,
            expansion_service: crate::DEFAULT_EXPANSION_SERVICE.to_string(),
        }
    }

    /// Message payload format. Defaults to [`PubsubWriteFormat::Raw`].
    pub fn with_format(mut self, format: PubsubWriteFormat) -> Self {
        self.format = format;
        self
    }

    /// Row fields to send as message attributes instead of payload.
    pub fn with_attributes(
        mut self,
        attributes: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.attributes = Some(attributes.into_iter().map(Into::into).collect());
        self
    }

    /// Map field to send as message attributes.
    pub fn with_attributes_map(mut self, attributes_map: impl Into<String>) -> Self {
        self.attributes_map = Some(attributes_map.into());
        self
    }

    /// Attribute that gets a unique message ID.
    pub fn with_id_attribute(mut self, id_attribute: impl Into<String>) -> Self {
        self.id_attribute = Some(id_attribute.into());
        self
    }

    /// Attribute that gets the publish timestamp.
    pub fn with_timestamp_attribute(mut self, timestamp_attribute: impl Into<String>) -> Self {
        self.timestamp_attribute = Some(timestamp_attribute.into());
        self
    }

    /// Defaults to [`crate::DEFAULT_EXPANSION_SERVICE`].
    pub fn with_expansion_service(mut self, endpoint: impl Into<String>) -> Self {
        self.expansion_service = endpoint.into();
        self
    }

    /// Destination Pub/Sub topic name.
    #[doc(hidden)]
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Configured payload format.
    #[doc(hidden)]
    pub fn format(&self) -> PubsubWriteFormat {
        self.format
    }

    /// Fields sent as message attributes instead of payload.
    #[doc(hidden)]
    pub fn attributes(&self) -> Option<&[String]> {
        self.attributes.as_deref()
    }

    /// Map field sent as message attributes.
    #[doc(hidden)]
    pub fn attributes_map(&self) -> Option<&str> {
        self.attributes_map.as_deref()
    }

    /// Attribute that receives the unique message ID.
    #[doc(hidden)]
    pub fn id_attribute(&self) -> Option<&str> {
        self.id_attribute.as_deref()
    }

    /// Attribute that receives the publish timestamp.
    #[doc(hidden)]
    pub fn timestamp_attribute(&self) -> Option<&str> {
        self.timestamp_attribute.as_deref()
    }

    /// Expansion service address.
    #[doc(hidden)]
    pub fn expansion_service(&self) -> &str {
        &self.expansion_service
    }

    /// Builds the `PubsubWriteSchemaTransformConfiguration` [`Row`]. Fails if the
    /// topic is empty.
    pub fn build_config_row(&self) -> Result<Row, ExpansionError> {
        if self.topic.is_empty() {
            return Err(ExpansionError::InvalidConfig(
                "PubsubWrite requires destination topic".to_string(),
            ));
        }
        let topic = self.topic.as_str();

        Row::builder(Arc::clone(&PUBSUB_WRITE_CONFIG_SCHEMA))
            .with_named(
                "attributes",
                self.attributes.as_ref().map(|attrs| {
                    FieldValue::Array(
                        attrs
                            .iter()
                            .map(|a| Some(FieldValue::String(a.clone())))
                            .collect(),
                    )
                }),
            )
            .with_named("attributes_map", self.attributes_map.as_deref())
            .with_named("error_handling", None::<FieldValue>)
            .with_named("format", Some(self.format.as_str()))
            .with_named("id_attribute", self.id_attribute.as_deref())
            .with_named("timestamp_attribute", self.timestamp_attribute.as_deref())
            .with_named("topic", Some(topic))
            .build()
            .map_err(|e| {
                ExpansionError::Encoding(format!("Failed to build PubsubWrite config: {e}"))
            })
    }

    /// Builds the underlying [`ExternalSink`] transform.
    pub fn build(&self) -> Result<ExternalSink, ExpansionError> {
        let config_row = self.build_config_row()?;
        let transform = ExternalTransform::schema_transform(
            URN_PUBSUB_WRITE,
            &self.expansion_service,
            &config_row,
        )?;

        Ok(ExternalSink::new(transform).with_input_tag("input"))
    }
}

impl TryPTransform<PCollection<Row>> for PubsubWrite {
    type Error = ExpansionError;

    fn try_expand(&self, input: &PCollection<Row>) -> Result<PDone, ExpansionError> {
        self.build()?.try_expand(input)
    }
}

impl PTransform<PCollection<Row>> for PubsubWrite {
    type Output = PDone;

    /// # Panics
    ///
    /// Panics if the expansion fails. Use `try_apply` to get the error.
    fn expand(&self, input: &PCollection<Row>) -> Self::Output {
        self.try_expand(input)
            .unwrap_or_else(|e| panic!("the transform could not be expanded: {e}"))
    }
}
