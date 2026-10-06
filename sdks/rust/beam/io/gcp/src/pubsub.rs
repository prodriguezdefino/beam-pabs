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
//! use gcp::pubsub::PubsubRead;
//!
//! let p = Pipeline::new();
//! let messages = p.apply(
//!     PubsubRead::new("PubsubRead")
//!         .with_subscription("projects/my-project/subscriptions/my-sub")
//!         .with_raw_format()
//! );
//! ```
//!
//! # Writing to Pub/Sub
//! ```no_run
//! use beam::prelude::*;
//! use gcp::pubsub::PubsubWrite;
//!
//! # let p = Pipeline::new();
//! # let rows: PCollection<Row> = unimplemented!();
//! rows.apply(
//!     PubsubWrite::new("PubsubWrite", "projects/my-project/topics/my-topic")
//!         .with_raw_format()
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

/// Standard External Transform URN for Pub/Sub Read.
pub const URN_PUBSUB_READ_TRANSFORM: &str = "beam:transform:org.apache.beam:pubsub_read:v1";

/// Standard External Transform URN for Pub/Sub Write.
pub const URN_PUBSUB_WRITE_TRANSFORM: &str = "beam:transform:org.apache.beam:pubsub_write:v1";

/// Default expansion service target for GCP SchemaTransforms.
pub const DEFAULT_EXPANSION_SERVICE: &str =
    "autojava::sdks:java:io:google-cloud-platform:expansion-service:runExpansionService";

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

pub fn raw_bytes_schema() -> Arc<Schema> {
    Arc::clone(&RAW_BYTES_SCHEMA)
}

pub fn raw_string_schema() -> Arc<Schema> {
    Arc::clone(&RAW_STRING_SCHEMA)
}

pub fn raw_bytes_row(payload: impl Into<Vec<u8>>) -> Row {
    Row::builder(raw_bytes_schema())
        .with_value(FieldValue::Bytes(payload.into()))
        .build()
        .expect("valid raw bytes row")
}

pub fn raw_string_row(payload: impl Into<String>) -> Row {
    Row::builder(raw_string_schema())
        .with_value(FieldValue::String(payload.into()))
        .build()
        .expect("valid raw string row")
}

/// Transform for reading from Cloud Pub/Sub.
#[derive(Clone, Debug)]
pub struct PubsubRead {
    name: String,
    topic: Option<String>,
    subscription: Option<String>,
    format: PubsubFormat,
    attributes: Option<Vec<String>>,
    attributes_map: Option<String>,
    id_attribute: Option<String>,
    timestamp_attribute: Option<String>,
    expansion_service: String,
}

impl PubsubRead {
    /// Set exactly one of [`with_topic`](Self::with_topic) or
    /// [`with_subscription`](Self::with_subscription).
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            topic: None,
            subscription: None,
            format: PubsubFormat::Raw,
            attributes: None,
            attributes_map: None,
            id_attribute: None,
            timestamp_attribute: None,
            expansion_service: DEFAULT_EXPANSION_SERVICE.to_string(),
        }
    }

    /// Reads `projects/${PROJECT}/topics/${TOPIC}`.
    pub fn with_topic(mut self, topic: impl Into<String>) -> Self {
        self.topic = Some(topic.into());
        self
    }

    /// Reads `projects/${PROJECT}/subscriptions/${SUBSCRIPTION}`.
    pub fn with_subscription(mut self, subscription: impl Into<String>) -> Self {
        self.subscription = Some(subscription.into());
        self
    }

    pub fn with_format(mut self, format: PubsubFormat) -> Self {
        self.format = format;
        self
    }

    /// Reads unparsed payload bytes.
    pub fn with_raw_format(mut self) -> Self {
        self.format = PubsubFormat::Raw;
        self
    }

    /// Decodes JSON payloads with the given JSON Schema.
    pub fn with_json_format(mut self, schema: impl Into<String>) -> Self {
        self.format = PubsubFormat::Json {
            schema: schema.into(),
        };
        self
    }

    /// Decodes Avro payloads with the given Avro schema.
    pub fn with_avro_format(mut self, schema: impl Into<String>) -> Self {
        self.format = PubsubFormat::Avro {
            schema: schema.into(),
        };
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

    /// Defaults to [`DEFAULT_EXPANSION_SERVICE`].
    pub fn with_expansion_service(mut self, endpoint: impl Into<String>) -> Self {
        self.expansion_service = endpoint.into();
        self
    }

    pub fn topic(&self) -> Option<&str> {
        self.topic.as_deref()
    }

    pub fn subscription(&self) -> Option<&str> {
        self.subscription.as_deref()
    }

    pub fn format(&self) -> &PubsubFormat {
        &self.format
    }

    pub fn attributes(&self) -> Option<&[String]> {
        self.attributes.as_deref()
    }

    pub fn attributes_map(&self) -> Option<&str> {
        self.attributes_map.as_deref()
    }

    pub fn id_attribute(&self) -> Option<&str> {
        self.id_attribute.as_deref()
    }

    pub fn timestamp_attribute(&self) -> Option<&str> {
        self.timestamp_attribute.as_deref()
    }

    pub fn expansion_service(&self) -> &str {
        &self.expansion_service
    }

    /// Builds the `PubsubReadSchemaTransformConfiguration` [`Row`]. Fails unless exactly
    /// one source is set.
    pub fn build_config_row(&self) -> Result<Row, ExpansionError> {
        if self.topic.is_none() && self.subscription.is_none() {
            return Err(ExpansionError::InvalidResponse(
                "PubsubRead requires either topic or subscription".to_string(),
            ));
        }

        if self.topic.is_some() && self.subscription.is_some() {
            return Err(ExpansionError::InvalidResponse(
                "PubsubRead requires either topic or subscription, not both".to_string(),
            ));
        }

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
            .with_named("subscription", self.subscription.as_deref())
            .with_named("timestamp_attribute", self.timestamp_attribute.as_deref())
            .with_named("topic", self.topic.as_deref())
            .build()
            .map_err(|e| {
                ExpansionError::Encoding(format!("Failed to build PubsubRead config: {e}"))
            })
    }

    pub fn build(&self) -> Result<ExternalSource, ExpansionError> {
        let config_row = self.build_config_row()?;
        let transform = ExternalTransform::schema_transform(
            self.name.clone(),
            URN_PUBSUB_READ,
            &self.expansion_service,
            &config_row,
        )?;

        Ok(ExternalSource::new(transform).with_output_tag("output"))
    }

    /// Fallible form of [`PTransform::expand`].
    pub fn try_expand(&self, input: &PBegin) -> Result<PCollection<Row>, ExpansionError> {
        self.build()?.try_expand(input)
    }
}

impl PTransform<PBegin> for PubsubRead {
    type Output = PCollection<Row>;

    fn expand(&self, input: &PBegin) -> Self::Output {
        self.try_expand(input)
            .unwrap_or_else(|e| panic!("{} could not be expanded: {e}", self.name))
    }
}

/// Transform for writing to Cloud Pub/Sub.
#[derive(Clone, Debug)]
pub struct PubsubWrite {
    name: String,
    topic: String,
    format: PubsubFormat,
    attributes: Option<Vec<String>>,
    attributes_map: Option<String>,
    id_attribute: Option<String>,
    timestamp_attribute: Option<String>,
    expansion_service: String,
}

impl PubsubWrite {
    /// `topic` has the form `projects/${PROJECT}/topics/${TOPIC}`.
    pub fn new(name: impl Into<String>, topic: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            topic: topic.into(),
            format: PubsubFormat::Raw,
            attributes: None,
            attributes_map: None,
            id_attribute: None,
            timestamp_attribute: None,
            expansion_service: DEFAULT_EXPANSION_SERVICE.to_string(),
        }
    }

    pub fn with_format(mut self, format: PubsubFormat) -> Self {
        self.format = format;
        self
    }

    /// Writes unparsed payload bytes or strings.
    pub fn with_raw_format(mut self) -> Self {
        self.format = PubsubFormat::Raw;
        self
    }

    /// Writes rows as JSON.
    pub fn with_json_format(mut self) -> Self {
        self.format = PubsubFormat::Json {
            schema: String::new(),
        };
        self
    }

    /// Writes rows as Avro.
    pub fn with_avro_format(mut self) -> Self {
        self.format = PubsubFormat::Avro {
            schema: String::new(),
        };
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

    /// Defaults to [`DEFAULT_EXPANSION_SERVICE`].
    pub fn with_expansion_service(mut self, endpoint: impl Into<String>) -> Self {
        self.expansion_service = endpoint.into();
        self
    }

    pub fn topic(&self) -> &str {
        &self.topic
    }

    pub fn format(&self) -> &PubsubFormat {
        &self.format
    }

    pub fn attributes(&self) -> Option<&[String]> {
        self.attributes.as_deref()
    }

    pub fn attributes_map(&self) -> Option<&str> {
        self.attributes_map.as_deref()
    }

    pub fn id_attribute(&self) -> Option<&str> {
        self.id_attribute.as_deref()
    }

    pub fn timestamp_attribute(&self) -> Option<&str> {
        self.timestamp_attribute.as_deref()
    }

    pub fn expansion_service(&self) -> &str {
        &self.expansion_service
    }

    /// Builds the `PubsubWriteSchemaTransformConfiguration` [`Row`]. Fails if the topic is
    /// empty.
    pub fn build_config_row(&self) -> Result<Row, ExpansionError> {
        if self.topic.is_empty() {
            return Err(ExpansionError::InvalidResponse(
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

    pub fn build(&self) -> Result<ExternalSink, ExpansionError> {
        let config_row = self.build_config_row()?;
        let transform = ExternalTransform::schema_transform(
            self.name.clone(),
            URN_PUBSUB_WRITE,
            &self.expansion_service,
            &config_row,
        )?;

        Ok(ExternalSink::new(transform).with_input_tag("input"))
    }

    /// Fallible form of [`PTransform::expand`].
    pub fn try_expand(&self, input: &PCollection<Row>) -> Result<PDone, ExpansionError> {
        self.build()?.try_expand(input)
    }
}

impl PTransform<PCollection<Row>> for PubsubWrite {
    type Output = PDone;

    fn expand(&self, input: &PCollection<Row>) -> Self::Output {
        self.try_expand(input)
            .unwrap_or_else(|e| panic!("{} could not be expanded: {e}", self.name))
    }
}
