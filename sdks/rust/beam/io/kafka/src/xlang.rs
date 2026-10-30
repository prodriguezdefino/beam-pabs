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

//! The Kafka connector of the Java SDK, through cross-language expansion.
//!
//! The builders construct the configuration row of the `kafka_read:v1` and `kafka_write:v1`
//! SchemaTransforms, then expand through `beam:transform:managed:v1` ([`managed_io`]),
//! so runners such as Dataflow can manage and upgrade them. [`KafkaRead::to_managed`] and
//! [`KafkaWrite::to_managed`] expose the Managed transform. These SchemaTransforms expose
//! only record *values*, not keys or headers.

use std::collections::BTreeMap;
use std::sync::{Arc, LazyLock};

use beam::prelude::*;
use beam::schema::{Field, FieldType, FieldValue};
use external::{ExpansionError, ExternalSink, ExternalSource};
use managed_io::{ManagedRead, ManagedWrite};

/// Standard SchemaTransform URN for Kafka Read (`ManagedTransforms.Urns.KAFKA_READ`).
pub const URN_KAFKA_READ: &str = managed_io::urns::KAFKA_READ;

/// Standard SchemaTransform URN for Kafka Write (`ManagedTransforms.Urns.KAFKA_WRITE`).
pub const URN_KAFKA_WRITE: &str = managed_io::urns::KAFKA_WRITE;

/// Default expansion service: the I/O expansion service with `KafkaIO`.
pub const DEFAULT_EXPANSION_SERVICE: &str = managed_io::IO_EXPANSION_SERVICE;

/// How record values are encoded in Kafka.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub enum KafkaFormat {
    /// Opaque bytes. Read rows are `(payload: BYTES)`; written rows must have exactly one
    /// `BYTES` field.
    #[default]
    Raw,
    /// UTF-8 text. Read rows are `(payload: STRING)`. Read only.
    String,
    /// JSON objects described by a JSON Schema. Read rows follow that schema; written
    /// rows are serialized from their own Beam schema, so `schema` is ignored on write.
    Json { schema: String },
    /// Avro records described by an Avro schema. As with JSON, write ignores `schema`.
    Avro { schema: String },
    /// Protocol Buffers message `message_name`, described either by a `.proto` source
    /// (`schema`) or a compiled descriptor set file (`file_descriptor_path`).
    Proto {
        message_name: String,
        schema: Option<String>,
        file_descriptor_path: Option<String>,
    },
}

impl KafkaFormat {
    /// Format name understood by the SchemaTransform providers.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Raw => "RAW",
            Self::String => "STRING",
            Self::Json { .. } => "JSON",
            Self::Avro { .. } => "AVRO",
            Self::Proto { .. } => "PROTO",
        }
    }

    fn schema(&self) -> Option<&str> {
        match self {
            Self::Json { schema } | Self::Avro { schema } => Some(schema),
            Self::Proto { schema, .. } => schema.as_deref(),
            Self::Raw | Self::String => None,
        }
    }

    fn message_name(&self) -> Option<&str> {
        match self {
            Self::Proto { message_name, .. } => Some(message_name),
            _ => None,
        }
    }

    fn file_descriptor_path(&self) -> Option<&str> {
        match self {
            Self::Proto {
                file_descriptor_path,
                ..
            } => file_descriptor_path.as_deref(),
            _ => None,
        }
    }
}

/// Where a consumer group with no committed offset starts reading.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OffsetReset {
    /// Resets to the earliest offset.
    Earliest,
    /// Resets to the latest offset.
    Latest,
}

impl OffsetReset {
    /// Returns the string representation of the offset reset strategy.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Earliest => "earliest",
            Self::Latest => "latest",
        }
    }
}

/// Producer batch compression codec (`compression.type`).
///
/// Kafka consumers decompress all supported codecs automatically from the record batch
/// header.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum Compression {
    /// Uncompressed record batches (`"none"`).
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
    /// Returns the Kafka `compression.type` property value.
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

static RAW_BYTES_SCHEMA: LazyLock<Arc<Schema>> =
    LazyLock::new(|| Arc::new(Schema::new(vec![Field::new("payload", FieldType::bytes())])));

static RAW_STRING_SCHEMA: LazyLock<Arc<Schema>> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![Field::new(
        "payload",
        FieldType::string(),
    )]))
});

/// Schema of [`KafkaFormat::Raw`] rows: `(payload: BYTES)`.
pub fn raw_bytes_schema() -> Arc<Schema> {
    Arc::clone(&RAW_BYTES_SCHEMA)
}

/// Schema of rows read with [`KafkaFormat::String`]: `(payload: STRING)`.
pub fn raw_string_schema() -> Arc<Schema> {
    Arc::clone(&RAW_STRING_SCHEMA)
}

/// Builds a [`raw_bytes_schema`] row.
pub fn raw_bytes_row(payload: impl Into<Vec<u8>>) -> Row {
    Row::new(
        raw_bytes_schema(),
        vec![Some(FieldValue::Bytes(payload.into()))],
    )
    .expect("row matches raw_bytes_schema")
}

/// Extracts the payload of a row read with [`KafkaFormat::Raw`] or [`KafkaFormat::String`].
pub fn payload_bytes(row: &Row) -> Option<&[u8]> {
    match row.get_value("payload")? {
        Some(FieldValue::Bytes(b)) => Some(b),
        Some(FieldValue::String(s)) => Some(s.as_bytes()),
        _ => None,
    }
}

/// Client properties that authenticate to a Google Cloud Managed Service for Apache Kafka
/// cluster (SASL_SSL + OAUTHBEARER) with the worker's application-default credentials.
/// The service account needs `roles/managedkafka.client`. The login callback handler ships
/// in the Java I/O expansion service jar.
pub fn google_managed_kafka_auth_config() -> BTreeMap<String, String> {
    [
        ("security.protocol", "SASL_SSL"),
        ("sasl.mechanism", "OAUTHBEARER"),
        (
            "sasl.login.callback.handler.class",
            "com.google.cloud.hosted.kafka.auth.GcpLoginCallbackHandler",
        ),
        (
            "sasl.jaas.config",
            "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;",
        ),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_string(), v.to_string()))
    .collect()
}

fn string_map(map: &BTreeMap<String, String>) -> Option<FieldValue> {
    (!map.is_empty()).then(|| {
        FieldValue::Map(
            map.iter()
                .map(|(k, v)| {
                    (
                        FieldValue::String(k.clone()),
                        Some(FieldValue::String(v.clone())),
                    )
                })
                .collect(),
        )
    })
}

fn string_map_type() -> FieldType {
    FieldType::map(FieldType::string(), FieldType::string())
}

/// `org.apache.beam.sdk.schemas.transforms.providers.ErrorHandling`: `{output: <tag>}`.
/// Null unless `with_error_handling` is set.
fn error_handling_schema() -> Schema {
    Schema::new(vec![Field::new("output", FieldType::string())])
}

fn error_handling_type() -> FieldType {
    FieldType::row(error_handling_schema())
}

fn error_handling_value(output: Option<&str>) -> Result<Option<FieldValue>, ExpansionError> {
    output
        .map(|tag| {
            Row::new(
                Arc::new(error_handling_schema()),
                vec![Some(FieldValue::String(tag.to_string()))],
            )
            .map(FieldValue::Row)
            .map_err(|e| ExpansionError::Encoding(format!("Kafka error_handling: {e}")))
        })
        .transpose()
}

fn require<'a>(value: &'a str, what: &str, op: &str) -> Result<&'a str, ExpansionError> {
    match value {
        v if !v.is_empty() => Ok(v),
        _ => Err(ExpansionError::InvalidResponse(format!(
            "{op} requires a non-empty {what}"
        ))),
    }
}

fn invalid(msg: impl Into<String>) -> ExpansionError {
    ExpansionError::InvalidResponse(msg.into())
}

/// Builder and transform for reading record values from a Kafka topic.
#[derive(Clone, Debug)]
pub struct KafkaRead {
    bootstrap_servers: String,
    topic: String,
    format: KafkaFormat,
    confluent_schema_registry: Option<(String, String)>,
    auto_offset_reset: Option<OffsetReset>,
    consumer_config: BTreeMap<String, String>,
    max_read_time_seconds: Option<i32>,
    redistributed: Option<bool>,
    redistribute_num_keys: Option<i32>,
    redistribute_by_record_key: Option<bool>,
    allow_duplicates: Option<bool>,
    offset_deduplication: Option<bool>,
    num_partitions: Option<i32>,
    with_gcp_adc: Option<bool>,
    error_output: Option<String>,
    expansion_service: String,
}

static KAFKA_READ_CONFIG_SCHEMA: LazyLock<Arc<Schema>> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::nullable("allow_duplicates", FieldType::boolean()),
        Field::nullable("auto_offset_reset_config", FieldType::string()),
        Field::new("bootstrap_servers", FieldType::string()),
        Field::nullable("confluent_schema_registry_subject", FieldType::string()),
        Field::nullable("confluent_schema_registry_url", FieldType::string()),
        Field::nullable("consumer_config_updates", string_map_type()),
        Field::nullable("error_handling", error_handling_type()),
        Field::nullable("file_descriptor_path", FieldType::string()),
        Field::nullable("format", FieldType::string()),
        Field::nullable("max_read_time_seconds", FieldType::int32()),
        Field::nullable("message_name", FieldType::string()),
        Field::nullable("num_partitions", FieldType::int32()),
        Field::nullable("offset_deduplication", FieldType::boolean()),
        Field::nullable("redistribute_by_record_key", FieldType::boolean()),
        Field::nullable("redistribute_num_keys", FieldType::int32()),
        Field::nullable("redistributed", FieldType::boolean()),
        Field::nullable("schema", FieldType::string()),
        Field::new("topic", FieldType::string()),
        Field::nullable("with_gcp_adc", FieldType::boolean()),
    ]))
});

impl KafkaRead {
    /// Reads from `topic` on the brokers `bootstrap_servers` (`host1:port1,host2:port2`).
    pub fn new(bootstrap_servers: impl Into<String>, topic: impl Into<String>) -> Self {
        Self {
            bootstrap_servers: bootstrap_servers.into(),
            topic: topic.into(),
            format: KafkaFormat::Raw,
            confluent_schema_registry: None,
            auto_offset_reset: None,
            consumer_config: BTreeMap::new(),
            max_read_time_seconds: None,
            redistributed: None,
            redistribute_num_keys: None,
            redistribute_by_record_key: None,
            allow_duplicates: None,
            offset_deduplication: None,
            num_partitions: None,
            with_gcp_adc: None,
            error_output: None,
            expansion_service: DEFAULT_EXPANSION_SERVICE.to_string(),
        }
    }

    /// Encoding of record values (default [`KafkaFormat::Raw`]).
    pub fn with_format(mut self, format: KafkaFormat) -> Self {
        self.format = format;
        self
    }

    /// Decodes Avro values with the writer schema under `subject` in a Confluent-compatible
    /// registry, overriding [`Self::with_format`]. Google Managed Kafka registries
    /// (`https://managedkafka.googleapis.com/...`) use the worker's Google credentials.
    pub fn with_confluent_schema_registry(
        mut self,
        url: impl Into<String>,
        subject: impl Into<String>,
    ) -> Self {
        self.confluent_schema_registry = Some((url.into(), subject.into()));
        self
    }

    /// Where to start when the consumer group has no committed offset (default: latest).
    pub fn with_auto_offset_reset(mut self, reset: OffsetReset) -> Self {
        self.auto_offset_reset = Some(reset);
        self
    }

    /// Adds a consumer property such as `group.id` or `sasl.jaas.config`, over the defaults.
    pub fn with_consumer_config(
        mut self,
        key: impl Into<String>,
        value: impl Into<String>,
    ) -> Self {
        self.consumer_config.insert(key.into(), value.into());
        self
    }

    /// Adds [`google_managed_kafka_auth_config`].
    pub fn with_google_managed_kafka_auth(mut self) -> Self {
        self.consumer_config
            .extend(google_managed_kafka_auth_config());
        self
    }

    /// Stops reading after this many seconds, making the read bounded.
    pub fn with_max_read_time_seconds(mut self, seconds: i32) -> Self {
        self.max_read_time_seconds = Some(seconds);
        self
    }

    /// Redistributes records after reading, optionally across `num_keys` keys.
    pub fn with_redistribute(mut self, num_keys: Option<i32>) -> Self {
        self.redistributed = Some(true);
        self.redistribute_num_keys = num_keys;
        self
    }

    /// When redistributing, key by the Kafka record key instead of a synthetic key.
    pub fn with_redistribute_by_record_key(mut self, by_record_key: bool) -> Self {
        self.redistribute_by_record_key = Some(by_record_key);
        self
    }

    /// Whether a redistributed read may emit duplicates (enables cheaper at-least-once).
    pub fn with_allow_duplicates(mut self, allow: bool) -> Self {
        self.allow_duplicates = Some(allow);
        self
    }

    /// Whether a redistributed read deduplicates by offset.
    pub fn with_offset_deduplication(mut self, dedup: bool) -> Self {
        self.offset_deduplication = Some(dedup);
        self
    }

    /// Reads partitions 0 to `num_partitions - 1`. The read assigns them when the pipeline is
    /// built, without querying Kafka.
    pub fn with_num_partitions(mut self, num_partitions: i32) -> Self {
        self.num_partitions = Some(num_partitions);
        self
    }

    /// Authenticates with a Google Managed Kafka cluster with the worker's Application Default
    /// Credentials.
    pub fn with_gcp_adc(mut self, enabled: bool) -> Self {
        self.with_gcp_adc = Some(enabled);
        self
    }

    /// Sends records that fail to decode to an extra output tagged `output`, as rows
    /// `(error_message: STRING, failed_row: BYTES)`, instead of failing the bundle. To read
    /// them, expand `to_managed()?.with_all_outputs()`. The provider ignores error handling
    /// with [`Self::with_confluent_schema_registry`], so expansion then reports a missing output.
    pub fn with_error_handling(mut self, output: impl Into<String>) -> Self {
        self.error_output = Some(output.into());
        self
    }

    /// The error output tag set by [`Self::with_error_handling`].
    pub fn error_output(&self) -> Option<&str> {
        self.error_output.as_deref()
    }

    /// Configures the cross-language expansion service address.
    pub fn with_expansion_service(mut self, endpoint: impl Into<String>) -> Self {
        self.expansion_service = endpoint.into();
        self
    }

    /// Returns the bootstrap servers string.
    pub fn bootstrap_servers(&self) -> &str {
        &self.bootstrap_servers
    }

    /// Returns the topic name.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Returns the configured Kafka format.
    pub fn format(&self) -> &KafkaFormat {
        &self.format
    }

    /// Returns the consumer configuration map.
    pub fn consumer_config(&self) -> &BTreeMap<String, String> {
        &self.consumer_config
    }

    /// Returns the expansion service address.
    pub fn expansion_service(&self) -> &str {
        &self.expansion_service
    }

    fn validate(&self) -> Result<(), ExpansionError> {
        if self.confluent_schema_registry.is_some() {
            return Ok(());
        }
        if let KafkaFormat::Proto {
            schema,
            file_descriptor_path,
            ..
        } = &self.format
            && schema.is_none()
            && file_descriptor_path.is_none()
        {
            return Err(invalid(
                "KafkaRead with PROTO format requires a schema or file_descriptor_path",
            ));
        }
        Ok(())
    }

    /// Constructs the configuration [`Row`] expected by
    /// `KafkaReadSchemaTransformConfiguration` (snake_case, sorted by name).
    pub fn build_config_row(&self) -> Result<Row, ExpansionError> {
        let op = "KafkaRead";
        let bootstrap_servers = require(&self.bootstrap_servers, "bootstrap_servers", op)?;
        let topic = require(&self.topic, "topic", op)?;
        self.validate()?;

        let (registry_url, registry_subject) = self
            .confluent_schema_registry
            .as_ref()
            .map(|(url, subject)| (url.as_str(), subject.as_str()))
            .unzip();

        Row::builder(Arc::clone(&KAFKA_READ_CONFIG_SCHEMA))
            .with_named("allow_duplicates", self.allow_duplicates)
            .with_named(
                "auto_offset_reset_config",
                self.auto_offset_reset.map(|r| r.as_str()),
            )
            .with_named("bootstrap_servers", Some(bootstrap_servers))
            .with_named("confluent_schema_registry_subject", registry_subject)
            .with_named("confluent_schema_registry_url", registry_url)
            .with_named("consumer_config_updates", string_map(&self.consumer_config))
            .with_named(
                "error_handling",
                error_handling_value(self.error_output.as_deref())?,
            )
            .with_named("file_descriptor_path", self.format.file_descriptor_path())
            .with_named("format", Some(self.format.as_str()))
            .with_named("max_read_time_seconds", self.max_read_time_seconds)
            .with_named("message_name", self.format.message_name())
            .with_named("num_partitions", self.num_partitions)
            .with_named("offset_deduplication", self.offset_deduplication)
            .with_named(
                "redistribute_by_record_key",
                self.redistribute_by_record_key,
            )
            .with_named("redistribute_num_keys", self.redistribute_num_keys)
            .with_named("redistributed", self.redistributed)
            .with_named("schema", self.format.schema())
            .with_named("topic", Some(topic))
            .with_named("with_gcp_adc", self.with_gcp_adc)
            .build()
            .map_err(|e| ExpansionError::Encoding(format!("Failed to build KafkaRead config: {e}")))
    }

    /// The equivalent [`ManagedRead`] of `kafka_read:v1`.
    pub fn to_managed(&self) -> Result<ManagedRead, ExpansionError> {
        let config_row = self.build_config_row()?;
        Ok(ManagedRead::new(URN_KAFKA_READ)
            .with_config_row(&config_row)
            .with_expansion_service(&self.expansion_service))
    }

    /// Builds the underlying [`ExternalSource`] transform, expanded through Managed.
    pub fn build(&self) -> Result<ExternalSource, ExpansionError> {
        self.to_managed()?.build()
    }
}

impl TryPTransform<PBegin> for KafkaRead {
    type Error = ExpansionError;

    fn try_expand(&self, input: &PBegin) -> Result<PCollection<Row>, ExpansionError> {
        self.build()?.try_expand(input)
    }
}

impl PTransform<PBegin> for KafkaRead {
    type Output = PCollection<Row>;

    /// # Panics
    ///
    /// Panics if the expansion fails. Use `try_apply` to get the error.
    fn expand(&self, input: &PBegin) -> Self::Output {
        self.try_expand(input)
            .unwrap_or_else(|e| panic!("the transform could not be expanded: {e}"))
    }
}

/// Builder and transform for writing rows as Kafka record values, with a null key.
#[derive(Clone, Debug)]
pub struct KafkaWrite {
    bootstrap_servers: String,
    topic: String,
    format: KafkaFormat,
    producer_config: BTreeMap<String, String>,
    with_gcp_adc: Option<bool>,
    error_output: Option<String>,
    expansion_service: String,
}

static KAFKA_WRITE_CONFIG_SCHEMA: LazyLock<Arc<Schema>> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("bootstrap_servers", FieldType::string()),
        Field::nullable("error_handling", error_handling_type()),
        Field::nullable("file_descriptor_path", FieldType::string()),
        Field::new("format", FieldType::string()),
        Field::nullable("message_name", FieldType::string()),
        Field::nullable("producer_config_updates", string_map_type()),
        Field::nullable("schema", FieldType::string()),
        Field::new("topic", FieldType::string()),
        Field::nullable("with_gcp_adc", FieldType::boolean()),
    ]))
});

impl KafkaWrite {
    /// Writes to `topic` on the brokers `bootstrap_servers` (`host1:port1,host2:port2`).
    pub fn new(bootstrap_servers: impl Into<String>, topic: impl Into<String>) -> Self {
        Self {
            bootstrap_servers: bootstrap_servers.into(),
            topic: topic.into(),
            format: KafkaFormat::Raw,
            producer_config: BTreeMap::new(),
            with_gcp_adc: None,
            error_output: None,
            expansion_service: DEFAULT_EXPANSION_SERVICE.to_string(),
        }
    }

    /// Encoding of record values (default [`KafkaFormat::Raw`]). [`KafkaFormat::String`]
    /// is not supported for writes; use `Raw` with UTF-8 bytes.
    pub fn with_format(mut self, format: KafkaFormat) -> Self {
        self.format = format;
        self
    }

    /// Adds a Kafka producer property, e.g. `compression.type`, `security.protocol` or
    /// `schema.registry.url` (which switches AVRO writes to the Confluent serializer).
    pub fn with_producer_config(
        mut self,
        key: impl Into<String>,
        value: impl Into<String>,
    ) -> Self {
        self.producer_config.insert(key.into(), value.into());
        self
    }

    /// Sets the producer record batch compression codec (`compression.type`).
    pub fn with_compression(self, compression: Compression) -> Self {
        self.with_producer_config("compression.type", compression.as_str())
    }

    /// Adds [`google_managed_kafka_auth_config`].
    pub fn with_google_managed_kafka_auth(mut self) -> Self {
        self.producer_config
            .extend(google_managed_kafka_auth_config());
        self
    }

    /// Authenticates with a Google Managed Kafka cluster with the worker's Application Default
    /// Credentials.
    pub fn with_gcp_adc(mut self, enabled: bool) -> Self {
        self.with_gcp_adc = Some(enabled);
        self
    }

    /// Sends rows that fail to serialize to an extra output tagged `output`, as rows
    /// `(error_message: STRING, failed_row: ROW)`, instead of failing the bundle. To read
    /// them, expand `to_managed()?.with_outputs()`.
    pub fn with_error_handling(mut self, output: impl Into<String>) -> Self {
        self.error_output = Some(output.into());
        self
    }

    /// The error output tag set by [`Self::with_error_handling`].
    pub fn error_output(&self) -> Option<&str> {
        self.error_output.as_deref()
    }

    /// Configures the cross-language expansion service address.
    pub fn with_expansion_service(mut self, endpoint: impl Into<String>) -> Self {
        self.expansion_service = endpoint.into();
        self
    }

    /// Returns the bootstrap servers string.
    pub fn bootstrap_servers(&self) -> &str {
        &self.bootstrap_servers
    }

    /// Returns the topic name.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Returns the configured Kafka format.
    pub fn format(&self) -> &KafkaFormat {
        &self.format
    }

    /// Returns the producer configuration map.
    pub fn producer_config(&self) -> &BTreeMap<String, String> {
        &self.producer_config
    }

    /// Returns the expansion service address.
    pub fn expansion_service(&self) -> &str {
        &self.expansion_service
    }

    /// Constructs the configuration [`Row`] expected by
    /// `KafkaWriteSchemaTransformConfiguration` (snake_case, sorted by name).
    pub fn build_config_row(&self) -> Result<Row, ExpansionError> {
        let op = "KafkaWrite";
        let bootstrap_servers = require(&self.bootstrap_servers, "bootstrap_servers", op)?;
        let topic = require(&self.topic, "topic", op)?;
        match &self.format {
            KafkaFormat::String => {
                return Err(invalid(
                    "KafkaWrite does not support STRING format; use RAW with UTF-8 bytes",
                ));
            }
            KafkaFormat::Proto {
                schema: Some(_),
                file_descriptor_path: Some(_),
                ..
            } => {
                return Err(invalid(
                    "KafkaWrite with PROTO format takes a schema or a file_descriptor_path, not both",
                ));
            }
            KafkaFormat::Proto {
                schema: None,
                file_descriptor_path: None,
                ..
            } => {
                return Err(invalid(
                    "KafkaWrite with PROTO format requires a schema or file_descriptor_path",
                ));
            }
            _ => {}
        }

        Row::builder(Arc::clone(&KAFKA_WRITE_CONFIG_SCHEMA))
            .with_named("bootstrap_servers", Some(bootstrap_servers))
            .with_named(
                "error_handling",
                error_handling_value(self.error_output.as_deref())?,
            )
            .with_named("file_descriptor_path", self.format.file_descriptor_path())
            .with_named("format", Some(self.format.as_str()))
            .with_named("message_name", self.format.message_name())
            .with_named("producer_config_updates", string_map(&self.producer_config))
            .with_named("schema", self.format.schema())
            .with_named("topic", Some(topic))
            .with_named("with_gcp_adc", self.with_gcp_adc)
            .build()
            .map_err(|e| {
                ExpansionError::Encoding(format!("Failed to build KafkaWrite config: {e}"))
            })
    }

    /// The equivalent [`ManagedWrite`] of `kafka_write:v1`.
    pub fn to_managed(&self) -> Result<ManagedWrite, ExpansionError> {
        let config_row = self.build_config_row()?;
        Ok(ManagedWrite::new(URN_KAFKA_WRITE)
            .with_config_row(&config_row)
            .with_expansion_service(&self.expansion_service))
    }

    /// Builds the underlying [`ExternalSink`] transform, expanded through Managed.
    pub fn build(&self) -> Result<ExternalSink, ExpansionError> {
        self.to_managed()?.build()
    }
}

impl TryPTransform<PCollection<Row>> for KafkaWrite {
    type Error = ExpansionError;

    fn try_expand(&self, input: &PCollection<Row>) -> Result<PDone, ExpansionError> {
        self.build()?.try_expand(input)
    }
}

impl PTransform<PCollection<Row>> for KafkaWrite {
    type Output = PDone;

    /// # Panics
    ///
    /// Panics if the expansion fails. Use `try_apply` to get the error.
    fn expand(&self, input: &PCollection<Row>) -> Self::Output {
        self.try_expand(input)
            .unwrap_or_else(|e| panic!("the transform could not be expanded: {e}"))
    }
}
