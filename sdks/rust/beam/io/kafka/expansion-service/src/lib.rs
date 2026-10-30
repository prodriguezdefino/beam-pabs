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

//! SchemaTransform providers for the native Kafka connector.
//!
//! The configuration of each provider has the field names and types of the Kafka
//! SchemaTransforms of the Managed API, so a caller can send the same configuration to either
//! expansion service. Options that only this connector has are extra nullable fields.

use std::collections::{BTreeMap, HashMap};
use std::time::Duration;

use beam::io::kafka::native::client::MAX_CONSUMERS;
use beam::io::kafka::native::codec::Decoder;
use beam::io::kafka::native::codec::managed::Format;
use beam::io::kafka::native::read::{ConsumerPool, PoolSize, Read, ReadOptions, StartOffset};
use beam::io::kafka::native::write::{Write, WriteOptions};
use beam::pipeline::Pipeline;
use beam::schema::{BeamRow, Row, Schema};
use beam::transforms::{DoFn, ParDo, ProcessContext};
use expansion::{
    PCollectionId, SchemaTransformProvider, TransformInputs, register_schema_transform,
};

/// Identifier of [`KafkaReadProvider`].
pub const URN_KAFKA_READ: &str = "beam:schematransform:org.apache.beam:rust:kafka_read:v1";

/// Identifier of [`KafkaWriteProvider`].
pub const URN_KAFKA_WRITE: &str = "beam:schematransform:org.apache.beam:rust:kafka_write:v1";

/// The output tag of the rejected rows when the configuration names no output.
const ERRORS_TAG: &str = "errors";

/// Where the rows that a transform rejects go: the output tag of the transform.
#[derive(Clone, Debug, Default, PartialEq, BeamRow)]
pub struct ErrorHandling {
    pub output: String,
}

/// The configuration of [`KafkaReadProvider`]. An empty string counts as unset.
#[derive(Clone, Debug, Default, PartialEq, BeamRow)]
pub struct KafkaReadConfig {
    /// `RAW`, `STRING`, `JSON` or `AVRO`.
    pub format: String,
    pub topic: String,
    pub bootstrap_servers: String,
    /// The JSON Schema of the `JSON` format, or the Avro schema of the `AVRO` format.
    pub schema: Option<String>,
    /// librdkafka consumer properties. A `group.id` makes the read commit offsets.
    pub consumer_config_updates: Option<BTreeMap<String, String>>,
    /// Where a partition with no committed offset starts: `earliest`, `latest` or `none`.
    /// Without it, the `auto.offset.reset` consumer property, or else `latest`. That consumer
    /// property also sets what librdkafka does when a fetch position is out of range, so set
    /// the start here to keep that reset at the default `earliest`.
    pub auto_offset_reset_config: Option<String>,
    /// The time after which the read stops, so the output is bounded.
    pub max_read_time_seconds: Option<i32>,
    pub error_handling: Option<ErrorHandling>,
    /// For the `AVRO` format with a schema registry, which this provider does not support yet.
    pub confluent_schema_registry_url: Option<String>,
    /// For the `AVRO` format with a schema registry, which this provider does not support yet.
    pub confluent_schema_registry_subject: Option<String>,
    /// For the `PROTO` format, which this provider does not support yet.
    pub file_descriptor_path: Option<String>,
    /// For the `PROTO` format, which this provider does not support yet.
    pub message_name: Option<String>,
    /// Authenticates with Google Application Default Credentials (SASL/OAUTHBEARER), for
    /// Google Cloud Managed Service for Apache Kafka.
    pub with_gcp_adc: Option<bool>,
    /// How long after a bundle its offset commit can still run. The default is 10 minutes.
    pub commit_expiry_seconds: Option<i64>,
    /// The longest time that one call of the reader reads a partition. The default is 5
    /// seconds. A shorter time makes smaller bundles, so a rejected bundle reads fewer
    /// records again. The watermark moves only when a call ends, so the data freshness of
    /// the job is at least this time.
    pub max_read_time_per_call_seconds: Option<i64>,
    /// The longest time in milliseconds that one call of the reader waits for the next
    /// record. The default is 1 second. A partition with no record for this time ends the
    /// call, and the read resumes it.
    pub max_idle_per_call_ms: Option<i64>,
    /// The number of consumers of the read in a worker process, from 1 to 16. The default is
    /// one consumer for each partition that the worker reads, at most 16.
    pub consumer_pool_size: Option<i64>,
    /// Spreads the partitions over the workers with a Reshuffle before the read. Only a
    /// bounded read, with `max_read_time_seconds`, accepts it. Use it on a batch runner that
    /// does not spread the input of a splittable DoFn over its workers.
    /// This is not the `redistribute` option of the Managed read, which runs after the read.
    pub reshuffle_partitions: Option<bool>,
}

impl KafkaReadConfig {
    /// The read options and the record format of this configuration, or why they are not
    /// valid.
    pub fn read_options(self) -> Result<(ReadOptions, Format), String> {
        let Self {
            format,
            topic,
            bootstrap_servers,
            schema,
            consumer_config_updates,
            auto_offset_reset_config,
            max_read_time_seconds,
            confluent_schema_registry_url,
            with_gcp_adc,
            commit_expiry_seconds,
            max_read_time_per_call_seconds,
            max_idle_per_call_ms,
            consumer_pool_size,
            reshuffle_partitions,
            ..
        } = self;
        if confluent_schema_registry_url.is_some_and(|url| !url.is_empty()) {
            return Err("The native Kafka read does not support a schema registry yet".to_string());
        }
        let properties = consumer_config_updates.unwrap_or_default();
        let start_offset: StartOffset = auto_offset_reset_config
            .filter(|reset| !reset.is_empty())
            .or_else(|| properties.get("auto.offset.reset").cloned())
            .map(|reset| reset.parse())
            .transpose()
            .map_err(|e: beam::Error| e.to_string())?
            .unwrap_or_default();
        let format = Format::parse(&format, schema.as_deref().filter(|s| !s.is_empty()))
            .and_then(|format| format.validate_read().map(|()| format))
            .map_err(|e| e.to_string())?;
        let seconds = |name: &str, value: i64| match u64::try_from(value) {
            Ok(seconds) if seconds > 0 => Ok(Duration::from_secs(seconds)),
            _ => Err(format!("{name} must be larger than zero, got {value}")),
        };
        let mut options = ReadOptions::new(bootstrap_servers, topic)
            .with_start_offset(start_offset)
            .with_consumer_properties(properties);
        if let Some(value) = max_read_time_seconds {
            options = options.with_max_read_time(seconds("max_read_time_seconds", value.into())?);
        }
        if let Some(value) = commit_expiry_seconds {
            options = options.with_commit_expiry(seconds("commit_expiry_seconds", value)?);
        }
        if let Some(value) = max_read_time_per_call_seconds {
            options = options
                .with_max_read_time_per_call(seconds("max_read_time_per_call_seconds", value)?);
        }
        if let Some(value) = max_idle_per_call_ms {
            let millis = u64::try_from(value)
                .ok()
                .filter(|millis| *millis > 0)
                .ok_or_else(|| {
                    format!("max_idle_per_call_ms must be larger than zero, got {value}")
                })?;
            options = options.with_max_idle_per_call(Duration::from_millis(millis));
        }
        if let Some(value) = consumer_pool_size {
            let size = usize::try_from(value)
                .ok()
                .and_then(|size| PoolSize::try_from(size).ok())
                .ok_or_else(|| {
                    format!("consumer_pool_size must be 1 to {MAX_CONSUMERS}, got {value}")
                })?;
            options = options.with_consumer_pool(ConsumerPool::Size(size));
        }
        if with_gcp_adc == Some(true) {
            options = options.with_google_auth();
        }
        let options = options.with_reshuffle_partitions(reshuffle_partitions == Some(true));
        options.validate().map_err(|e| e.to_string())?;
        Ok((options, format))
    }
}

/// Reads the records of a Kafka topic into Beam rows. See [`KafkaReadConfig`] for the
/// configuration.
///
/// The rows go to the output `output`. With `error_handling`, records that do not convert go
/// to the output that it names, with the fields `failed_row` and `error_message`. Without it,
/// such a record fails the pipeline.
#[derive(Default)]
pub struct KafkaReadProvider;

impl SchemaTransformProvider for KafkaReadProvider {
    fn identifier(&self) -> &'static str {
        URN_KAFKA_READ
    }

    fn description(&self) -> &'static str {
        "Reads the records of a Kafka topic as RAW, STRING, JSON or AVRO values into Beam rows."
    }

    fn config_schema(&self) -> Schema {
        (**KafkaReadConfig::beam_schema()).clone()
    }

    fn input_tags(&self) -> Vec<String> {
        Vec::new()
    }

    fn build_transform(
        &self,
        config: Row,
        _inputs: TransformInputs,
        pipeline: &mut Pipeline,
    ) -> beam::Result<HashMap<String, PCollectionId>> {
        let mut config = KafkaReadConfig::from_row(&config).map_err(beam::Error::msg)?;
        let errors = config
            .error_handling
            .take()
            .map(|ErrorHandling { output }| output)
            .filter(|output| !output.is_empty());
        let (options, format) = config.read_options()?;
        let begin = pipeline.begin();
        let read = Read::new(options);
        match format {
            Format::Raw(codec) => read_rows(read.with_decoder(codec), &begin, errors),
            Format::String(codec) => read_rows(read.with_decoder(codec), &begin, errors),
            Format::Json(codec) => read_rows(read.with_decoder(codec), &begin, errors),
            Format::Avro(codec) => read_rows(read.with_decoder(codec), &begin, errors),
        }
    }
}

/// Expands `read` into the output `output`, and into the output `errors` if it is set.
fn read_rows<D: Decoder<Output = Row> + Clone>(
    read: Read<D>,
    begin: &beam::values::PBegin,
    errors: Option<String>,
) -> beam::Result<HashMap<String, PCollectionId>> {
    Ok(match errors {
        Some(errors) => {
            let outputs = begin.try_apply("ReadFromKafka", read.with_failures())?;
            HashMap::from([
                ("output".to_string(), outputs.output.id().into()),
                (errors, outputs.failures.id().into()),
            ])
        }
        None => {
            let rows = begin.try_apply("ReadFromKafka", read)?;
            HashMap::from([("output".to_string(), rows.id().into())])
        }
    })
}

register_schema_transform!(KafkaReadProvider);

/// The configuration of [`KafkaWriteProvider`]. An empty string counts as unset.
#[derive(Clone, Debug, Default, PartialEq, BeamRow)]
pub struct KafkaWriteConfig {
    /// `RAW`, `STRING`, `JSON` or `AVRO`.
    pub format: String,
    pub topic: String,
    pub bootstrap_servers: String,
    /// librdkafka producer properties.
    pub producer_config_updates: Option<BTreeMap<String, String>>,
    pub error_handling: Option<ErrorHandling>,
    /// For the `PROTO` format, which this provider does not support yet.
    pub file_descriptor_path: Option<String>,
    /// For the `PROTO` format, which this provider does not support yet.
    pub message_name: Option<String>,
    /// For the `AVRO` format: the Avro schema of the datums, whose fields match the fields of
    /// the input rows. Without it, the datums have the Avro schema of the Beam schema of the
    /// rows. `JSON` writes the fields of the input rows.
    pub schema: Option<String>,
    /// Authenticates with Google Application Default Credentials (SASL/OAUTHBEARER), for
    /// Google Cloud Managed Service for Apache Kafka.
    pub with_gcp_adc: Option<bool>,
}

impl KafkaWriteConfig {
    /// The write options and the record format of this configuration, or why they are not
    /// valid.
    pub fn write_options(self) -> Result<(WriteOptions, Format), String> {
        let Self {
            format,
            topic,
            bootstrap_servers,
            producer_config_updates,
            schema,
            with_gcp_adc,
            ..
        } = self;
        let format = Format::parse(&format, schema.as_deref().filter(|s| !s.is_empty()))
            .map_err(|e| e.to_string())?;
        let options = WriteOptions::new(bootstrap_servers, topic)
            .with_producer_properties(producer_config_updates.unwrap_or_default());
        let options = match with_gcp_adc {
            Some(true) => options.with_google_auth(),
            _ => options,
        };
        options.validate().map_err(|e| e.to_string())?;
        Ok((options, format))
    }
}

/// Writes rows to a Kafka topic, at least once. See [`KafkaWriteConfig`] for the
/// configuration.
///
/// The rejected rows go to the output that `error_handling` names, with the fields
/// `failed_row` and `error_message`. Without `error_handling`, a rejected row fails the
/// pipeline, and the output `errors` is empty.
#[derive(Default)]
pub struct KafkaWriteProvider;

impl SchemaTransformProvider for KafkaWriteProvider {
    fn identifier(&self) -> &'static str {
        URN_KAFKA_WRITE
    }

    fn description(&self) -> &'static str {
        "Writes Beam rows to a Kafka topic as RAW, STRING, JSON or AVRO record values, at least once."
    }

    fn config_schema(&self) -> Schema {
        (**KafkaWriteConfig::beam_schema()).clone()
    }

    fn input_tags(&self) -> Vec<String> {
        vec!["input".to_string()]
    }

    fn build_transform(
        &self,
        config: Row,
        inputs: TransformInputs,
        pipeline: &mut Pipeline,
    ) -> beam::Result<HashMap<String, PCollectionId>> {
        let mut config = KafkaWriteConfig::from_row(&config).map_err(beam::Error::msg)?;
        let output = config
            .error_handling
            .take()
            .map(|ErrorHandling { output }| output)
            .filter(|output| !output.is_empty());
        let rows = inputs
            .get::<Row>(pipeline, "input")
            .map_err(beam::Error::msg)?;
        let (options, format) = config.write_options()?;
        let write = Write::new(options);
        let errors = match format {
            Format::Raw(codec) => rows.try_apply("WriteToKafka", write.with_encoder(codec)),
            Format::String(codec) => rows.try_apply("WriteToKafka", write.with_encoder(codec)),
            Format::Json(codec) => rows.try_apply("WriteToKafka", write.with_encoder(codec)),
            Format::Avro(codec) => rows.try_apply("WriteToKafka", write.with_encoder(codec)),
        }?;
        let (tag, errors) = match output {
            Some(output) => (output, errors),
            None => (
                ERRORS_TAG.to_string(),
                errors.apply("FailOnError", ParDo::new(FailOnErrorFn)),
            ),
        };
        Ok(HashMap::from([(tag, errors.id().into())]))
    }
}

register_schema_transform!(KafkaWriteProvider);

/// Fails the bundle on the first rejected row, so its output has no elements.
#[derive(Clone)]
struct FailOnErrorFn;

impl DoFn for FailOnErrorFn {
    type In = Row;
    type Out = Row;

    fn process_element(&mut self, row: Row, _ctx: &mut ProcessContext<'_, Row>) -> beam::Result {
        let reason = row
            .get_string("error_message")
            .ok()
            .flatten()
            .unwrap_or_default();
        Err(beam::Error::msg(format!(
            "Kafka rejected a row, and the write has no error_handling output: {reason}"
        )))
    }
}
