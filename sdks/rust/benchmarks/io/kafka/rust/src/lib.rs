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

//! Kafka benchmark in a Rust pipeline: the streaming `copy` shape with the Kafka connector of
//! the Rust SDK.
//!
//! The pipeline reads `--topic` with `Read`, sends each element through a Rust step, and
//! writes the elements to `--destination_topic` with `Write`. All steps run in one Rust SDK
//! harness, with no cross-language transform. The format picks the codec of both ends:
//! `JSON` reads and writes rows with the `Json` codec, `RAW` rows of one `BYTES` field (the
//! Managed `RAW` format), and `BYTES` the record values as `Vec<u8>` (the `Raw` codec), so with
//! `BYTES` no step builds a row, decodes JSON or encodes JSON.
//!
//! The Rust step (`Touch`) counts `e2e/rows` and records `e2e/latency_ms`: the wall time minus
//! the element timestamp, which is the Kafka record timestamp. It also counts each latency in
//! one `e2e/latency_le_<ms>` bucket. `benchmarks/tools/io_report.py` reads these metrics, so keep
//! their names the same as in `../java/KafkaBenchmark.java` and `../python/kafka_benchmark.py`.
//!
//! The benchmark script drains the job, not this program. `./gradlew
//! :sdks:rust:kafkaBenchmark -Ppipeline=rust` starts the job with `--async_job`, and
//! `benchmarks/tools/job_wait.py` polls Dataflow until the job is done.
//!
//! On Prism, `--max_read_seconds` stops the read, and the program prints the `e2e` metrics:
//!
//! ```text
//! ./gradlew :sdks:rust:prism -Pexample=apache-beam-benchmark-kafka -PextraArgs="--bootstrap_servers=localhost:9092 \
//!   --topic=in --destination_topic=out --with_gcp_adc=false --max_read_seconds=60"
//! ```

use std::sync::Arc;
use std::time::Duration;

use beam::io::kafka::codec::Payload;
use beam::io::kafka::codec::{Decoder, Encoder, Json, Raw};
use beam::io::kafka::read::{ConsumerPool, PoolSize, Read, ReadOptions, StartOffset};
use beam::io::kafka::write::{Write, WriteOptions};
use beam::options::PipelineOptionGroup;
use beam::prelude::*;
use beam::schema::Schema;
use beam_benchmark_io::{
    AutoOffsetReset, Connector, TouchCopyFn, parse_consumer_pool_size, parse_property,
};
use clap::{Args as ClapArgs, ValueEnum};
use serde::{Deserialize, Serialize};

/// The JSON Schema of `bigquery-public-data:samples.wikipedia`. Keep it the same as
/// `WIKIPEDIA_JSON_SCHEMA` in `KafkaBenchmark.java` and `kafka_benchmark.py`.
pub const WIKIPEDIA_JSON_SCHEMA: &str = r#"{"type":"object","properties":{
"title":{"type":"string"},
"id":{"type":"integer","nullable":true},
"language":{"type":"string"},
"wp_namespace":{"type":"integer"},
"is_redirect":{"type":"boolean","nullable":true},
"revision_id":{"type":"integer","nullable":true},
"contributor_ip":{"type":"string","nullable":true},
"contributor_id":{"type":"integer","nullable":true},
"contributor_username":{"type":"string","nullable":true},
"timestamp":{"type":"integer"},
"is_minor":{"type":"boolean","nullable":true},
"is_bot":{"type":"boolean","nullable":true},
"reversion_id":{"type":"integer","nullable":true},
"comment":{"type":"string","nullable":true},
"num_characters":{"type":"integer"}},
"required":["title","id","language","wp_namespace","is_redirect","revision_id",
"contributor_ip","contributor_id","contributor_username","timestamp","is_minor","is_bot",
"reversion_id","comment","num_characters"]}"#;

/// The shape of the benchmark. This pipeline runs only the streaming copy.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, ValueEnum)]
#[serde(rename_all = "lowercase")]
pub enum Shape {
    /// Reads a topic and writes each row to another topic.
    Copy,
}

/// The format of the Kafka records.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, ValueEnum)]
#[serde(rename_all = "UPPERCASE")]
#[value(rename_all = "UPPERCASE")]
pub enum Format {
    /// JSON records of the Wikipedia schema, read into rows.
    Json,
    /// The record bytes, in rows of one `BYTES` field.
    Raw,
    /// The record bytes as `Vec<u8>`, with no row anywhere: the `Raw` codec on both ends and a
    /// `Vec<u8>` step.
    Bytes,
}

/// Benchmark flags. The flags of the runner come from the pipeline options.
#[derive(ClapArgs, Serialize, Deserialize, Clone, Debug)]
pub struct Args {
    /// The connector of the read.
    #[arg(long, value_enum, default_value_t = Connector::Rust)]
    pub reader: Connector,
    /// The connector of the write.
    #[arg(long, value_enum, default_value_t = Connector::Rust)]
    pub writer: Connector,
    /// The shape of the benchmark.
    #[arg(long, value_enum, default_value_t = Shape::Copy)]
    pub shape: Shape,
    /// The format of the records.
    #[arg(long, value_enum, ignore_case = true, default_value_t = Format::Json)]
    pub format: Format,
    /// Kafka bootstrap servers (host:port).
    #[arg(long)]
    pub bootstrap_servers: String,
    /// Topic that copy reads.
    #[arg(long)]
    pub topic: String,
    /// Topic that copy writes to.
    #[arg(long)]
    pub destination_topic: String,
    /// Where a partition with no committed offset starts.
    #[arg(long, value_enum, ignore_case = true, default_value_t = AutoOffsetReset::Earliest)]
    pub auto_offset_reset: AutoOffsetReset,
    /// Kafka consumer group.id. With a group, the read commits the offsets. If not set, the
    /// read has no group and commits no offsets.
    #[arg(long)]
    pub group_id: Option<String>,
    /// More properties of the Kafka consumer, as `key=value` pairs with a comma between them.
    /// A property here replaces the same property of the other flags, for example `group.id`.
    /// The flag does not trim spaces, and a key that is empty or has a space is an error.
    #[arg(long, value_delimiter = ',', value_parser = parse_property)]
    pub consumer_properties: Vec<(String, String)>,
    /// More properties of the Kafka producer, as `key=value` pairs with a comma between them.
    /// The flag does not trim spaces, and a key that is empty or has a space is an error.
    #[arg(long, value_delimiter = ',', value_parser = parse_property)]
    pub producer_properties: Vec<(String, String)>,
    /// The number of consumers of the read in a worker, from 1 to 16. If not set, the read
    /// uses one consumer for each partition that the worker reads, at most 16.
    #[arg(long, value_parser = parse_consumer_pool_size)]
    pub consumer_pool_size: Option<usize>,
    /// Authenticate with Google Application Default Credentials, for Google Cloud Managed
    /// Service for Apache Kafka. Set `--with_gcp_adc=false` for a local cluster.
    #[arg(
        long,
        num_args = 0..=1,
        default_missing_value = "true",
        default_value_t = true,
        action = clap::ArgAction::Set
    )]
    pub with_gcp_adc: bool,
    /// Stops the read after this time, so the pipeline ends. For local runs only. If not set,
    /// the read has no end and the job is a streaming job.
    #[arg(long)]
    pub max_read_seconds: Option<u64>,
    /// The longest time in milliseconds that one call of the reader reads a partition. If not
    /// set, the read uses the default of the connector. On Dataflow, pass it in
    /// `-PextraArgs`.
    #[arg(long, value_parser = clap::value_parser!(u64).range(1..))]
    pub max_read_time_per_call_ms: Option<u64>,
    /// The longest time in milliseconds that one call of the reader waits for the next
    /// record. If not set, the read uses the default of the connector. On Dataflow, pass it
    /// in `-PextraArgs`.
    #[arg(long, value_parser = clap::value_parser!(u64).range(1..))]
    pub max_idle_per_call_ms: Option<u64>,
}

impl PipelineOptionGroup for Args {}

/// The options of the read of `--topic`: the start offset, the consumer group
/// and properties, the consumer pool, Google authentication, the max read time, the max read
/// time per call and the max idle time per call of the flags.
///
/// # Errors
///
/// Fails if `--consumer_pool_size` is not a valid [`PoolSize`].
pub fn read_options(args: &Args) -> Result<ReadOptions> {
    let start = match args.auto_offset_reset {
        AutoOffsetReset::Earliest => StartOffset::Earliest,
        AutoOffsetReset::Latest => StartOffset::Latest,
    };
    let mut options =
        ReadOptions::new(&args.bootstrap_servers, &args.topic).with_start_offset(start);
    if let Some(group_id) = &args.group_id {
        options = options.with_consumer_properties([("group.id".to_string(), group_id.clone())]);
    }
    options = options.with_consumer_properties(args.consumer_properties.iter().cloned());
    if let Some(size) = args.consumer_pool_size {
        options = options.with_consumer_pool(ConsumerPool::Size(PoolSize::try_from(size)?));
    }
    if args.with_gcp_adc {
        options = options.with_google_auth();
    }
    if let Some(seconds) = args.max_read_seconds {
        options = options.with_max_read_time(Duration::from_secs(seconds));
    }
    if let Some(millis) = args.max_read_time_per_call_ms {
        options = options.with_max_read_time_per_call(Duration::from_millis(millis));
    }
    if let Some(millis) = args.max_idle_per_call_ms {
        options = options.with_max_idle_per_call(Duration::from_millis(millis));
    }
    Ok(options)
}

/// The options of the write to `--destination_topic`: the producer properties of
/// `--producer_properties`, and Google authentication if the flags set it.
pub fn write_options(args: &Args) -> WriteOptions {
    let options = WriteOptions::new(&args.bootstrap_servers, &args.destination_topic)
        .with_producer_properties(args.producer_properties.iter().cloned());
    if args.with_gcp_adc {
        options.with_google_auth()
    } else {
        options
    }
}

/// Builds the copy pipeline: read, `Touch` and write. With `BYTES`, the three steps pass
/// `Vec<u8>` and no step builds a row.
pub fn build_pipeline(options: &PipelineOptions, args: &Args) -> Result<Pipeline> {
    let Args {
        reader: Connector::Rust,
        writer: Connector::Rust,
        shape: Shape::Copy,
        format,
        ..
    } = args;
    let p = Pipeline::create(options);
    let read = Read::new(read_options(args)?);
    let write = Write::new(write_options(args));
    // The write sends the records that Kafka rejects to its error output. This pipeline does
    // not read that output.
    match format {
        Format::Bytes => {
            p.apply("Read", read.with_decoder(Raw))
                .par_do("Touch", TouchCopyFn::<Vec<u8>>::new())
                .apply("Write", write.with_encoder(Raw));
        }
        Format::Raw => {
            let codec = Payload::raw();
            let schema = Arc::clone(codec.row_schema());
            copy_rows(
                &p,
                read.with_decoder(codec.clone()),
                &schema,
                write.with_encoder(codec),
            );
        }
        Format::Json => {
            let codec = Json::schema(WIKIPEDIA_JSON_SCHEMA)?;
            let schema = codec
                .row_schema()
                .ok_or_else(|| Error::msg("Json::schema always defines a row schema"))?;
            copy_rows(
                &p,
                read.with_decoder(codec),
                &schema,
                write.with_encoder(Json::new()),
            );
        }
    }
    Ok(p)
}

/// Adds `read`, `Touch` and `write` of rows of `schema` to `p`.
fn copy_rows<D, E>(p: &Pipeline, read: Read<D>, schema: &Arc<Schema>, write: Write<E>)
where
    D: Decoder<Output = Row> + Clone,
    E: Encoder<Input = Row>,
{
    p.apply("Read", read)
        .par_do("Touch", TouchCopyFn::<Row>::new())
        // A Row-to-Row ParDo does not keep the row schema of its input, and the write needs it.
        .with_row_schema(schema)
        .apply("Write", write);
}
