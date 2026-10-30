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

//! Kafka benchmark in a Rust pipeline: the streaming `copy` shape with the native Kafka
//! connector.
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
//! one `e2e/latency_le_<ms>` bucket. `../../io_report.py` reads these metrics, so keep their
//! names the same as in `../java/KafkaBenchmark.java` and `../python/kafka_benchmark.py`.
//!
//! The benchmark script drains the job, not this program. `./gradlew
//! :sdks:rust:kafkaBenchmark -Ppipeline=rust` starts the job with `--async_job`, and
//! `../../job_wait.py` polls Dataflow until the job is done.
//!
//! On Prism, `--max_read_seconds` stops the read, and the program prints the `e2e` metrics:
//!
//! ```text
//! ./gradlew :sdks:rust:prism -Pexample=kafka_benchmark -PextraArgs="--bootstrap_servers=localhost:9092 \
//!   --topic=in --destination_topic=out --with_gcp_adc=false --max_read_seconds=60"
//! ```

use std::iter;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;

use beam::coders::DefaultCoder;
use beam::io::kafka::native::codec::managed::Payload;
use beam::io::kafka::native::codec::{Decoder, Encoder, Json, Raw};
use beam::io::kafka::native::read::{ConsumerPool, PoolSize, Read, ReadOptions, StartOffset};
use beam::io::kafka::native::write::{Write, WriteOptions};
use beam::metrics::MetricResults;
use beam::options::PipelineOptionGroup;
use beam::prelude::*;
use beam::schema::Schema;
use clap::{Args as ClapArgs, ValueEnum};
use serde::{Deserialize, Serialize};

/// The namespace of the metrics of the `Touch` step.
pub const NAMESPACE: &str = "e2e";

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

/// Upper bounds (ms) of the latency buckets. `e2e/latency_le_<bound>` counts the rows with a
/// latency above the previous bound and at most this bound; `e2e/latency_le_inf` counts the
/// rest. Keep in sync with `LATENCY_BOUNDS_MS` in `KafkaBenchmark.java`, in
/// `kafka_benchmark.py` and in `io_report.py`.
pub const LATENCY_BOUNDS_MS: [i64; 12] = [
    50, 100, 250, 500, 1_000, 2_000, 5_000, 10_000, 30_000, 60_000, 120_000, 300_000,
];

/// The Kafka connector of the read or the write. The launcher sends `--reader` and `--writer`
/// for every pipeline, and this pipeline has only the native connector.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, ValueEnum)]
#[serde(rename_all = "lowercase")]
pub enum Connector {
    /// The native Kafka connector of the Rust SDK.
    Rust,
}

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

/// Where a partition with no committed offset starts, as the Kafka property
/// `auto.offset.reset`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, ValueEnum)]
#[serde(rename_all = "lowercase")]
pub enum AutoOffsetReset {
    /// The first record of the partition.
    Earliest,
    /// The next record that a producer writes to the partition.
    Latest,
}

impl From<AutoOffsetReset> for StartOffset {
    fn from(reset: AutoOffsetReset) -> Self {
        match reset {
            AutoOffsetReset::Earliest => StartOffset::Earliest,
            AutoOffsetReset::Latest => StartOffset::Latest,
        }
    }
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

/// Splits one `key=value` pair of `--consumer_properties` at the first `=`.
///
/// # Errors
///
/// Fails if the pair has no `=`, or if the key is empty or has a space.
pub fn parse_property(pair: &str) -> Result<(String, String), String> {
    match pair.split_once('=') {
        Some((key, _)) if key.is_empty() || key.contains(char::is_whitespace) => Err(format!(
            "The key of '{pair}' must not be empty or have a space"
        )),
        Some((key, value)) => Ok((key.to_string(), value.to_string())),
        None => Err(format!("'{pair}' is not a key=value pair")),
    }
}

/// Parses `--consumer_pool_size`.
///
/// # Errors
///
/// Fails if `value` is not an integer that [`PoolSize`] accepts.
pub fn parse_consumer_pool_size(value: &str) -> Result<usize, String> {
    let size = value
        .parse::<usize>()
        .map_err(|err| format!("'{value}' is not a valid consumer_pool_size: {err}"))?;
    PoolSize::try_from(size)
        .map(PoolSize::get)
        .map_err(|err| format!("consumer_pool_size: {err}"))
}

/// The names of the latency bucket counters: one for each bound of `LATENCY_BOUNDS_MS`, in
/// order, then `latency_le_inf`.
pub fn bucket_names() -> impl Iterator<Item = String> {
    LATENCY_BOUNDS_MS
        .iter()
        .map(|bound| format!("latency_le_{bound}"))
        .chain(iter::once("latency_le_inf".to_string()))
}

/// Counts `e2e/rows` and records `e2e/latency_ms` from the element timestamp, as a
/// distribution and as bucket counters. It emits each element of type `T` (a row, or the
/// record bytes) with no change.
pub struct TouchCopyFn<T = Row> {
    rows: Counter,
    latency_ms: Distribution,
    /// One counter for each name of [`bucket_names`].
    latency_buckets: Vec<Counter>,
    element: PhantomData<fn(T) -> T>,
}

impl<T> TouchCopyFn<T> {
    pub fn new() -> Self {
        Self {
            rows: Metrics::counter(NAMESPACE, "rows"),
            latency_ms: Metrics::distribution(NAMESPACE, "latency_ms"),
            latency_buckets: bucket_names()
                .map(|name| Metrics::counter(NAMESPACE, name))
                .collect(),
            element: PhantomData,
        }
    }
}

impl<T> Clone for TouchCopyFn<T> {
    fn clone(&self) -> Self {
        Self {
            rows: self.rows.clone(),
            latency_ms: self.latency_ms.clone(),
            latency_buckets: self.latency_buckets.clone(),
            element: PhantomData,
        }
    }
}

impl<T> Default for TouchCopyFn<T> {
    fn default() -> Self {
        Self::new()
    }
}

/// The index of the bucket of `latency`: the first bound that is not smaller than `latency`,
/// or `LATENCY_BOUNDS_MS.len()` (the `inf` bucket).
pub fn bucket(latency: i64) -> usize {
    LATENCY_BOUNDS_MS.partition_point(|&bound| latency > bound)
}

impl<T: DefaultCoder> DoFn for TouchCopyFn<T> {
    type In = T;
    type Out = T;

    fn process_element(&mut self, element: T, ctx: &mut ProcessContext<'_, T>) -> Result {
        self.rows.inc();
        let timestamp_ms = ctx.timestamp().as_millis();
        let now_ms = Instant::now().as_millis();
        if timestamp_ms > 0 && now_ms >= timestamp_ms {
            let latency = now_ms - timestamp_ms;
            self.latency_ms.update(latency);
            self.latency_buckets[bucket(latency)].inc();
        }
        ctx.emit(element)
    }
}

/// The options of the read of `--topic`: the start offset, the consumer group
/// and properties, the consumer pool, Google authentication, the max read time, the max read
/// time per call and the max idle time per call of the flags.
///
/// # Errors
///
/// Fails if `--consumer_pool_size` is not a valid [`PoolSize`].
pub fn read_options(args: &Args) -> Result<ReadOptions> {
    let mut options = ReadOptions::new(&args.bootstrap_servers, &args.topic)
        .with_start_offset(args.auto_offset_reset.into());
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
                .expect("a JSON Schema gives a row schema");
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

/// Prints the `e2e` metrics of a run.
pub fn print_metrics(metrics: &MetricResults) {
    let counter = |name: &str| metrics.counter(NAMESPACE, name).unwrap_or(0);
    println!("e2e/rows={}", counter("rows"));
    if let Some(latency) = metrics.distribution(NAMESPACE, "latency_ms") {
        let mean = if latency.count > 0 {
            latency.sum / latency.count
        } else {
            0
        };
        println!(
            "e2e/latency_ms count={} mean={mean} min={} max={}",
            latency.count, latency.min, latency.max
        );
    }
    let buckets = bucket_names()
        .map(|name| {
            let label = name.strip_prefix("latency_").unwrap_or(&name);
            format!("{label}={}", counter(&name))
        })
        .collect::<Vec<_>>();
    println!("e2e/latency buckets: {}", buckets.join(" "));
}
