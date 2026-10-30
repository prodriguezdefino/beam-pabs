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

//! Throughput and CPU cost of a Kafka copy with the native read and the native write, on
//! Prism against a local broker.
//!
//! The bench writes `--records` records to a new topic. Then it copies that topic to a new
//! topic, once for each format and run. For each copy it reports the time until the output
//! topic has all records, the records each second, and the CPU time of this process for each
//! million records. The workers run in this process (LOOPBACK), so the CPU time is that of
//! the connector and the SDK. Prism runs in its own process and does not count.
//!
//! Start a broker, then run the bench:
//!
//! ```text
//! docker run -d --name beam-kafka-bench -p 9092:9092 apache/kafka:3.9.0
//! cargo bench -p apache-beam-io-kafka --features native --bench copy_throughput -- \
//!   --records=2000000 --runs=3 --formats=bytes,json
//! ```
//!
//! With no broker at `--bootstrap_servers`, the bench says so and stops. To profile one copy:
//!
//! ```text
//! ./gradlew :sdks:rust:profilePrism -Ppkg=apache-beam-io-kafka -PcargoBench=copy_throughput \
//!   -PexampleFeatures=native -PextraArgs="--runs=1 --formats=bytes"
//! ```

use std::ffi::OsString;
use std::fmt::Write as _;
use std::mem::MaybeUninit;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use beam_sdk::options::{PipelineOptionGroup, PipelineOptions};
use beam_sdk::prelude::*;
use clap::{Args as ClapArgs, ValueEnum};
use kafka_io::native::codec::Json;
use kafka_io::native::read::{Read, ReadOptions, StartOffset};
use kafka_io::native::write::{Write, WriteOptions};
use rdkafka::ClientConfig;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
use serde::{Deserialize, Serialize};

type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// The JSON Schema of `bigquery-public-data:samples.wikipedia`, the schema of the Dataflow
/// benchmark.
const WIKIPEDIA_JSON_SCHEMA: &str = r#"{"type":"object","properties":{
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

/// The longest wait for the broker when the bench starts.
const BROKER_TIMEOUT: Duration = Duration::from_secs(5);

/// The time between two checks of the output topic.
const CHECK_INTERVAL: Duration = Duration::from_millis(250);

/// How a copy reads and writes the records.
#[derive(ValueEnum, Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
enum Format {
    /// The record values as bytes, with no conversion.
    Bytes,
    /// JSON records decoded into rows and encoded into JSON again.
    Json,
}

#[derive(ClapArgs, Serialize, Deserialize, Clone, Debug)]
struct Args {
    /// Kafka bootstrap servers (host:port).
    #[arg(long, default_value = "localhost:9092")]
    bootstrap_servers: String,
    /// The records of the input topic.
    #[arg(long, default_value_t = 1_000_000)]
    records: u64,
    /// The partitions of the input topic and of each output topic.
    #[arg(long, default_value_t = 16)]
    partitions: i32,
    /// The copies of each format.
    #[arg(long, default_value_t = 3)]
    runs: usize,
    /// The formats to copy, with commas between them.
    #[arg(long, value_enum, value_delimiter = ',', default_value = "bytes,json")]
    formats: Vec<Format>,
    /// The time after which the read of a copy stops. Make it longer than one copy.
    #[arg(long, default_value_t = 60)]
    max_read_seconds: u64,
    /// `cargo bench` gives this flag to each bench. The bench does not use it.
    #[arg(long, hide = true)]
    bench: bool,
}

impl PipelineOptionGroup for Args {}

/// The result of one copy.
#[derive(Clone, Copy, Debug)]
struct CopyResult {
    format: Format,
    /// The time until the output topic had all records, or `None` if it did not.
    elapsed: Option<Duration>,
    /// The CPU time of this process for the copy.
    cpu: Duration,
}

impl CopyResult {
    fn records_per_second(&self, records: u64) -> Option<f64> {
        self.elapsed
            .map(|elapsed| records as f64 / elapsed.as_secs_f64())
    }

    fn cpu_per_million(&self, records: u64) -> f64 {
        self.cpu.as_secs_f64() * 1e6 / records as f64
    }
}

/// The arguments of the process, with the Prism runner and the LOOPBACK workers unless they
/// set other ones.
fn arguments() -> Vec<OsString> {
    let given: Vec<OsString> = std::env::args_os().collect();
    let defaults = ["--runner=PrismRunner", "--environment_type=LOOPBACK"]
        .into_iter()
        .filter(|default| {
            let (flag, _) = default.split_once('=').unwrap_or((default, ""));
            !given
                .iter()
                .any(|arg| arg.to_string_lossy().starts_with(flag))
        })
        .map(OsString::from);
    given
        .iter()
        .take(1)
        .cloned()
        .chain(defaults)
        .chain(given.iter().skip(1).cloned())
        .collect()
}

/// The CPU time of this process: user time and system time of all its threads.
fn process_cpu() -> Duration {
    let mut usage = MaybeUninit::<libc::rusage>::zeroed();
    // SAFETY: `usage` is valid for a write of one `rusage`. With `RUSAGE_SELF`, `getrusage`
    // fills it and does not fail. The zeroed value is also a valid `rusage`.
    let usage = unsafe {
        libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr());
        usage.assume_init()
    };
    let time = |value: libc::timeval| {
        Duration::from_secs(u64::try_from(value.tv_sec).unwrap_or(0))
            + Duration::from_micros(u64::try_from(value.tv_usec).unwrap_or(0))
    };
    time(usage.ru_utime) + time(usage.ru_stime)
}

/// A client configuration for `servers`.
fn config(servers: &str) -> ClientConfig {
    let mut config = ClientConfig::new();
    config.set("bootstrap.servers", servers);
    config
}

/// A topic name that no other run uses.
fn topic_name(kind: &str) -> String {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |time| time.as_millis());
    format!("bench-copy-{kind}-{millis}")
}

async fn create_topic(
    admin: &AdminClient<DefaultClientContext>,
    name: &str,
    partitions: i32,
) -> Result<(), BoxError> {
    let topic = NewTopic::new(name, partitions, TopicReplication::Fixed(1));
    admin
        .create_topics([&topic], &AdminOptions::new())
        .await?
        .into_iter()
        .try_for_each(|result| {
            result
                .map(|_| ())
                .map_err(|(topic, code)| format!("Cannot create topic {topic}: {code}"))
        })?;
    Ok(())
}

/// Writes JSON record `index` of the Wikipedia schema to `buffer`. The records differ in their
/// fields and sizes.
fn wikipedia_record(index: u64, buffer: &mut String) {
    buffer.clear();
    let minor = index.is_multiple_of(3);
    let comment = "edit ".repeat(1 + usize::try_from(index % 7).unwrap_or(0));
    let _written = write!(
        buffer,
        r#"{{"title":"Page {index}","id":{index},"language":"en","wp_namespace":{},"is_redirect":false,"revision_id":{},"contributor_ip":null,"contributor_id":{},"contributor_username":"user{}","timestamp":{},"is_minor":{minor},"is_bot":false,"reversion_id":null,"comment":"{comment}","num_characters":{}}}"#,
        index % 16,
        index * 7,
        index % 10_000,
        index % 10_000,
        1_100_000_000 + index,
        200 + index % 5_000,
    );
}

/// Writes `records` records to `topic`.
fn seed(servers: &str, topic: &str, records: u64) -> Result<(), BoxError> {
    let producer: BaseProducer = config(servers)
        .set("linger.ms", "20")
        .set("queue.buffering.max.messages", "1000000")
        .create()?;
    let mut buffer = String::new();
    (0..records).try_for_each(|index| {
        wikipedia_record(index, &mut buffer);
        loop {
            match producer.send(BaseRecord::<(), [u8]>::to(topic).payload(buffer.as_bytes())) {
                Ok(()) => break Ok::<(), KafkaError>(()),
                Err((KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull), _)) => {
                    producer.poll(Duration::from_millis(100));
                }
                Err((error, _)) => break Err(error),
            }
        }
    })?;
    producer.flush(Duration::from_secs(120))?;
    Ok(())
}

/// The records of `topic` in its `partitions`, from the high watermarks.
fn topic_records(consumer: &BaseConsumer, topic: &str, partitions: i32) -> u64 {
    (0..partitions)
        .filter_map(|partition| {
            consumer
                .fetch_watermarks(topic, partition, BROKER_TIMEOUT)
                .ok()
        })
        .map(|(low, high)| u64::try_from(high - low).unwrap_or(0))
        .sum()
}

/// Waits until `topic` has `records` records, at most until `deadline`. Returns the time from
/// `started` and the CPU time of this process at that moment, or `None` at the deadline.
fn wait_for_records(
    servers: String,
    topic: String,
    partitions: i32,
    records: u64,
    started: Instant,
    deadline: Instant,
) -> thread::JoinHandle<Option<(Duration, Duration)>> {
    thread::spawn(move || {
        let consumer: BaseConsumer = config(&servers).create().ok()?;
        loop {
            match topic_records(&consumer, &topic, partitions) >= records {
                true => return Some((started.elapsed(), process_cpu())),
                false if Instant::now() >= deadline => return None,
                false => thread::sleep(CHECK_INTERVAL),
            }
        }
    })
}

/// Copies `input` to `output` in `format`.
async fn copy(
    options: &PipelineOptions,
    args: &Args,
    format: Format,
    input: &str,
    output: &str,
) -> Result<(), BoxError> {
    let p = Pipeline::create(options);
    let read = ReadOptions::new(&args.bootstrap_servers, input)
        .with_start_offset(StartOffset::Earliest)
        .with_max_read_time(Duration::from_secs(args.max_read_seconds));
    let write = WriteOptions::new(&args.bootstrap_servers, output);
    match format {
        Format::Bytes => {
            p.apply("Read", Read::new(read))
                .apply("Write", Write::new(write));
        }
        Format::Json => {
            let json = Json::schema(WIKIPEDIA_JSON_SCHEMA).map_err(|e| e.to_string())?;
            p.apply("Read", Read::new(read).with_decoder(json))
                .apply("Write", Write::new(write).with_encoder(Json::new()));
        }
    }
    p.run().await?;
    Ok(())
}

/// Runs one copy of `input` to a new topic, and measures it.
async fn measure(
    options: &PipelineOptions,
    args: &Args,
    admin: &AdminClient<DefaultClientContext>,
    format: Format,
    input: &str,
) -> Result<CopyResult, BoxError> {
    let output = topic_name("out");
    create_topic(admin, &output, args.partitions).await?;
    let started = Instant::now();
    let cpu_before = process_cpu();
    let deadline = started + Duration::from_secs(args.max_read_seconds + 60);
    let watcher = wait_for_records(
        args.bootstrap_servers.clone(),
        output.clone(),
        args.partitions,
        args.records,
        started,
        deadline,
    );
    copy(options, args, format, input, &output).await?;
    let cpu_after = process_cpu();
    let done = watcher.join().ok().flatten();
    let _deleted = admin
        .delete_topics(&[output.as_str()], &AdminOptions::new())
        .await;
    Ok(CopyResult {
        format,
        elapsed: done.map(|(elapsed, _)| elapsed),
        cpu: done
            .map_or(cpu_after, |(_, cpu)| cpu)
            .saturating_sub(cpu_before),
    })
}

/// The median of `values`, or `None` for no values.
fn median(mut values: Vec<f64>) -> Option<f64> {
    values.sort_by(f64::total_cmp);
    values.get(values.len() / 2).copied()
}

fn report(copies: &[CopyResult], args: &Args) {
    println!("\n| Format | Copy time (s) | Records/s | Rust CPU (s) | CPU s per 1M records |");
    println!("|---|---|---|---|---|");
    copies.iter().for_each(|copy| {
        let elapsed = copy.elapsed.map_or("incomplete".to_string(), |elapsed| {
            format!("{:.2}", elapsed.as_secs_f64())
        });
        let rate = copy
            .records_per_second(args.records)
            .map_or("-".to_string(), |rate| format!("{rate:.0}"));
        println!(
            "| {:?} | {elapsed} | {rate} | {:.2} | {:.3} |",
            copy.format,
            copy.cpu.as_secs_f64(),
            copy.cpu_per_million(args.records)
        );
    });
    println!("\nMedians:");
    args.formats.iter().for_each(|&format| {
        let of_format: Vec<&CopyResult> =
            copies.iter().filter(|copy| copy.format == format).collect();
        let rate = median(
            of_format
                .iter()
                .filter_map(|copy| copy.records_per_second(args.records))
                .collect(),
        );
        let cpu = median(
            of_format
                .iter()
                .map(|copy| copy.cpu_per_million(args.records))
                .collect(),
        );
        println!(
            "  {format:?}: {} records/s, {} CPU s per 1M records",
            rate.map_or("-".to_string(), |rate| format!("{rate:.0}")),
            cpu.map_or("-".to_string(), |cpu| format!("{cpu:.3}"))
        );
    });
}

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    beam_sdk::logging::init_logging();
    let (options, args) = beam_sdk::options::parse_from::<Args, _, _>(arguments());

    let probe: BaseConsumer = config(&args.bootstrap_servers).create()?;
    if let Err(e) = probe.fetch_metadata(None, BROKER_TIMEOUT) {
        println!(
            "No Kafka broker at {} ({e}). Start one, for example with \
             `docker run -d --name beam-kafka-bench -p 9092:9092 apache/kafka:3.9.0`.",
            args.bootstrap_servers
        );
        return Ok(());
    }
    let admin: AdminClient<DefaultClientContext> = config(&args.bootstrap_servers).create()?;
    let input = topic_name("in");
    create_topic(&admin, &input, args.partitions).await?;
    println!("Writing {} records to {input}...", args.records);
    let servers = args.bootstrap_servers.clone();
    let (topic, records) = (input.clone(), args.records);
    tokio::task::spawn_blocking(move || seed(&servers, &topic, records)).await??;

    let plan: Vec<Format> = args
        .formats
        .iter()
        .flat_map(|&format| std::iter::repeat_n(format, args.runs))
        .collect();
    let mut copies = Vec::with_capacity(plan.len());
    for format in plan {
        let copy = measure(&options, &args, &admin, format, &input).await?;
        println!("{copy:?}");
        copies.push(copy);
    }
    let _deleted = admin
        .delete_topics(&[input.as_str()], &AdminOptions::new())
        .await;
    report(&copies, &args);
    Ok(())
}
