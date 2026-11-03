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

//! Iceberg benchmark in a Rust pipeline: the streaming `ingest` shape with the native Kafka
//! read and the native Iceberg write.
//!
//! The pipeline reads the DeviceMetric JSON records of `--topic`, counts them in the `Touch`
//! step of the Kafka benchmark (`e2e/rows`, `e2e/latency_ms`), parses each record into a
//! [`DeviceMetric`], and appends the rows to `--table` of a REST catalog. The parse follows
//! `ParseAndValidateMessage` of the Python reference job: a record that does not parse is
//! dropped and counted in `e2e/parse_errors`.
//!
//! The benchmark script drains the job, not this program: `./gradlew
//! :sdks:rust:icebergBenchmark -Ppipeline=rust` starts the job with `--async_job`.

use std::time::Duration;

use beam::io::iceberg::{Catalog, PartitionField, Transform, Write, WriteOptions};
use beam::io::kafka::native::codec::Raw;
use beam::io::kafka::native::read::{ConsumerPool, PoolSize, Read, ReadOptions, StartOffset};
use beam::options::PipelineOptionGroup;
use beam::prelude::*;
use beam_benchmark_io::{
    AutoOffsetReset, Connector, NAMESPACE, TouchCopyFn, parse_consumer_pool_size, parse_property,
};
use chrono::{DateTime, Utc};
use clap::{Args as ClapArgs, ValueEnum};
use serde::{Deserialize, Serialize};

/// The REST endpoint of the BigLake Iceberg catalog.
pub const BIGLAKE_CATALOG: &str = "https://biglake.googleapis.com/iceberg/v1/restcatalog";

/// The location of a record with no `location`, as in the Python reference job.
pub const DEFAULT_LOCATION: &str = "us-central1";

/// The shape of the benchmark. This pipeline runs only the streaming ingest.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, ValueEnum)]
#[serde(rename_all = "lowercase")]
pub enum Shape {
    /// Reads a Kafka topic and appends each record to an Iceberg table.
    Ingest,
}

/// One telemetry record of a device, the row of the Iceberg table.
#[derive(Clone, Debug, PartialEq, BeamRow)]
pub struct DeviceMetric {
    pub id: i64,
    pub timestamp_ms: DateTime<Utc>,
    pub device_id: String,
    pub payload: f64,
    pub location: String,
}

/// The JSON record of the topic.
#[derive(Deserialize)]
struct Record {
    id: i64,
    /// Milliseconds since the epoch.
    timestamp: i64,
    device_id: String,
    payload: f64,
    location: Option<String>,
}

/// The metric of the JSON record `value`, or `None` if it does not parse.
pub fn parse_device_metric(value: &[u8]) -> Option<DeviceMetric> {
    let record: Record = serde_json::from_slice(value).ok()?;
    Some(DeviceMetric {
        id: record.id,
        timestamp_ms: DateTime::from_timestamp_millis(record.timestamp)?,
        device_id: record.device_id,
        payload: record.payload,
        location: record
            .location
            .unwrap_or_else(|| DEFAULT_LOCATION.to_string()),
    })
}

/// Benchmark flags. The flags of the runner come from the pipeline options.
#[derive(ClapArgs, Serialize, Deserialize, Clone, Debug)]
pub struct Args {
    /// The Iceberg writer.
    #[arg(long, value_enum, default_value_t = Connector::Rust)]
    pub writer: Connector,
    /// The shape of the benchmark.
    #[arg(long, value_enum, default_value_t = Shape::Ingest)]
    pub shape: Shape,
    /// Kafka bootstrap servers (host:port).
    #[arg(long)]
    pub bootstrap_servers: String,
    /// The topic of the DeviceMetric JSON records.
    #[arg(long)]
    pub topic: String,
    /// Where a partition with no committed offset starts.
    #[arg(long, value_enum, ignore_case = true, default_value_t = AutoOffsetReset::Earliest)]
    pub auto_offset_reset: AutoOffsetReset,
    /// Kafka consumer group.id. Default: a new group for each job. The read commits the offsets.
    #[arg(long)]
    pub group_id: Option<String>,
    /// More properties of the Kafka consumer, as `key=value` pairs with a comma between them.
    #[arg(long, value_delimiter = ',', value_parser = parse_property)]
    pub consumer_properties: Vec<(String, String)>,
    /// The number of consumers of the read in a worker, from 1 to 16.
    #[arg(long, value_parser = parse_consumer_pool_size)]
    pub consumer_pool_size: Option<usize>,
    /// Authenticate to Kafka with Google Application Default Credentials. Set
    /// `--with_gcp_adc=false` for a local cluster.
    #[arg(
        long,
        num_args = 0..=1,
        default_missing_value = "true",
        default_value_t = true,
        action = clap::ArgAction::Set
    )]
    pub with_gcp_adc: bool,
    /// Stops the read after this time, so the pipeline ends. For local runs only.
    #[arg(long)]
    pub max_read_seconds: Option<u64>,
    /// The REST catalog endpoint.
    #[arg(long, default_value = BIGLAKE_CATALOG)]
    pub catalog_uri: String,
    /// The warehouse of the catalog, for example `bl://projects/<p>/catalogs/<c>`.
    #[arg(long)]
    pub warehouse: String,
    /// The table, as `namespace.name`.
    #[arg(long)]
    pub table: String,
    /// The project that BigLake bills, as the `x-goog-user-project` header.
    #[arg(long)]
    pub user_project: Option<String>,
    /// The region of the catalog, as the `gcp_location` catalog property.
    #[arg(long)]
    pub gcp_location: Option<String>,
    /// How often the write commits a snapshot.
    #[arg(long, default_value_t = 60)]
    pub triggering_frequency_seconds: u64,
    /// The number of writers. If not set, one for each worker thread that encodes rows.
    #[arg(long)]
    pub num_shards: Option<usize>,
    /// Hash-prefixed data file paths (`write.object-storage.enabled`).
    #[arg(long)]
    pub object_storage: bool,
}

impl PipelineOptionGroup for Args {}

/// The options of the read of `--topic`.
///
/// # Errors
///
/// Fails if `--consumer_pool_size` is not a valid [`PoolSize`].
pub fn read_options(args: &Args) -> Result<ReadOptions> {
    let start = match args.auto_offset_reset {
        AutoOffsetReset::Earliest => StartOffset::Earliest,
        AutoOffsetReset::Latest => StartOffset::Latest,
    };
    // A new group for each job, as in the Java and Python pipelines. The read commits its
    // offsets, and the check after the job reads the size of the topic from them.
    let group_id = args.group_id.clone().unwrap_or_else(|| {
        let millis = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |elapsed| elapsed.as_millis());
        format!("iceberg-bench-rust-{millis}")
    });
    let mut options = ReadOptions::new(&args.bootstrap_servers, &args.topic)
        .with_start_offset(start)
        .with_consumer_properties([("group.id".to_string(), group_id)])
        .with_consumer_properties(args.consumer_properties.iter().cloned());
    if let Some(size) = args.consumer_pool_size {
        options = options.with_consumer_pool(ConsumerPool::Size(PoolSize::try_from(size)?));
    }
    if args.with_gcp_adc {
        options = options.with_google_auth();
    }
    if let Some(seconds) = args.max_read_seconds {
        options = options.with_max_read_time(Duration::from_secs(seconds));
    }
    Ok(options)
}

/// The options of the write: the BigLake catalog with Google auth and vended credentials,
/// and the table of the Python reference job, partitioned by the hour of `timestamp_ms`. A
/// local run with `--max_read_seconds` has a bounded read, so it sets no triggering frequency.
pub fn write_options(args: &Args) -> WriteOptions {
    let catalog = Catalog::rest(&args.catalog_uri)
        .with_warehouse(&args.warehouse)
        .with_google_auth()
        .with_vended_credentials()
        .with_metrics_reporting();
    let catalog = match &args.user_project {
        Some(project) => catalog.with_header("x-goog-user-project", project),
        None => catalog,
    };
    let catalog = match &args.gcp_location {
        Some(location) => catalog.with_property("gcp_location", location),
        None => catalog,
    };
    let options = WriteOptions::new(catalog, &args.table)
        .with_partition_fields([PartitionField::new("timestamp_ms", Transform::Hour)])
        .with_table_property("gcp.biglake.table-management.enabled", "true")
        .with_table_property("gcp.biglake.bigquery-dml.enabled", "true")
        .with_table_property("write.parquet.compression-codec", "zstd");
    // A read with an end is bounded, and a bounded write commits once at the end.
    let options = match args.max_read_seconds {
        Some(_) => options,
        None => options
            .with_triggering_frequency(Duration::from_secs(args.triggering_frequency_seconds)),
    };
    let options = match args.num_shards {
        Some(shards) => options.with_num_shards(shards),
        None => options,
    };
    match args.object_storage {
        true => options.with_table_property("write.object-storage.enabled", "true"),
        false => options,
    }
}

/// Builds the ingest pipeline: read, `Touch`, parse and write.
///
/// # Errors
///
/// Fails if the read options are not valid.
pub fn build_pipeline(options: &PipelineOptions, args: &Args) -> Result<Pipeline> {
    let Args {
        writer: Connector::Rust,
        shape: Shape::Ingest,
        ..
    } = args;
    let p = Pipeline::create(options);
    p.apply("Read", Read::new(read_options(args)?).with_decoder(Raw))
        .par_do("Touch", TouchCopyFn::<Vec<u8>>::new())
        .flat_map("Parse", |value: Vec<u8>| {
            let metric = parse_device_metric(&value);
            if metric.is_none() {
                Metrics::counter(NAMESPACE, "parse_errors").inc();
            }
            metric
        })
        .apply("Write", Write::<DeviceMetric>::new(write_options(args)));
    Ok(p)
}
