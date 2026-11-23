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

//! Iceberg benchmark in a Rust pipeline. The shapes:
//!
//! - `ingest` reads the DeviceMetric JSON records of `--topic`, counts them in the `Touch` step
//!   of the Kafka benchmark (`e2e/rows`, `e2e/latency_ms`), parses each record into a
//!   [`DeviceMetric`], and appends the rows to `--table` of a REST catalog. The parse follows
//!   `ParseAndValidateMessage` of the Python reference job: a record that does not parse is
//!   dropped and counted in `e2e/parse_errors`.
//! - `count` reads `--table`, counts the rows, and adds the total to `e2e/rows`.
//! - `load` reads the BigQuery table `--source_table`, counts each row in `Touch`, and appends
//!   the rows to `--table`. It writes the table that `count` reads.
//!
//! The benchmark script drains the job, not this program: `./gradlew
//! :sdks:rust:icebergBenchmark -Ppipeline=rust` starts the job with `--async_job`.

use std::time::Duration;

use beam::io::gcp::bigquery;
use beam::io::iceberg::{
    Catalog, PartitionField, PartitionTransform, ReadRows, Write, WriteOptions, WriteRows,
};
use beam::io::kafka::codec::Raw;
use beam::io::kafka::{ConsumerPool, PoolSize, Read, ReadOptions, StartOffset};
use beam::options::PipelineOptionGroup;
use beam::prelude::*;
use beam_benchmark_io::{
    AutoOffsetReset, Connector, NAMESPACE, RecordRowsFn, TouchCopyFn, TouchFn,
    parse_consumer_pool_size, parse_property,
};
use chrono::{DateTime, Utc};
use clap::{Args as ClapArgs, ValueEnum};
use serde::{Deserialize, Serialize};

/// The REST endpoint of the BigLake Iceberg catalog.
pub const BIGLAKE_CATALOG: &str = "https://biglake.googleapis.com/iceberg/v1/restcatalog";

/// The location of a record with no `location`, as in the Python reference job.
pub const DEFAULT_LOCATION: &str = "us-central1";

/// The shape of the benchmark.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, ValueEnum)]
#[serde(rename_all = "lowercase")]
pub enum Shape {
    /// Reads a Kafka topic and appends each record to an Iceberg table.
    Ingest,
    /// Reads an Iceberg table and counts the rows.
    Count,
    /// Copies a BigQuery table to an Iceberg table.
    Load,
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
    /// The Iceberg reader of `count`.
    #[arg(long, value_enum)]
    pub reader: Option<Connector>,
    /// The Iceberg writer of `ingest` and `load`.
    #[arg(long, value_enum)]
    pub writer: Option<Connector>,
    /// The shape of the benchmark.
    #[arg(long, value_enum, default_value_t = Shape::Ingest)]
    pub shape: Shape,
    /// Kafka bootstrap servers (host:port) of `ingest`.
    #[arg(long)]
    pub bootstrap_servers: Option<String>,
    /// The topic of the DeviceMetric JSON records of `ingest`.
    #[arg(long)]
    pub topic: Option<String>,
    /// The BigQuery table that `load` copies, as `project:dataset.table`.
    #[arg(long)]
    pub source_table: Option<String>,
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

/// Checks that the flags fit the shape.
///
/// # Errors
///
/// Fails if a flag of another shape is set, or a flag of this shape is missing.
pub fn check_shape(args: &Args) -> Result {
    let needs = |set: bool, flag: &str| {
        if set {
            Ok(())
        } else {
            Err(Error::msg(format!("{:?} needs {flag}", args.shape)))
        }
    };
    let refuses = |set: bool, flag: &str| {
        if set {
            Err(Error::msg(format!(
                "{flag} does not apply to {:?}",
                args.shape
            )))
        } else {
            Ok(())
        }
    };
    let kafka = args.bootstrap_servers.is_some() && args.topic.is_some();
    match args.shape {
        Shape::Ingest => {
            needs(args.writer.is_some(), "--writer")?;
            needs(kafka, "--bootstrap_servers and --topic")?;
            refuses(args.reader.is_some(), "--reader")?;
            refuses(args.source_table.is_some(), "--source_table")
        }
        Shape::Count => {
            needs(args.reader.is_some(), "--reader")?;
            refuses(args.writer.is_some(), "--writer")?;
            refuses(args.source_table.is_some(), "--source_table")
        }
        Shape::Load => {
            needs(args.writer.is_some(), "--writer")?;
            needs(args.source_table.is_some(), "--source_table")?;
            refuses(args.reader.is_some(), "--reader")
        }
    }
}

/// The options of the read of `--topic`.
///
/// # Errors
///
/// Fails if `--bootstrap_servers` or `--topic` is not set, or if `--consumer_pool_size` is
/// not a valid [`PoolSize`].
pub fn read_options(args: &Args) -> Result<ReadOptions> {
    let (Some(servers), Some(topic)) = (&args.bootstrap_servers, &args.topic) else {
        return Err(Error::msg("set --bootstrap_servers and --topic"));
    };
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
    let mut options = ReadOptions::new(servers, topic)
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

/// The BigLake catalog with Google auth and vended credentials.
pub fn catalog(args: &Args) -> Catalog {
    let catalog = Catalog::rest(&args.catalog_uri)
        .with_warehouse(&args.warehouse)
        .with_google_auth()
        .with_vended_credentials()
        .with_metrics_reporting();
    let catalog = match &args.user_project {
        Some(project) => catalog.with_header("x-goog-user-project", project),
        None => catalog,
    };
    match &args.gcp_location {
        Some(location) => catalog.with_property("gcp_location", location),
        None => catalog,
    }
}

/// The options of a write to `--table`, with the table properties of the Python reference job.
fn table_write_options(args: &Args) -> WriteOptions {
    let options = WriteOptions::new(catalog(args), &args.table)
        .with_table_property("gcp.biglake.table-management.enabled", "true")
        .with_table_property("gcp.biglake.bigquery-dml.enabled", "true")
        .with_table_property("write.parquet.compression-codec", "zstd");
    let options = match args.num_shards {
        Some(shards) => options.with_num_shards(shards),
        None => options,
    };
    if args.object_storage {
        options.with_table_property("write.object-storage.enabled", "true")
    } else {
        options
    }
}

/// The options of the `ingest` write: the table of the Python reference job, partitioned by
/// the hour of `timestamp_ms`. A local run with `--max_read_seconds` has a bounded read, so
/// it sets no triggering frequency.
pub fn write_options(args: &Args) -> WriteOptions {
    let options = table_write_options(args).with_partition_fields([PartitionField::new(
        "timestamp_ms",
        PartitionTransform::Hour,
    )]);
    // A read with an end is bounded, and a bounded write commits once at the end.
    match args.max_read_seconds {
        Some(_) => options,
        None => options
            .with_triggering_frequency(Duration::from_secs(args.triggering_frequency_seconds)),
    }
}

/// Builds the pipeline of the shape of `args`.
///
/// # Errors
///
/// Fails if the flags do not fit the shape, or if the read options are not valid.
pub fn build_pipeline(options: &PipelineOptions, args: &Args) -> Result<Pipeline> {
    check_shape(args)?;
    let p = Pipeline::create(options);
    match args.shape {
        Shape::Ingest => {
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
        }
        Shape::Count => {
            let read = beam::io::iceberg::ReadOptions::new(catalog(args), &args.table);
            p.apply("Read", ReadRows::new(read))
                .apply("CountRows", CountGlobally::new())
                .par_do("RecordRows", RecordRowsFn::new());
        }
        Shape::Load => {
            let Some(source) = &args.source_table else {
                return Err(Error::msg("set --source_table"));
            };
            let read = bigquery::ReadOptions::new(source);
            let rows = p.apply("Read", bigquery::ReadRows::new(read));
            let schema = rows
                .row_schema()
                .ok_or_else(|| Error::msg("the BigQuery read gives no row schema"))?;
            rows.par_do("Touch", TouchFn::<Row>::new())
                // A Row-to-Row ParDo does not keep the row schema of its input.
                .with_row_schema(&schema)
                .apply("Write", WriteRows::new(table_write_options(args)));
        }
    }
    Ok(p)
}
