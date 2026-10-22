/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

//! Copies a Kafka topic to another topic with the native read and the native write.
//!
//! This program is for local CPU and memory profiles of the connector on Prism. It reads JSON
//! records of the Wikipedia benchmark schema, decodes them into rows, encodes the rows into
//! JSON again and writes them. The read stops after `--max_read_seconds`.
//!
//! ```text
//! ./gradlew :sdks:rust:profilePrism -Ppkg=apache-beam-io-kafka -PcargoExample=copy_profile \
//!   -Pargs="--bootstrap_servers=localhost:9092 --input_topic=in --output_topic=out"
//! ```

use std::time::Duration;

use beam_sdk::options::PipelineOptionGroup;
use beam_sdk::prelude::*;
use clap::Args as ClapArgs;
use kafka_io::native::format::{ReadFormat, WriteFormat};
use kafka_io::native::read::{ConsumerPool, PoolSize, ReadOptions, ReadRecords, StartOffset};
use kafka_io::native::write::{WriteOptions, WriteRecords};
use serde::{Deserialize, Serialize};

/// The JSON Schema of `bigquery-public-data:samples.wikipedia`, as in the Dataflow benchmark.
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

#[derive(ClapArgs, Serialize, Deserialize, Clone, Debug)]
struct Args {
    /// Kafka bootstrap servers (host:port).
    #[arg(long, default_value = "localhost:9092")]
    bootstrap_servers: String,
    /// The topic to read from the earliest offset.
    #[arg(long)]
    input_topic: String,
    /// The topic to write to.
    #[arg(long)]
    output_topic: String,
    /// The time after which the read stops.
    #[arg(long, default_value_t = 60)]
    max_read_seconds: u64,
    /// The consumer group of the read. With a group, the read commits its offsets in the
    /// bundle finalizer. Use a new group for each run, so that each run reads all records.
    #[arg(long)]
    group_id: Option<String>,
    /// librdkafka producer properties of the write, as `name=value` pairs with commas
    /// between them, for example `linger.ms=0,batch.size=16384`.
    #[arg(long, default_value = "")]
    producer_properties: String,
    /// The longest time in milliseconds that one call of the reader reads a partition. A
    /// short call makes many short bundles, as on Dataflow. The default is that of the read.
    #[arg(long)]
    max_read_call_ms: Option<u64>,
    /// The number of consumers of the read in the worker, from 1 to 16. The default is one
    /// consumer for each partition, at most 16.
    #[arg(long)]
    consumer_pool_size: Option<usize>,
}

impl PipelineOptionGroup for Args {}

/// The `name=value` pairs of `text`, with commas between them.
fn properties(text: &str) -> Result<Vec<(String, String)>, String> {
    text.split(',')
        .filter(|pair| !pair.trim().is_empty())
        .map(|pair| match pair.split_once('=') {
            Some((name, value)) => Ok((name.trim().to_string(), value.trim().to_string())),
            None => Err(format!("The property '{pair}' has no '='")),
        })
        .collect()
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    beam_sdk::logging::init_logging();

    let (options, args) = beam_sdk::options::parse::<Args>();
    let p = Pipeline::create(&options);

    let group = args
        .group_id
        .iter()
        .map(|group| ("group.id".to_string(), group.clone()));
    let read = ReadOptions::new(
        &args.bootstrap_servers,
        &args.input_topic,
        ReadFormat::json(WIKIPEDIA_JSON_SCHEMA).map_err(|e| e.to_string())?,
    )
    .with_start_offset(StartOffset::Earliest)
    .with_consumer_properties(group)
    .with_max_read_time(Duration::from_secs(args.max_read_seconds));
    let read = match args.max_read_call_ms {
        Some(millis) => read.with_max_read_time_per_call(Duration::from_millis(millis)),
        None => read,
    };
    let read = match args.consumer_pool_size {
        Some(size) => read.with_consumer_pool(ConsumerPool::Size(
            PoolSize::try_from(size).map_err(|e| e.to_string())?,
        )),
        None => read,
    };
    let write = WriteOptions::new(&args.bootstrap_servers, &args.output_topic)
        .with_format(WriteFormat::Json)
        .with_producer_properties(properties(&args.producer_properties)?);

    p.apply(ReadRecords::new("Read", read))
        .apply(WriteRecords::new("Write", write));

    p.run().await?;
    Ok(())
}
