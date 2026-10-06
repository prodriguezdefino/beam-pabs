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

//! Cross-language Pub/Sub Ouroboros streaming example.
//!
//! Processes cyclical or iterative event streams:
//! - Consumes messages from a Pub/Sub subscription or topic through Java
//!   `PubsubRead::new("PubsubRead")`.
//! - Parses message state, executes state transitions, and tracks iteration count in Rust.
//! - Evaluates termination versus re-entry: active iterations cycle back into the loop topic;
//!   terminal messages that have completed all cycles exit the loop.
//! - Publishes updated messages back to Cloud Pub/Sub through Java `PubsubWrite`.

use beam::io::gcp::DEFAULT_EXPANSION_SERVICE;
use beam::io::gcp::pubsub::{
    PubsubFormat, PubsubRead, PubsubWrite, PubsubWriteFormat, raw_bytes_row, raw_bytes_schema,
};
use beam::prelude::*;
use clap::Args as ClapArgs;
pub use kafka_ouroboros::{OuroborosMessage, evolve_ouroboros};
use serde::{Deserialize, Serialize};

/// Default maximum cycles for an Ouroboros entity before reaching completion.
pub const DEFAULT_MAX_CYCLES: i64 = 5;

/// Command-line arguments for the Cross-Language Pub/Sub Ouroboros example.
#[derive(ClapArgs, Serialize, Deserialize, Debug, Clone)]
#[command(
    name = "pubsub_ouroboros",
    about = "Apache Beam Rust SDK Pub/Sub Ouroboros Streaming Example"
)]
pub struct Args {
    /// Pub/Sub subscription to consume from.
    /// Format: `projects/${PROJECT}/subscriptions/${SUBSCRIPTION}`.
    #[arg(long, alias = "inputSubscription")]
    pub input_subscription: Option<String>,

    /// Pub/Sub topic to consume from instead of subscription.
    /// Format: `projects/${PROJECT}/topics/${TOPIC}`.
    #[arg(long, alias = "inputTopic")]
    pub input_topic: Option<String>,

    /// Destination Pub/Sub topic where active cycling messages loop back to.
    /// Format: `projects/${PROJECT}/topics/${TOPIC}`.
    #[arg(long, alias = "loopTopic")]
    pub loop_topic: Option<String>,

    /// Expansion service address (e.g. `localhost:8097` or autojava target).
    #[arg(
        long,
        alias = "expansionService",
        default_value = DEFAULT_EXPANSION_SERVICE
    )]
    pub expansion_service: String,

    /// Maximum iterations before the ouroboros cycle completes.
    #[arg(
        long,
        alias = "maxCycles",
        default_value_t = DEFAULT_MAX_CYCLES
    )]
    pub max_cycles: i64,
}

impl PipelineOptionGroup for Args {}

/// Processes an incoming RAW Pub/Sub message row, evolves its state, and serializes back to a RAW row.
pub fn process_raw_message_row(row: &Row) -> Option<Row> {
    let payload = match row.get_bytes("payload").ok().flatten() {
        Some(bytes) => bytes,
        None => {
            // Fall back to string payload if sent as string
            row.get_string("payload")
                .ok()
                .flatten()
                .map(|s| s.as_bytes())?
        }
    };

    let msg = OuroborosMessage::from_json_slice(payload)?;
    if msg.is_complete() {
        // Completed messages exit the loop.
        return None;
    }
    let evolved = evolve_ouroboros(&msg);
    let evolved_bytes = evolved.to_json_bytes();
    Some(raw_bytes_row(evolved_bytes))
}

/// Builds the Cross-Language Pub/Sub Ouroboros pipeline according to [`Args`].
///
/// # Panics
///
/// Panics if neither `--loop_topic` nor `--input_topic` is provided.
pub fn build_pipeline(options: &PipelineOptions, args: &Args) -> Pipeline {
    let p = Pipeline::create(options);

    let loop_topic = args
        .loop_topic
        .as_deref()
        .or(args.input_topic.as_deref())
        .expect("Pub/Sub topic destination is required: specify --loop_topic or --input_topic");

    // Read messages from Cloud Pub/Sub through Java SchemaTransform (RAW format).
    let mut read_builder = PubsubRead::new("PubsubRead").with_format(PubsubFormat::Raw);
    if let Some(sub) = &args.input_subscription {
        read_builder = read_builder.with_subscription(sub);
    } else if let Some(topic) = &args.input_topic {
        read_builder = read_builder.with_topic(topic);
    } else {
        read_builder = read_builder.with_topic(loop_topic);
    }
    read_builder = read_builder.with_expansion_service(&args.expansion_service);

    let incoming_rows = p.apply(read_builder);

    // Inject one seed message to bootstrap the cycle.
    let raw_schema = raw_bytes_schema();
    let seed_message = OuroborosMessage::new("ouroboros-seed", args.max_cycles);
    let seed_row = raw_bytes_row(seed_message.to_json_bytes());
    let seed_rows = p
        .apply(Create::new("SeedOuroborosMessage", vec![seed_row]))
        .with_row_schema(&raw_schema);

    let merged_rows = Flatten::pcollections("MergeIncomingAndSeed", &[&incoming_rows, &seed_rows])
        .with_row_schema(&raw_schema);

    // Parse the JSON payload, evolve the state machine in Rust, and serialize back to raw
    // bytes for re-entry into Pub/Sub.
    let cycling_rows = merged_rows
        .flat_map("EvolveOuroborosState", |row: Row| {
            process_raw_message_row(&row)
        })
        .with_row_schema(&raw_schema);

    // Write cycling messages back to Cloud Pub/Sub loop topic through Java SchemaTransform.
    let write_transform = PubsubWrite::new("PubsubWrite", loop_topic)
        .with_format(PubsubWriteFormat::Raw)
        .with_expansion_service(&args.expansion_service);

    cycling_rows.apply(write_transform);

    p
}
