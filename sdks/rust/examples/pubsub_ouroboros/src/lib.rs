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

use std::sync::Arc;

use beam::io::gcp::pubsub::{
    DEFAULT_EXPANSION_SERVICE, PubsubRead, PubsubWrite, raw_bytes_row, raw_bytes_schema,
};
use beam::prelude::*;
use beam::schema::{Field, FieldType};
use clap::Args as ClapArgs;
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

/// Message payload representing an entity evolving through the Ouroboros cycle.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OuroborosMessage {
    /// Unique identifier for the cycle or entity (e.g. `cycle-001`, `ouroboros-alpha`).
    pub cycle_id: String,
    /// Current iteration / hop count.
    pub iteration: i64,
    /// Maximum allowed iterations before completion.
    pub max_iterations: i64,
    /// Current evolutionary state (e.g. `egg`, `shedding_skin`, `devouring_tail`, `ascended`).
    pub state: String,
    /// Audit log of previous state transitions.
    pub history: String,
}

impl OuroborosMessage {
    /// Creates a newly born Ouroboros entity ready to enter the cycle.
    pub fn new(cycle_id: impl Into<String>, max_iterations: i64) -> Self {
        Self {
            cycle_id: cycle_id.into(),
            iteration: 0,
            max_iterations,
            state: "egg".to_string(),
            history: "born".to_string(),
        }
    }

    /// Serializes this message into JSON bytes.
    pub fn to_json_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(self).unwrap_or_default()
    }

    /// Deserializes a message from JSON bytes.
    pub fn from_json_slice(slice: &[u8]) -> Option<Self> {
        serde_json::from_slice(slice).ok()
    }

    /// Checks if this message has finished all iterations in the Ouroboros cycle.
    pub fn is_complete(&self) -> bool {
        self.iteration >= self.max_iterations
    }
}

/// Evolves an [`OuroborosMessage`] to its next lifecycle state and increments its iteration count.
pub fn evolve_ouroboros(msg: &OuroborosMessage) -> OuroborosMessage {
    let next_iteration = msg.iteration + 1;
    let next_state = match next_iteration {
        n if n >= msg.max_iterations => "ouroboros_ascended",
        n if n % 4 == 1 => "shedding_skin",
        n if n % 4 == 2 => "growing_coils",
        n if n % 4 == 3 => "devouring_tail",
        _ => "regenerating",
    };

    let next_history = format!("{} -> {}#{}", msg.history, next_state, next_iteration);

    OuroborosMessage {
        cycle_id: msg.cycle_id.clone(),
        iteration: next_iteration,
        max_iterations: msg.max_iterations,
        state: next_state.to_string(),
        history: next_history,
    }
}

static OUROBOROS_SCHEMA: std::sync::LazyLock<Arc<Schema>> = std::sync::LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("cycle_id", FieldType::string()),
        Field::new("iteration", FieldType::int64()),
        Field::new("max_iterations", FieldType::int64()),
        Field::new("state", FieldType::string()),
        Field::new("history", FieldType::string()),
    ]))
});

/// Beam schema corresponding to an [`OuroborosMessage`].
pub fn ouroboros_schema() -> Arc<Schema> {
    Arc::clone(&OUROBOROS_SCHEMA)
}

/// Converts an [`OuroborosMessage`] into a Beam [`Row`].
pub fn message_to_row(msg: &OuroborosMessage) -> Option<Row> {
    Row::builder(ouroboros_schema())
        .with_value(msg.cycle_id.as_str())
        .with_value(msg.iteration)
        .with_value(msg.max_iterations)
        .with_value(msg.state.as_str())
        .with_value(msg.history.as_str())
        .build()
        .ok()
}

/// Extracts an [`OuroborosMessage`] from a Beam [`Row`].
pub fn row_to_message(row: &Row) -> Option<OuroborosMessage> {
    let cycle_id = row.get_string("cycle_id").ok().flatten()?.to_string();
    let iteration = row.get_i64("iteration").ok().flatten()?;
    let max_iterations = row.get_i64("max_iterations").ok().flatten()?;
    let state = row.get_string("state").ok().flatten()?.to_string();
    let history = row.get_string("history").ok().flatten()?.to_string();

    Some(OuroborosMessage {
        cycle_id,
        iteration,
        max_iterations,
        state,
        history,
    })
}

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
pub fn build_pipeline(options: &PipelineOptions, args: &Args) -> Pipeline {
    let p = Pipeline::create(options);

    let loop_topic = args
        .loop_topic
        .as_deref()
        .or(args.input_topic.as_deref())
        .expect("Pub/Sub topic destination is required: specify --loop_topic or --input_topic");

    // Read messages from Cloud Pub/Sub through Java SchemaTransform (RAW format).
    let mut read_builder = PubsubRead::new("PubsubRead").with_raw_format();
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
        .with_raw_format()
        .with_expansion_service(&args.expansion_service);

    cycling_rows.apply(write_transform);

    p
}
