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

//! Cross-language Kafka Ouroboros streaming example.
//!
//! Streaming pipeline that reads from and writes back to Kafka:
//! - Consumes JSON records from a Kafka topic through the Java `kafka_read` SchemaTransform
//!   (unbounded, RAW format).
//! - Injects `--num_seeds` seed messages to bootstrap the cycle.
//! - Evolves the state of each message in Rust.
//! - Publishes surviving messages back to the same topic through the Java `kafka_write`
//!   SchemaTransform, where the read picks them up again. Messages that complete
//!   `--max_cycles` exit the loop.
//!
//! Progress is reported through `ouroboros` counters (`seeded`, `evolved`,
//! `ascended`, `malformed`). Dataflow shows them as custom job metrics.
//!
//! Both Kafka transforms use Managed error handling. Records that Java fails to decode or
//! serialize go to [`READ_ERRORS`] or [`WRITE_ERRORS`] and do not fail the bundle. The
//! pipeline counts them as `kafka_read_errors` and `kafka_write_errors`.

use std::sync::Arc;

use beam::external::ExpansionError;
use beam::io::kafka::{
    KafkaRead, KafkaWrite, OffsetReset, payload_bytes, raw_bytes_row, raw_bytes_schema,
};
use beam::io::managed;
use beam::prelude::*;
use clap::Args as ClapArgs;
use serde::{Deserialize, Serialize};

/// Default maximum cycles for an Ouroboros entity before reaching completion.
pub const DEFAULT_MAX_CYCLES: i64 = 5;

/// Namespace of the pipeline's user counters.
pub const METRICS_NAMESPACE: &str = "ouroboros";

/// Command-line arguments for the Cross-Language Kafka Ouroboros example.
#[derive(ClapArgs, Serialize, Deserialize, Debug, Clone)]
#[command(
    name = "kafka_ouroboros",
    about = "Apache Beam Rust SDK Kafka Ouroboros Streaming Example"
)]
pub struct Args {
    /// Kafka bootstrap servers, as `host1:port1,host2:port2`.
    #[arg(long, alias = "bootstrapServers")]
    pub bootstrap_servers: String,

    /// Topic the ouroboros loops through: read from and written back to.
    #[arg(long, alias = "loopTopic")]
    pub loop_topic: String,

    /// Maximum iterations before an ouroboros completes its cycle.
    #[arg(
        long,
        alias = "maxCycles",
        default_value_t = DEFAULT_MAX_CYCLES
    )]
    pub max_cycles: i64,

    /// Number of seed messages injected at startup, each with its own `cycle_id`.
    #[arg(long, alias = "numSeeds", default_value_t = 1)]
    pub num_seeds: i64,

    /// Kafka consumer group id. Defaults to one derived from the read configuration, so
    /// a rerun with the same flags resumes from the committed offsets.
    #[arg(long, alias = "groupId")]
    pub group_id: Option<String>,

    /// Optional expansion service address override (host:port).
    /// Defaults to the automated Java I/O expansion service if omitted.
    #[arg(long, alias = "expansionService")]
    pub expansion_service: Option<String>,

    /// Authenticate to a Google Cloud Managed Service for Apache Kafka cluster
    /// (SASL_SSL/OAUTHBEARER with the workers' Google credentials).
    #[arg(
        long,
        default_value_t = false,
        num_args = 0..=1,
        default_missing_value = "true",
        action = clap::ArgAction::Set
    )]
    pub gmk: bool,
}

impl PipelineOptionGroup for Args {}

/// Message payload representing an entity evolving through the Ouroboros cycle.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OuroborosMessage {
    /// Unique identifier for the cycle or entity.
    pub cycle_id: String,
    /// Current iteration / hop count.
    pub iteration: i64,
    /// Maximum allowed iterations before completion.
    pub max_iterations: i64,
    /// Current evolutionary state (e.g. `egg`, `shedding_skin`, `ouroboros_ascended`).
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

    /// Whether this message has finished all iterations of the cycle.
    pub fn is_complete(&self) -> bool {
        self.iteration >= self.max_iterations
    }
}

/// Evolves an [`OuroborosMessage`] to its next lifecycle state.
pub fn evolve_ouroboros(msg: &OuroborosMessage) -> OuroborosMessage {
    let next_iteration = msg.iteration + 1;
    let next_state = match next_iteration {
        n if n >= msg.max_iterations => "ouroboros_ascended",
        n if n % 4 == 1 => "shedding_skin",
        n if n % 4 == 2 => "growing_coils",
        n if n % 4 == 3 => "devouring_tail",
        _ => "regenerating",
    };

    OuroborosMessage {
        cycle_id: msg.cycle_id.clone(),
        iteration: next_iteration,
        max_iterations: msg.max_iterations,
        state: next_state.to_string(),
        history: format!("{} -> {}#{}", msg.history, next_state, next_iteration),
    }
}

/// What happens to a record on one trip around the loop.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Step {
    /// Evolved; loops back into the topic.
    Continue(OuroborosMessage),
    /// Evolved into its final state; leaves the loop.
    Ascended(OuroborosMessage),
    /// Arrived already complete (e.g. replayed from an earlier run); dropped.
    AlreadyComplete,
    /// Not an ouroboros message; dropped.
    Malformed,
}

/// Decides the fate of a raw Kafka payload.
pub fn step(payload: &[u8]) -> Step {
    let Some(msg) = OuroborosMessage::from_json_slice(payload) else {
        return Step::Malformed;
    };
    if msg.is_complete() {
        return Step::AlreadyComplete;
    }
    let evolved = evolve_ouroboros(&msg);
    if evolved.is_complete() {
        Step::Ascended(evolved)
    } else {
        Step::Continue(evolved)
    }
}

/// Processes one record read from the loop topic, returning the record to publish, if any.
pub fn process_record(row: &Row) -> Option<Row> {
    let payload = payload_bytes(row)?;
    match step(payload) {
        Step::Continue(next) => {
            Metrics::counter(METRICS_NAMESPACE, "evolved").inc();
            tracing::info!(cycle_id = %next.cycle_id, iteration = next.iteration, state = %next.state, "ouroboros evolved");
            Some(raw_bytes_row(next.to_json_bytes()))
        }
        Step::Ascended(done) => {
            Metrics::counter(METRICS_NAMESPACE, "evolved").inc();
            Metrics::counter(METRICS_NAMESPACE, "ascended").inc();
            tracing::info!(cycle_id = %done.cycle_id, history = %done.history, "ouroboros ascended");
            None
        }
        Step::AlreadyComplete => None,
        Step::Malformed => {
            Metrics::counter(METRICS_NAMESPACE, "malformed").inc();
            None
        }
    }
}

/// The seed ("egg") messages that bootstrap the loop.
pub fn seed_messages(num_seeds: i64, max_cycles: i64) -> Vec<OuroborosMessage> {
    (0..num_seeds.max(0))
        .map(|i| OuroborosMessage::new(format!("ouroboros-{i}"), max_cycles))
        .collect()
}

/// Error output of the Kafka read: records that Java failed to decode.
pub const READ_ERRORS: &str = "read_errors";
/// Error output of the Kafka write: rows that Java failed to serialize.
pub const WRITE_ERRORS: &str = "write_errors";

/// Counts the rows of a connector error output under `counter` and logs each one.
fn count_errors(errors: &PCollection<Row>, counter: &'static str) {
    errors.inspect(format!("Count_{counter}"), move |row: &Row| {
        Metrics::counter(METRICS_NAMESPACE, counter).inc();
        let message = row.get_string("error_message").ok().flatten().unwrap_or("");
        tracing::warn!(counter, message, "kafka record failed");
    });
}

/// Builds the streaming Kafka Ouroboros pipeline.
pub fn build_pipeline(options: &PipelineOptions, args: &Args) -> Result<Pipeline, ExpansionError> {
    let p = Pipeline::create(options);
    let raw_schema: Arc<Schema> = raw_bytes_schema();

    // Read the loop topic from the earliest offset. This job also publishes the seeds, so
    // the read must consume them in any order of read and write. Records that Java cannot
    // decode go to `READ_ERRORS` and do not fail the bundle.
    let mut read = KafkaRead::new("KafkaRead", &args.bootstrap_servers, &args.loop_topic)
        .with_auto_offset_reset(OffsetReset::Earliest)
        .with_error_handling(READ_ERRORS);
    if let Some(group) = &args.group_id {
        read = read.with_consumer_config("group.id", group);
    }
    if args.gmk {
        read = read.with_google_managed_kafka_auth();
    }
    if let Some(service) = &args.expansion_service {
        read = read.with_expansion_service(service);
    }
    let read_outputs = p.apply(read.to_managed()?.with_all_outputs());
    let incoming = read_outputs.expect(managed::OUTPUT)?;
    count_errors(&read_outputs.expect(READ_ERRORS)?, "kafka_read_errors");

    // Seed messages to bootstrap the cycle.
    let seeds = p
        .apply(Create::new(
            "SeedOuroborosMessages",
            seed_messages(args.num_seeds, args.max_cycles)
                .iter()
                .map(|m| raw_bytes_row(m.to_json_bytes()))
                .collect::<Vec<_>>(),
        ))
        .with_row_schema(&raw_schema)
        .map("CountSeeds", |row: Row| {
            Metrics::counter(METRICS_NAMESPACE, "seeded").inc();
            row
        })
        .with_row_schema(&raw_schema);

    let merged = Flatten::pcollections("MergeIncomingAndSeeds", &[&incoming, &seeds])
        .with_row_schema(&raw_schema);

    // Evolve message states in Rust; completed messages exit the loop.
    let cycling = merged
        .flat_map("EvolveOuroborosState", |row: Row| process_record(&row))
        .with_row_schema(&raw_schema);

    // Write surviving messages back to the loop topic. Rows that Java cannot serialize go
    // to `WRITE_ERRORS`.
    let mut write = KafkaWrite::new("KafkaWrite", &args.bootstrap_servers, &args.loop_topic)
        .with_error_handling(WRITE_ERRORS);
    if args.gmk {
        write = write.with_google_managed_kafka_auth();
    }
    if let Some(service) = &args.expansion_service {
        write = write.with_expansion_service(service);
    }
    let write_outputs = cycling.apply(write.to_managed()?.with_outputs());
    count_errors(&write_outputs.expect(WRITE_ERRORS)?, "kafka_write_errors");

    Ok(p)
}
