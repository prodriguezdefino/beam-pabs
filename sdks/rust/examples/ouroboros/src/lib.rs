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

//! A streaming loop: the pipeline reads messages from a topic and writes them back to it.
//!
//! The pipeline adds `--num_seeds` new messages to the loop. Each pass through
//! [`Ouroboros`] moves a message to its next [`Stage`]. A message leaves the loop when it
//! gets to [`Stage::Ascended`] after `--max_cycles` passes.
//!
//! `--transport` selects the topic:
//!
//! - `kafka`: the cross-language Kafka read and write, expanded through Managed. Records that
//!   the Java transforms cannot decode or serialize go to an error output.
//! - `pubsub`: the cross-language Pub/Sub read and write.
//!
//! The `ouroboros` counters `seeded`, `evolved`, `ascended` and `malformed` show the progress.

pub mod kafka;
pub mod pubsub;

use beam::prelude::*;
use clap::{Args as ClapArgs, ValueEnum};
use serde::{Deserialize, Serialize};

/// Namespace of the counters of the pipeline.
const METRICS_NAMESPACE: &str = "ouroboros";

/// The messaging system of the loop topic.
#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Transport {
    /// A Kafka topic.
    Kafka,
    /// A Pub/Sub topic.
    Pubsub,
}

/// Options for this pipeline, on top of the standard Beam options.
#[derive(ClapArgs, Serialize, Deserialize, Clone, Debug)]
#[command(name = "ouroboros", about = "Apache Beam Ouroboros streaming example")]
pub struct Args {
    /// The messaging system of the loop topic.
    #[arg(long, value_enum)]
    pub transport: Transport,

    /// Topic to read from and write back to: a Kafka topic name, or
    /// `projects/<project>/topics/<topic>` for Pub/Sub.
    #[arg(long)]
    pub loop_topic: String,

    /// Number of passes through the loop before a message leaves it.
    #[arg(long, default_value_t = 5)]
    pub max_cycles: i64,

    /// Number of new messages to add to the loop when the pipeline starts.
    #[arg(long, default_value_t = 1)]
    pub num_seeds: i64,

    /// Kafka bootstrap servers, as `host1:port1,host2:port2`. Required with `--transport=kafka`.
    #[arg(long, required_if_eq("transport", "kafka"))]
    pub bootstrap_servers: Option<String>,

    /// Kafka consumer group ID. By default, the read makes one from its configuration.
    #[arg(long)]
    pub group_id: Option<String>,

    /// Authenticate to a Google Cloud Managed Service for Apache Kafka cluster with the
    /// Google credentials of the workers.
    #[arg(long)]
    pub gmk: bool,

    /// Pub/Sub subscription, `projects/<project>/subscriptions/<subscription>`, to read
    /// instead of `--loop_topic`.
    #[arg(long)]
    pub subscription: Option<String>,

    /// Address (host:port) of an expansion service to use instead of the Java one that the SDK starts.
    #[arg(long)]
    pub expansion_service: Option<String>,
}

impl PipelineOptionGroup for Args {}

/// The stages of a message in the loop.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Stage {
    Egg,
    SheddingSkin,
    GrowingCoils,
    DevouringTail,
    Regenerating,
    Ascended,
}

/// The payload of a message in the loop, encoded as JSON.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OuroborosMessage {
    pub cycle_id: String,
    pub iteration: i64,
    pub max_iterations: i64,
    pub stage: Stage,
}

impl OuroborosMessage {
    /// A new message, before its first pass through the loop.
    pub fn new(cycle_id: impl Into<String>, max_iterations: i64) -> Self {
        Self {
            cycle_id: cycle_id.into(),
            iteration: 0,
            max_iterations,
            stage: Stage::Egg,
        }
    }

    /// The message after one more pass through the loop.
    pub fn evolve(self) -> Self {
        let iteration = self.iteration + 1;
        let stage = match iteration % 4 {
            _ if iteration >= self.max_iterations => Stage::Ascended,
            1 => Stage::SheddingSkin,
            2 => Stage::GrowingCoils,
            3 => Stage::DevouringTail,
            _ => Stage::Regenerating,
        };
        Self {
            iteration,
            stage,
            ..self
        }
    }
}

/// What happens to a message on one pass through the loop.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Step {
    /// The message evolved and goes back to the topic.
    Continue(OuroborosMessage),
    /// The message evolved into [`Stage::Ascended`] and leaves the loop.
    Ascended(OuroborosMessage),
    /// The message was ascended when it arrived, for example from an earlier run. It is dropped.
    AlreadyAscended,
}

/// Decides what happens to a payload read from the loop topic.
///
/// # Errors
///
/// Returns an error if the payload is not an [`OuroborosMessage`].
pub fn step(payload: &[u8]) -> Result<Step, serde_json::Error> {
    let message: OuroborosMessage = serde_json::from_slice(payload)?;
    Ok(match message.stage {
        Stage::Ascended => Step::AlreadyAscended,
        _ => match message.evolve() {
            next @ OuroborosMessage {
                stage: Stage::Ascended,
                ..
            } => Step::Ascended(next),
            next => Step::Continue(next),
        },
    })
}

/// Adds the seed messages to the payloads read from the loop topic, and evolves each
/// message by one pass. The output holds the payloads to write back to the topic.
///
/// A payload that is not an [`OuroborosMessage`] is counted as `malformed` and dropped.
pub struct Ouroboros {
    /// Number of new messages to add to the loop.
    pub num_seeds: i64,
    /// Number of passes through the loop before a message leaves it.
    pub max_cycles: i64,
}

impl PTransform<PCollection<Vec<u8>>> for Ouroboros {
    type Output = PCollection<Vec<u8>>;

    fn expand(&self, incoming: &PCollection<Vec<u8>>) -> Self::Output {
        let seeded = Metrics::counter(METRICS_NAMESPACE, "seeded");
        let evolved = Metrics::counter(METRICS_NAMESPACE, "evolved");
        let ascended = Metrics::counter(METRICS_NAMESPACE, "ascended");
        let malformed = Metrics::counter(METRICS_NAMESPACE, "malformed");
        let max_cycles = self.max_cycles;

        let seeds = incoming
            .pipeline()
            .apply(
                "CreateSeedIds",
                Create::new((0..self.num_seeds).collect::<Vec<i64>>()),
            )
            .par_do_fn("ToSeedMessages", move |i: i64, out| {
                let seed = OuroborosMessage::new(format!("ouroboros-{i}"), max_cycles);
                out.emit(serde_json::to_vec(&seed)?)
            })
            .inspect("CountSeeds", move |_: &Vec<u8>| seeded.inc());

        Flatten::pcollections("AddSeeds", &[incoming, &seeds]).par_do_fn(
            "Evolve",
            move |payload: Vec<u8>, out| match step(&payload) {
                Ok(Step::Continue(next)) => {
                    evolved.inc();
                    out.emit(serde_json::to_vec(&next)?)
                }
                Ok(Step::Ascended(OuroborosMessage { cycle_id, .. })) => {
                    evolved.inc();
                    ascended.inc();
                    tracing::info!(%cycle_id, "ouroboros ascended");
                    Ok(())
                }
                Ok(Step::AlreadyAscended) => Ok(()),
                Err(error) => {
                    malformed.inc();
                    tracing::warn!(%error, "dropped a payload that is not an ouroboros message");
                    Ok(())
                }
            },
        )
    }
}
