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

//! Command-line option group and configuration resolution for the Nexmark launcher.

use beam::options::PipelineOptionGroup;
use clap::Args;
use serde::{Deserialize, Serialize};

use crate::config::{
    CoderStrategy, NexmarkConfig, NexmarkQueryName, NexmarkSuite, PubsubMode, RateShape, RateUnit,
    SideInputType, SinkType, SourceType,
};

/// Command-line flags for selecting and tuning Nexmark benchmark runs.
#[derive(Args, Clone, Debug, PartialEq, Serialize, Deserialize)]
#[command(name = "nexmark", about = "Nexmark benchmark launcher")]
pub struct NexmarkArgs {
    /// Predefined benchmark preset (`DEFAULT`, `SMOKE`, `STRESS`, `FULL_THROTTLE`).
    #[arg(long, default_value_t = NexmarkSuite::Default)]
    pub suite: NexmarkSuite,

    /// Single query number (`0..=16`) or name (`PASSTHROUGH`, `HOT_ITEMS`, etc.).
    #[arg(long)]
    pub query: Option<NexmarkQueryName>,

    /// Event source implementation (`DIRECT`).
    #[arg(long)]
    pub source_type: Option<SourceType>,

    /// Result sink mode (`COUNT_ONLY`, `DEVNULL`, `TEXT`).
    #[arg(long)]
    pub sink_type: Option<SinkType>,

    /// Directory or URI prefix for Query 10 files and `TEXT` sink outputs.
    #[arg(long)]
    pub output_path: Option<String>,

    /// Worker count multiplier for Query 10 log shard calculation (`5` shards per worker).
    #[arg(long, default_value_t = 5)]
    pub query10_max_workers: i32,

    /// Override for synthetic event count (`0` means unbounded in streaming mode).
    #[arg(long)]
    pub num_events: Option<i64>,

    /// Parallel generator split count.
    #[arg(long)]
    pub num_event_generators: Option<i32>,

    /// Rate curve waveform (`SINE` or `SQUARE`).
    #[arg(long)]
    pub rate_shape: Option<RateShape>,

    /// Initial synthetic event rate.
    #[arg(long)]
    pub first_event_rate: Option<i32>,

    /// Secondary synthetic event rate.
    #[arg(long)]
    pub next_event_rate: Option<i32>,

    /// Time unit for rate flags (`PER_SECOND` or `PER_MINUTE`).
    #[arg(long)]
    pub rate_unit: Option<RateUnit>,

    /// Full period of the rate curve in seconds.
    #[arg(long)]
    pub rate_period_sec: Option<i32>,

    /// Pace unbounded generation against wallclock inter-event delays.
    #[arg(long, num_args = 0..=1, default_missing_value = "true")]
    pub is_rate_limited: Option<bool>,

    /// Anchor event timestamps to wallclock start time instead of the fixed epoch.
    #[arg(long, num_args = 0..=1, default_missing_value = "true")]
    pub use_wallclock_event_time: Option<bool>,

    /// Primary window duration in seconds.
    #[arg(long)]
    pub window_size_sec: Option<i64>,

    /// Sliding window period in seconds.
    #[arg(long)]
    pub window_period_sec: Option<i64>,

    /// Unbounded source watermark lag behind event timestamps in seconds.
    #[arg(long)]
    pub watermark_holdback_sec: Option<i64>,

    /// Concurrent open auctions in the generator model.
    #[arg(long)]
    pub num_in_flight_auctions: Option<i32>,

    /// Concurrent active users in the generator model.
    #[arg(long)]
    pub num_active_people: Option<i32>,

    /// Modulus filter divisor for Query 2.
    #[arg(long)]
    pub auction_skip: Option<i32>,

    /// Combiner fanout factor for Queries 4, 5, and 7.
    #[arg(long)]
    pub fanout: Option<i32>,

    /// State retention horizon in seconds for Query 3 person records.
    #[arg(long)]
    pub max_auctions_waiting_time: Option<i32>,

    /// Inactivity gap in seconds for session-windowed queries.
    #[arg(long)]
    pub session_gap_sec: Option<i64>,

    /// Synthetic late-event delay in seconds.
    #[arg(long)]
    pub occasional_delay_sec: Option<i64>,

    /// Fraction of events delayed by `occasional_delay_sec` (`0.0..=1.0`).
    #[arg(long)]
    pub prob_delayed_event: Option<f64>,

    /// Maximum events per pane or log file in Queries 10 and 11.
    #[arg(long)]
    pub max_log_events: Option<i32>,

    /// Deterministic permutation block size (`1` keeps strict event ID order).
    #[arg(long)]
    pub out_of_order_group_size: Option<i64>,

    /// Enrichment lookup table row count for side-input join queries.
    #[arg(long)]
    pub side_input_row_count: Option<i32>,

    /// Shard keys of the shuffles in Queries 15 (`PORTABILITY_BATCH`) and 16 (`RESHUFFLE`).
    #[arg(long)]
    pub num_key_buckets: Option<i32>,

    /// Synthetic CPU load multiplier for Queries 13 and 14.
    #[arg(long)]
    pub pardo_cpu_factor: Option<f64>,

    /// Compare query output with its reference model in batch mode. A mismatch fails the job.
    #[arg(long)]
    pub assert_correctness: bool,

    /// Log each generated event at the `info` level.
    #[arg(long)]
    pub log_events: bool,

    /// Log each query result at the `info` level.
    #[arg(long)]
    pub log_results: bool,

    /// Seconds that a streaming run may last after its first event before the launcher
    /// cancels it.
    #[arg(long)]
    pub stream_timeout: Option<i32>,

    /// Cancel a finished streaming job instead of draining it.
    #[arg(long)]
    pub cancel_streaming_job_after_finish: bool,

    /// Side of the Pub/Sub or Kafka event exchange (`COMBINED`, `PUBLISH_ONLY`, `SUBSCRIBE_ONLY`).
    #[arg(long)]
    pub pubsub_mode: Option<PubsubMode>,

    /// Topic for events, as `projects/<project>/topics/<topic>`.
    #[arg(long)]
    pub pubsub_topic: Option<String>,

    /// Subscription to read events from, as `projects/<project>/subscriptions/<name>`.
    #[arg(long)]
    pub pubsub_subscription: Option<String>,

    /// Topic for query results with `--sink_type=PUBSUB`, as `projects/<project>/topics/<topic>`.
    #[arg(long)]
    pub pubsub_results_topic: Option<String>,

    /// Use the Pub/Sub publish time as event time instead of a timestamp attribute.
    #[arg(long, num_args = 0..=1, default_missing_value = "true")]
    pub use_pubsub_publish_time: Option<bool>,

    /// Kafka topic for events.
    #[arg(long)]
    pub kafka_topic: Option<String>,

    /// Kafka topic for query results with `--sink_type=KAFKA`.
    #[arg(long)]
    pub kafka_results_topic: Option<String>,

    /// Kafka bootstrap servers, as `host:port[,host:port...]`.
    #[arg(long)]
    pub bootstrap_servers: Option<String>,

    /// Authenticate the Kafka read and write to Google Cloud Managed Service for Apache Kafka.
    #[arg(long)]
    pub google_managed_kafka_auth: bool,

    /// Expansion service for the Pub/Sub transforms, instead of the default Java service.
    #[arg(long)]
    pub expansion_service: Option<String>,

    /// File prefix of the Avro events for `--source_type=AVRO`; the files are `<prefix>*.avro`.
    #[arg(long)]
    pub input_path: Option<String>,

    /// Coders of the event stream (`HAND` or `ROW`).
    #[arg(long)]
    pub coder_strategy: Option<CoderStrategy>,

    /// Source of the side-input join table (`DIRECT` or `CSV`).
    #[arg(long)]
    pub side_input_type: Option<SideInputType>,

    /// File prefix of the `CSV` side-input table; the files are `<prefix>*`.
    #[arg(long)]
    pub side_input_url: Option<String>,

    /// Number of text files in the `CSV` side-input table.
    #[arg(long)]
    pub side_input_num_shards: Option<i32>,

    /// Milliseconds of CPU work for each event before the query.
    #[arg(long)]
    pub cpu_delay_ms: Option<i64>,

    /// Bytes written to keyed state for each event before the query.
    #[arg(long)]
    pub disk_busy_bytes: Option<i64>,

    /// Seconds that the run-time limit subtracts from `--running_time_minutes`.
    #[arg(long)]
    pub preload_seconds: Option<i32>,

    /// Minutes after submission when the launcher cancels a job that still runs.
    #[arg(long)]
    pub running_time_minutes: Option<i64>,

    /// Queries to leave out, as numbers or names separated by commas.
    #[arg(long, value_delimiter = ',')]
    pub skip_queries: Vec<NexmarkQueryName>,

    /// Number of queries that run at the same time.
    #[arg(long, default_value_t = 1)]
    pub nexmark_parallel: usize,

    /// Write one summary row for each query to `--bigquery_table` after the suite.
    #[arg(long)]
    pub export_summary_to_bigquery: bool,

    /// Summary table, as `<project>:<dataset>.<table>`.
    #[arg(long)]
    pub bigquery_table: Option<String>,
}

impl Default for NexmarkArgs {
    fn default() -> Self {
        Self {
            suite: NexmarkSuite::Default,
            query: None,
            source_type: None,
            sink_type: None,
            output_path: None,
            query10_max_workers: 5,
            num_events: None,
            num_event_generators: None,
            rate_shape: None,
            first_event_rate: None,
            next_event_rate: None,
            rate_unit: None,
            rate_period_sec: None,
            is_rate_limited: None,
            use_wallclock_event_time: None,
            window_size_sec: None,
            window_period_sec: None,
            watermark_holdback_sec: None,
            num_in_flight_auctions: None,
            num_active_people: None,
            auction_skip: None,
            fanout: None,
            max_auctions_waiting_time: None,
            session_gap_sec: None,
            occasional_delay_sec: None,
            prob_delayed_event: None,
            max_log_events: None,
            out_of_order_group_size: None,
            side_input_row_count: None,
            num_key_buckets: None,
            pardo_cpu_factor: None,
            assert_correctness: false,
            log_events: false,
            log_results: false,
            stream_timeout: None,
            cancel_streaming_job_after_finish: false,
            pubsub_mode: None,
            pubsub_topic: None,
            pubsub_subscription: None,
            pubsub_results_topic: None,
            use_pubsub_publish_time: None,
            kafka_topic: None,
            kafka_results_topic: None,
            bootstrap_servers: None,
            google_managed_kafka_auth: false,
            expansion_service: None,
            input_path: None,
            coder_strategy: None,
            side_input_type: None,
            side_input_url: None,
            side_input_num_shards: None,
            cpu_delay_ms: None,
            disk_busy_bytes: None,
            preload_seconds: None,
            running_time_minutes: None,
            skip_queries: Vec::new(),
            nexmark_parallel: 1,
            export_summary_to_bigquery: false,
            bigquery_table: None,
        }
    }
}

impl PipelineOptionGroup for NexmarkArgs {}

impl NexmarkArgs {
    /// Expands the selected suite and CLI overrides into concrete per-query configurations.
    pub fn resolve_configs(&self) -> Vec<NexmarkConfig> {
        let base_configs = match (self.suite, self.query) {
            (NexmarkSuite::Default, selected) => vec![NexmarkConfig {
                query: Some(selected.unwrap_or(NexmarkQueryName::Passthrough)),
                ..NexmarkConfig::default()
            }],
            (
                suite @ (NexmarkSuite::Smoke | NexmarkSuite::Stress | NexmarkSuite::FullThrottle),
                Some(selected),
            ) => suite
                .configs()
                .into_iter()
                .filter(|cfg| cfg.query == Some(selected))
                .collect(),
            (
                suite @ (NexmarkSuite::Smoke | NexmarkSuite::Stress | NexmarkSuite::FullThrottle),
                None,
            ) => suite.configs(),
        };
        base_configs
            .into_iter()
            .map(|base| self.apply_overrides(base))
            .collect()
    }

    /// Clones these arguments pinned to one query so remote workers construct a single graph.
    pub fn pin_query(&self, query: NexmarkQueryName) -> Self {
        Self {
            query: Some(query),
            ..self.clone()
        }
    }

    /// Clones these arguments with `mode`, so the workers of a job build the side that it runs.
    pub fn pin_pubsub_mode(&self, mode: PubsubMode) -> Self {
        Self {
            pubsub_mode: Some(mode),
            ..self.clone()
        }
    }

    fn apply_overrides(&self, base: NexmarkConfig) -> NexmarkConfig {
        NexmarkConfig {
            query: self.query.or(base.query),
            source_type: self.source_type.unwrap_or(base.source_type),
            sink_type: self.sink_type.unwrap_or(base.sink_type),
            pubsub_mode: self.pubsub_mode.unwrap_or(base.pubsub_mode),
            coder_strategy: self.coder_strategy.unwrap_or(base.coder_strategy),
            side_input_type: self.side_input_type.unwrap_or(base.side_input_type),
            side_input_num_shards: self
                .side_input_num_shards
                .unwrap_or(base.side_input_num_shards),
            cpu_delay_ms: self.cpu_delay_ms.unwrap_or(base.cpu_delay_ms),
            disk_busy_bytes: self.disk_busy_bytes.unwrap_or(base.disk_busy_bytes),
            preload_seconds: self.preload_seconds.unwrap_or(base.preload_seconds),
            use_pubsub_publish_time: self
                .use_pubsub_publish_time
                .unwrap_or(base.use_pubsub_publish_time),
            side_input_row_count: self
                .side_input_row_count
                .unwrap_or(base.side_input_row_count),
            session_gap_sec: self.session_gap_sec.unwrap_or(base.session_gap_sec),
            num_events: self.num_events.unwrap_or(base.num_events),
            num_event_generators: self
                .num_event_generators
                .unwrap_or(base.num_event_generators),
            rate_shape: self.rate_shape.unwrap_or(base.rate_shape),
            first_event_rate: self.first_event_rate.unwrap_or(base.first_event_rate),
            next_event_rate: self.next_event_rate.unwrap_or(base.next_event_rate),
            rate_unit: self.rate_unit.unwrap_or(base.rate_unit),
            rate_period_sec: self.rate_period_sec.unwrap_or(base.rate_period_sec),
            stream_timeout: self.stream_timeout.unwrap_or(base.stream_timeout),
            is_rate_limited: self.is_rate_limited.unwrap_or(base.is_rate_limited),
            use_wallclock_event_time: self
                .use_wallclock_event_time
                .unwrap_or(base.use_wallclock_event_time),
            window_size_sec: self.window_size_sec.unwrap_or(base.window_size_sec),
            window_period_sec: self.window_period_sec.unwrap_or(base.window_period_sec),
            watermark_holdback_sec: self
                .watermark_holdback_sec
                .unwrap_or(base.watermark_holdback_sec),
            num_in_flight_auctions: self
                .num_in_flight_auctions
                .unwrap_or(base.num_in_flight_auctions),
            num_active_people: self.num_active_people.unwrap_or(base.num_active_people),
            auction_skip: self.auction_skip.unwrap_or(base.auction_skip),
            fanout: self.fanout.unwrap_or(base.fanout),
            max_auctions_waiting_time: self
                .max_auctions_waiting_time
                .unwrap_or(base.max_auctions_waiting_time),
            occasional_delay_sec: self
                .occasional_delay_sec
                .unwrap_or(base.occasional_delay_sec),
            prob_delayed_event: self.prob_delayed_event.unwrap_or(base.prob_delayed_event),
            max_log_events: self.max_log_events.unwrap_or(base.max_log_events),
            out_of_order_group_size: self
                .out_of_order_group_size
                .unwrap_or(base.out_of_order_group_size),
            num_key_buckets: self.num_key_buckets.unwrap_or(base.num_key_buckets),
            pardo_cpu_factor: self.pardo_cpu_factor.unwrap_or(base.pardo_cpu_factor),
            ..base
        }
    }
}
