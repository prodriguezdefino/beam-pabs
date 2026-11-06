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

//! Pipeline graph assembly for Nexmark sources, monitors, queries, checks, and sinks.

use std::fmt::Debug;
use std::time::Duration;

use beam::coders::DefaultCoder;
use beam::io::file::textio;
use beam::options::PipelineOptions;
use beam::prelude::*;
use beam::windowing::{
    AccumulationMode, FixedWindows, GlobalWindows, Trigger, WindowInto, is_globally_windowed,
};

use super::NexmarkArgs;
use super::avro::{read_avro_events, with_coder_strategy, write_avro_events};
use super::error::LauncherError;
use super::monitor::{EVENTS, Monitor, RESULTS};
use super::transport::{
    read_kafka_events, read_pubsub_events, write_kafka_events, write_kafka_results,
    write_pubsub_events, write_pubsub_results,
};
use crate::config::{NexmarkConfig, NexmarkQueryName, PubsubMode, SinkType, SourceType};
use crate::correctness::{assert_final_seller_prices, assert_timestamped};
use crate::model::{Event, KnownSize};
use crate::models;
use crate::queries::{
    BoundedSideInputJoin, Query0, Query1, Query2, Query3, Query4, Query5, Query6, Query7, Query8,
    Query9, Query10, Query11, Query12, Query13, Query14, SessionSideInputJoin,
};
use crate::source::{BoundedEventSource, UnboundedEventSource};

/// Name of the `--assert_correctness` check. A pipeline holds at most one.
const CORRECTNESS_CHECK: &str = "AssertCorrectness";

/// Checks whether `--runner` selects the local Prism runner.
pub fn is_prism_runner(runner: &str) -> bool {
    let trimmed = runner.trim();
    trimmed.is_empty()
        || trimmed.eq_ignore_ascii_case("prism")
        || trimmed.eq_ignore_ascii_case("prismrunner")
}

/// Assembles a complete [`Pipeline`] for one resolved [`NexmarkConfig`].
///
/// The [`CoderStrategy`](crate::config::CoderStrategy) and the event [`Monitor`] follow the
/// source, and the result [`Monitor`] precedes the sink. With a Pub/Sub or Kafka source,
/// `PUBLISH_ONLY` gives the publisher graph: generator, monitor and topic, with no query.
/// `SUBSCRIBE_ONLY` and `COMBINED` give the query graph that reads the topic;
/// [`run_nexmark`](super::run_nexmark) builds the `COMBINED` publisher separately.
pub fn build_nexmark_pipeline(
    options: &PipelineOptions,
    args: &NexmarkArgs,
    config: &NexmarkConfig,
) -> Result<Pipeline, LauncherError> {
    let output_path = args
        .output_path
        .as_deref()
        .map(str::trim)
        .filter(|path| !path.is_empty());
    let needs_output_path = match config.sink_type {
        SinkType::Text => Some("--sink_type=TEXT"),
        SinkType::Avro => Some("--sink_type=AVRO"),
        SinkType::CountOnly | SinkType::Devnull | SinkType::Pubsub | SinkType::Kafka => None,
    };
    if let (Some(needed_by), None) = (needs_output_path, output_path) {
        return Err(missing_output_path(needed_by));
    }

    let pipeline = Pipeline::create(options);
    let source = config.source_type;
    let mode = config.pubsub_mode;
    let events = match (source, mode, options.streaming) {
        (SourceType::Direct, _, _)
        | (SourceType::Pubsub | SourceType::Kafka, PubsubMode::PublishOnly, _) => {
            generated_events(&pipeline, options, config)
        }
        (
            SourceType::Pubsub | SourceType::Kafka,
            PubsubMode::Combined | PubsubMode::SubscribeOnly,
            false,
        ) => return Err(LauncherError::NeedsStreaming(source)),
        (SourceType::Pubsub, PubsubMode::Combined | PubsubMode::SubscribeOnly, true) => {
            read_pubsub_events(&pipeline, args, config)?
        }
        (SourceType::Kafka, PubsubMode::Combined | PubsubMode::SubscribeOnly, true) => {
            read_kafka_events(&pipeline, args)?
        }
        (SourceType::Avro, _, _) => read_avro_events(&pipeline, args)?,
    };
    let events = with_coder_strategy(events, config.coder_strategy).apply(
        "MonitorEvents",
        Monitor::new(EVENTS).with_logging(args.log_events),
    );

    match (source, mode) {
        (SourceType::Pubsub, PubsubMode::PublishOnly) => write_pubsub_events(&events, args, config),
        (SourceType::Kafka, PubsubMode::PublishOnly) => write_kafka_events(&events, args),
        (SourceType::Direct | SourceType::Avro, _)
        | (
            SourceType::Pubsub | SourceType::Kafka,
            PubsubMode::Combined | PubsubMode::SubscribeOnly,
        ) => {
            if let (SinkType::Avro, Some(path)) = (config.sink_type, output_path) {
                write_avro_events(&events, path, config, options.streaming);
            }
            add_query(
                &events,
                &QueryTail {
                    options,
                    args,
                    config,
                    query: config.query.unwrap_or(NexmarkQueryName::Passthrough),
                    output_path,
                },
            )
        }
    }?;
    Ok(pipeline)
}

fn missing_output_path(needed_by: &'static str) -> LauncherError {
    LauncherError::MissingFlag {
        flag: "--output_path",
        needed_by,
    }
}

/// Events from the generator: unbounded in streaming mode, bounded otherwise.
fn generated_events(
    pipeline: &Pipeline,
    options: &PipelineOptions,
    config: &NexmarkConfig,
) -> PCollection<Event> {
    match options.streaming {
        true => pipeline.apply("ReadUnboundedEvents", UnboundedEventSource::new(config)),
        false => pipeline.apply("ReadBoundedEvents", BoundedEventSource::new(config)),
    }
}

/// Adds the selected query, its correctness check, the result monitor and the sink.
fn add_query(events: &PCollection<Event>, tail: &QueryTail<'_>) -> Result<(), LauncherError> {
    let QueryTail {
        args,
        config,
        query,
        output_path,
        ..
    } = *tail;
    match query {
        NexmarkQueryName::Passthrough => tail.finish(
            &events.apply("Query0", Query0),
            Some(&|out| assert_timestamped(CORRECTNESS_CHECK, out, models::query0(config))),
        ),
        NexmarkQueryName::CurrencyConversion => tail.finish(
            &events.apply("Query1", Query1),
            Some(&|out| assert_timestamped(CORRECTNESS_CHECK, out, models::query1(config))),
        ),
        NexmarkQueryName::Selection => tail.finish(
            &events.apply("Query2", Query2::from_config(config)),
            Some(&|out| assert_timestamped(CORRECTNESS_CHECK, out, models::query2(config))),
        ),
        NexmarkQueryName::LocalItemSuggestion => tail.finish(
            &events.apply("Query3", Query3::from_config(config)),
            Some(&|out| assert_timestamped(CORRECTNESS_CHECK, out, models::query3(config))),
        ),
        NexmarkQueryName::AveragePriceForCategory => tail.finish(
            &events.apply("Query4", Query4::from_config(config)),
            Some(&|out| assert_timestamped(CORRECTNESS_CHECK, out, models::query4(config))),
        ),
        NexmarkQueryName::HotItems => tail.finish(
            &events.apply("Query5", Query5::from_config(config)),
            Some(&|out| assert_timestamped(CORRECTNESS_CHECK, out, models::query5(config))),
        ),
        NexmarkQueryName::AverageSellingPriceBySeller => tail.finish(
            &events.apply("Query6", Query6::from_config(config)),
            Some(&|out| assert_final_seller_prices(CORRECTNESS_CHECK, out, models::query6(config))),
        ),
        NexmarkQueryName::HighestBid => tail.finish(
            &events.apply("Query7", Query7::from_config(config)),
            Some(&|out| assert_timestamped(CORRECTNESS_CHECK, out, models::query7(config))),
        ),
        NexmarkQueryName::MonitorNewUsers => tail.finish(
            &events.apply("Query8", Query8::from_config(config)),
            Some(&|out| assert_timestamped(CORRECTNESS_CHECK, out, models::query8(config))),
        ),
        NexmarkQueryName::WinningBids => tail.finish(
            &events.apply("Query9", Query9::from_config(config)),
            Some(&|out| assert_timestamped(CORRECTNESS_CHECK, out, models::query9(config))),
        ),
        NexmarkQueryName::LogToShardedFiles => {
            let q10_dir =
                output_path.map(|base| format!("{}/{}", base.trim_end_matches('/'), query.slug()));
            let query10 = Query10::from_config(config)
                .with_output_path(q10_dir)
                .with_max_num_workers(args.query10_max_workers);
            tail.finish(&events.apply("Query10", query10), None)
        }
        NexmarkQueryName::UserSessions => {
            tail.finish(&events.apply("Query11", Query11::from_config(config)), None)
        }
        NexmarkQueryName::ProcessingTimeWindows => {
            tail.finish(&events.apply("Query12", Query12::from_config(config)), None)
        }
        NexmarkQueryName::PortabilityBatch => {
            tail.finish(&events.apply("Query13", Query13::from_config(config)), None)
        }
        NexmarkQueryName::Reshuffle => tail.finish(&events.apply("Query14", Query14), None),
        NexmarkQueryName::BoundedSideInputJoin => tail.finish(
            &events.apply(
                "BoundedSideInputJoin",
                BoundedSideInputJoin::from_config(config),
            ),
            Some(&|out| {
                assert_timestamped(
                    CORRECTNESS_CHECK,
                    out,
                    models::bounded_side_input_join(config),
                );
            }),
        ),
        NexmarkQueryName::SessionSideInputJoin => tail.finish(
            &events.apply(
                "SessionSideInputJoin",
                SessionSideInputJoin::from_config(config),
            ),
            Some(&|out| {
                assert_timestamped(
                    CORRECTNESS_CHECK,
                    out,
                    models::session_side_input_join(config),
                );
            }),
        ),
    }
}

/// Adds the `--assert_correctness` check of one query to its output.
type ModelCheck<'a, T> = &'a dyn Fn(&PCollection<T>);

/// Settings that the steps after a query need: the correctness check, the result monitor
/// and the sink.
#[derive(Clone, Copy)]
struct QueryTail<'a> {
    options: &'a PipelineOptions,
    args: &'a NexmarkArgs,
    config: &'a NexmarkConfig,
    query: NexmarkQueryName,
    output_path: Option<&'a str>,
}

impl QueryTail<'_> {
    /// Adds the correctness check when it applies, then the result monitor and the sink.
    ///
    /// `check` is `None` for a query without a reference model. The check applies only in
    /// batch mode.
    fn finish<T>(
        &self,
        output: &PCollection<T>,
        check: Option<ModelCheck<'_, T>>,
    ) -> Result<(), LauncherError>
    where
        T: DefaultCoder + KnownSize + Debug + Send + Sync + 'static,
    {
        if let (true, false, Some(check)) =
            (self.args.assert_correctness, self.options.streaming, check)
        {
            check(output);
        }
        let results = output.apply(
            "MonitorResults",
            Monitor::new(RESULTS).with_logging(self.args.log_results),
        );
        self.apply_sink(&results)
    }

    fn apply_sink<T>(&self, output: &PCollection<T>) -> Result<(), LauncherError>
    where
        T: DefaultCoder + Debug + Send + Sync + 'static,
    {
        match self.config.sink_type {
            SinkType::CountOnly => {}
            SinkType::Devnull => {
                let _ = output.filter("DevnullSink", |elem: &T| {
                    std::hint::black_box(format!("{elem:?}"));
                    false
                });
            }
            SinkType::Text => {
                let base = self
                    .output_path
                    .ok_or_else(|| missing_output_path("--sink_type=TEXT"))?;
                let prefix = format!("{}/{}", base.trim_end_matches('/'), self.query.slug());
                let formatted = output.map("FormatSinkText", |elem: T| format!("{elem:?}"));
                let rewindowed = match (self.options.streaming, is_globally_windowed(&formatted)) {
                    (true, true) => formatted.window_into(
                        "SinkFixedWindows",
                        FixedWindows::of(Duration::from_secs(
                            self.config.window_size_sec.max(1) as u64
                        )),
                    ),
                    (false, false) => formatted.apply(
                        "SinkGlobalWindow",
                        WindowInto::new(GlobalWindows)
                            .with_trigger(Trigger::Default)
                            .with_accumulation_mode(AccumulationMode::Discarding),
                    ),
                    (true, false) | (false, true) => formatted,
                };
                let write = textio::Write::new(prefix).with_suffix(".txt");
                let _ = match self.options.streaming {
                    true => rewindowed.apply("WriteTextSink", write.with_windowed_writes()),
                    false => rewindowed.apply("WriteTextSink", write),
                };
            }
            SinkType::Pubsub => write_pubsub_results(output, self.args, self.config)?,
            SinkType::Kafka => write_kafka_results(output, self.args)?,
            // The Avro sink writes the input events, after the source, and discards results.
            SinkType::Avro => {}
        }
        Ok(())
    }
}
