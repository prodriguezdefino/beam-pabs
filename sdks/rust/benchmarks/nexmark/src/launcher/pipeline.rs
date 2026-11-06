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

use std::fmt::{self, Debug};
use std::time::Duration;

use beam::coders::DefaultCoder;
use beam::io::file::textio;
use beam::options::{OptionsError, PipelineOptions};
use beam::prelude::*;
use beam::runners::{JobState, RunnerError};
use beam::windowing::{
    AccumulationMode, FixedWindows, GlobalWindows, Trigger, WindowInto, is_globally_windowed,
};

use super::NexmarkArgs;
use super::monitor::{EVENTS, Monitor, RESULTS};
use crate::config::{NexmarkConfig, NexmarkQueryName, SinkType, SourceType};
use crate::correctness::{assert_final_seller_prices, assert_timestamped};
use crate::model::KnownSize;
use crate::models;
use crate::queries::{
    BoundedSideInputJoin, Query0, Query1, Query2, Query3, Query4, Query5, Query6, Query7, Query8,
    Query9, Query10, Query11, Query12, Query13, Query14, SessionSideInputJoin,
};
use crate::source::{BoundedEventSource, UnboundedEventSource};

/// Name of the `--assert_correctness` check. A pipeline holds at most one.
const CORRECTNESS_CHECK: &str = "AssertCorrectness";

/// Failure modes encountered while configuring or running a Nexmark pipeline.
#[derive(Debug)]
pub enum LauncherError {
    /// `SinkType::Text` was selected without providing `--output_path`.
    MissingOutputPath,
    /// Updating or snapshotting `PipelineOptions` failed.
    Options(OptionsError),
    /// Runner execution failed.
    Runner(RunnerError),
    /// The job ended in a state other than `DONE` or `DRAINED`.
    Unsuccessful(JobState),
    /// A `--assert_correctness` check did not run or did not pass.
    Correctness(String),
    /// The streaming monitor cancelled the job because of fatal errors or no progress.
    Stopped(String),
}

impl fmt::Display for LauncherError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingOutputPath => {
                f.write_str("--output_path is required when --sink_type=TEXT")
            }
            Self::Options(err) => write!(f, "pipeline options error: {err}"),
            Self::Runner(err) => write!(f, "pipeline execution error: {err}"),
            Self::Unsuccessful(state) => write!(f, "job ended in state {state}"),
            Self::Correctness(reason) => write!(f, "correctness check failed: {reason}"),
            Self::Stopped(reason) => write!(f, "streaming job cancelled: {reason}"),
        }
    }
}

impl std::error::Error for LauncherError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Options(err) => Some(err),
            Self::Runner(err) => Some(err),
            Self::MissingOutputPath
            | Self::Unsuccessful(_)
            | Self::Correctness(_)
            | Self::Stopped(_) => None,
        }
    }
}

impl From<OptionsError> for LauncherError {
    fn from(err: OptionsError) -> Self {
        Self::Options(err)
    }
}

impl From<RunnerError> for LauncherError {
    fn from(err: RunnerError) -> Self {
        Self::Runner(err)
    }
}

/// Checks whether `--runner` selects the local Prism runner.
pub fn is_prism_runner(runner: &str) -> bool {
    let trimmed = runner.trim();
    trimmed.is_empty()
        || trimmed.eq_ignore_ascii_case("prism")
        || trimmed.eq_ignore_ascii_case("prismrunner")
}

/// Assembles a complete source-to-sink [`Pipeline`] for one resolved [`NexmarkConfig`].
///
/// The event [`Monitor`] follows the source and the result [`Monitor`] precedes the sink.
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
    if config.sink_type == SinkType::Text && output_path.is_none() {
        return Err(LauncherError::MissingOutputPath);
    }

    let query = config.query.unwrap_or(NexmarkQueryName::Passthrough);
    let pipeline = Pipeline::create(options);
    let raw_events = match (config.source_type, options.streaming) {
        (SourceType::Direct, true) => {
            pipeline.apply("ReadUnboundedEvents", UnboundedEventSource::new(config))
        }
        (SourceType::Direct, false) => {
            pipeline.apply("ReadBoundedEvents", BoundedEventSource::new(config))
        }
    };
    let events = raw_events.apply(
        "MonitorEvents",
        Monitor::new(EVENTS).with_logging(args.log_events),
    );

    let tail = QueryTail {
        options,
        args,
        config,
        query,
        output_path,
    };
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
    }?;

    Ok(pipeline)
}

/// Adds the `--assert_correctness` check of one query to its output.
type ModelCheck<'a, T> = &'a dyn Fn(&PCollection<T>);

/// Settings that the steps after a query need: the correctness check, the result monitor
/// and the sink.
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
                let base = self.output_path.ok_or(LauncherError::MissingOutputPath)?;
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
        }
        Ok(())
    }
}
