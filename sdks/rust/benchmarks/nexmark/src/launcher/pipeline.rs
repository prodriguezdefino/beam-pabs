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

//! Pipeline graph assembly for Nexmark sources, queries, metrics, and sinks.

use std::fmt::{self, Debug};
use std::time::Duration;

use beam::coders::DefaultCoder;
use beam::io::file::textio;
use beam::metrics::Metrics;
use beam::options::{OptionsError, PipelineOptions};
use beam::prelude::*;
use beam::runners::RunnerError;
use beam::windowing::{
    AccumulationMode, FixedWindows, GlobalWindows, Trigger, WindowInto, is_globally_windowed,
};

use super::NexmarkArgs;
use crate::config::{NexmarkConfig, NexmarkQueryName, SinkType, SourceType};
use crate::model::{Event, KnownSize};
use crate::queries::{
    BoundedSideInputJoin, Query0, Query1, Query2, Query3, Query4, Query5, Query6, Query7, Query8,
    Query9, Query10, Query11, Query12, Query13, Query14, SessionSideInputJoin,
};
use crate::source::{BoundedEventSource, UnboundedEventSource};

/// Metric namespace reported by Nexmark source and sink stages.
pub const METRIC_NAMESPACE: &str = "nexmark";

/// Counter name for generated input events.
pub const COUNTER_SOURCE_EVENTS: &str = "source_events";

/// Counter name for estimated generated input bytes.
pub const COUNTER_SOURCE_BYTES: &str = "source_bytes";

/// Counter name for emitted query output elements.
pub const COUNTER_SINK_RECORDS: &str = "sink_records";

/// Counter name for estimated emitted query output bytes.
pub const COUNTER_SINK_BYTES: &str = "sink_bytes";

/// Failure modes encountered while configuring or running a Nexmark pipeline.
#[derive(Debug)]
pub enum LauncherError {
    /// `SinkType::Text` was selected without providing `--output_path`.
    MissingOutputPath,
    /// Updating or snapshotting `PipelineOptions` failed.
    Options(OptionsError),
    /// Runner execution failed.
    Runner(RunnerError),
}

impl fmt::Display for LauncherError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingOutputPath => {
                f.write_str("--output_path is required when --sink_type=TEXT")
            }
            Self::Options(err) => write!(f, "pipeline options error: {err}"),
            Self::Runner(err) => write!(f, "pipeline execution error: {err}"),
        }
    }
}

impl std::error::Error for LauncherError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::MissingOutputPath => None,
            Self::Options(err) => Some(err),
            Self::Runner(err) => Some(err),
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
pub fn build_nexmark_pipeline(
    options: &PipelineOptions,
    args: &NexmarkArgs,
    config: &NexmarkConfig,
) -> Result<Pipeline, LauncherError> {
    let clean_output_path = args
        .output_path
        .as_deref()
        .map(str::trim)
        .filter(|path| !path.is_empty());
    if config.sink_type == SinkType::Text && clean_output_path.is_none() {
        return Err(LauncherError::MissingOutputPath);
    }

    let query = config.query.unwrap_or(NexmarkQueryName::Passthrough);
    let pipeline = Pipeline::create(options);
    let raw_events = match config.source_type {
        SourceType::Direct => {
            if options.streaming {
                pipeline.apply("ReadUnboundedEvents", UnboundedEventSource::new(config))
            } else {
                pipeline.apply("ReadBoundedEvents", BoundedEventSource::new(config))
            }
        }
    };

    let events = raw_events.map("RecordSourceMetrics", |event: Event| {
        Metrics::counter(METRIC_NAMESPACE, COUNTER_SOURCE_EVENTS).inc();
        Metrics::counter(METRIC_NAMESPACE, COUNTER_SOURCE_BYTES)
            .inc_by(event.estimated_size_bytes() as i64);
        event
    });

    match query {
        NexmarkQueryName::Passthrough => {
            let out = events.apply("Query0", Query0);
            apply_sink(&out, options, clean_output_path, config, query)?;
        }
        NexmarkQueryName::CurrencyConversion => {
            let out = events.apply("Query1", Query1);
            apply_sink(&out, options, clean_output_path, config, query)?;
        }
        NexmarkQueryName::Selection => {
            let out = events.apply("Query2", Query2::from_config(config));
            apply_sink(&out, options, clean_output_path, config, query)?;
        }
        NexmarkQueryName::LocalItemSuggestion => {
            let out = events.apply("Query3", Query3::from_config(config));
            apply_sink(&out, options, clean_output_path, config, query)?;
        }
        NexmarkQueryName::AveragePriceForCategory => {
            let out = events.apply("Query4", Query4::from_config(config));
            apply_sink(&out, options, clean_output_path, config, query)?;
        }
        NexmarkQueryName::HotItems => {
            let out = events.apply("Query5", Query5::from_config(config));
            apply_sink(&out, options, clean_output_path, config, query)?;
        }
        NexmarkQueryName::AverageSellingPriceBySeller => {
            let out = events.apply("Query6", Query6::from_config(config));
            apply_sink(&out, options, clean_output_path, config, query)?;
        }
        NexmarkQueryName::HighestBid => {
            let out = events.apply("Query7", Query7::from_config(config));
            apply_sink(&out, options, clean_output_path, config, query)?;
        }
        NexmarkQueryName::MonitorNewUsers => {
            let out = events.apply("Query8", Query8::from_config(config));
            apply_sink(&out, options, clean_output_path, config, query)?;
        }
        NexmarkQueryName::WinningBids => {
            let out = events.apply("Query9", Query9::from_config(config));
            apply_sink(&out, options, clean_output_path, config, query)?;
        }
        NexmarkQueryName::LogToShardedFiles => {
            let q10_dir = clean_output_path
                .map(|base| format!("{}/{}", base.trim_end_matches('/'), query.slug()));
            let out = events.apply(
                "Query10",
                Query10::from_config(config)
                    .with_output_path(q10_dir)
                    .with_max_num_workers(args.query10_max_workers),
            );
            apply_sink(&out, options, clean_output_path, config, query)?;
        }
        NexmarkQueryName::UserSessions => {
            let out = events.apply("Query11", Query11::from_config(config));
            apply_sink(&out, options, clean_output_path, config, query)?;
        }
        NexmarkQueryName::ProcessingTimeWindows => {
            let out = events.apply("Query12", Query12::from_config(config));
            apply_sink(&out, options, clean_output_path, config, query)?;
        }
        NexmarkQueryName::PortabilityBatch => {
            let out = events.apply("Query13", Query13::from_config(config));
            apply_sink(&out, options, clean_output_path, config, query)?;
        }
        NexmarkQueryName::Reshuffle => {
            let out = events.apply("Query14", Query14);
            apply_sink(&out, options, clean_output_path, config, query)?;
        }
        NexmarkQueryName::BoundedSideInputJoin => {
            let out = events.apply(
                "BoundedSideInputJoin",
                BoundedSideInputJoin::from_config(config),
            );
            apply_sink(&out, options, clean_output_path, config, query)?;
        }
        NexmarkQueryName::SessionSideInputJoin => {
            let out = events.apply(
                "SessionSideInputJoin",
                SessionSideInputJoin::from_config(config),
            );
            apply_sink(&out, options, clean_output_path, config, query)?;
        }
    }

    Ok(pipeline)
}

fn apply_sink<T>(
    output: &PCollection<T>,
    options: &PipelineOptions,
    clean_output_path: Option<&str>,
    config: &NexmarkConfig,
    query: NexmarkQueryName,
) -> Result<(), LauncherError>
where
    T: DefaultCoder + KnownSize + Debug + Send + Sync + 'static,
{
    match config.sink_type {
        SinkType::CountOnly => {
            let _ = output.filter("CountOnlySink", |elem: &T| {
                Metrics::counter(METRIC_NAMESPACE, COUNTER_SINK_RECORDS).inc();
                Metrics::counter(METRIC_NAMESPACE, COUNTER_SINK_BYTES)
                    .inc_by(elem.estimated_size_bytes() as i64);
                false
            });
        }
        SinkType::Devnull => {
            let _ = output.filter("DevnullSink", |elem: &T| {
                Metrics::counter(METRIC_NAMESPACE, COUNTER_SINK_RECORDS).inc();
                Metrics::counter(METRIC_NAMESPACE, COUNTER_SINK_BYTES)
                    .inc_by(elem.estimated_size_bytes() as i64);
                std::hint::black_box(format!("{elem:?}"));
                false
            });
        }
        SinkType::Text => {
            let base = clean_output_path.ok_or(LauncherError::MissingOutputPath)?;
            let prefix = format!("{}/{}", base.trim_end_matches('/'), query.slug());
            let formatted = output.map("FormatSinkText", |elem: T| {
                Metrics::counter(METRIC_NAMESPACE, COUNTER_SINK_RECORDS).inc();
                Metrics::counter(METRIC_NAMESPACE, COUNTER_SINK_BYTES)
                    .inc_by(elem.estimated_size_bytes() as i64);
                format!("{elem:?}")
            });
            if options.streaming {
                let windowed = if is_globally_windowed(&formatted) {
                    formatted.window_into(
                        "SinkFixedWindows",
                        FixedWindows::of(Duration::from_secs(config.window_size_sec.max(1) as u64)),
                    )
                } else {
                    formatted
                };
                let _ = windowed.apply(
                    "WriteTextSink",
                    textio::Write::new(prefix)
                        .with_suffix(".txt")
                        .with_windowed_writes(),
                );
            } else {
                let global = if is_globally_windowed(&formatted) {
                    formatted
                } else {
                    formatted.apply(
                        "SinkGlobalWindow",
                        WindowInto::new(GlobalWindows)
                            .with_trigger(Trigger::Default)
                            .with_accumulation_mode(AccumulationMode::Discarding),
                    )
                };
                let _ = global.apply(
                    "WriteTextSink",
                    textio::Write::new(prefix).with_suffix(".txt"),
                );
            }
        }
    }
    Ok(())
}
