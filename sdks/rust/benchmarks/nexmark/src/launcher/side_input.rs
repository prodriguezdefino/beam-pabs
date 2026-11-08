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

//! Writes the `CSV` side-input table once, before the join queries of a suite run.

use beam::io::file::textio;
use beam::options::PipelineOptions;
use beam::prelude::*;

use super::NexmarkArgs;
use super::error::LauncherError;
use crate::config::{NexmarkConfig, NexmarkQueryName, SideInputType};
use crate::queries::SideInput;
use crate::queries::side_input::{format_side_input_line, side_input_table};

/// Checks whether `query` reads the side-input table.
const fn joins_side_input(query: NexmarkQueryName) -> bool {
    match query {
        NexmarkQueryName::BoundedSideInputJoin | NexmarkQueryName::SessionSideInputJoin => true,
        NexmarkQueryName::Passthrough
        | NexmarkQueryName::CurrencyConversion
        | NexmarkQueryName::Selection
        | NexmarkQueryName::LocalItemSuggestion
        | NexmarkQueryName::AveragePriceForCategory
        | NexmarkQueryName::HotItems
        | NexmarkQueryName::AverageSellingPriceBySeller
        | NexmarkQueryName::HighestBid
        | NexmarkQueryName::MonitorNewUsers
        | NexmarkQueryName::WinningBids
        | NexmarkQueryName::LogToShardedFiles
        | NexmarkQueryName::UserSessions
        | NexmarkQueryName::ProcessingTimeWindows
        | NexmarkQueryName::PortabilityBatch
        | NexmarkQueryName::Reshuffle => false,
    }
}

/// The side-input source of `config`. `CSV` needs `--side_input_url`.
pub(super) fn side_input_of(
    config: &NexmarkConfig,
    args: &NexmarkArgs,
) -> Result<SideInput, LauncherError> {
    match config.side_input_type {
        SideInputType::Direct => Ok(SideInput::Direct),
        SideInputType::Csv => csv_url(args).map(|url| SideInput::Csv {
            url: url.to_owned(),
        }),
    }
}

fn csv_url(args: &NexmarkArgs) -> Result<&str, LauncherError> {
    args.side_input_url
        .as_deref()
        .map(str::trim)
        .filter(|url| !url.is_empty())
        .ok_or(LauncherError::MissingFlag {
            flag: "--side_input_url",
            needed_by: "--side_input_type=CSV",
        })
}

/// Builds the batch job that writes the `CSV` table of `configs`, or `None` when no join
/// query of `configs` reads a `CSV` table.
///
/// The table holds the rows of the largest join, so it also covers each smaller join.
pub fn build_side_input_pipeline(
    options: &PipelineOptions,
    args: &NexmarkArgs,
    configs: &[NexmarkConfig],
) -> Result<Option<Pipeline>, LauncherError> {
    let csv_joins: Vec<&NexmarkConfig> = configs
        .iter()
        .filter(|config| config.query.is_some_and(joins_side_input))
        .filter(|config| config.side_input_type == SideInputType::Csv)
        .collect();
    let (Some(row_count), Some(num_shards)) = (
        csv_joins
            .iter()
            .map(|config| config.side_input_row_count)
            .max(),
        csv_joins
            .iter()
            .map(|config| config.side_input_num_shards)
            .max(),
    ) else {
        return Ok(None);
    };
    let url = csv_url(args)?;

    let mut write_options = options.clone();
    write_options.streaming = false;
    write_options.job_name = options
        .job_name
        .as_ref()
        .map(|name| format!("{name}-side-input"));
    let pipeline = Pipeline::create(&write_options);
    let lines: Vec<String> = side_input_table(row_count)
        .map(|row| format_side_input_line(&row))
        .collect();
    let _ = pipeline.apply("SideInputLines", Create::new(lines)).apply(
        "WriteSideInput",
        textio::Write::new(url).with_num_shards(u32::try_from(num_shards.max(1)).unwrap_or(1)),
    );
    Ok(Some(pipeline))
}

/// Runs the job of [`build_side_input_pipeline`] to its end, when `configs` need one.
pub(super) async fn write_side_input(
    options: &PipelineOptions,
    args: &NexmarkArgs,
    configs: &[NexmarkConfig],
) -> Result<(), LauncherError> {
    let Some(pipeline) = build_side_input_pipeline(options, args, configs)? else {
        return Ok(());
    };
    let result = pipeline.submit().await?.wait_until_finish(None).await?;
    if result.state.is_successful() {
        Ok(())
    } else {
        Err(LauncherError::Unsuccessful(result.state))
    }
}
