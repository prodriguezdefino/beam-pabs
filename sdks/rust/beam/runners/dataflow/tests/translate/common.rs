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

//! Helpers of the translation tests.

use beam::options::{
    DebugOptions, PipelineOptionGroup, PipelineOptions, WorkerOptions, try_parse_from,
};
use beam::transforms::display_data::DisplayDataItem;
use beam_io_gcp::options::GcpOptions;
use beam_runner_dataflow::{DataflowJobOptions, DataflowOptions};
use serde::{Deserialize, Serialize};

/// Stands in for a pipeline's own option group.
#[derive(clap::Args, Serialize, Deserialize, Clone, Debug, PartialEq)]
pub(crate) struct CustomArgs {
    #[arg(long)]
    custom_string: String,
    #[arg(long)]
    custom_int: i64,
    #[arg(long)]
    custom_bool: bool,
}

impl PipelineOptionGroup for CustomArgs {}

/// Parses a pipeline's command line, as `beam::options::parse` does.
pub(crate) fn custom_pipeline(args: &[&str]) -> (PipelineOptions, CustomArgs) {
    try_parse_from::<CustomArgs, _, _>(args.iter().copied()).expect("valid command line")
}

/// Job options for `options` with these groups set, resolved as the runner does.
pub(crate) fn job_options_for(
    options: &PipelineOptions,
    gcp: &GcpOptions,
    worker: &WorkerOptions,
    debug: &DebugOptions,
    dataflow: &DataflowOptions,
) -> DataflowJobOptions {
    options.set(gcp.clone()).expect("valid GCP options");
    options.set(worker.clone()).expect("valid worker options");
    options.set(debug.clone()).expect("valid debug options");
    options
        .set(dataflow.clone())
        .expect("valid Dataflow options");
    DataflowJobOptions::try_from(options).expect("job options resolve")
}

/// Job options with these groups set and every other option at its default.
pub(crate) fn job_options(
    gcp: &GcpOptions,
    worker: &WorkerOptions,
    debug: &DebugOptions,
    dataflow: &DataflowOptions,
) -> DataflowJobOptions {
    job_options_for(
        &PipelineOptions::with_runner("dataflow"),
        gcp,
        worker,
        debug,
        dataflow,
    )
}

/// Returns the only display data item named `key`, failing if it is missing or repeated.
pub(crate) fn single<'a>(dd: &'a [DisplayDataItem], key: &str) -> &'a DisplayDataItem {
    let matches: Vec<&DisplayDataItem> = dd.iter().filter(|d| d.key == key).collect();
    assert_eq!(
        matches.len(),
        1,
        "expected exactly one display data item '{key}', got {matches:?}"
    );
    matches[0]
}

/// Asserts the only item named `key` has this value and type.
pub(crate) fn assert_item(dd: &[DisplayDataItem], key: &str, value: &str, item_type: &str) {
    let item = single(dd, key);
    assert_eq!(
        (item.value.to_string().as_str(), item.item_type()),
        (value, item_type),
        "display data '{key}'"
    );
}

/// Asserts no two display data items share a key.
pub(crate) fn assert_unique_keys(dd: &[DisplayDataItem]) {
    let mut keys: Vec<&str> = dd.iter().map(|d| d.key.as_str()).collect();
    keys.sort_unstable();
    let before = keys.len();
    keys.dedup();
    assert_eq!(keys.len(), before, "duplicate display data keys in {dd:?}");
}
