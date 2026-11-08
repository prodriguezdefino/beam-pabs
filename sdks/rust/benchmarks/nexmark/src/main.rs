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

//! Command-line entry point for the Nexmark benchmark suite.

use beam::prelude::*;
use nexmark::launcher::{MonitorTiming, NexmarkArgs, NexmarkRunOutcome, run_nexmark};

#[tokio::main]
async fn main() -> Result {
    beam::logging::init_logging();
    let (options, args) = beam::options::parse::<NexmarkArgs>();
    let summaries = run_nexmark(&options, &args, &MonitorTiming::default()).await?;
    let failed = summaries
        .iter()
        .filter(|summary| matches!(summary.outcome, NexmarkRunOutcome::Failed { .. }))
        .count();
    match failed {
        0 => Ok(()),
        n => Err(format!("{n} Nexmark query run(s) failed").into()),
    }
}
