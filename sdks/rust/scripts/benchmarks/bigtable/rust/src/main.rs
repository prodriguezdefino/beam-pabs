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

//! Binary of the Cloud Bigtable benchmark in a Rust pipeline. See the library for the pipeline.

use std::error::Error;

use beam::logging::init_logging;
use beam::options::parse;
use bigtable_benchmark::{Args, build_pipeline, print_metrics};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    init_logging();

    let (options, args) = parse::<Args>();
    let p = build_pipeline(&options, &args).map_err(|e| e.to_string())?;
    let result = p.run().await?;
    // The benchmark script finds the job by its name; this line is for the log.
    println!("JOB_ID={}", result.job_id);
    if let Some(metrics) = result.metrics() {
        print_metrics(metrics);
    }
    Ok(())
}
