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

//! Main entrypoint for the Traffic Routes Avro Analytics Example.
//!
//! # Running Locally on Prism:
//! ```bash
//! cargo run -p traffic_routes -- \
//!   --runner=prism \
//!   --output=/tmp/traffic_stats
//! ```
//!
//! # Running on Google Cloud Dataflow:
//! ```bash
//! cargo run -p traffic_routes -- \
//!   --runner=dataflow \
//!   --project=YOUR_PROJECT \
//!   --region=us-central1 \
//!   --temp_location=gs://<YOUR_BUCKET>/temp \
//!   --output=gs://<YOUR_BUCKET>/output/traffic_stats
//! ```

use beam::prelude::*;
use clap::Args;
use serde::{Deserialize, Serialize};
use traffic_routes::{DEFAULT_TRAFFIC_CSV, build_traffic_pipeline};

/// Command-line arguments for Traffic Routes Avro Analytics.
#[derive(Args, Serialize, Deserialize, Debug, Clone)]
#[command(
    name = "traffic_routes",
    about = "Apache Beam Rust SDK - Traffic Routes Avro Analytics"
)]
pub struct TrafficRoutesArgs {
    /// Input CSV file or pattern. Defaults to official Beam sample on GCS.
    #[arg(long, default_value = DEFAULT_TRAFFIC_CSV)]
    pub input: String,

    /// Output Avro destination prefix (required by executor).
    /// Examples: /tmp/traffic_stats or gs://<YOUR_BUCKET>/traffic/stats
    #[arg(long, required = true)]
    pub output: String,
}

impl PipelineOptionGroup for TrafficRoutesArgs {}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    beam::harness::init_logging();

    let (options, args) = beam::options::parse::<TrafficRoutesArgs>();

    tracing::info!(
        "Executing Traffic Routes Avro Analytics pipeline: input='{}', output='{}'",
        args.input,
        args.output
    );

    let pipeline = Pipeline::create(&options);
    let _ = build_traffic_pipeline(&pipeline, &args.input, &args.output);

    let result = pipeline.run().await?;
    tracing::info!(
        "Traffic routes analytics pipeline completed with state: {}",
        result.state
    );

    Ok(())
}
