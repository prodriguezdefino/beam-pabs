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

//! Binary for the NYC Taxi Parquet analytics example.
//!
//! # Running Locally on Prism:
//! ```bash
//! # Run on sample data or GCS
//! cargo run -p nyc_taxi -- --runner=prism --output=/tmp/nyc_taxi_stats
//! ```
//!
//! # Running on Google Cloud Dataflow:
//! ```bash
//! cargo run -p nyc_taxi -- \
//!   --runner=dataflow \
//!   --project=YOUR_PROJECT \
//!   --region=us-central1 \
//!   --temp_location=gs://YOUR_BUCKET/temp \
//!   --output=gs://YOUR_BUCKET/output/nyc_taxi_stats
//! ```

use beam::prelude::*;
use clap::Args;
use nyc_taxi::{DEFAULT_NYC_TRIP_PARQUET, build_nyc_taxi_pipeline};
use serde::{Deserialize, Serialize};

/// Command-line arguments for NYC Taxi Parquet Analytics.
#[derive(Args, Serialize, Deserialize, Debug, Clone)]
#[command(
    name = "nyc_taxi",
    about = "Apache Beam Rust SDK - NYC Taxi & Rideshare Parquet Analytics"
)]
pub struct NycTaxiArgs {
    /// Input Parquet file or pattern. Defaults to official Beam public dataset on GCS.
    #[arg(long, default_value = DEFAULT_NYC_TRIP_PARQUET)]
    pub input: String,

    /// Output Parquet destination prefix (required by executor).
    /// Examples: /tmp/nyc_taxi_output or gs://my-bucket/nyc_taxi/stats
    #[arg(long, required = true)]
    pub output: String,
}

impl PipelineOptionGroup for NycTaxiArgs {}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    beam::harness::init_logging();

    let (options, args) = beam::options::parse::<NycTaxiArgs>();

    tracing::info!(
        "Executing NYC Taxi Parquet Analytics pipeline: input='{}', output='{}'",
        args.input,
        args.output
    );

    let pipeline = Pipeline::create(&options);
    let _ = build_nyc_taxi_pipeline(&pipeline, &args.input, &args.output);

    let result = pipeline.run().await?;
    tracing::info!(
        "NYC Taxi analytics pipeline completed with state: {}",
        result.state
    );

    Ok(())
}
