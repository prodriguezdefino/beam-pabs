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

//! BigQuery Tornadoes binary.
//!
//! Reads weather data from BigQuery, computes tornado counts per month, and writes results
//! back to BigQuery through the Java expansion service.
//!
//! Example usage:
//!
//! Run locally with the portable Prism runner:
//! ```bash
//! cargo run -p bigquery_tornadoes -- \
//!   --runner=prism \
//!   --output=my-project:my_dataset.tornadoes
//! ```
//!
//! Run on Google Cloud Dataflow:
//! ```bash
//! cargo run -p bigquery_tornadoes -- \
//!   --runner=dataflow \
//!   --project=my-gcp-project \
//!   --region=us-central1 \
//!   --temp_location=gs://my-bucket/temp \
//!   --output=my-project:my_dataset.tornadoes
//! ```

use bigquery_tornadoes::{Args, build_pipeline};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    beam::harness::init_logging();

    let (options, args) = beam::options::parse::<Args>();

    let pipeline = build_pipeline(&options, &args);
    pipeline.run().await?;

    Ok(())
}
