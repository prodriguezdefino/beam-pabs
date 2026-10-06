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

//! Binary for the Partition example pipeline.
//!
//! Run on Prism, the default runner:
//! ```text
//! cargo run -p partition
//! ```

use beam::prelude::*;
use partition::{build_partition_pipeline, default_students};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    beam::harness::init_logging();

    let pipeline = Pipeline::create(&PipelineOptions::from_args());
    let _ = build_partition_pipeline(&pipeline, default_students());

    tracing::info!("Running partition example pipeline...");
    let result = pipeline.run().await?;
    tracing::info!("Partition pipeline completed with state: {}", result.state);

    Ok(())
}
