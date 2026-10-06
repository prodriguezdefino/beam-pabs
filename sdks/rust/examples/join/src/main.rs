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

//! Binary for the Join example pipeline.
//!
//! Run on Prism, the default runner:
//! ```text
//! cargo run -p join
//! ```

use beam::prelude::*;
use join::{JoinArgs, build_join_pipeline, default_orders, default_users};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    beam::harness::init_logging();

    let (options, _) = beam::options::parse::<JoinArgs>();

    let pipeline = Pipeline::create(&options);
    let _ = build_join_pipeline(&pipeline, default_users(), default_orders());

    tracing::info!("Running join example pipeline...");
    let result = pipeline.run().await?;
    tracing::info!("Join pipeline completed with state: {}", result.state);

    Ok(())
}
