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

//! Cross-Language Pub/Sub Ouroboros Streaming Pipeline Binary.
//!
//! Runs a portable streaming pipeline in Rust that reads from and writes
//! back to Cloud Pub/Sub through Java SchemaTransforms.
//!
//! Example usage:
//!
//! ```bash
//! ./gradlew :sdks:rust:run -Papp=pubsub_ouroboros --args=" \
//!   --runner=prism \
//!   --expansion_service=localhost:8097 \
//!   --input_subscription=projects/my-project/subscriptions/ouroboros-sub \
//!   --loop_topic=projects/my-project/topics/ouroboros-topic"
//! ```

use pubsub_ouroboros::{Args, build_pipeline};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    beam::harness::init_logging();

    let (options, args) = beam::options::parse::<Args>();

    let pipeline = build_pipeline(&options, &args);
    pipeline.run().await?;

    Ok(())
}
