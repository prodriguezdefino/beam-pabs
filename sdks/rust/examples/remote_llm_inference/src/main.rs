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

//! Remote LLM inference with Gemini.
//!
//! Run on Prism, the default runner:
//! ```text
//! cargo run -p remote_llm_inference -- --input=prompts.txt --api_key=env:GEMINI_API_KEY
//! ```
//!
//! Or against Vertex AI with a user access token off Google Cloud. Workers use their service
//! account:
//! ```text
//! GCP_TOKEN=$(gcloud auth print-access-token) cargo run -p remote_llm_inference -- \
//!   --input=prompts.txt --cloud_project=<project> --bearer_token=env:GCP_TOKEN
//! ```
//!
//! The key is never passed itself, only where it is held: `env:<VAR>`, `file:<PATH>` or a
//! Secret Manager version, `gcp:projects/<p>/secrets/<s>/versions/<v>`. Workers resolve it.

use remote_llm_inference::{Args, build_pipeline};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    beam::logging::init_logging();

    let (options, args) = beam::options::parse::<Args>();
    build_pipeline(&options, &args).run().await?;

    Ok(())
}
