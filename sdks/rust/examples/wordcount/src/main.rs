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

//! An example that counts words in Shakespeare.
//!
//! This class, `wordcount`, is the second in a series of four successively more
//! detailed Apache Beam examples. After you've looked at this one, see
//! `minimal_wordcount` for the simplest possible pipeline.
//!
//! Basic concepts, also in `minimal_wordcount`: reading text files, counting a
//! PCollection, and writing text files.
//!
//! New concepts:
//!
//! - Executing a pipeline on a selectable runner through `--runner`
//! - Defining custom pipeline options
//! - Writing and testing composite transforms
//!
//! Run on Prism, the default runner:
//!
//! ```text
//! cargo run -p wordcount -- --output=/tmp/output.txt
//! ```
//!
//! To run it on Google Cloud Dataflow, name the worker image. On the SDK base
//! image, stage a Linux build of this example (from
//! `./gradlew :sdks:rust:buildWorker -Pexample=wordcount`):
//!
//! ```text
//! cargo run -p wordcount -- \
//!   --runner=dataflow \
//!   --project=my-gcp-project \
//!   --region=us-central1 \
//!   --temp_location=gs://my-bucket/temp \
//!   --output=gs://my-bucket/output.txt \
//!   --sdk_container_image=us-central1-docker.pkg.dev/my-gcp-project/beam/beam_rust_sdk:<beam-version> \
//!   --worker_binary=build/linux_amd64/wordcount
//! ```
//!
//! Or bake the binary into an image with this example's `Dockerfile` and pass
//! only `--sdk_container_image`.

use wordcount::{Args, build_pipeline};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    beam::harness::init_logging();

    let (options, args) = beam::options::parse::<Args>();
    let pipeline = build_pipeline(&options, &args);
    pipeline.run().await?;

    Ok(())
}
