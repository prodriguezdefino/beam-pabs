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

//! Counts the ticks of a periodic impulse in fixed windows.
//!
//! Run on Prism, the default runner:
//!
//! ```text
//! cargo run -p streaming -- --limit=20 --output=/tmp/streaming_output.txt
//! ```
//!
//! Run on Google Cloud Dataflow:
//!
//! ```text
//! cargo run -p streaming -- \
//!   --runner=dataflow \
//!   --streaming \
//!   --project=my-gcp-project \
//!   --region=us-central1 \
//!   --temp_location=gs://my-bucket/temp \
//!   --output=gs://my-bucket/streaming_output.txt
//! ```

use std::time::Duration;

use beam::prelude::*;
use streaming::{Args, CountTicks};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    beam::logging::init_logging();
    let (options, args) = beam::options::parse::<Args>();

    let p = Pipeline::create(&options);
    p.apply("PeriodicImpulse", PeriodicImpulse::from(&args))
        .apply(
            "CountTicks",
            CountTicks {
                window_size: Duration::from_secs(args.window_size_secs),
            },
        )
        .apply("WriteLines", textio::Write::new(&args.output));
    p.run().await?;

    Ok(())
}
