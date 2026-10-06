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

//! Counts the long words of King Lear with two side inputs.
//!
//! Run on Prism, the default runner:
//!
//! ```text
//! cargo run -p side_inputs -- --output=/tmp/side_inputs.txt
//! ```
//!
//! To run it on Google Cloud Dataflow:
//!
//! ```text
//! cargo run -p side_inputs -- \
//!   --runner=dataflow \
//!   --project=my-gcp-project \
//!   --region=us-central1 \
//!   --temp_location=gs://my-bucket/temp \
//!   --output=gs://my-bucket/side_inputs.txt
//! ```

use beam::prelude::*;
use side_inputs::{Args, CountLongWords};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    beam::logging::init_logging();
    let (options, args) = beam::options::parse::<Args>();

    let p = Pipeline::create(&options);
    p.apply("ReadLines", textio::Read::new(&args.input))
        .apply(
            "CountLongWords",
            CountLongWords {
                stopwords: args.stopwords,
            },
        )
        .map("FormatCounts", |(word, count): (String, i64)| {
            format!("{word}: {count}")
        })
        .apply("WriteLines", textio::Write::new(&args.output));
    p.run().await?;

    Ok(())
}
