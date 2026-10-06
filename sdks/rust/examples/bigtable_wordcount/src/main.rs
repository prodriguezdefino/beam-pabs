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

//! Cross-language Bigtable WordCount pipeline binary.
//!
//! Write word counts to Bigtable on Dataflow:
//! ```bash
//! cargo run -p bigtable_wordcount -- \
//!   --runner=dataflow --project=my-project --region=us-central1 \
//!   --temp_location=gs://my-bucket/temp \
//!   --bigtable_project=my-project --bigtable_instance=my-instance \
//!   --bigtable_table=wordcount
//! ```
//!
//! Read them back into a text file:
//! ```bash
//! cargo run -p bigtable_wordcount -- --mode=read --output=gs://my-bucket/counts \
//!   ... same flags as above ...
//! ```

use bigtable_wordcount::{Args, build_pipeline};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    beam::harness::init_logging();

    let (options, args) = beam::options::parse::<Args>();

    let pipeline = build_pipeline(&options, &args);
    pipeline.run().await?;

    Ok(())
}
