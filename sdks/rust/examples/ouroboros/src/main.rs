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

//! Runs the Ouroboros loop on a Kafka or Pub/Sub topic.
//!
//! On Kafka, on Google Cloud Dataflow:
//!
//! ```text
//! cargo run -p ouroboros -- \
//!   --runner=dataflow --streaming=true \
//!   --project=my-project --region=us-central1 --temp_location=gs://my-bucket/temp \
//!   --transport=kafka --bootstrap_servers=broker:9092 --loop_topic=ouroboros --num_seeds=3
//! ```
//!
//! On Pub/Sub, replace the last line with:
//!
//! ```text
//!   --transport=pubsub --loop_topic=projects/my-project/topics/ouroboros
//! ```

use beam::prelude::*;
use ouroboros::{Args, Ouroboros, Transport, kafka, pubsub};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    beam::logging::init_logging();

    let (options, args) = beam::options::parse::<Args>();
    let pipeline = Pipeline::create(&options);

    let incoming = match args.transport {
        Transport::Kafka => kafka::read(&pipeline, &args)?,
        Transport::Pubsub => pubsub::read(&pipeline, &args)?,
    };
    let outgoing = incoming.apply(
        "Ouroboros",
        Ouroboros {
            num_seeds: args.num_seeds,
            max_cycles: args.max_cycles,
        },
    );
    match args.transport {
        Transport::Kafka => kafka::write(&outgoing, &args)?,
        Transport::Pubsub => pubsub::write(&outgoing, &args)?,
    }

    pipeline.run().await?;
    Ok(())
}
