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

//! The loop topic on Pub/Sub, through the cross-language Pub/Sub transforms.

use beam::external::ExpansionError;
use beam::io::gcp::pubsub::{
    PubsubFormat, PubsubRead, PubsubWrite, PubsubWriteFormat, raw_bytes_row, raw_bytes_schema,
};
use beam::prelude::*;

use crate::Args;

/// Reads the payloads of `--subscription`, or of the loop topic if there is no subscription.
///
/// # Errors
///
/// Returns an error if the expansion fails.
pub fn read(p: &Pipeline, args: &Args) -> Result<PCollection<Vec<u8>>, ExpansionError> {
    let read = PubsubRead::new().with_format(PubsubFormat::Raw);
    let read = match &args.subscription {
        Some(subscription) => read.with_subscription(subscription),
        None => read.with_topic(&args.loop_topic),
    };
    let read = match &args.expansion_service {
        Some(service) => read.with_expansion_service(service),
        None => read,
    };
    Ok(p.try_apply("ReadPubsub", read)?
        // A Row does not give up its fields, so the payload is copied.
        .flat_map("ToPayloads", |row: Row| {
            row.get_bytes("payload").ok().flatten().map(<[u8]>::to_vec)
        }))
}

/// Writes `payloads` to the loop topic.
///
/// # Errors
///
/// Returns an error if the expansion fails.
pub fn write(payloads: &PCollection<Vec<u8>>, args: &Args) -> Result<(), ExpansionError> {
    let write = PubsubWrite::new(&args.loop_topic).with_format(PubsubWriteFormat::Raw);
    let write = match &args.expansion_service {
        Some(service) => write.with_expansion_service(service),
        None => write,
    };
    payloads
        .map("ToRows", raw_bytes_row)
        .with_row_schema(&raw_bytes_schema())
        .try_apply("WritePubsub", write)?;
    Ok(())
}
