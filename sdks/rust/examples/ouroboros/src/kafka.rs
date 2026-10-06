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

//! The loop topic on Kafka, through the cross-language Kafka transforms expanded by Managed.
//!
//! Both transforms use Managed error handling. A record that the Java transforms cannot
//! decode or serialize goes to an error output and does not fail the bundle. The pipeline
//! counts these records as `kafka_read_errors` and `kafka_write_errors`.

use beam::external::ExpansionError;
use beam::io::kafka::{
    KafkaRead, KafkaWrite, OffsetReset, payload_bytes, raw_bytes_row, raw_bytes_schema,
};
use beam::io::managed;
use beam::prelude::*;

use crate::{Args, METRICS_NAMESPACE};

/// Output tag of the records that the read cannot decode.
const READ_ERRORS: &str = "read_errors";
/// Output tag of the records that the write cannot serialize.
const WRITE_ERRORS: &str = "write_errors";

fn bootstrap_servers(args: &Args) -> Result<&str, ExpansionError> {
    args.bootstrap_servers.as_deref().ok_or_else(|| {
        ExpansionError::InvalidConfig(
            "--bootstrap_servers is required with --transport=kafka".to_string(),
        )
    })
}

/// Counts the rows of a connector error output as `counter`, and logs each one.
fn count_errors(errors: &PCollection<Row>, name: &str, counter: &'static str) {
    let errors_counter = Metrics::counter(METRICS_NAMESPACE, counter);
    errors.inspect(name, move |row: &Row| {
        errors_counter.inc();
        let message = row
            .get_string("error_message")
            .ok()
            .flatten()
            .unwrap_or_default();
        tracing::warn!(counter, message, "a Kafka record failed");
    });
}

/// Reads the payloads of the loop topic from the earliest offset.
///
/// The earliest offset makes sure that the read gets the seeds that the same job writes.
///
/// # Errors
///
/// Returns an error if `--bootstrap_servers` is missing or the expansion fails.
pub fn read(p: &Pipeline, args: &Args) -> Result<PCollection<Vec<u8>>, ExpansionError> {
    let read = KafkaRead::new(bootstrap_servers(args)?, &args.loop_topic)
        .with_auto_offset_reset(OffsetReset::Earliest)
        .with_error_handling(READ_ERRORS);
    let read = match &args.group_id {
        Some(group) => read.with_consumer_config("group.id", group),
        None => read,
    };
    let read = if args.gmk {
        read.with_google_managed_kafka_auth()
    } else {
        read
    };
    let read = match &args.expansion_service {
        Some(service) => read.with_expansion_service(service),
        None => read,
    };

    let outputs = p.apply("ReadKafka", read.to_managed()?.with_all_outputs());
    count_errors(
        &outputs.try_get(READ_ERRORS)?,
        "CountReadErrors",
        "kafka_read_errors",
    );
    Ok(outputs
        .try_get(managed::OUTPUT)?
        // A Row does not give up its fields, so the payload is copied.
        .flat_map("ToPayloads", |row: Row| {
            payload_bytes(&row).map(<[u8]>::to_vec)
        }))
}

/// Writes `payloads` to the loop topic.
///
/// # Errors
///
/// Returns an error if `--bootstrap_servers` is missing or the expansion fails.
pub fn write(payloads: &PCollection<Vec<u8>>, args: &Args) -> Result<(), ExpansionError> {
    let write = KafkaWrite::new(bootstrap_servers(args)?, &args.loop_topic)
        .with_error_handling(WRITE_ERRORS);
    let write = if args.gmk {
        write.with_google_managed_kafka_auth()
    } else {
        write
    };
    let write = match &args.expansion_service {
        Some(service) => write.with_expansion_service(service),
        None => write,
    };

    let outputs = payloads
        .map("ToRows", raw_bytes_row)
        .with_row_schema(&raw_bytes_schema())
        .apply("WriteKafka", write.to_managed()?.with_outputs());
    count_errors(
        &outputs.try_get(WRITE_ERRORS)?,
        "CountWriteErrors",
        "kafka_write_errors",
    );
    Ok(())
}
