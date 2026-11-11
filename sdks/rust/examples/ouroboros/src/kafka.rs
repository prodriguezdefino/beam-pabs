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

//! The loop topic on Kafka, through the Kafka read and write of `beam::io::kafka`.
//!
//! A record that the read cannot decode (a record with no value), and a payload that Kafka
//! rejects, go to a failure output and do not fail the bundle. The pipeline counts them as
//! `kafka_read_errors` and `kafka_write_errors`.

use beam::io::kafka::read::{Read, ReadOptions, StartOffset};
use beam::io::kafka::write::{Write, WriteOptions};
use beam::prelude::*;

use crate::{Args, METRICS_NAMESPACE};

fn bootstrap_servers(args: &Args) -> Result<&str> {
    args.bootstrap_servers
        .as_deref()
        .ok_or_else(|| Error::msg("--bootstrap_servers is required with --transport=kafka"))
}

/// Counts the rows of a connector failure output as `counter`, and logs each one.
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
/// Returns an error if `--bootstrap_servers` is missing or the options are not valid.
pub fn read(p: &Pipeline, args: &Args) -> Result<PCollection<Vec<u8>>> {
    let options = ReadOptions::new(bootstrap_servers(args)?, &args.loop_topic)
        .with_start_offset(StartOffset::Earliest)
        .with_consumer_properties(
            args.group_id
                .iter()
                .map(|group| ("group.id".to_string(), group.clone())),
        );
    let options = if args.gmk {
        options.with_google_auth()
    } else {
        options
    };
    let WithFailures { output, failures } =
        p.try_apply("ReadKafka", Read::new(options).with_failures())?;
    count_errors(&failures, "CountReadErrors", "kafka_read_errors");
    Ok(output)
}

/// Writes `payloads` to the loop topic.
///
/// # Errors
///
/// Returns an error if `--bootstrap_servers` is missing or the options are not valid.
pub fn write(payloads: &PCollection<Vec<u8>>, args: &Args) -> Result {
    let options = WriteOptions::new(bootstrap_servers(args)?, &args.loop_topic);
    let options = if args.gmk {
        options.with_google_auth()
    } else {
        options
    };
    let failures = payloads.try_apply("WriteKafka", Write::new(options))?;
    count_errors(&failures, "CountWriteErrors", "kafka_write_errors");
    Ok(())
}
