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

//! Event and result exchange through Cloud Pub/Sub and Apache Kafka.
//!
//! The transforms are cross-language: the graph build calls a Java expansion service. Events
//! travel as [`EventCoder`] bytes, the wire format of the Java Nexmark `Event.CODER`. Results
//! travel as UTF-8 text, the `{:?}` form that the text sink also writes.

use std::fmt::Debug;
use std::sync::Once;

use beam::coders::{Coder, CoderError, Context, DefaultCoder};
use beam::io::gcp::pubsub::{self, PubsubFormat, PubsubRead, PubsubWrite, PubsubWriteFormat};
use beam::io::kafka::{self, KafkaRead, KafkaWrite, OffsetReset};
use beam::metrics::Metrics;
use beam::prelude::*;

use super::NexmarkArgs;
use super::error::LauncherError;
use super::monitor::METRIC_NAMESPACE;
use crate::config::NexmarkConfig;
use crate::model::{Event, EventCoder};

/// Pub/Sub attribute that holds a unique message ID, so a reader drops duplicates.
pub const PUBSUB_ID_ATTRIBUTE: &str = "id";

/// Pub/Sub attribute that holds the event time in milliseconds since the Unix epoch.
pub const PUBSUB_TIMESTAMP_ATTRIBUTE: &str = "timestamp";

/// Counter, in [`METRIC_NAMESPACE`], of messages that do not decode to an [`Event`].
pub const DECODE_ERRORS: &str = "decode_errors";

/// Encodes `event` as a message payload.
///
/// # Errors
///
/// Returns the coder error. An event from the generator always encodes.
pub fn encode_event(event: &Event) -> Result<Vec<u8>, CoderError> {
    let mut payload = Vec::new();
    EventCoder.encode(event, &mut payload, Context::WholeStream)?;
    Ok(payload)
}

/// Decodes a message payload that [`encode_event`] or the Java Nexmark publisher wrote.
///
/// # Errors
///
/// Returns the coder error for bytes that are not one whole event.
pub fn decode_event(payload: &[u8]) -> Result<Event, CoderError> {
    let mut reader = payload;
    EventCoder.decode(&mut reader, Context::WholeStream)
}

/// The trimmed value of `flag`, or an error that names the flag and its purpose.
pub(super) fn require<'a>(
    value: Option<&'a str>,
    flag: &'static str,
    needed_by: &'static str,
) -> Result<&'a str, LauncherError> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or(LauncherError::MissingFlag { flag, needed_by })
}

/// Reads events from `--pubsub_subscription`.
///
/// Event time is the timestamp attribute that the publisher wrote, or the publish time with
/// `--use_pubsub_publish_time`.
pub(super) fn read_pubsub_events(
    pipeline: &Pipeline,
    args: &NexmarkArgs,
    config: &NexmarkConfig,
) -> Result<PCollection<Event>, LauncherError> {
    let subscription = require(
        args.pubsub_subscription.as_deref(),
        "--pubsub_subscription",
        "reading events from Pub/Sub",
    )?;
    let read = PubsubRead::new()
        .with_subscription(subscription)
        .with_format(PubsubFormat::Raw)
        .with_id_attribute(PUBSUB_ID_ATTRIBUTE);
    let read = match config.use_pubsub_publish_time {
        false => read.with_timestamp_attribute(PUBSUB_TIMESTAMP_ATTRIBUTE),
        true => read,
    };
    let read = with_service(
        read,
        args.expansion_service.as_deref(),
        PubsubRead::with_expansion_service,
    );
    let rows = pipeline.try_apply("ReadPubsubEvents", read)?;
    Ok(decode_rows(&rows, "DecodePubsubEvents", pubsub_payload))
}

/// The payload of a raw Pub/Sub row.
fn pubsub_payload(row: &Row) -> Option<&[u8]> {
    row.get_bytes("payload").ok().flatten()
}

/// Reads events from `--kafka_topic`, from the earliest offset.
///
/// The Kafka transforms do not carry record timestamps, so each event gets its own
/// `date_time` as timestamp.
pub(super) fn read_kafka_events(
    pipeline: &Pipeline,
    args: &NexmarkArgs,
) -> Result<PCollection<Event>, LauncherError> {
    let (servers, topic) = kafka_target(args, args.kafka_topic.as_deref(), "--kafka_topic")?;
    let read = with_service(
        KafkaRead::new(servers, topic).with_auto_offset_reset(OffsetReset::Earliest),
        args.expansion_service.as_deref(),
        KafkaRead::with_expansion_service,
    );
    let rows = pipeline.try_apply("ReadKafkaEvents", read)?;
    let events = decode_rows(&rows, "DecodeKafkaEvents", kafka::payload_bytes);
    Ok(with_event_time_from_data(&events))
}

/// Publishes `events` to `--pubsub_topic`, with the ID and, unless
/// `--use_pubsub_publish_time`, the timestamp attribute.
pub(super) fn write_pubsub_events(
    events: &PCollection<Event>,
    args: &NexmarkArgs,
    config: &NexmarkConfig,
) -> Result<(), LauncherError> {
    let topic = require(
        args.pubsub_topic.as_deref(),
        "--pubsub_topic",
        "publishing events to Pub/Sub",
    )?;
    let rows = events
        .par_do_fn("EncodeEvents", |event: Event, ctx| {
            ctx.emit(pubsub::raw_bytes_row(encode_event(&event)?))
        })
        .with_row_schema(&pubsub::raw_bytes_schema());
    let _ = rows.try_apply("WritePubsubEvents", pubsub_write(topic, args, config))?;
    Ok(())
}

/// Writes `events` to `--kafka_topic`.
pub(super) fn write_kafka_events(
    events: &PCollection<Event>,
    args: &NexmarkArgs,
) -> Result<(), LauncherError> {
    let (servers, topic) = kafka_target(args, args.kafka_topic.as_deref(), "--kafka_topic")?;
    let rows = events
        .par_do_fn("EncodeEvents", |event: Event, ctx| {
            ctx.emit(kafka::raw_bytes_row(encode_event(&event)?))
        })
        .with_row_schema(&kafka::raw_bytes_schema());
    let _ = rows.try_apply("WriteKafkaEvents", kafka_write(servers, topic, args))?;
    Ok(())
}

/// Publishes the text form of each result to `--pubsub_results_topic`.
pub(super) fn write_pubsub_results<T>(
    results: &PCollection<T>,
    args: &NexmarkArgs,
    config: &NexmarkConfig,
) -> Result<(), LauncherError>
where
    T: DefaultCoder + Debug + Send + Sync + 'static,
{
    let topic = require(
        args.pubsub_results_topic.as_deref(),
        "--pubsub_results_topic",
        "--sink_type=PUBSUB",
    )?;
    let rows = results
        .map("FormatResults", |result: T| {
            pubsub::raw_bytes_row(format!("{result:?}").into_bytes())
        })
        .with_row_schema(&pubsub::raw_bytes_schema());
    let _ = rows.try_apply("WritePubsubResults", pubsub_write(topic, args, config))?;
    Ok(())
}

/// Writes the text form of each result to `--kafka_results_topic`.
pub(super) fn write_kafka_results<T>(
    results: &PCollection<T>,
    args: &NexmarkArgs,
) -> Result<(), LauncherError>
where
    T: DefaultCoder + Debug + Send + Sync + 'static,
{
    let (servers, topic) = kafka_target(
        args,
        args.kafka_results_topic.as_deref(),
        "--kafka_results_topic",
    )?;
    let rows = results
        .map("FormatResults", |result: T| {
            kafka::raw_bytes_row(format!("{result:?}").into_bytes())
        })
        .with_row_schema(&kafka::raw_bytes_schema());
    let _ = rows.try_apply("WriteKafkaResults", kafka_write(servers, topic, args))?;
    Ok(())
}

fn pubsub_write(topic: &str, args: &NexmarkArgs, config: &NexmarkConfig) -> PubsubWrite {
    let write = PubsubWrite::new(topic)
        .with_format(PubsubWriteFormat::Raw)
        .with_id_attribute(PUBSUB_ID_ATTRIBUTE);
    let write = match config.use_pubsub_publish_time {
        false => write.with_timestamp_attribute(PUBSUB_TIMESTAMP_ATTRIBUTE),
        true => write,
    };
    with_service(
        write,
        args.expansion_service.as_deref(),
        PubsubWrite::with_expansion_service,
    )
}

fn kafka_write(servers: &str, topic: &str, args: &NexmarkArgs) -> KafkaWrite {
    with_service(
        KafkaWrite::new(servers, topic),
        args.expansion_service.as_deref(),
        KafkaWrite::with_expansion_service,
    )
}

/// `--bootstrap_servers` and the topic of `flag`.
fn kafka_target<'a>(
    args: &'a NexmarkArgs,
    topic: Option<&'a str>,
    flag: &'static str,
) -> Result<(&'a str, &'a str), LauncherError> {
    let servers = require(
        args.bootstrap_servers.as_deref(),
        "--bootstrap_servers",
        "Kafka",
    )?;
    Ok((servers, require(topic, flag, "Kafka")?))
}

/// Applies `--expansion_service` to a transform builder when the flag is set.
fn with_service<B>(builder: B, service: Option<&str>, set: fn(B, String) -> B) -> B {
    match service {
        Some(service) => set(builder, service.to_owned()),
        None => builder,
    }
}

/// Decodes the payload of each row. A row that does not decode counts in [`DECODE_ERRORS`]
/// and is dropped, so a stray message on a shared topic does not stop the job.
fn decode_rows(
    rows: &PCollection<Row>,
    name: &str,
    payload: fn(&Row) -> Option<&[u8]>,
) -> PCollection<Event> {
    decode_each(rows, name, move |row: Row| {
        payload(&row)
            .ok_or_else(|| "the message has no payload".to_owned())
            .and_then(|bytes| decode_event(bytes).map_err(|err| err.to_string()))
    })
}

/// Decodes each element to an [`Event`]. An element that does not decode counts in
/// [`DECODE_ERRORS`] and is dropped, so one stray record does not stop the job.
pub(super) fn decode_each<I, F>(input: &PCollection<I>, name: &str, decode: F) -> PCollection<Event>
where
    I: DefaultCoder + Send + Sync + 'static,
    F: Fn(I) -> Result<Event, String> + Send + Sync + 'static,
{
    static FIRST_ERROR: Once = Once::new();
    let errors = Metrics::counter(METRIC_NAMESPACE, DECODE_ERRORS);
    input.flat_map(name, move |element: I| {
        decode(element)
            .inspect_err(|reason| {
                errors.inc();
                FIRST_ERROR.call_once(|| {
                    tracing::warn!("dropping a record that is not a Nexmark event: {reason}");
                });
            })
            .ok()
    })
}

/// Gives each event its own `date_time` as timestamp, for sources without event time.
pub(super) fn with_event_time_from_data(events: &PCollection<Event>) -> PCollection<Event> {
    events.par_do_fn("EventTimeFromData", |event: Event, ctx| {
        let timestamp = event.date_time();
        ctx.output(event).at(timestamp).emit()
    })
}
