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

//! Pub/Sub and Kafka modes: flag checks, payloads, and graphs built without an expansion
//! service.

use beam::io::gcp::pubsub::{URN_PUBSUB_READ, URN_PUBSUB_WRITE};
use beam::io::kafka::{URN_KAFKA_READ, URN_KAFKA_WRITE};
use beam::options::PipelineOptions;
use beam::pipeline::URN_EXPANSION_SCHEMA_TRANSFORM;
use beam::prelude::*;
use beam_test_utils::xlang::decode_payload;
use nexmark::config::{NexmarkConfig, NexmarkQueryName, PubsubMode, SinkType, SourceType};
use nexmark::generator::{Generator, GeneratorConfig};
use nexmark::launcher::{
    LauncherError, NexmarkArgs, PUBSUB_ID_ATTRIBUTE, PUBSUB_TIMESTAMP_ATTRIBUTE,
    build_nexmark_pipeline, decode_event, encode_event, publisher_options,
};

const TOPIC: &str = "projects/p/topics/events";
const SUBSCRIPTION: &str = "projects/p/subscriptions/events-sub";
const RESULTS_TOPIC: &str = "projects/p/topics/results";

fn options(streaming: bool, worker: bool) -> PipelineOptions {
    let mut options = PipelineOptions::default();
    options.streaming = streaming;
    // A worker builds cross-language transforms as placeholders, without a service.
    options.harness.worker = worker;
    options
}

fn args(source: SourceType, mode: PubsubMode, sink: SinkType) -> NexmarkArgs {
    NexmarkArgs {
        query: Some(NexmarkQueryName::CurrencyConversion),
        source_type: Some(source),
        pubsub_mode: Some(mode),
        sink_type: Some(sink),
        num_events: Some(100),
        ..NexmarkArgs::default()
    }
}

fn build(streaming: bool, worker: bool, args: &NexmarkArgs) -> Result<Pipeline, LauncherError> {
    let config = args.resolve_configs().remove(0);
    build_nexmark_pipeline(&options(streaming, worker), args, &config)
}

/// `(identifier, config row)` of each cross-language transform, sorted by identifier.
fn external_transforms(pipeline: &Pipeline) -> Vec<(String, Row)> {
    let components = pipeline.to_proto().components.expect("pipeline components");
    let mut found: Vec<(String, Row)> = components
        .transforms
        .values()
        .filter_map(|transform| transform.spec.as_ref())
        .filter(|spec| spec.urn == URN_EXPANSION_SCHEMA_TRANSFORM)
        .map(|spec| decode_payload(&spec.payload))
        .collect();
    found.sort_by(|a, b| a.0.cmp(&b.0));
    found
}

fn text(row: &Row, field: &str) -> Option<String> {
    row.get_string(field).ok().flatten().map(str::to_owned)
}

/// The Kafka target and JSON config of a Managed transform row.
fn managed(row: &Row) -> (String, serde_json::Value) {
    let target = text(row, "transform_identifier").expect("managed target");
    let config = serde_json::from_str(&text(row, "config").expect("managed config"))
        .expect("managed config JSON");
    (target, config)
}

#[test]
fn events_round_trip_through_the_payload_format() {
    let config = GeneratorConfig::from_nexmark_config(&NexmarkConfig {
        num_events: 60,
        ..NexmarkConfig::default()
    });
    Generator::new(config).for_each(|next| {
        let payload = encode_event(&next.event).expect("event encodes");
        assert_eq!(decode_event(&payload).expect("payload decodes"), next.event);
        assert_eq!(next.event.date_time().as_millis(), next.event_timestamp);
    });
    assert!(decode_event(b"not an event").is_err());
    assert!(decode_event(&[]).is_err());
}

#[test]
fn missing_names_and_batch_reads_are_errors() {
    let cases = [
        (
            args(
                SourceType::Pubsub,
                PubsubMode::PublishOnly,
                SinkType::Devnull,
            ),
            false,
            "--pubsub_topic",
        ),
        (
            args(
                SourceType::Pubsub,
                PubsubMode::SubscribeOnly,
                SinkType::Devnull,
            ),
            true,
            "--pubsub_subscription",
        ),
        (
            args(SourceType::Kafka, PubsubMode::Combined, SinkType::Devnull),
            true,
            "--bootstrap_servers",
        ),
        (
            NexmarkArgs {
                bootstrap_servers: Some("broker:9092".into()),
                ..args(
                    SourceType::Kafka,
                    PubsubMode::PublishOnly,
                    SinkType::Devnull,
                )
            },
            false,
            "--kafka_topic",
        ),
        (
            args(SourceType::Direct, PubsubMode::Combined, SinkType::Pubsub),
            false,
            "--pubsub_results_topic",
        ),
        (
            NexmarkArgs {
                bootstrap_servers: Some(" ".into()),
                ..args(SourceType::Direct, PubsubMode::Combined, SinkType::Kafka)
            },
            false,
            "--bootstrap_servers",
        ),
    ];
    for (args, streaming, expected_flag) in cases {
        match build(streaming, false, &args) {
            Err(LauncherError::MissingFlag { flag, .. }) => assert_eq!(flag, expected_flag),
            other => panic!("expected missing {expected_flag}, got {:?}", other.err()),
        }
    }

    let batch_read = NexmarkArgs {
        pubsub_subscription: Some(SUBSCRIPTION.into()),
        ..args(
            SourceType::Pubsub,
            PubsubMode::SubscribeOnly,
            SinkType::Devnull,
        )
    };
    assert!(matches!(
        build(false, false, &batch_read),
        Err(LauncherError::NeedsStreaming(SourceType::Pubsub))
    ));
}

#[test]
fn pubsub_publisher_writes_raw_events_with_id_and_timestamp_attributes() {
    let publisher = NexmarkArgs {
        pubsub_topic: Some(TOPIC.into()),
        ..args(
            SourceType::Pubsub,
            PubsubMode::PublishOnly,
            SinkType::Devnull,
        )
    };
    let transforms = external_transforms(&build(false, true, &publisher).expect("graph"));
    let [(urn, row)] = transforms.as_slice() else {
        panic!("expected one external transform, got {transforms:?}");
    };
    assert_eq!(urn, URN_PUBSUB_WRITE);
    assert_eq!(text(row, "topic").as_deref(), Some(TOPIC));
    assert_eq!(text(row, "format").as_deref(), Some("RAW"));
    assert_eq!(
        text(row, "id_attribute").as_deref(),
        Some(PUBSUB_ID_ATTRIBUTE)
    );
    assert_eq!(
        text(row, "timestamp_attribute").as_deref(),
        Some(PUBSUB_TIMESTAMP_ATTRIBUTE)
    );

    let publish_time = NexmarkArgs {
        use_pubsub_publish_time: Some(true),
        ..publisher
    };
    let transforms = external_transforms(&build(false, true, &publish_time).expect("graph"));
    assert_eq!(text(&transforms[0].1, "timestamp_attribute"), None);
}

#[test]
fn pubsub_query_reads_the_subscription_and_publishes_results() {
    for mode in [PubsubMode::SubscribeOnly, PubsubMode::Combined] {
        let query = NexmarkArgs {
            pubsub_subscription: Some(SUBSCRIPTION.into()),
            pubsub_results_topic: Some(RESULTS_TOPIC.into()),
            ..args(SourceType::Pubsub, mode, SinkType::Pubsub)
        };
        let transforms = external_transforms(&build(true, true, &query).expect("graph"));
        let [(read_urn, read), (write_urn, write)] = transforms.as_slice() else {
            panic!("expected a read and a write, got {transforms:?}");
        };
        assert_eq!(read_urn, URN_PUBSUB_READ);
        assert_eq!(text(read, "subscription").as_deref(), Some(SUBSCRIPTION));
        assert_eq!(text(read, "topic"), None);
        assert_eq!(text(read, "format").as_deref(), Some("RAW"));
        assert_eq!(
            text(read, "timestamp_attribute").as_deref(),
            Some(PUBSUB_TIMESTAMP_ATTRIBUTE)
        );
        assert_eq!(write_urn, URN_PUBSUB_WRITE);
        assert_eq!(text(write, "topic").as_deref(), Some(RESULTS_TOPIC));
    }
}

#[test]
fn kafka_modes_use_managed_read_and_write() {
    let kafka = |mode, sink| NexmarkArgs {
        bootstrap_servers: Some("broker:9092".into()),
        kafka_topic: Some("events".into()),
        kafka_results_topic: Some("results".into()),
        expansion_service: Some("localhost:8097".into()),
        ..args(SourceType::Kafka, mode, sink)
    };

    let publisher = build(
        false,
        true,
        &kafka(PubsubMode::PublishOnly, SinkType::Devnull),
    );
    let transforms = external_transforms(&publisher.expect("graph"));
    let targets: Vec<(String, serde_json::Value)> =
        transforms.iter().map(|(_, row)| managed(row)).collect();
    let [(target, config)] = targets.as_slice() else {
        panic!("expected one Kafka write, got {targets:?}");
    };
    assert_eq!(target, URN_KAFKA_WRITE);
    assert_eq!(config["topic"], "events");
    assert_eq!(config["bootstrap_servers"], "broker:9092");

    let query = build(
        true,
        true,
        &kafka(PubsubMode::SubscribeOnly, SinkType::Kafka),
    );
    let transforms = external_transforms(&query.expect("graph"));
    let mut targets: Vec<(String, serde_json::Value)> =
        transforms.iter().map(|(_, row)| managed(row)).collect();
    targets.sort_by(|a, b| a.0.cmp(&b.0));
    let [(read_target, read), (write_target, write)] = targets.as_slice() else {
        panic!("expected a Kafka read and write, got {targets:?}");
    };
    assert_eq!(read_target, URN_KAFKA_READ);
    assert_eq!(read["topic"], "events");
    assert_eq!(read["auto_offset_reset_config"], "earliest");
    assert_eq!(write_target, URN_KAFKA_WRITE);
    assert_eq!(write["topic"], "results");
}

#[test]
fn source_sink_and_mode_names_parse() {
    assert_eq!("pubsub".parse::<SourceType>(), Ok(SourceType::Pubsub));
    assert_eq!("KAFKA".parse::<SourceType>(), Ok(SourceType::Kafka));
    assert_eq!("PUBSUB".parse::<SinkType>(), Ok(SinkType::Pubsub));
    assert_eq!("kafka".parse::<SinkType>(), Ok(SinkType::Kafka));
    assert_eq!(
        "publish_only".parse::<PubsubMode>(),
        Ok(PubsubMode::PublishOnly)
    );
    assert_eq!(
        "SUBSCRIBE_ONLY".parse::<PubsubMode>(),
        Ok(PubsubMode::SubscribeOnly)
    );
    assert_eq!("COMBINED".parse::<PubsubMode>(), Ok(PubsubMode::Combined));
    assert!("BIGQUERY".parse::<SinkType>().is_err());
    assert!("BOTH".parse::<PubsubMode>().is_err());
}

#[test]
fn a_combined_run_gives_each_job_its_own_mode() {
    let mut query_options = options(true, false);
    query_options.job_name = Some("run".to_string());
    let base = args(SourceType::Pubsub, PubsubMode::Combined, SinkType::Devnull);
    let query = NexmarkQueryName::HotItems;
    query_options
        .set(
            base.pin_query(query)
                .pin_pubsub_mode(PubsubMode::SubscribeOnly),
        )
        .expect("query args");

    let publisher = publisher_options(&query_options, &base, query).expect("publisher options");

    let mode = |options: &PipelineOptions| {
        options
            .view_as::<NexmarkArgs>()
            .expect("nexmark args")
            .pubsub_mode
    };
    assert_eq!(mode(&query_options), Some(PubsubMode::SubscribeOnly));
    assert_eq!(mode(&publisher), Some(PubsubMode::PublishOnly));
    assert_eq!(publisher.job_name.as_deref(), Some("run-publisher"));
    assert_eq!(query_options.job_name.as_deref(), Some("run"));
}

#[test]
fn google_managed_kafka_auth_reaches_the_read_and_the_write() {
    let has_auth = |config: &serde_json::Value| config.to_string().contains("OAUTHBEARER");
    for auth in [false, true] {
        let query = NexmarkArgs {
            bootstrap_servers: Some("broker:9092".into()),
            kafka_topic: Some("events".into()),
            kafka_results_topic: Some("results".into()),
            expansion_service: Some("localhost:8097".into()),
            google_managed_kafka_auth: auth,
            ..args(
                SourceType::Kafka,
                PubsubMode::SubscribeOnly,
                SinkType::Kafka,
            )
        };
        let transforms = external_transforms(&build(true, true, &query).expect("graph"));
        let configs: Vec<serde_json::Value> =
            transforms.iter().map(|(_, row)| managed(row).1).collect();
        assert_eq!(configs.len(), 2, "{configs:?}");
        assert!(
            configs.iter().all(|config| has_auth(config) == auth),
            "{configs:?}"
        );
    }
}
