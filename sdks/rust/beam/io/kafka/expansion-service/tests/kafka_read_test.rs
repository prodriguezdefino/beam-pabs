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

//! Configuration and registration of the Kafka read provider. No test connects to a broker.

use std::collections::BTreeMap;
use std::time::Duration;

use beam::io::kafka::native::codec::managed::Format;
use beam::io::kafka::native::read::StartOffset;
use beam::schema::BeamRow;
use expansion::{SchemaTransformProvider, SchemaTransformRegistration};
use kafka_expansion::{KafkaReadConfig, KafkaReadProvider, URN_KAFKA_READ};

fn config(format: &str) -> KafkaReadConfig {
    KafkaReadConfig {
        format: format.to_string(),
        topic: "events".to_string(),
        bootstrap_servers: "broker:9092".to_string(),
        ..Default::default()
    }
}

fn properties(entries: &[(&str, &str)]) -> Option<BTreeMap<String, String>> {
    Some(
        entries
            .iter()
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect(),
    )
}

#[test]
fn the_provider_is_registered_with_no_inputs() {
    let found = expansion::inventory::iter::<SchemaTransformRegistration>
        .into_iter()
        .any(|reg| (reg.provider)().identifier() == URN_KAFKA_READ);
    assert!(found);
    assert!(KafkaReadProvider.input_tags().is_empty());
}

#[test]
fn the_config_has_the_fields_of_the_managed_kafka_read() {
    let schema = KafkaReadProvider.config_schema();
    let names: Vec<&str> = schema.fields.iter().map(|f| f.name.as_str()).collect();
    assert_eq!(
        names,
        [
            "format",
            "topic",
            "bootstrap_servers",
            "schema",
            "consumer_config_updates",
            "auto_offset_reset_config",
            "max_read_time_seconds",
            "error_handling",
            "confluent_schema_registry_url",
            "confluent_schema_registry_subject",
            "file_descriptor_path",
            "message_name",
            "with_gcp_adc",
            "commit_expiry_seconds",
            "max_read_time_per_call_seconds",
            "max_idle_per_call_ms",
            "consumer_pool_size",
            "reshuffle_partitions",
        ]
    );
}

#[test]
fn the_config_row_round_trips() {
    let config = KafkaReadConfig {
        consumer_config_updates: properties(&[("group.id", "g")]),
        max_read_time_seconds: Some(30),
        commit_expiry_seconds: Some(60),
        max_read_time_per_call_seconds: Some(2),
        max_idle_per_call_ms: Some(1_500),
        consumer_pool_size: Some(3),
        reshuffle_partitions: Some(true),
        ..config("STRING")
    };
    let row = config.to_row().unwrap();
    assert_eq!(KafkaReadConfig::from_row(&row).unwrap(), config);
}

#[test]
fn a_valid_config_gives_the_read_options() {
    let options = KafkaReadConfig {
        consumer_config_updates: properties(&[("group.id", "g")]),
        auto_offset_reset_config: Some("earliest".to_string()),
        max_read_time_seconds: Some(30),
        commit_expiry_seconds: Some(60),
        max_read_time_per_call_seconds: Some(2),
        max_idle_per_call_ms: Some(1_500),
        consumer_pool_size: Some(3),
        reshuffle_partitions: Some(true),
        ..config("string")
    }
    .read_options()
    .unwrap();
    let (options, format) = options;
    assert!(matches!(format, Format::String(_)));
    assert_eq!(options.topic, "events");
    assert_eq!(options.connection.bootstrap_servers, "broker:9092");
    assert_eq!(options.connection.group_id(), Some("g"));
    assert_eq!(options.start_offset, StartOffset::Earliest);
    assert_eq!(options.max_read_time, Some(Duration::from_secs(30)));
    assert_eq!(options.commit_expiry, Duration::from_secs(60));
    assert_eq!(options.max_read_time_per_call, Duration::from_secs(2));
    assert_eq!(options.max_idle_per_call, Duration::from_millis(1_500));
    assert_eq!(options.consumer_pool.size(), 3);
    assert!(options.reshuffle_partitions);
    assert!(!config("RAW").read_options().unwrap().0.reshuffle_partitions);
}

#[test]
fn the_start_offset_falls_back_to_the_consumer_property_then_to_latest() {
    let from_property = KafkaReadConfig {
        consumer_config_updates: properties(&[("auto.offset.reset", "earliest")]),
        ..config("RAW")
    };
    let explicit = KafkaReadConfig {
        auto_offset_reset_config: Some("none".to_string()),
        ..from_property.clone()
    };
    let starts: Vec<StartOffset> = [config("RAW"), from_property, explicit]
        .into_iter()
        .map(|config| config.read_options().unwrap().0.start_offset)
        .collect();
    assert_eq!(
        starts,
        [
            StartOffset::Latest,
            StartOffset::Earliest,
            StartOffset::Committed
        ]
    );
}

#[test]
fn json_reads_need_a_json_schema() {
    let schema = r#"{"type":"object","properties":{"id":{"type":"integer"}},"required":["id"]}"#;
    let options = KafkaReadConfig {
        schema: Some(schema.to_string()),
        ..config("JSON")
    }
    .read_options()
    .unwrap();
    let Format::Json(json) = options.1 else {
        panic!("the JSON format")
    };
    let names: Vec<String> = json
        .row_schema()
        .expect("the schema of the JSON Schema")
        .fields
        .iter()
        .map(|f| f.name.clone())
        .collect();
    assert_eq!(names, ["id"]);
}

#[test]
fn an_invalid_config_says_why() {
    let cases = [
        (config("AVRO"), "Avro schema"),
        (config("PROTO"), "not support"),
        (config("XML"), "Unknown format"),
        (config("JSON"), "JSON Schema"),
        (
            KafkaReadConfig {
                topic: String::new(),
                ..config("RAW")
            },
            "topic",
        ),
        (
            KafkaReadConfig {
                bootstrap_servers: String::new(),
                ..config("RAW")
            },
            "bootstrap servers",
        ),
        (
            KafkaReadConfig {
                auto_offset_reset_config: Some("smallest".to_string()),
                ..config("RAW")
            },
            "start offset",
        ),
        (
            KafkaReadConfig {
                max_read_time_seconds: Some(0),
                ..config("RAW")
            },
            "max_read_time_seconds",
        ),
        (
            KafkaReadConfig {
                commit_expiry_seconds: Some(-1),
                ..config("RAW")
            },
            "commit_expiry_seconds",
        ),
        (
            KafkaReadConfig {
                max_read_time_per_call_seconds: Some(0),
                ..config("RAW")
            },
            "max_read_time_per_call_seconds",
        ),
        (
            KafkaReadConfig {
                max_idle_per_call_ms: Some(0),
                ..config("RAW")
            },
            "max_idle_per_call_ms",
        ),
        (
            KafkaReadConfig {
                max_idle_per_call_ms: Some(-5),
                ..config("RAW")
            },
            "max_idle_per_call_ms",
        ),
        (
            KafkaReadConfig {
                consumer_pool_size: Some(0),
                ..config("RAW")
            },
            "consumer_pool_size",
        ),
        (
            KafkaReadConfig {
                consumer_pool_size: Some(-2),
                ..config("RAW")
            },
            "consumer_pool_size",
        ),
        (
            KafkaReadConfig {
                consumer_pool_size: Some(17),
                ..config("RAW")
            },
            "consumer_pool_size",
        ),
        (
            KafkaReadConfig {
                reshuffle_partitions: Some(true),
                ..config("RAW")
            },
            "max read time",
        ),
        (
            KafkaReadConfig {
                confluent_schema_registry_url: Some("http://registry".to_string()),
                ..config("RAW")
            },
            "schema registry",
        ),
    ];
    cases.into_iter().for_each(|(config, expected)| {
        let error = config.read_options().unwrap_err();
        assert!(error.contains(expected), "{error}");
    });
}
