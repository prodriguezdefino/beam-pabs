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

//! Configuration and registration of the Kafka write provider. No test connects to a broker.

use std::collections::BTreeMap;

use beam::io::kafka::native::codec::managed::Format;
use beam::schema::BeamRow;
use expansion::{SchemaTransformProvider, SchemaTransformRegistration};
use kafka_expansion::{KafkaWriteConfig, KafkaWriteProvider, URN_KAFKA_WRITE};

fn config(format: &str) -> KafkaWriteConfig {
    KafkaWriteConfig {
        format: format.to_string(),
        topic: "events".to_string(),
        bootstrap_servers: "broker:9092".to_string(),
        ..Default::default()
    }
}

#[test]
fn the_provider_is_registered() {
    let found = expansion::inventory::iter::<SchemaTransformRegistration>
        .into_iter()
        .any(|reg| (reg.provider)().identifier() == URN_KAFKA_WRITE);
    assert!(found);
}

#[test]
fn the_config_has_the_fields_of_the_managed_kafka_write() {
    let schema = KafkaWriteProvider.config_schema();
    let names: Vec<&str> = schema.fields.iter().map(|f| f.name.as_str()).collect();
    assert_eq!(
        names,
        [
            "format",
            "topic",
            "bootstrap_servers",
            "producer_config_updates",
            "error_handling",
            "file_descriptor_path",
            "message_name",
            "schema",
            "with_gcp_adc",
        ]
    );
    assert_eq!(KafkaWriteProvider.input_tags(), ["input"]);
}

#[test]
fn the_config_row_round_trips() {
    let config = KafkaWriteConfig {
        producer_config_updates: Some(BTreeMap::from([("linger.ms".into(), "5".into())])),
        ..config("JSON")
    };
    let row = config.to_row().unwrap();
    assert_eq!(KafkaWriteConfig::from_row(&row).unwrap(), config);
}

#[test]
fn a_valid_config_gives_the_write_options() {
    let options = KafkaWriteConfig {
        producer_config_updates: Some(BTreeMap::from([("linger.ms".into(), "5".into())])),
        ..config("json")
    }
    .write_options()
    .unwrap();
    let (options, format) = options;
    assert!(matches!(format, Format::Json(_)));
    assert_eq!(options.topic, "events");
    assert_eq!(options.connection.bootstrap_servers, "broker:9092");
    assert_eq!(
        options
            .connection
            .properties
            .get("linger.ms")
            .map(String::as_str),
        Some("5")
    );
}

#[test]
fn an_invalid_config_says_why() {
    let cases = [
        (config("PROTO"), "not support"),
        (config("XML"), "Unknown format"),
        (
            KafkaWriteConfig {
                topic: String::new(),
                ..config("RAW")
            },
            "topic",
        ),
        (
            KafkaWriteConfig {
                bootstrap_servers: String::new(),
                ..config("RAW")
            },
            "bootstrap servers",
        ),
    ];
    cases.into_iter().for_each(|(config, expected)| {
        let error = config.write_options().unwrap_err();
        assert!(error.contains(expected), "{error}");
    });
}
