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

use beam::coders::URN_ROW;
use beam::pipeline::Pipeline;
use beam::schema::{BeamRow, FieldType, Schema};
use beam::values::IsBounded;
use beam_expansion::{
    PCollectionId, SchemaTransformProvider, SchemaTransformRegistration, TransformInputs,
};
use beam_io_kafka_expansion::Format;
use beam_io_kafka_expansion::{KafkaWriteConfig, KafkaWriteProvider, URN_KAFKA_WRITE};

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
    let found = beam_expansion::inventory::iter::<SchemaTransformRegistration>
        .into_iter()
        .any(|reg| (reg.provider)().identifier() == URN_KAFKA_WRITE);
    assert!(found);
    assert_eq!(
        KafkaWriteProvider.managed_identifiers(),
        ["beam:schematransform:org.apache.beam:kafka_write:v1"]
    );
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
    assert_eq!(options.topic(), "events");
    assert_eq!(options.connection().bootstrap_servers(), "broker:9092");
    assert_eq!(
        options
            .connection()
            .properties()
            .get("linger.ms")
            .map(String::as_str),
        Some("5")
    );
    let (_, raw) = config("BYTES").write_options().unwrap();
    assert!(matches!(raw, Format::Raw(_)));
}

#[test]
fn an_invalid_config_says_why() {
    let cases = [
        (config("PROTO"), "file_descriptor_path"),
        (
            KafkaWriteConfig {
                producer_config_updates: Some(BTreeMap::from([(
                    "schema.registry.url".into(),
                    "http://registry:8081".into(),
                )])),
                ..config("RAW")
            },
            "AVRO",
        ),
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

#[test]
fn a_schema_registry_makes_avro_writes_register_their_schema() {
    let (options, format) = KafkaWriteConfig {
        producer_config_updates: Some(BTreeMap::from([
            ("schema.registry.url".into(), "http://registry:8081".into()),
            ("basic.auth.user.info".into(), "user:secret".into()),
            ("linger.ms".into(), "5".into()),
        ])),
        ..config("AVRO")
    }
    .write_options()
    .unwrap();
    assert!(matches!(format, Format::Avro(_)), "{format:?}");
    let keys: Vec<&str> = options
        .connection()
        .properties()
        .keys()
        .map(String::as_str)
        .collect();
    assert_eq!(keys, ["linger.ms"]);
}

#[test]
fn fail_on_error_output_keeps_the_error_row_schema() {
    let schema = Schema::builder()
        .field("value", FieldType::string())
        .build();
    let mut p = Pipeline::new();
    let coder = p.register_row_coder(&schema);
    let input = p.add_pcollection::<beam::schema::Row>("caller_input", &coder, IsBounded::Bounded);
    let inputs: TransformInputs = [("input".to_string(), PCollectionId::from(input.id()))]
        .into_iter()
        .collect();
    let outputs = KafkaWriteProvider
        .build_transform(config("JSON").to_row().unwrap(), inputs, &mut p)
        .expect("write expands without error_handling");
    let errors_id = outputs.get("errors").expect("default errors output");

    let components = p.to_proto().components.expect("pipeline components");
    let coder_id = &components.pcollections[errors_id.as_str()].coder_id;
    let spec = components.coders[coder_id]
        .spec
        .as_ref()
        .expect("errors coder spec");
    assert_eq!(spec.urn, URN_ROW);
    let errors_schema = Schema::from_proto_bytes(&spec.payload).expect("valid error row schema");
    let names: Vec<&str> = errors_schema
        .fields
        .iter()
        .map(|f| f.name.as_str())
        .collect();
    assert_eq!(names, ["failed_row", "error_message"]);
}
