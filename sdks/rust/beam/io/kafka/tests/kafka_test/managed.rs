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

//! The Managed payloads the builders produce, and their declared error outputs.

use beam::prelude::*;
use beam::schema::FieldValue;
use external::URN_EXPANSION_SCHEMA_TRANSFORM;
use kafka_io::{
    DEFAULT_EXPANSION_SERVICE, KafkaFormat, KafkaRead, KafkaWrite, URN_KAFKA_READ, URN_KAFKA_WRITE,
};
use managed_io::URN_MANAGED;

use crate::common::decode_payload;

/// Decodes a Managed payload into (underlying transform URN, inline config as JSON).
pub fn managed_target(payload: &[u8]) -> (String, serde_json::Value) {
    let (identifier, row) = decode_payload(payload);
    assert_eq!(identifier, URN_MANAGED);
    let urn = row
        .get_string("transform_identifier")
        .expect("valid test payload")
        .expect("valid test payload")
        .to_string();
    let config = serde_json::from_str(
        row.get_string("config")
            .expect("valid test payload")
            .expect("valid test payload"),
    )
    .expect("valid test payload");
    (urn, config)
}

#[test]
fn test_error_handling_reaches_the_managed_config_and_declared_outputs() {
    let read = KafkaRead::new("KafkaRead", "b:9092", "t").with_error_handling("bad_records");
    assert_eq!(read.error_output(), Some("bad_records"));

    let row = read.build_config_row().unwrap();
    let Some(Some(FieldValue::Row(error_handling))) = row.get_value("error_handling") else {
        panic!("error_handling must be a row: {row:?}");
    };
    assert_eq!(
        error_handling.get_string("output").unwrap(),
        Some("bad_records")
    );

    let managed = read.to_managed().unwrap();
    assert_eq!(managed.error_output(), Some("bad_records"));
    assert_eq!(managed.declared_outputs(), ["bad_records"]);
    let source = read.build().unwrap();
    let (_, config) = managed_target(&source.transform().payload);
    assert_eq!(
        config["error_handling"],
        serde_json::json!({"output": "bad_records"})
    );
    assert!(source.transform().output_tags.contains("bad_records"));

    let write = KafkaWrite::new("KafkaWrite", "b:9092", "t").with_error_handling("unwritable");
    assert_eq!(
        write.to_managed().unwrap().declared_outputs(),
        ["unwritable"]
    );

    // In placeholder mode `to_managed()` still exposes the error outputs downstream.
    let p = Pipeline::new().with_expansion_mode(beam::pipeline::ExpansionMode::Placeholder);
    let outputs = p.apply(read.to_managed().unwrap().with_all_outputs());
    assert_eq!(
        outputs.tags().collect::<Vec<_>>(),
        ["bad_records", "output"]
    );
    let records = outputs.expect(managed_io::OUTPUT).unwrap();
    let written = records.apply(write.to_managed().unwrap().with_outputs());
    assert!(written.expect("unwritable").is_ok());
}

#[test]
fn test_builder_getters() {
    let json = KafkaFormat::Json {
        schema: "{}".into(),
    };
    let reads = [
        (
            KafkaRead::new("r", "b:9092", "t"),
            (
                "b:9092",
                "t",
                KafkaFormat::Raw,
                DEFAULT_EXPANSION_SERVICE,
                None,
            ),
        ),
        (
            KafkaRead::new("r", "h1:1,h2:2", "events")
                .with_format(json.clone())
                .with_error_handling("bad")
                .with_expansion_service("localhost:1"),
            (
                "h1:1,h2:2",
                "events",
                json.clone(),
                "localhost:1",
                Some("bad"),
            ),
        ),
    ];
    for (read, (servers, topic, format, endpoint, error)) in reads {
        assert_eq!(read.bootstrap_servers(), servers);
        assert_eq!(read.topic(), topic);
        assert_eq!(read.format(), &format);
        assert_eq!(read.expansion_service(), endpoint);
        assert_eq!(read.error_output(), error);
        let source = read.build().unwrap();
        assert_eq!(source.transform().urn, URN_EXPANSION_SCHEMA_TRANSFORM);
        assert_eq!(
            managed_target(&source.transform().payload).0,
            URN_KAFKA_READ
        );
        assert_eq!(source.transform().name, "r");
        assert_eq!(source.transform().endpoint, endpoint);
        assert_eq!(source.main_output_tag(), "output");
    }

    let writes = [
        (
            KafkaWrite::new("w", "b:9092", "t"),
            (
                "b:9092",
                "t",
                KafkaFormat::Raw,
                DEFAULT_EXPANSION_SERVICE,
                None,
            ),
        ),
        (
            KafkaWrite::new("w", "h1:1,h2:2", "events")
                .with_format(json.clone())
                .with_error_handling("unwritable")
                .with_expansion_service("localhost:1"),
            (
                "h1:1,h2:2",
                "events",
                json,
                "localhost:1",
                Some("unwritable"),
            ),
        ),
    ];
    for (write, (servers, topic, format, endpoint, error)) in writes {
        assert_eq!(write.bootstrap_servers(), servers);
        assert_eq!(write.topic(), topic);
        assert_eq!(write.format(), &format);
        assert_eq!(write.expansion_service(), endpoint);
        assert_eq!(write.error_output(), error);
        assert_eq!(
            write.to_managed().unwrap().transform_identifier(),
            Some(URN_KAFKA_WRITE)
        );
        let sink = write.build().unwrap();
        assert_eq!(sink.transform().name, "w");
        assert_eq!(sink.transform().endpoint, endpoint);
        assert_eq!(sink.input_tag(), "input");
    }
}
