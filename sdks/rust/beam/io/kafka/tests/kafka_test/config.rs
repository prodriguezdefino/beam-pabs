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

//! Configuration rows: exact schema vs the Java providers, exact values, validation.

use std::collections::BTreeMap;

use beam::prelude::*;
use beam::schema::{Field, FieldType, FieldValue};
use external::ExpansionError;
use kafka_io::{
    KafkaFormat, KafkaRead, KafkaWrite, OffsetReset, google_managed_kafka_auth_config,
    payload_bytes, raw_bytes_row, raw_bytes_schema, raw_string_schema,
};

use crate::common::{describe, fixture, values};
use crate::fixtures::{KAFKA_READ_CONFIG, KAFKA_WRITE_CONFIG, ReadExpect, WriteExpect, string_map};

fn invalid(msg: &str) -> ExpansionError {
    ExpansionError::InvalidResponse(msg.to_string())
}

#[test]
fn test_google_managed_kafka_auth() {
    let expected: BTreeMap<String, String> = [
        (
            "sasl.jaas.config",
            "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;",
        ),
        (
            "sasl.login.callback.handler.class",
            "com.google.cloud.hosted.kafka.auth.GcpLoginCallbackHandler",
        ),
        ("sasl.mechanism", "OAUTHBEARER"),
        ("security.protocol", "SASL_SSL"),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_string(), v.to_string()))
    .collect();
    assert_eq!(google_managed_kafka_auth_config(), expected);

    // Auth is merged on top of user properties, and reaches the config row.
    let read = KafkaRead::new("KafkaRead", "b:9092", "t")
        .with_consumer_config("group.id", "g")
        .with_google_managed_kafka_auth();
    let mut with_group = expected.clone();
    with_group.insert("group.id".into(), "g".into());
    assert_eq!(read.consumer_config(), &with_group);

    let write = KafkaWrite::new("KafkaWrite", "b:9092", "t").with_google_managed_kafka_auth();
    assert_eq!(write.producer_config(), &expected);
    let row = write.build_config_row().unwrap();
    let pairs: Vec<(&str, &str)> = expected
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect();
    assert_eq!(
        row.get_value("producer_config_updates"),
        Some(&string_map(&pairs))
    );
}

#[test]
fn test_read_config_row() {
    let row = KafkaRead::new("KafkaRead", "b1:9092,b2:9092", "events")
        .with_format(KafkaFormat::Json {
            schema: r#"{"type":"object"}"#.into(),
        })
        .with_auto_offset_reset(OffsetReset::Earliest)
        .with_consumer_config("security.protocol", "SASL_SSL")
        .with_consumer_config("group.id", "g1")
        .with_max_read_time_seconds(30)
        .with_redistribute(Some(8))
        .with_redistribute_by_record_key(true)
        .with_allow_duplicates(true)
        .with_offset_deduplication(false)
        .build_config_row()
        .unwrap();

    assert_eq!(describe(row.schema()), fixture(KAFKA_READ_CONFIG));
    assert_eq!(
        values(&row),
        ReadExpect {
            allow_duplicates: Some(true),
            auto_offset_reset_config: Some("earliest"),
            bootstrap_servers: "b1:9092,b2:9092",
            consumer_config_updates: string_map(&[
                ("group.id", "g1"),
                ("security.protocol", "SASL_SSL")
            ]),
            format: "JSON",
            max_read_time_seconds: Some(30),
            offset_deduplication: Some(false),
            redistribute_by_record_key: Some(true),
            redistribute_num_keys: Some(8),
            redistributed: Some(true),
            schema: Some(r#"{"type":"object"}"#),
            topic: "events",
            ..Default::default()
        }
        .values()
    );

    // The row must survive the portable row coder the payload uses.
    let bytes = row.to_row_bytes().unwrap();
    assert_eq!(Row::from_row_bytes(row.schema(), &bytes).unwrap(), row);
}

/// Defaults: RAW format, and every optional field null so Java applies its defaults.
#[test]
fn test_read_minimal_config_leaves_optionals_null() {
    let row = KafkaRead::new("KafkaRead", "b:9092", "t")
        .build_config_row()
        .unwrap();
    assert_eq!(describe(row.schema()), fixture(KAFKA_READ_CONFIG));
    assert_eq!(
        values(&row),
        ReadExpect {
            bootstrap_servers: "b:9092",
            format: "RAW",
            topic: "t",
            ..Default::default()
        }
        .values()
    );
    // `with_redistribute(None)` enables redistribution without fixing the key count.
    let row = KafkaRead::new("KafkaRead", "b:9092", "t")
        .with_format(KafkaFormat::String)
        .with_auto_offset_reset(OffsetReset::Latest)
        .with_redistribute(None)
        .build_config_row()
        .unwrap();
    assert_eq!(
        values(&row),
        ReadExpect {
            auto_offset_reset_config: Some("latest"),
            bootstrap_servers: "b:9092",
            format: "STRING",
            redistributed: Some(true),
            topic: "t",
            ..Default::default()
        }
        .values()
    );
}

#[test]
fn test_read_schema_registry_and_proto() {
    let row = KafkaRead::new("KafkaRead", "b:9092", "t")
        .with_confluent_schema_registry("https://registry", "t-value")
        .build_config_row()
        .unwrap();
    assert_eq!(
        values(&row),
        ReadExpect {
            bootstrap_servers: "b:9092",
            format: "RAW",
            registry_subject: Some("t-value"),
            registry_url: Some("https://registry"),
            topic: "t",
            ..Default::default()
        }
        .values()
    );

    let row = KafkaRead::new("KafkaRead", "b:9092", "t")
        .with_format(KafkaFormat::Proto {
            message_name: "pkg.Msg".into(),
            schema: None,
            file_descriptor_path: Some("gs://b/desc.pb".into()),
        })
        .build_config_row()
        .unwrap();
    assert_eq!(
        values(&row),
        ReadExpect {
            bootstrap_servers: "b:9092",
            file_descriptor_path: Some("gs://b/desc.pb"),
            format: "PROTO",
            message_name: Some("pkg.Msg"),
            topic: "t",
            ..Default::default()
        }
        .values()
    );
}

#[test]
fn test_read_validation() {
    let err = |read: KafkaRead| read.build().expect_err("invalid read");
    assert_eq!(
        err(KafkaRead::new("KafkaRead", "", "")),
        invalid("KafkaRead requires a non-empty bootstrap_servers")
    );
    assert_eq!(
        err(KafkaRead::new("KafkaRead", "", "t")),
        invalid("KafkaRead requires a non-empty bootstrap_servers")
    );
    assert_eq!(
        err(KafkaRead::new("KafkaRead", "b:9092", "")),
        invalid("KafkaRead requires a non-empty topic")
    );
    let proto = || {
        KafkaRead::new("KafkaRead", "b:9092", "t").with_format(KafkaFormat::Proto {
            message_name: "m".into(),
            schema: None,
            file_descriptor_path: None,
        })
    };
    assert_eq!(
        err(proto()),
        invalid("KafkaRead with PROTO format requires a schema or file_descriptor_path")
    );
    // A schema registry supplies the schema, so the PROTO check is skipped.
    assert!(
        proto()
            .with_confluent_schema_registry("https://r", "s")
            .build_config_row()
            .is_ok()
    );
}

#[test]
fn test_write_config_row() {
    let row = KafkaWrite::new("KafkaWrite", "b:9092", "out")
        .with_format(KafkaFormat::Avro {
            schema: "{}".into(),
        })
        .with_producer_config("compression.type", "zstd")
        .with_error_handling("bad")
        .build_config_row()
        .unwrap();
    assert_eq!(describe(row.schema()), fixture(KAFKA_WRITE_CONFIG));
    assert_eq!(
        values(&row),
        WriteExpect {
            bootstrap_servers: "b:9092",
            error_handling: Some("bad"),
            format: "AVRO",
            producer_config_updates: string_map(&[("compression.type", "zstd")]),
            schema: Some("{}"),
            topic: "out",
            ..Default::default()
        }
        .values()
    );

    let row = KafkaWrite::new("KafkaWrite", "b:9092", "out")
        .with_format(KafkaFormat::Proto {
            message_name: "pkg.Msg".into(),
            schema: Some("syntax = \"proto3\";".into()),
            file_descriptor_path: None,
        })
        .build_config_row()
        .unwrap();
    assert_eq!(
        values(&row),
        WriteExpect {
            bootstrap_servers: "b:9092",
            format: "PROTO",
            message_name: Some("pkg.Msg"),
            schema: Some("syntax = \"proto3\";"),
            topic: "out",
            ..Default::default()
        }
        .values()
    );
}

#[test]
fn test_write_validation() {
    let err = |write: KafkaWrite| write.build().expect_err("invalid write");
    assert_eq!(
        err(KafkaWrite::new("KafkaWrite", "", "")),
        invalid("KafkaWrite requires a non-empty bootstrap_servers")
    );
    let base = || KafkaWrite::new("KafkaWrite", "b:9092", "t");
    assert_eq!(
        err(KafkaWrite::new("KafkaWrite", "b:9092", "")),
        invalid("KafkaWrite requires a non-empty topic")
    );
    assert_eq!(
        err(base().with_format(KafkaFormat::String)),
        invalid("KafkaWrite does not support STRING format; use RAW with UTF-8 bytes")
    );
    let proto = |schema: Option<&str>, path: Option<&str>| {
        base().with_format(KafkaFormat::Proto {
            message_name: "m".into(),
            schema: schema.map(Into::into),
            file_descriptor_path: path.map(Into::into),
        })
    };
    assert_eq!(
        err(proto(Some("s"), Some("p"))),
        invalid("KafkaWrite with PROTO format takes a schema or a file_descriptor_path, not both")
    );
    assert_eq!(
        err(proto(None, None)),
        invalid("KafkaWrite with PROTO format requires a schema or file_descriptor_path")
    );
}

/// RAW rows are `(payload: BYTES)` and STRING reads `(payload: STRING)`, the single
/// non-null field the Java RAW/STRING formats produce/accept.
#[test]
fn test_raw_helpers() {
    assert_eq!(
        describe(&raw_bytes_schema()),
        fixture(&[("payload", "BYTES")])
    );
    assert_eq!(
        describe(&raw_string_schema()),
        fixture(&[("payload", "STRING")])
    );

    let row = raw_bytes_row(b"hi".to_vec());
    assert_eq!(row.schema(), &raw_bytes_schema());
    assert_eq!(payload_bytes(&row), Some(&b"hi"[..]));

    let text = Row::new(
        raw_string_schema(),
        vec![Some(FieldValue::String("yo".into()))],
    )
    .unwrap();
    assert_eq!(payload_bytes(&text), Some(&b"yo"[..]));

    // Anything other than a BYTES/STRING `payload` is not a raw payload.
    let other = Row::new(
        std::sync::Arc::new(Schema::new(vec![Field::new("payload", FieldType::int32())])),
        vec![Some(FieldValue::Int32(1))],
    )
    .unwrap();
    assert_eq!(payload_bytes(&other), None);
}
