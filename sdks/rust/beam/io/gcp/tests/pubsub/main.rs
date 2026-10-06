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

use beam::pipeline::URN_EXPANSION_SCHEMA_TRANSFORM;
use beam::prelude::*;
use beam::schema::FieldValue;
use external::ExpansionError;
use gcp::DEFAULT_EXPANSION_SERVICE;
use gcp::pubsub::{
    PubsubFormat, PubsubRead, PubsubWrite, PubsubWriteFormat, URN_PUBSUB_READ, URN_PUBSUB_WRITE,
    raw_bytes_row, raw_bytes_schema, raw_string_row, raw_string_schema,
};
use testutils::xlang::{
    Route, assert_spliced, decode_payload, describe, fixture, start_mock, string_array,
    string_value, values,
};

// `(topic, subscription, format, attributes, attributes_map, id_attribute,
// timestamp_attribute, expansion_service)`.
type ReadGetters<'a> = (
    Option<&'a str>,
    Option<&'a str>,
    &'a PubsubFormat,
    Option<&'a [String]>,
    Option<&'a str>,
    Option<&'a str>,
    Option<&'a str>,
    &'a str,
);

// `(topic, format, attributes, attributes_map, id_attribute, timestamp_attribute,
// expansion_service)`.
type WriteGetters<'a> = (
    &'a str,
    PubsubWriteFormat,
    Option<&'a [String]>,
    Option<&'a str>,
    Option<&'a str>,
    Option<&'a str>,
    &'a str,
);

// Configuration schema that Java derives for `PubsubReadSchemaTransformConfiguration.java`.
//
// Java derives the names with `AutoValueSchema(getters).sorted().toSnakeCase()`: camelCase
// getter names are sorted, then converted to snake_case. `@Nullable` getters are nullable.
// `ErrorHandling` is a nested AutoValue with a non-null `getOutput()`. `client_factory` and
// `clock` are test-only interface types without getters, so Java infers empty rows. This
// test cannot verify them offline without the Java expansion service.
const PUBSUB_READ_CONFIG: &[(&str, &str)] = &[
    ("attributes", "ARRAY<STRING>?"),
    ("attributes_map", "STRING?"),
    ("client_factory", "ROW<>?"),
    ("clock", "ROW<>?"),
    ("error_handling", "ROW<output: STRING>?"),
    ("format", "STRING"),
    ("id_attribute", "STRING?"),
    ("schema", "STRING"),
    ("subscription", "STRING?"),
    ("timestamp_attribute", "STRING?"),
    ("topic", "STRING?"),
];

// Configuration schema that Java derives for `PubsubWriteSchemaTransformConfiguration.java`,
// with the same naming rules as [`PUBSUB_READ_CONFIG`].
const PUBSUB_WRITE_CONFIG: &[(&str, &str)] = &[
    ("attributes", "ARRAY<STRING>?"),
    ("attributes_map", "STRING?"),
    ("error_handling", "ROW<output: STRING>?"),
    ("format", "STRING"),
    ("id_attribute", "STRING?"),
    ("timestamp_attribute", "STRING?"),
    ("topic", "STRING"),
];

// Expected `(field, value)` list for a read config row, in the Java sorted field order.
#[allow(
    clippy::too_many_arguments,
    reason = "mirrors the Java config fields positionally"
)]
fn read_values(
    attributes: Option<FieldValue>,
    attributes_map: Option<FieldValue>,
    format: &str,
    id_attribute: Option<FieldValue>,
    schema: &str,
    subscription: Option<FieldValue>,
    timestamp_attribute: Option<FieldValue>,
    topic: Option<FieldValue>,
) -> Vec<(String, Option<FieldValue>)> {
    vec![
        ("attributes".into(), attributes),
        ("attributes_map".into(), attributes_map),
        ("client_factory".into(), None),
        ("clock".into(), None),
        ("error_handling".into(), None),
        ("format".into(), string_value(format)),
        ("id_attribute".into(), id_attribute),
        ("schema".into(), string_value(schema)),
        ("subscription".into(), subscription),
        ("timestamp_attribute".into(), timestamp_attribute),
        ("topic".into(), topic),
    ]
}

// Verifies default config rows, endpoints, and decoded payloads when only required fields are set.
#[test]
fn test_pubsub_default_config_rows_and_payloads() {
    // Read: format defaults to RAW with an empty schema string. Both are non-null in Java.
    // Optional fields are null.
    let read = PubsubRead::new().with_subscription("projects/p/subscriptions/s");
    let row = read.build_config_row().unwrap();
    assert_eq!(describe(row.schema()), fixture(PUBSUB_READ_CONFIG));
    assert_eq!(
        values(&row),
        read_values(
            None,
            None,
            "RAW",
            None,
            "",
            string_value("projects/p/subscriptions/s"),
            None,
            None
        )
    );
    let source = read.build().unwrap();
    assert_eq!(source.transform().endpoint(), DEFAULT_EXPANSION_SERVICE);
    assert_eq!(source.main_output_tag(), "output");
    assert_eq!(source.transform().urn(), URN_EXPANSION_SCHEMA_TRANSFORM);
    let (identifier, config) = decode_payload(source.transform().payload());
    assert_eq!(identifier, URN_PUBSUB_READ);
    assert_eq!(describe(config.schema()), fixture(PUBSUB_READ_CONFIG));
    assert_eq!(values(&config), values(&row));
    let source = read.with_expansion_service("host:1").build().unwrap();
    assert_eq!(source.transform().endpoint(), "host:1");

    // Write: format defaults to RAW and optional fields are null.
    let write = PubsubWrite::new("projects/p/topics/t");
    let row = write.build_config_row().unwrap();
    assert_eq!(describe(row.schema()), fixture(PUBSUB_WRITE_CONFIG));
    assert_eq!(
        values(&row),
        vec![
            ("attributes".to_string(), None),
            ("attributes_map".to_string(), None),
            ("error_handling".to_string(), None),
            ("format".to_string(), string_value("RAW")),
            ("id_attribute".to_string(), None),
            ("timestamp_attribute".to_string(), None),
            ("topic".to_string(), string_value("projects/p/topics/t")),
        ]
    );
    let sink = write.build().unwrap();
    assert_eq!(sink.transform().endpoint(), DEFAULT_EXPANSION_SERVICE);
    assert_eq!(sink.input_tag(), "input");
    assert_eq!(sink.transform().urn(), URN_EXPANSION_SCHEMA_TRANSFORM);
    let (identifier, config) = decode_payload(sink.transform().payload());
    assert_eq!(identifier, URN_PUBSUB_WRITE);
    assert_eq!(values(&config), values(&row));
}

#[test]
fn test_pubsub_read_topic_config_row() {
    let row = PubsubRead::new()
        .with_topic("projects/my-project/topics/my-topic")
        .with_format(PubsubFormat::Raw)
        .with_attributes(["attr1", "attr2"])
        .with_attributes_map("attr_map")
        .with_id_attribute("msg_id")
        .with_timestamp_attribute("ts")
        .build_config_row()
        .expect("build config row");

    assert_eq!(describe(row.schema()), fixture(PUBSUB_READ_CONFIG));
    assert_eq!(
        values(&row),
        read_values(
            string_array(&["attr1", "attr2"]),
            string_value("attr_map"),
            "RAW",
            string_value("msg_id"),
            "",
            None,
            string_value("ts"),
            string_value("projects/my-project/topics/my-topic"),
        )
    );
    // The row must round-trip through the portable row coder used in the payload.
    let bytes = row.to_row_bytes().unwrap();
    assert_eq!(Row::from_row_bytes(row.schema(), &bytes).unwrap(), row);
}

#[test]
fn test_pubsub_read_json_and_avro_formats_carry_their_schema() {
    let avro_schema = r#"{"type":"record","name":"User","fields":[]}"#;
    for (read, format, schema) in [
        (
            PubsubRead::new().with_format(PubsubFormat::json(r#"{"type":"object"}"#)),
            "JSON",
            r#"{"type":"object"}"#,
        ),
        (
            PubsubRead::new().with_format(PubsubFormat::avro(avro_schema)),
            "AVRO",
            avro_schema,
        ),
    ] {
        let row = read
            .with_subscription("projects/p/subscriptions/s")
            .build_config_row()
            .unwrap();
        assert_eq!(
            values(&row),
            read_values(
                None,
                None,
                format,
                None,
                schema,
                string_value("projects/p/subscriptions/s"),
                None,
                None
            ),
            "{format}"
        );
    }
}

#[test]
fn test_pubsub_read_requires_source() {
    let err = PubsubRead::new().build().expect_err("no source");
    assert_eq!(
        err,
        ExpansionError::InvalidConfig("PubsubRead requires either topic or subscription".into())
    );
}

#[test]
fn test_pubsub_read_source_overwriting() {
    let read = PubsubRead::new().with_topic("projects/p/topics/t");
    assert_eq!(read.topic(), Some("projects/p/topics/t"));
    assert_eq!(read.subscription(), None);

    // with_subscription overwrites topic source
    let read = read.with_subscription("projects/p/subscriptions/s");
    assert_eq!(read.topic(), None);
    assert_eq!(read.subscription(), Some("projects/p/subscriptions/s"));

    // with_topic overwrites subscription source
    let read = read.with_topic("projects/p/topics/t2");
    assert_eq!(read.topic(), Some("projects/p/topics/t2"));
    assert_eq!(read.subscription(), None);
}

#[test]
fn test_pubsub_write_config_row() {
    let row = PubsubWrite::new("projects/my-project/topics/egress")
        .with_format(PubsubWriteFormat::Json)
        .with_attributes(["region", "source"])
        .with_attributes_map("all_attrs")
        .with_id_attribute("event_id")
        .with_timestamp_attribute("published_at")
        .build_config_row()
        .expect("build config row");

    assert_eq!(describe(row.schema()), fixture(PUBSUB_WRITE_CONFIG));
    assert_eq!(
        values(&row),
        vec![
            (
                "attributes".to_string(),
                string_array(&["region", "source"])
            ),
            ("attributes_map".to_string(), string_value("all_attrs")),
            ("error_handling".to_string(), None),
            ("format".to_string(), string_value("JSON")),
            ("id_attribute".to_string(), string_value("event_id")),
            (
                "timestamp_attribute".to_string(),
                string_value("published_at")
            ),
            (
                "topic".to_string(),
                string_value("projects/my-project/topics/egress")
            ),
        ]
    );
}

#[test]
fn test_pubsub_write_requires_topic() {
    let err = PubsubWrite::new("").build().expect_err("no topic");
    assert_eq!(
        err,
        ExpansionError::InvalidConfig("PubsubWrite requires destination topic".into())
    );
}

// The Java RAW read emits `(payload: BYTES)`. The Java RAW write accepts a single non-null
// BYTES or STRING field. See `PubsubReadSchemaTransformProvider` and
// `PubsubWriteSchemaTransformProvider`.
#[test]
fn test_pubsub_raw_helpers_match_java_raw_schemas() {
    assert_eq!(
        describe(&raw_bytes_schema()),
        fixture(&[("payload", "BYTES")])
    );
    assert_eq!(
        describe(&raw_string_schema()),
        fixture(&[("payload", "STRING")])
    );

    let row = raw_bytes_row(b"hello".to_vec());
    assert_eq!(row.schema(), &raw_bytes_schema());
    assert_eq!(
        values(&row),
        [(
            "payload".to_string(),
            Some(FieldValue::Bytes(b"hello".to_vec()))
        )]
    );
    let row = raw_string_row("text");
    assert_eq!(row.schema(), &raw_string_schema());
    assert_eq!(
        values(&row),
        [("payload".to_string(), string_value("text"))]
    );
}

#[test]
fn test_pubsub_pipeline_expansion() {
    let mock = start_mock(&[
        (URN_PUBSUB_READ, Route::source().unbounded()),
        (URN_PUBSUB_WRITE, Route::sink()),
    ]);
    let p = Pipeline::new();
    let messages = p.apply(
        "PubsubRead",
        PubsubRead::new()
            .with_subscription("projects/test/subscriptions/sub")
            .with_id_attribute("id")
            .with_expansion_service(mock.endpoint()),
    );
    messages.apply(
        "PubsubWrite",
        PubsubWrite::new("projects/test/topics/out-topic")
            .with_attributes(["k"])
            .with_expansion_service(mock.endpoint()),
    );

    let seen = mock.seen();
    assert_eq!(seen.len(), 2, "{seen:?}");
    let (read, write) = (&seen[0], &seen[1]);

    assert_eq!(read.unique_name, "PubsubRead");
    assert_eq!(read.spec_urn, URN_EXPANSION_SCHEMA_TRANSFORM);
    assert_eq!(read.identifier, URN_PUBSUB_READ);
    assert!(read.inputs.is_empty());
    assert_eq!(describe(read.config.schema()), fixture(PUBSUB_READ_CONFIG));
    assert_eq!(
        values(&read.config),
        read_values(
            None,
            None,
            "RAW",
            string_value("id"),
            "",
            string_value("projects/test/subscriptions/sub"),
            None,
            None
        )
    );

    assert_eq!(write.unique_name, "PubsubWrite");
    assert_eq!(write.identifier, URN_PUBSUB_WRITE);
    assert_eq!(
        describe(write.config.schema()),
        fixture(PUBSUB_WRITE_CONFIG)
    );
    assert_eq!(
        values(&write.config),
        vec![
            ("attributes".to_string(), string_array(&["k"])),
            ("attributes_map".to_string(), None),
            ("error_handling".to_string(), None),
            ("format".to_string(), string_value("RAW")),
            ("id_attribute".to_string(), None),
            ("timestamp_attribute".to_string(), None),
            (
                "topic".to_string(),
                string_value("projects/test/topics/out-topic")
            ),
        ]
    );

    // The read output PCollection and coder are the ones that the service returned. The
    // write consumes exactly that PCollection.
    let read_out = format!("{}/output", read.namespace);
    assert_eq!(messages.id(), read_out);
    assert_eq!(messages.coder_id(), format!("{}/row_coder", read.namespace));
    assert_eq!(write.inputs["input"], read_out);
    assert_spliced(&p, read, &[], &[("output", &read_out)]);
    assert_spliced(&p, write, &[("input", &read_out)], &[]);
}

// The mock is strict, so a passing pipeline test shows that the URN matched.
#[test]
fn test_pubsub_mock_rejects_unrouted_urn() {
    let mock = start_mock(&[(URN_PUBSUB_READ, Route::source().unbounded())]);
    let p = Pipeline::new();
    let messages = p.apply(
        "PubsubRead",
        PubsubRead::new()
            .with_subscription("projects/test/subscriptions/sub")
            .with_expansion_service(mock.endpoint()),
    );
    let err = messages
        .try_apply(
            "PubsubWrite",
            PubsubWrite::new("projects/test/topics/t").with_expansion_service(mock.endpoint()),
        )
        .expect_err("write URN is not routed");
    assert_eq!(
        err,
        ExpansionError::Rpc(format!(
            "Client specified an invalid argument: unexpected URN {URN_PUBSUB_WRITE}"
        ))
    );
    assert_eq!(mock.seen().len(), 1);
}

fn read_getters(read: &PubsubRead) -> ReadGetters<'_> {
    (
        read.topic(),
        read.subscription(),
        read.format(),
        read.attributes(),
        read.attributes_map(),
        read.id_attribute(),
        read.timestamp_attribute(),
        read.expansion_service(),
    )
}

#[test]
fn test_pubsub_read_getters() {
    let attrs = ["a".to_string(), "b".to_string()];
    let json = PubsubFormat::Json {
        schema: "{}".into(),
    };
    let avro = PubsubFormat::Avro {
        schema: "{\"type\":\"record\"}".into(),
    };
    let cases: [(&str, PubsubRead, ReadGetters<'_>); 4] = [
        (
            "defaults",
            PubsubRead::new(),
            (
                None,
                None,
                &PubsubFormat::Raw,
                None,
                None,
                None,
                None,
                DEFAULT_EXPANSION_SERVICE,
            ),
        ),
        (
            "every field",
            PubsubRead::new()
                .with_topic("projects/p/topics/t")
                .with_format(PubsubFormat::json("{}"))
                .with_attributes(["a", "b"])
                .with_attributes_map("m")
                .with_id_attribute("id")
                .with_timestamp_attribute("ts")
                .with_expansion_service("host:1"),
            (
                Some("projects/p/topics/t"),
                None,
                &json,
                Some(&attrs),
                Some("m"),
                Some("id"),
                Some("ts"),
                "host:1",
            ),
        ),
        (
            "subscription and avro",
            PubsubRead::new()
                .with_subscription("projects/p/subscriptions/s")
                .with_format(PubsubFormat::avro("{\"type\":\"record\"}")),
            (
                None,
                Some("projects/p/subscriptions/s"),
                &avro,
                None,
                None,
                None,
                None,
                DEFAULT_EXPANSION_SERVICE,
            ),
        ),
        (
            "raw format resets",
            PubsubRead::new()
                .with_format(json.clone())
                .with_format(PubsubFormat::Raw),
            (
                None,
                None,
                &PubsubFormat::Raw,
                None,
                None,
                None,
                None,
                DEFAULT_EXPANSION_SERVICE,
            ),
        ),
    ];
    for (name, read, expected) in &cases {
        assert_eq!(read_getters(read), *expected, "{name}");
    }
}

fn write_getters(write: &PubsubWrite) -> WriteGetters<'_> {
    (
        write.topic(),
        write.format(),
        write.attributes(),
        write.attributes_map(),
        write.id_attribute(),
        write.timestamp_attribute(),
        write.expansion_service(),
    )
}

#[test]
fn test_pubsub_write_getters() {
    let attrs = ["k".to_string()];
    let cases: [(&str, PubsubWrite, WriteGetters<'_>); 4] = [
        (
            "defaults",
            PubsubWrite::new("projects/p/topics/t"),
            (
                "projects/p/topics/t",
                PubsubWriteFormat::Raw,
                None,
                None,
                None,
                None,
                DEFAULT_EXPANSION_SERVICE,
            ),
        ),
        (
            "every field",
            PubsubWrite::new("projects/p/topics/out")
                .with_format(PubsubWriteFormat::Json)
                .with_attributes(["k"])
                .with_attributes_map("m")
                .with_id_attribute("id")
                .with_timestamp_attribute("ts")
                .with_expansion_service("host:2"),
            (
                "projects/p/topics/out",
                PubsubWriteFormat::Json,
                Some(&attrs),
                Some("m"),
                Some("id"),
                Some("ts"),
                "host:2",
            ),
        ),
        (
            "avro",
            PubsubWrite::new("t").with_format(PubsubWriteFormat::Avro),
            (
                "t",
                PubsubWriteFormat::Avro,
                None,
                None,
                None,
                None,
                DEFAULT_EXPANSION_SERVICE,
            ),
        ),
        (
            "raw format resets",
            PubsubWrite::new("t")
                .with_format(PubsubWriteFormat::Json)
                .with_format(PubsubWriteFormat::Raw),
            (
                "t",
                PubsubWriteFormat::Raw,
                None,
                None,
                None,
                None,
                DEFAULT_EXPANSION_SERVICE,
            ),
        ),
    ];
    for (name, write, expected) in &cases {
        assert_eq!(write_getters(write), *expected, "{name}");
    }
}
