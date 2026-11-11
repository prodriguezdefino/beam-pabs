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

//! Reads in the Confluent wire format with the schemas of a registry.

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use beam::schema::{FieldValue, RowElement};
use beam_io_kafka::codec::{Avro, Decoder};

use super::common::{FakeRegistry, Route, framed, record, schema_answer};

/// The latest schema of the subject `events-value`, with the ID 7.
const LATEST: &str = r#"{"type":"record","name":"Event","fields":[
    {"name":"a","type":"int"},{"name":"b","type":["null","string"]}]}"#;
/// An older schema, with the ID 3: no field `b`.
const OLDER: &str = r#"{"type":"record","name":"Event","fields":[{"name":"a","type":"int"}]}"#;
/// A schema whose field `a` has another type, with the ID 4.
const OTHER_TYPE: &str =
    r#"{"type":"record","name":"Event","fields":[{"name":"a","type":"string"}]}"#;
/// A schema with no field `a`, which the read schema needs, with the ID 5.
const NO_A: &str = r#"{"type":"record","name":"Event","fields":[
    {"name":"b","type":["null","string"]}]}"#;

fn registry() -> FakeRegistry {
    FakeRegistry::start(vec![
        Route::get(
            "/subjects/events-value/versions/latest",
            schema_answer(Some(7), LATEST),
        ),
        Route::get("/schemas/ids/7", schema_answer(None, LATEST)),
        Route::get("/schemas/ids/3", schema_answer(None, OLDER)),
        Route::get("/schemas/ids/4", schema_answer(None, OTHER_TYPE)),
        Route::get("/schemas/ids/5", schema_answer(None, NO_A)),
        Route::get(
            "/subjects/protos-value/versions/latest",
            r#"{"id":8,"schemaType":"PROTOBUF","schema":"syntax = \"proto3\";"}"#,
        ),
    ])
}

fn values(codec: &Avro, value: &[u8]) -> Vec<Option<FieldValue>> {
    let mut state = Default::default();
    match codec.decode(&mut state, &record(value)).expect("a row") {
        RowElement::Row(row) => row.values().to_vec(),
        RowElement::Encoded(_) => panic!("a decoded row"),
    }
}

#[test]
fn the_latest_schema_of_the_subject_reads_records_of_every_version() {
    let fake = registry();
    let codec = Avro::latest(
        fake.client().with_basic_auth("user", "secret"),
        "events-value",
    )
    .expect("the latest schema");
    let names: Vec<String> = codec
        .row_schema()
        .expect("a schema")
        .fields
        .iter()
        .map(|f| f.name.clone())
        .collect();
    assert_eq!(names, ["a", "b"]);

    // a = 5, b = "x".
    let latest = framed(7, &[0x0a, 0x02, 0x02, b'x']);
    assert_eq!(
        values(&codec, &latest),
        [
            Some(FieldValue::Int32(5)),
            Some(FieldValue::String("x".to_string()))
        ]
    );
    let older = framed(3, &[0x0a]);
    assert_eq!(values(&codec, &older), [Some(FieldValue::Int32(5)), None]);
    assert_eq!(values(&codec, &older), [Some(FieldValue::Int32(5)), None]);

    // The codec asks for each writer schema once, and never for its own ID.
    assert_eq!(fake.calls("/schemas/ids/3"), 1);
    assert_eq!(fake.calls("/schemas/ids/7"), 0);
    let expected = format!("Basic {}", STANDARD.encode("user:secret"));
    assert!(
        fake.requests()
            .iter()
            .all(|r| r.authorization.as_deref() == Some(expected.as_str()))
    );
}

#[test]
fn a_registry_with_a_given_read_schema_looks_up_every_writer_schema() {
    let fake = registry();
    let codec = Avro::schema(LATEST).unwrap().with_registry(fake.client());
    let latest = framed(7, &[0x0a, 0x00]);
    assert_eq!(values(&codec, &latest), [Some(FieldValue::Int32(5)), None]);
    assert_eq!(fake.calls("/schemas/ids/7"), 1);
    assert!(fake.requests().iter().all(|r| r.authorization.is_none()));
}

#[test]
fn an_unknown_id_fails_the_record_and_is_not_asked_again_during_the_backoff() {
    let fake = registry();
    let codec = Avro::latest(fake.client(), "events-value").unwrap();
    let unknown = framed(9, &[0x0a]);
    let mut state = Default::default();
    for _ in 0..3 {
        let error = codec.decode(&mut state, &record(&unknown)).unwrap_err();
        assert!(error.message().contains("404"), "{error}");
    }
    assert_eq!(fake.calls("/schemas/ids/9"), 1);
}

#[test]
fn readers_that_need_the_same_new_id_make_one_request() {
    let fake = registry();
    let codec = Avro::latest(fake.client(), "events-value").unwrap();
    let older = framed(3, &[0x0a]);
    std::thread::scope(|scope| {
        for _ in 0..8 {
            scope.spawn(|| {
                assert_eq!(values(&codec, &older), [Some(FieldValue::Int32(5)), None]);
            });
        }
    });
    assert_eq!(fake.calls("/schemas/ids/3"), 1);
}

#[test]
fn a_writer_schema_that_does_not_fit_fails_each_record_with_one_request() {
    let fake = registry();
    let codec = Avro::latest(fake.client(), "events-value").unwrap();
    let cases = [(4, "resolve"), (5, "resolve")];
    let mut state = Default::default();
    cases.iter().for_each(|(id, expected)| {
        for _ in 0..2 {
            let error = codec
                .decode(&mut state, &record(&framed(*id, &[0x02])))
                .unwrap_err();
            assert!(
                error.message().contains(&format!("writer schema {id}"))
                    && error.message().contains(expected),
                "{error}"
            );
        }
        assert_eq!(fake.calls(&format!("/schemas/ids/{id}")), 1);
    });
}

#[test]
fn a_value_out_of_the_wire_format_fails_without_a_request() {
    let fake = registry();
    let codec = Avro::latest(fake.client(), "events-value").unwrap();
    let mut state = Default::default();
    let error = codec.decode(&mut state, &record(&[1, 0, 0])).unwrap_err();
    assert!(error.message().contains("Confluent"), "{error}");
    assert_eq!(fake.requests().len(), 1);
}

#[test]
fn the_registry_calls_fail_with_the_reason() {
    let fake = registry();
    let error = Avro::latest(fake.client(), "protos-value")
        .unwrap_err()
        .to_string();
    assert!(error.contains("PROTOBUF"), "{error}");
    let error = Avro::latest(fake.client(), "a/b c")
        .unwrap_err()
        .to_string();
    assert!(error.contains("404"), "{error}");
    assert_eq!(fake.calls("/subjects/a%2Fb%20c/versions/latest"), 1);
    let closed = beam_io_kafka::codec::SchemaRegistry::new("http://127.0.0.1:1");
    assert!(Avro::latest(closed, "events-value").is_err());
}

#[test]
fn the_debug_form_of_a_registry_has_no_password() {
    let fake = registry();
    let debug = format!("{:?}", fake.client().with_basic_auth("user", "secret"));
    assert!(
        debug.contains("basic") && !debug.contains("secret"),
        "{debug}"
    );
}

#[test]
fn one_batch_reads_records_of_several_writer_schemas_in_their_order() {
    let fake = registry();
    let codec = Avro::latest(fake.client(), "events-value").unwrap();
    let latest = framed(7, &[0x0a, 0x02, 0x02, b'x']);
    let older = framed(3, &[0x0c]);
    let unknown = framed(9, &[0x0a]);
    let mut state = Default::default();

    // First batch sees only writer schema 7; second batch adds writer schema 3 on the same state.
    let mut first = Vec::new();
    codec.decode_batch(&mut state, &[record(&latest)], &mut first);
    assert_eq!(first.len(), 1);
    assert!(first[0].is_ok());

    let records = [
        record(&latest),
        record(&older),
        record(&unknown),
        record(&latest),
        record(&older),
    ];
    let mut decoded = Vec::new();
    codec.decode_batch(&mut state, &records, &mut decoded);
    let values: Vec<Option<Vec<Option<FieldValue>>>> = decoded
        .into_iter()
        .map(|row| match row {
            Ok(RowElement::Row(row)) => Some(row.values().to_vec()),
            _ => None,
        })
        .collect();
    let x = Some(FieldValue::String("x".to_string()));
    assert_eq!(
        values,
        [
            Some(vec![Some(FieldValue::Int32(5)), x.clone()]),
            Some(vec![Some(FieldValue::Int32(6)), None]),
            None,
            Some(vec![Some(FieldValue::Int32(5)), x]),
            Some(vec![Some(FieldValue::Int32(6)), None]),
        ]
    );
    assert_eq!(fake.calls("/schemas/ids/3"), 1);
}
