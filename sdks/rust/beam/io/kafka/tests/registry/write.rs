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

//! Writes in the Confluent wire format with a schema ID of a registry.

use std::sync::Arc;

use beam::schema::{Field, FieldType, FieldValue, Row, RowElement, Schema};
use beam_io_kafka::codec::{Avro, Decoder, Encoder};

use super::common::{FakeRegistry, Route, bind, framed, record, schema_answer};

const LATEST: &str = r#"{"type":"record","name":"Event","fields":[
    {"name":"a","type":"int"},{"name":"b","type":["null","string"]}]}"#;

fn registry() -> FakeRegistry {
    FakeRegistry::start(vec![
        Route::get(
            "/subjects/events-value/versions/latest",
            schema_answer(Some(7), LATEST),
        ),
        Route::post("/subjects/orders-value/versions", r#"{"id":11}"#),
    ])
}

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("a", FieldType::int32()),
        Field::new("b", FieldType::string().with_nullable(true)),
    ]))
}

fn encode(codec: &Avro, row: Row) -> Vec<u8> {
    let element = RowElement::Row(row);
    let mut state = Default::default();
    let mut buffer = Vec::new();
    let outgoing = codec
        .encode(&mut state, &element, &mut buffer)
        .expect("the row encodes");
    outgoing.value.expect("a value").to_vec()
}

fn row(schema: &Arc<Schema>) -> Row {
    Row::new(
        Arc::clone(schema),
        vec![
            Some(FieldValue::Int32(5)),
            Some(FieldValue::String("x".to_string())),
        ],
    )
    .expect("values of the schema")
}

#[test]
fn a_write_registers_the_schema_of_its_rows_under_the_subject() {
    let fake = registry();
    let schema = schema();
    let mut codec = Avro::new()
        .with_registry(fake.client())
        .with_subject("orders-value");
    bind(&mut codec, &schema).unwrap();
    let requests = fake.requests();
    let [post] = requests.as_slice() else {
        panic!("one request, got {requests:?}")
    };
    assert_eq!(post.method, "POST");
    assert_eq!(post.path, "/subjects/orders-value/versions");
    assert_eq!(
        post.content_type.as_deref(),
        Some("application/vnd.schemaregistry.v1+json")
    );

    // The registered schema reads the datums of the write.
    let body: serde_json::Value = serde_json::from_str(&post.body).unwrap();
    let registered = body["schema"].as_str().expect("a schema");
    let reader = Avro::schema(registered).expect("an Avro schema");
    assert_eq!(reader.row_schema().as_deref(), Some(schema.as_ref()));

    let value = encode(&codec, row(&schema));
    assert_eq!(value[..5], [0, 0, 0, 0, 11]);
    let mut state = Default::default();
    let RowElement::Row(read) = reader.decode(&mut state, &record(&value[5..])).unwrap() else {
        panic!("a decoded row")
    };
    assert_eq!(read.values(), row(&schema).values());
}

#[test]
fn a_write_with_the_latest_schema_writes_its_id_and_registers_nothing() {
    let fake = registry();
    let schema = schema();
    let mut codec = Avro::latest(fake.client(), "events-value").unwrap();
    bind(&mut codec, &schema).unwrap();
    assert_eq!(
        encode(&codec, row(&schema)),
        framed(7, &[0x0a, 0x02, 0x02, b'x'])
    );
    assert!(fake.requests().iter().all(|r| r.method == "GET"));
}

#[test]
fn a_write_with_a_registry_needs_a_schema_id() {
    let fake = registry();
    let mut codec = Avro::new().with_registry(fake.client());
    let error = bind(&mut codec, &schema()).unwrap_err().to_string();
    assert!(error.contains("schema ID"), "{error}");

    let mut codec = Avro::new().with_subject("orders-value");
    let error = bind(&mut codec, &schema()).unwrap_err().to_string();
    assert!(error.contains("schema registry"), "{error}");

    let mut codec = Avro::new()
        .with_registry(fake.client())
        .with_subject("unknown-value");
    let error = bind(&mut codec, &schema()).unwrap_err().to_string();
    assert!(error.contains("404"), "{error}");
}
