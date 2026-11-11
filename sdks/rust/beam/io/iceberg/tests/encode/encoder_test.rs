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

//! The encoder turns elements into IPC batches of the table, and bad elements into error rows.

use std::sync::Arc;

use beam::coders::RowCoder;
use beam::schema::{BeamRow, FieldValue, RowElement};
use beam_io_arrow::{BeamRowCodec, SchemaRowCodec};
use beam_io_iceberg::arrow::{decode_batch, schema_bytes};
use beam_io_iceberg::encode::Encoder;

use crate::common::{device_metric, metric, table};

#[derive(Clone, Debug, PartialEq, BeamRow)]
struct Reading {
    id: i64,
    device_id: String,
    payload: f64,
}

#[test]
fn rows_encode_to_one_batch_that_decodes_with_the_table_schema() {
    let schema = device_metric();
    let encoder = Encoder::new(Arc::clone(&schema), &table(&schema)).expect("an encoder");
    let codec = SchemaRowCodec::new(Arc::clone(&schema));
    let rows: Vec<RowElement> = (0..10)
        .map(|id| RowElement::Row(metric(&schema, id, 1_000 * id, Some("us-central1"))))
        .collect();

    let encoded = encoder.encode(&codec, &rows).expect("the rows encode");

    assert!(encoded.errors.is_empty());
    let (batch, ipc) = encoded.batch.expect("a batch");
    assert_eq!(ipc.rows, 10);
    let decoded = decode_batch(
        &schema_bytes(&batch.schema()).expect("a schema message"),
        ipc.bytes,
    )
    .expect("the IPC bytes decode");
    assert_eq!(decoded, batch);
}

#[test]
fn encoded_rows_take_the_byte_path() {
    let schema = device_metric();
    let encoder = Encoder::new(Arc::clone(&schema), &table(&schema)).expect("an encoder");
    let codec = SchemaRowCodec::new(Arc::clone(&schema));
    let bytes = metric(&schema, 1, 5, None)
        .to_row_bytes()
        .expect("row bytes");

    let encoded = encoder
        .encode(&codec, &[RowElement::Encoded(bytes)])
        .expect("the row encodes");

    assert_eq!(encoded.batch.expect("a batch").1.rows, 1);
}

#[test]
fn structs_encode_through_their_derived_schema() {
    let schema = Reading::beam_schema();
    let encoder = Encoder::new(Arc::clone(&schema), &table(&schema)).expect("an encoder");
    let readings: Vec<Reading> = (0..3)
        .map(|id| Reading {
            id,
            device_id: format!("d{id}"),
            payload: 0.5,
        })
        .collect();

    let encoded = encoder
        .encode(&BeamRowCodec::<Reading>::new(), &readings)
        .expect("the structs encode");

    assert_eq!(encoded.batch.expect("a batch").0.num_rows(), 3);
}

#[test]
fn a_bad_element_becomes_an_error_row_and_the_others_stay_in_the_batch() {
    let schema = device_metric();
    let encoder = Encoder::new(Arc::clone(&schema), &table(&schema)).expect("an encoder");
    let codec = SchemaRowCodec::new(Arc::clone(&schema));
    let bad = metric(&schema, 2, i64::MAX, None);
    let rows = vec![
        RowElement::Row(metric(&schema, 1, 1, None)),
        RowElement::Row(bad.clone()),
        RowElement::Row(metric(&schema, 3, 3, None)),
    ];

    let encoded = encoder.encode(&codec, &rows).expect("the rows encode");

    assert_eq!(encoded.batch.expect("a batch").1.rows, 2);
    let [error] = encoded.errors.as_slice() else {
        panic!("one error row: {:?}", encoded.errors);
    };
    assert_eq!(error.schema(), encoder.error_schema());
    assert_eq!(error.values()[0], Some(FieldValue::Row(bad)));
    let Some(FieldValue::String(reason)) = &error.values()[1] else {
        panic!("a reason");
    };
    assert!(reason.contains("out of the range"), "{reason}");
    let mut bytes = Vec::new();
    RowCoder::encode_row(error, &mut bytes).expect("the error row encodes");
}

#[test]
fn no_elements_give_no_batch() {
    let schema = device_metric();
    let encoder = Encoder::new(Arc::clone(&schema), &table(&schema)).expect("an encoder");
    let encoded = encoder
        .encode(&SchemaRowCodec::new(Arc::clone(&schema)), &[])
        .expect("nothing to encode");
    assert!(encoded.batch.is_none() && encoded.errors.is_empty());
}
