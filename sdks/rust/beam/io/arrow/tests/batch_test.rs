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

use std::sync::Arc;

use arrow_io::batch::{
    ArrowBeamRowBatchConverter, ArrowRecordBatch, ArrowRecordBatchCoder, ArrowRowBatchConverter,
};
use beam::coders::{Coder, CoderRegistry, Context, DefaultCoder};
use beam::schema::{BeamRow, FieldType, FieldValue, Row, Schema};
use beam::transforms::BatchConverter;

#[derive(Clone, Debug, PartialEq, BeamRow)]
struct MetricRecord {
    id: String,
    score: f64,
    count: i64,
}

#[test]
fn test_arrow_beam_row_batch_converter_roundtrip() {
    let converter = ArrowBeamRowBatchConverter::<MetricRecord>::new();

    let records = vec![
        MetricRecord {
            id: "cpu".to_string(),
            score: 0.85,
            count: 42,
        },
        MetricRecord {
            id: "mem".to_string(),
            score: 0.62,
            count: 100,
        },
    ];

    let mut buffer = converter.create_buffer();
    for rec in records.clone() {
        converter.push(&mut buffer, rec).expect("push succeeds");
    }
    assert_eq!(converter.buffer_len(&buffer), 2);

    let batch: ArrowRecordBatch = converter.finish_batch(buffer).expect("finish succeeds");
    assert_eq!(batch.num_rows(), 2);
    assert_eq!(batch.num_columns(), 3);

    let decoded: Vec<MetricRecord> = converter.explode(batch).expect("explode succeeds");
    assert_eq!(decoded, records);
}

#[test]
fn test_arrow_row_batch_converter_roundtrip() {
    let schema = Arc::new(
        Schema::builder()
            .field("tag", FieldType::string())
            .field("val", FieldType::int64())
            .build(),
    );
    let converter = ArrowRowBatchConverter::new(schema.clone());

    let rows = vec![
        Row::new(
            schema.clone(),
            vec![
                Some(FieldValue::String("alpha".into())),
                Some(FieldValue::Int64(10)),
            ],
        )
        .expect("row 1"),
        Row::new(
            schema,
            vec![
                Some(FieldValue::String("beta".into())),
                Some(FieldValue::Int64(20)),
            ],
        )
        .expect("row 2"),
    ];

    let mut buffer = converter.create_buffer();
    for row in rows.clone() {
        converter.push(&mut buffer, row).expect("push row");
    }
    assert_eq!(converter.buffer_len(&buffer), 2);

    let batch = converter.finish_batch(buffer).expect("finish batch");
    assert_eq!(batch.num_rows(), 2);

    let decoded = converter.explode(batch).expect("explode row");
    assert_eq!(decoded, rows);
}

#[test]
fn test_arrow_record_batch_coder_roundtrip() {
    let converter = ArrowBeamRowBatchConverter::<MetricRecord>::new();
    let records = vec![MetricRecord {
        id: "disk".to_string(),
        score: 0.99,
        count: 12,
    }];

    let mut buffer = converter.create_buffer();
    for rec in records {
        converter.push(&mut buffer, rec).expect("push");
    }
    let batch = converter.finish_batch(buffer).expect("finish");

    let coder = ArrowRecordBatch::coder();
    let mut bytes = Vec::new();
    coder
        .encode(&batch, &mut bytes, Context::WholeStream)
        .expect("encode succeeds");

    let mut cursor = std::io::Cursor::new(bytes);
    let decoded = coder
        .decode(&mut cursor, Context::WholeStream)
        .expect("decode succeeds");

    assert_eq!(decoded.num_rows(), batch.num_rows());
    assert_eq!(decoded.num_columns(), batch.num_columns());
}

/// Registry that records each registration and returns its position as the id.
#[derive(Default)]
struct RecordingRegistry(std::cell::RefCell<Vec<(String, Vec<String>)>>);

impl CoderRegistry for RecordingRegistry {
    fn register_coder(&self, urn: &str, component_coder_ids: Vec<String>) -> String {
        let mut seen = self.0.borrow_mut();
        seen.push((urn.to_string(), component_coder_ids));
        format!("c{}", seen.len())
    }
}

#[test]
fn test_arrow_record_batch_coder_urn_and_element_round_trip() {
    const URN: &str = "beam:coder:arrow_record_batch:v1";
    assert_eq!(ArrowRecordBatchCoder.urn(), URN);
    assert_eq!(ArrowRecordBatch::coder().urn(), URN);

    let registry = RecordingRegistry::default();
    assert_eq!(ArrowRecordBatch::register_coder(&registry), "c1");
    assert_eq!(*registry.0.borrow(), [(URN.to_string(), Vec::new())]);

    let converter = ArrowBeamRowBatchConverter::<MetricRecord>::new();
    let record = MetricRecord {
        id: "net".to_string(),
        score: 0.5,
        count: 3,
    };
    let mut buffer = converter.create_buffer();
    converter.push(&mut buffer, record.clone()).expect("push");
    let batch = converter.finish_batch(buffer).expect("finish");

    let mut bytes = Vec::new();
    batch.encode_element(&mut bytes).expect("encode");
    assert!(!bytes.is_empty());
    let decoded = ArrowRecordBatch::decode_element(&mut bytes.as_slice()).expect("decode");
    assert_eq!(converter.explode(decoded).expect("explode"), [record]);
}
