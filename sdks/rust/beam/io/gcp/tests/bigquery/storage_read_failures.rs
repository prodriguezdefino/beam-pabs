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

//! The error output of the Storage Read connector: the error rows, and the rows that
//! become error rows.

use std::sync::Arc;

use beam::coders::{DefaultCoder, RowCoder};
use beam::schema::{BeamRow, Field, FieldType, FieldValue, RowElement, Schema};
use beam_io_arrow::arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use beam_io_arrow::{BeamRowCodec, RowOutput, SchemaRowCodec};
use beam_io_gcp::bigquery::read::failures::{failure_row, read_outputs};
use beam_io_gcp::bigquery::read_failure_schema;

#[test]
fn an_error_row_decodes_with_the_error_schema() {
    let batch =
        RecordBatch::try_from_iter([("id", Arc::new(Int64Array::from(vec![7])) as ArrayRef)])
            .unwrap();
    let failure = failure_row(&batch, &beam::Error::msg("bad"), ("s", 3)).unwrap();
    let bytes = failure.encode().unwrap();
    assert_eq!(
        RowCoder::decode_row(read_failure_schema(), &mut bytes.as_slice()).unwrap(),
        failure
    );
}

#[test]
fn only_the_rows_that_fail_become_error_rows() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", FieldType::int16()),
        Field::new("name", FieldType::string()),
    ]));
    let batch = RecordBatch::try_from_iter([
        (
            "id",
            Arc::new(Int64Array::from(vec![1, 99_999, 3])) as ArrayRef,
        ),
        (
            "name",
            Arc::new(StringArray::from(vec!["a", "b", "c"])) as ArrayRef,
        ),
    ])
    .unwrap();
    for output in [RowOutput::Values, RowOutput::Encoded] {
        let codec = SchemaRowCodec::new(Arc::clone(&schema)).with_output(output);
        let outputs = read_outputs(&codec, &batch, ("stream-1", 10)).unwrap();
        assert_eq!(outputs.len(), 3);
        assert!(outputs[0].is_ok());
        assert!(outputs[2].is_ok());
        let Err(failure) = &outputs[1] else {
            panic!("the second row fails: {outputs:?}");
        };
        let failed_row = failure.get_string("failed_row").unwrap().unwrap();
        assert!(
            failed_row.contains("99999") && failed_row.contains("\"b\""),
            "{failed_row}"
        );
        assert!(!failed_row.contains('\n'));
        let message = failure.get_string("error_message").unwrap().unwrap();
        assert!(message.contains("id"), "{message}");
        assert_eq!(failure.get_string("stream").unwrap().unwrap(), "stream-1");
        assert_eq!(failure.values()[3], Some(FieldValue::Int64(11)));
    }
}

#[test]
fn a_batch_that_converts_gives_only_rows() {
    let schema = Arc::new(Schema::new(vec![Field::new("id", FieldType::int64())]));
    let batch =
        RecordBatch::try_from_iter([("id", Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef)])
            .unwrap();
    let codec = SchemaRowCodec::new(Arc::clone(&schema)).with_output(RowOutput::Encoded);
    let outputs = read_outputs(&codec, &batch, ("s", 0)).unwrap();
    assert!(
        outputs
            .iter()
            .all(|o| matches!(o, Ok(RowElement::Encoded(_))))
    );
    assert_eq!(outputs.len(), 2);
}

#[derive(Clone, Debug, PartialEq, Eq, BeamRow)]
struct NarrowRow {
    id: i16,
    name: String,
}

#[test]
fn typed_read_outputs_decode_structs_and_capture_failed_rows() {
    let batch = RecordBatch::try_from_iter([
        (
            "id",
            Arc::new(Int64Array::from(vec![4, 99_999, 8])) as ArrayRef,
        ),
        (
            "name",
            Arc::new(StringArray::from(vec!["x", "bad", "z"])) as ArrayRef,
        ),
    ])
    .unwrap();
    let outputs =
        read_outputs(&BeamRowCodec::<NarrowRow>::new(), &batch, ("stream-2", 20)).unwrap();
    assert_eq!(outputs.len(), 3);
    assert_eq!(
        outputs[0].as_ref().ok(),
        Some(&NarrowRow {
            id: 4,
            name: "x".to_string()
        })
    );
    assert_eq!(
        outputs[2].as_ref().ok(),
        Some(&NarrowRow {
            id: 8,
            name: "z".to_string()
        })
    );
    let Err(failure) = &outputs[1] else {
        panic!("expected failure row at index 1: {outputs:?}");
    };
    assert_eq!(failure.get_string("stream").unwrap(), Some("stream-2"));
    assert_eq!(failure.values()[3], Some(FieldValue::Int64(21)));
}
