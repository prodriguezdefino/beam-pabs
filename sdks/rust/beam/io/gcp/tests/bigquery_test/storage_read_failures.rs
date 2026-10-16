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

//! The error output of the native Storage Read connector: the error rows, and the rows that
//! become error rows.

use std::sync::Arc;

use arrow_io::RowOutput;
use arrow_io::arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use beam::coders::{DefaultCoder, RowCoder};
use beam::schema::{Field, FieldType, FieldValue, RowElement, Schema};
use gcp::bigquery::storage::read::failures::{error_row, error_schema, read_outputs};

#[test]
fn an_error_row_decodes_with_the_error_schema() {
    let batch =
        RecordBatch::try_from_iter([("id", Arc::new(Int64Array::from(vec![7])) as ArrayRef)])
            .unwrap();
    let failure = error_row(&batch, &beam::Error::msg("bad"), ("s", 3)).unwrap();
    let bytes = failure.encode().unwrap();
    assert_eq!(
        RowCoder::decode_row(error_schema(), &mut bytes.as_slice()).unwrap(),
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
        let outputs = read_outputs(&batch, &schema, output, ("stream-1", 10)).unwrap();
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
    let outputs = read_outputs(&batch, &schema, RowOutput::Encoded, ("s", 0)).unwrap();
    assert!(
        outputs
            .iter()
            .all(|o| matches!(o, Ok(RowElement::Encoded(_))))
    );
    assert_eq!(outputs.len(), 2);
}
