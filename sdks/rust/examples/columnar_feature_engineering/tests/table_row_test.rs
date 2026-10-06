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

//! Parsing, feature extraction and output formatting of the table row inference example.
//!
//! Fixtures come from `export_model.py --fixture_dir`: `table_rows_sample.jsonl` holds
//! benchmark input lines and `python_output.jsonl` holds expected output lines.

use columnar_feature_engineering::{
    FEATURE_COLUMNS, NUM_FEATURES, TablePrediction, TableRow, TableRowInferenceArgs,
    feature_matrix, python_float_repr, to_python_json,
};

const SAMPLE_ROWS: &str = include_str!("fixtures/table_rows_sample.jsonl");
const PYTHON_OUTPUT: &str = include_str!("fixtures/python_output.jsonl");
const PYTHON_MODEL_ID: &str = "gs://apache-beam-ml/models/sklearn_table_classifier.pkl";

fn sample_rows() -> Vec<TableRow> {
    SAMPLE_ROWS
        .lines()
        .map(|line| TableRow::parse(line).expect("fixture rows are valid"))
        .collect()
}

/// Expected class labels for the sample rows, read from `python_output.jsonl`.
fn python_labels() -> Vec<i64> {
    PYTHON_OUTPUT
        .lines()
        .map(|line| {
            let value: serde_json::Value = serde_json::from_str(line).expect("valid JSON");
            value["prediction"].as_f64().expect("numeric prediction") as i64
        })
        .collect()
}

fn row(values: [f64; NUM_FEATURES]) -> TableRow {
    let [feature1, feature2, feature3, feature4, feature5] = values;
    TableRow {
        id: "row_x".to_string(),
        feature1,
        feature2,
        feature3,
        feature4,
        feature5,
    }
}

#[test]
fn parses_benchmark_line() {
    let parsed = TableRow::parse(
        r#"{"id":"row_0","feature1":0.6394267984578837,"feature2":0.025010755222666936,"feature3":0.27502931836911926,"feature4":0.22321073814882275,"feature5":0.7364712141640124}"#,
    )
    .expect("benchmark line parses");
    assert_eq!(
        parsed,
        TableRow {
            id: "row_0".to_string(),
            feature1: 0.6394267984578837,
            feature2: 0.025010755222666936,
            feature3: 0.27502931836911926,
            feature4: 0.22321073814882275,
            feature5: 0.7364712141640124,
        }
    );
}

#[test]
fn parse_accepts_crlf_and_extra_keys_and_integers() {
    // The benchmark file has CRLF line endings. Keys outside the schema are dropped, and
    // integers are converted to float.
    let parsed = TableRow::parse(
        "{\"id\":\"r\",\"extra\":\"x\",\"feature1\":1,\"feature2\":2,\"feature3\":3,\"feature4\":4,\"feature5\":5}\r",
    )
    .expect("parses");
    assert_eq!(parsed.id, "r");
    assert_eq!(parsed.features(), [1.0, 2.0, 3.0, 4.0, 5.0]);
}

#[test]
fn parse_rejects_malformed_rows() {
    let cases = [
        (
            "missing id",
            r#"{"feature1":1,"feature2":2,"feature3":3,"feature4":4,"feature5":5}"#,
        ),
        (
            "missing feature5",
            r#"{"id":"r","feature1":1,"feature2":2,"feature3":3,"feature4":4}"#,
        ),
        (
            "non-numeric feature",
            r#"{"id":"r","feature1":"a","feature2":2,"feature3":3,"feature4":4,"feature5":5}"#,
        ),
        ("not JSON", "row_0,1,2,3,4,5"),
        ("empty", ""),
    ];
    for (name, line) in cases {
        assert!(TableRow::parse(line).is_err(), "{name} must be rejected");
    }
}

#[test]
fn feature_columns_match_row_schema() {
    let value = serde_json::to_value(row([1.0, 2.0, 3.0, 4.0, 5.0])).expect("serializes");
    let keys: Vec<&str> = value
        .as_object()
        .expect("object")
        .keys()
        .map(String::as_str)
        .filter(|key| *key != "id")
        .collect();
    assert_eq!(keys, FEATURE_COLUMNS);
}

#[test]
fn features_follow_column_order_as_float32() {
    let values = [0.1, 0.2, 0.3, 0.4, 0.5];
    // Casts f64 to f32 rounding to nearest.
    assert_eq!(row(values).features(), values.map(|v| v as f32));
    assert_eq!(
        row([1.0, 2.0, 3.0, 4.0, 5.0]).features(),
        [1.0, 2.0, 3.0, 4.0, 5.0]
    );
}

#[test]
fn feature_matrix_is_row_major() {
    let rows = [
        row([1.0, 2.0, 3.0, 4.0, 5.0]),
        row([6.0, 7.0, 8.0, 9.0, 10.0]),
    ];
    assert_eq!(
        feature_matrix(&rows),
        [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]
    );
    assert!(feature_matrix(&[]).is_empty());
}

#[test]
fn float_repr_matches_python() {
    // (value, Python `repr(value)`) pairs, checked against CPython.
    let cases = [
        (0.0, "0.0"),
        (-0.0, "-0.0"),
        (1.0, "1.0"),
        (100.0, "100.0"),
        (-2.5, "-2.5"),
        (0.6394267984578837, "0.6394267984578837"),
        (0.0001, "0.0001"),
        (0.00012, "0.00012"),
        (9.234639492805563e-05, "9.234639492805563e-05"),
        (3.539821784581676e-06, "3.539821784581676e-06"),
        (1.5e-5, "1.5e-05"),
        (123456789012345.6, "123456789012345.6"),
        (1e15, "1000000000000000.0"),
        (1e16, "1e+16"),
        (1.5e300, "1.5e+300"),
        (5e-324, "5e-324"),
    ];
    for (value, expected) in cases {
        assert_eq!(python_float_repr(value), expected, "repr({value:e})");
    }
}

#[test]
fn json_separators_match_python() {
    let value = serde_json::json!({"a": [1, 2.0], "b": "x"});
    assert_eq!(
        to_python_json(&value).expect("serializes"),
        r#"{"a": [1, 2.0], "b": "x"}"#
    );
}

#[test]
fn prediction_lines_match_python_output() {
    let actual: Vec<String> = sample_rows()
        .into_iter()
        .zip(python_labels())
        .map(|(row, label)| {
            TablePrediction::new(row, label, PYTHON_MODEL_ID)
                .to_json_line()
                .expect("serializes")
        })
        .collect();
    let expected: Vec<&str> = PYTHON_OUTPUT.lines().collect();
    assert_eq!(actual, expected);
}

fn parse_args(flags: &[&str]) -> Result<TableRowInferenceArgs, String> {
    let argv = std::iter::once("columnar_feature_engineering").chain(flags.iter().copied());
    beam::options::try_parse_from::<TableRowInferenceArgs, _, _>(argv)
        .map(|(_, args)| args)
        .map_err(|e| e.to_string())
}

#[test]
fn defaults_target_the_benchmark() {
    let args = parse_args(&["--model_path=table_row_rf.onnx"]).expect("defaults parse");
    assert_eq!(
        args.input,
        "gs://apache-beam-ml/testing/inputs/table_rows_100k_benchmark.jsonl"
    );
    assert_eq!(args.model_path, "table_row_rf.onnx");
    assert_eq!(args.input_expand_factor, 1);
    let bounds = args.batch_bounds();
    assert_eq!((bounds.min_batch_size, bounds.max_batch_size), (1, 10_000));
    assert_eq!(bounds.max_batch_duration, None);
    assert_eq!(args.intra_op_threads, 1);

    assert!(parse_args(&[]).is_err(), "--model_path is required");
}

#[test]
fn benchmark_flags_parsed_and_zero_factor_rejected() {
    let args = parse_args(&[
        "--input_expand_factor=100",
        "--model_path=/models/table_row_rf.onnx",
        "--max_batch_size=512",
    ])
    .expect("benchmark flags parse");
    assert_eq!(args.input_expand_factor, 100);
    assert_eq!(args.model_path, "/models/table_row_rf.onnx");
    assert_eq!(args.batch_bounds().max_batch_size, 512);

    assert!(parse_args(&["--input_expand_factor=0", "--model_path=m.onnx"]).is_err());
}
