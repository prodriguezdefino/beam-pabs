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

//! Pipeline tests for the table row inference example on the Prism runner.

use std::fs;
use std::path::{Path, PathBuf};

use beam::prelude::*;
use beam::testing::{TestPipeline, passert};
use columnar_feature_engineering::{
    OUTPUT_SUFFIX, TableRow, TableRowInferenceArgs, build_pipeline, expand_lines, parse_table_rows,
};

const SAMPLE_ROWS: &str = include_str!("fixtures/table_rows_sample.jsonl");
const PYTHON_OUTPUT: &str = include_str!("fixtures/python_output.jsonl");
const PYTHON_MODEL_ID: &str = "gs://apache-beam-ml/models/sklearn_table_classifier.pkl";

fn sample_lines() -> Vec<String> {
    SAMPLE_ROWS.lines().map(String::from).collect()
}

fn sample_rows() -> Vec<TableRow> {
    SAMPLE_ROWS
        .lines()
        .map(|line| TableRow::parse(line).expect("fixture rows are valid"))
        .collect()
}

fn fixture_path(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(name)
}

#[tokio::test]
async fn parses_and_expands_lines() {
    let p = TestPipeline::new();
    let lines = p.apply(Create::new("Create", sample_lines()));
    let rows = parse_table_rows(&expand_lines(&lines, 3));

    let expected: Vec<TableRow> = sample_rows()
        .into_iter()
        .flat_map(|row| std::iter::repeat_n(row, 3))
        .collect();
    passert::that("AssertRows", &rows).contains_in_any_order(expected);

    p.run().await.expect("pipeline succeeds");
}

#[tokio::test]
async fn malformed_line_fails_the_pipeline() {
    let p = TestPipeline::new();
    let lines = p.apply(Create::new("Create", vec!["not json".to_string()]));
    let _rows = parse_table_rows(&lines);

    assert!(p.run().await.is_err());
}

/// Runs the full pipeline with ONNX Runtime on the fixture rows. Expects the lines of
/// `python_output.jsonl` with only `model_id` changed.
///
/// Needs ONNX Runtime and the exported model on local disk:
/// `ORT_DYLIB_PATH=<libonnxruntime> TABLE_ROW_ONNX_MODEL=<table_row_rf.onnx>
/// cargo test -p columnar_feature_engineering -- --ignored`.
#[tokio::test]
#[ignore = "needs ORT_DYLIB_PATH and TABLE_ROW_ONNX_MODEL"]
async fn end_to_end_matches_python_output() {
    let model_path = std::env::var("TABLE_ROW_ONNX_MODEL")
        .expect("TABLE_ROW_ONNX_MODEL must name the exported ONNX model");
    let out_dir = std::env::temp_dir().join(format!("table_row_e2e_{}", std::process::id()));
    fs::create_dir_all(&out_dir).expect("create output dir");
    let output = out_dir.join("predictions");

    let (options, args) = beam::options::try_parse_from::<TableRowInferenceArgs, _, _>([
        "columnar_feature_engineering".to_string(),
        format!(
            "--input={}",
            fixture_path("table_rows_sample.jsonl").display()
        ),
        format!("--output={}", output.display()),
        format!("--model_path={model_path}"),
        "--max_batch_size=16".to_string(),
    ])
    .expect("args parse");
    let result = build_pipeline(&options, &args)
        .run()
        .await
        .expect("pipeline succeeds");
    assert_eq!(result.state, "DONE");

    let written = fs::read_to_string(format!("{}{OUTPUT_SUFFIX}", output.display()))
        .expect("single output file");
    let mut actual: Vec<&str> = written.lines().collect();
    actual.sort_unstable();

    let python_id = serde_json::to_string(PYTHON_MODEL_ID).expect("json string");
    let rust_id = serde_json::to_string(&model_path).expect("json string");
    let mut expected: Vec<String> = PYTHON_OUTPUT
        .lines()
        .map(|line| line.replace(&python_id, &rust_id))
        .collect();
    expected.sort_unstable();

    assert_eq!(actual, expected);
    fs::remove_dir_all(&out_dir).expect("clean up");
}
