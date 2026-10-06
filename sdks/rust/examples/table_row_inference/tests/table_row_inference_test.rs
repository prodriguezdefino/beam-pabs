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

use std::fs;
use std::path::Path;

use beam::ml::{InferenceArgs, ModelHandler};
use beam::prelude::*;
use beam::testing::{TestPipeline, passert};
use beam::transforms::VecBatchConverter;
use serde_json::Value;
use table_row_inference::{Args, ClassifyRows, TableRow, build_pipeline, feature_matrix};

/// Predicts class 1 when `feature1` is above 0.5, else class 0.
#[derive(Clone)]
struct ThresholdModel;

impl ModelHandler<TableRow, i64> for ThresholdModel {
    type Model = ();
    type Batch = Vec<TableRow>;
    type Converter = VecBatchConverter<TableRow>;

    fn load_model(&self) -> Result<()> {
        Ok(())
    }

    fn run_inference(
        &self,
        batch: &Vec<TableRow>,
        _: &(),
        _: Option<&InferenceArgs>,
    ) -> Result<Vec<i64>> {
        Ok(batch
            .iter()
            .map(|row| i64::from(row.feature1 > 0.5))
            .collect())
    }

    fn get_batch_converter(&self) -> Self::Converter {
        VecBatchConverter::new()
    }
}

fn parse_args(flags: &[&str]) -> std::result::Result<Args, String> {
    let argv = std::iter::once("table_row_inference").chain(flags.iter().copied());
    beam::options::try_parse_from::<Args, _, _>(argv)
        .map(|(_, args)| args)
        .map_err(|e| e.to_string())
}

#[tokio::test]
async fn test_classify_rows_expands_lines_and_routes_bad_lines_to_failures() {
    let p = TestPipeline::new();
    let lines = [
        r#"{"id":"a","feature1":0.75,"feature2":1,"feature3":2,"feature4":3,"feature5":4}"#,
        r#"{"id":"b","feature1":0.25,"feature2":1,"feature3":2,"feature4":3,"feature5":4}"#,
        "not json",
    ]
    .map(String::from);
    let WithFailures { output, failures } = p.apply("Create", Create::new(lines.to_vec())).apply(
        "ClassifyRows",
        ClassifyRows::new(ThresholdModel, "m").with_expand_factor(2),
    );

    let prediction = |id: &str, class: f64, feature1: f64| {
        format!(
            r#"{{"row_key":"{id}","prediction":{class:?},"model_id":"m","input_feature1":{feature1:?},"input_feature2":1.0,"input_feature3":2.0,"input_feature4":3.0,"input_feature5":4.0}}"#
        )
    };
    let (a, b) = (prediction("a", 1.0, 0.75), prediction("b", 0.0, 0.25));
    passert::that("AssertPredictions", &output).contains_in_any_order([a.clone(), a, b.clone(), b]);
    passert::that("AssertFailedLines", &failures.map("Line", |f| f.input))
        .contains_in_any_order(["not json", "not json"].map(String::from));

    p.run().await.expect("pipeline must succeed");
}

#[test]
fn test_parse_accepts_crlf_extra_keys_and_integers_and_rejects_bad_rows() {
    let row: TableRow =
        "{\"id\":\"r\",\"extra\":\"x\",\"feature1\":1,\"feature2\":2,\"feature3\":3,\"feature4\":4,\"feature5\":5}\r"
            .parse()
            .expect("valid row");
    assert_eq!(row.id, "r");
    assert_eq!(row.features(), [1.0, 2.0, 3.0, 4.0, 5.0]);

    let bad = [
        r#"{"feature1":1,"feature2":2,"feature3":3,"feature4":4,"feature5":5}"#,
        r#"{"id":"r","feature1":"a","feature2":2,"feature3":3,"feature4":4,"feature5":5}"#,
        "row_0,1,2,3,4,5",
    ];
    for line in bad {
        assert!(line.parse::<TableRow>().is_err(), "{line} must be rejected");
    }
}

#[test]
fn test_feature_matrix_is_row_major() {
    let rows: Vec<TableRow> = [
        r#"{"id":"a","feature1":1,"feature2":2,"feature3":3,"feature4":4,"feature5":5}"#,
        r#"{"id":"b","feature1":6,"feature2":7,"feature3":8,"feature4":9,"feature5":10}"#,
    ]
    .iter()
    .map(|line| line.parse().expect("valid row"))
    .collect();
    assert_eq!(
        feature_matrix(&rows),
        [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]
    );
}

#[test]
fn test_flags_need_a_model_and_a_positive_expand_factor() {
    let args = parse_args(&["--model_path=m.onnx", "--input_expand_factor=100"]).expect("parse");
    assert_eq!(args.input_expand_factor, 100);
    assert!(parse_args(&[]).is_err());
    assert!(parse_args(&["--model_path=m.onnx", "--input_expand_factor=0"]).is_err());
}

/// Runs the pipeline with ONNX Runtime on `fixtures/table_rows_sample.jsonl`. Expects the
/// objects of `fixtures/python_output.jsonl`, which `export_model.py --fixture_dir` writes,
/// with only `model_id` changed.
///
/// Needs ONNX Runtime and the exported model on local disk:
/// `ORT_DYLIB_PATH=<libonnxruntime> TABLE_ROW_ONNX_MODEL=<table_row_rf.onnx>
/// ./gradlew :sdks:rust:test -Ppkg=table_row_inference -PtestArgs=--ignored`.
#[tokio::test]
#[ignore = "needs ORT_DYLIB_PATH and TABLE_ROW_ONNX_MODEL"]
async fn test_end_to_end_matches_expected_predictions() {
    let model_path = std::env::var("TABLE_ROW_ONNX_MODEL").expect("TABLE_ROW_ONNX_MODEL is set");
    let fixtures = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");
    let out_dir = std::env::temp_dir().join(format!("table_row_e2e_{}", std::process::id()));
    let output = out_dir.join("predictions");

    let (options, args) = beam::options::try_parse_from::<Args, _, _>([
        "table_row_inference".to_string(),
        format!(
            "--input={}",
            fixtures.join("table_rows_sample.jsonl").display()
        ),
        format!("--output={}", output.display()),
        format!("--model_path={model_path}"),
        "--max_batch_size=16".to_string(),
    ])
    .expect("args parse");
    build_pipeline(&options, &args)
        .run()
        .await
        .expect("pipeline must succeed");

    let parse_sorted = |text: &str| {
        let mut values: Vec<Value> = text
            .lines()
            .map(|line| serde_json::from_str(line).expect("valid JSON"))
            .collect();
        values.sort_by_key(|value| value["row_key"].to_string());
        values
    };
    let written = fs::read_to_string(format!("{}.jsonl", output.display())).expect("output");
    let mut expected =
        parse_sorted(&fs::read_to_string(fixtures.join("python_output.jsonl")).expect("fixture"));
    expected
        .iter_mut()
        .for_each(|value| value["model_id"] = Value::from(model_path.as_str()));
    assert_eq!(parse_sorted(&written), expected);
    fs::remove_dir_all(&out_dir).expect("clean up");
}
