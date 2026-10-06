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

//! Table row inference: a RandomForest classifier over JSONL table rows on ONNX Runtime.
//!
//! The pipeline reads rows of `{"id", "feature1".."feature5"}`, optionally repeats each line
//! `--input_expand_factor` times, and classifies the features with `RunInference`. It writes
//! one JSON object for each row to `<output>.jsonl`. Lines that are not valid rows go to a
//! dead-letter file.

use std::str::FromStr;

use beam::ml::onnx::{
    OnnxAdapter, OnnxConfig, OnnxDeviceOptions, OnnxModelHandler, SessionInputs, SessionOutputs,
    ort,
};
use beam::ml::{BatchBounds, ModelHandler, PredictionResult, RunInference};
use beam::prelude::*;
use clap::Args as ClapArgs;
use serde::{Deserialize, Serialize};

/// Number of model input features.
pub const NUM_FEATURES: usize = 5;
/// ONNX graph input: `float32[N, NUM_FEATURES]`.
pub const ONNX_INPUT: &str = "input";
/// ONNX graph output with the predicted class, `int64[N]` (skl2onnx with `zipmap=False`).
pub const ONNX_LABEL_OUTPUT: &str = "label";

/// Options for this pipeline, on top of the standard Beam options.
#[derive(ClapArgs, Serialize, Deserialize, Debug, Clone)]
#[command(
    name = "table_row_inference",
    about = "Apache Beam RandomForest table row inference with ONNX Runtime"
)]
pub struct Args {
    /// JSONL input with `id` and `feature1`..`feature5` on each line.
    #[arg(
        long,
        default_value = "gs://apache-beam-ml/testing/inputs/table_rows_100k_benchmark.jsonl"
    )]
    pub input: String,

    /// Output prefix. The predictions go to `<output>.jsonl`.
    #[arg(long, default_value = "/tmp/table_row_predictions")]
    pub output: String,

    /// Path of the file to write `line<TAB>error` lines to, for lines that are not rows.
    #[arg(long, default_value = "/tmp/table_row_failures.txt")]
    pub dlq_output: String,

    /// ONNX RandomForest that `export_model.py` exports from
    /// `gs://apache-beam-ml/models/sklearn_table_classifier.pkl`.
    #[arg(long)]
    pub model_path: String,

    /// ONNX Runtime device: `--device`, `--device_id`, `--allow_cpu_fallback`, `--dylib_path`.
    #[command(flatten)]
    #[serde(flatten)]
    pub accelerator: OnnxDeviceOptions,

    /// Number of copies of each input line (100 in the benchmark).
    #[arg(long, default_value_t = 1, value_parser = clap::value_parser!(u32).range(1..))]
    pub input_expand_factor: u32,

    /// Minimum RunInference batch size.
    #[arg(long, default_value_t = 1)]
    pub min_batch_size: usize,

    /// Maximum RunInference batch size. A batch is flushed at the end of a bundle.
    #[arg(long, default_value_t = 10_000)]
    pub max_batch_size: usize,

    /// ONNX Runtime intra-op threads for each session.
    #[arg(long, default_value_t = 1)]
    pub intra_op_threads: usize,

    /// Number of output shards. `0` writes one file without a shard suffix.
    #[arg(long, default_value_t = 0)]
    pub num_shards: u32,
}

impl PipelineOptionGroup for Args {}

impl From<&Args> for OnnxConfig {
    fn from(args: &Args) -> Self {
        OnnxConfig::new(&args.model_path)
            .with_device_options(&args.accelerator)
            .with_intra_threads(args.intra_op_threads)
            .with_batch_bounds(BatchBounds::new(args.min_batch_size, args.max_batch_size))
    }
}

/// One input row. Unknown JSON keys are ignored.
#[derive(Clone, Debug, PartialEq, Deserialize, BeamRow)]
pub struct TableRow {
    pub id: String,
    pub feature1: f64,
    pub feature2: f64,
    pub feature3: f64,
    pub feature4: f64,
    pub feature5: f64,
}

impl FromStr for TableRow {
    type Err = serde_json::Error;

    fn from_str(line: &str) -> Result<Self, Self::Err> {
        serde_json::from_str(line)
    }
}

impl TableRow {
    /// The features as the 32-bit floats that the model reads.
    pub fn features(&self) -> [f32; NUM_FEATURES] {
        [
            self.feature1,
            self.feature2,
            self.feature3,
            self.feature4,
            self.feature5,
        ]
        .map(|value| value as f32)
    }
}

/// Stacks the features of `rows` into a row-major `N x NUM_FEATURES` buffer.
pub fn feature_matrix(rows: &[TableRow]) -> Vec<f32> {
    rows.iter().flat_map(TableRow::features).collect()
}

/// One output object. The field order sets the key order of the JSON output.
#[derive(Debug, Serialize)]
struct TablePrediction<'a> {
    row_key: &'a str,
    prediction: f64,
    model_id: &'a str,
    input_feature1: f64,
    input_feature2: f64,
    input_feature3: f64,
    input_feature4: f64,
    input_feature5: f64,
}

/// Runs the RandomForest model: `input: float32[N, 5]` in, `label: int64[N]` out.
#[derive(Clone, Copy, Debug, Default)]
pub struct RandomForestAdapter;

impl OnnxAdapter<TableRow, i64> for RandomForestAdapter {
    fn prepare_inputs<'a>(&self, batch: &'a [TableRow]) -> Result<SessionInputs<'a, 'a>> {
        let tensor =
            ort::value::Tensor::from_array(([batch.len(), NUM_FEATURES], feature_matrix(batch)))?;
        Ok(ort::inputs![ONNX_INPUT => tensor].into())
    }

    fn parse_outputs(&self, batch: &[TableRow], outputs: SessionOutputs<'_>) -> Result<Vec<i64>> {
        let label = outputs
            .get(ONNX_LABEL_OUTPUT)
            .ok_or_else(|| format!("ONNX model has no '{ONNX_LABEL_OUTPUT}' output"))?;
        let (_, labels) = label.try_extract_tensor::<i64>()?;
        match labels.len() == batch.len() {
            true => Ok(labels.to_vec()),
            false => Err(format!(
                "ONNX model returned {} labels for {} rows",
                labels.len(),
                batch.len()
            )
            .into()),
        }
    }
}

/// Classifies JSONL table rows into JSON prediction lines.
///
/// The failures have one record for each line that is not a valid row.
pub struct ClassifyRows<H> {
    handler: H,
    model_id: String,
    expand_factor: u32,
}

impl<H: ModelHandler<TableRow, i64>> ClassifyRows<H> {
    /// Classifies with `handler` and writes `model_id` in each output object.
    pub fn new(handler: H, model_id: impl Into<String>) -> Self {
        Self {
            handler,
            model_id: model_id.into(),
            expand_factor: 1,
        }
    }

    /// Repeats each input line `factor` times before it is parsed.
    pub fn with_expand_factor(mut self, factor: u32) -> Self {
        self.expand_factor = factor;
        self
    }
}

impl<H: ModelHandler<TableRow, i64>> PTransform<PCollection<String>> for ClassifyRows<H> {
    type Output = WithFailures<String, Failure<String>>;

    fn expand(&self, lines: &PCollection<String>) -> Self::Output {
        let lines = lines.reshuffle("Reshuffle");
        let lines = match self.expand_factor {
            1 => lines,
            factor => lines.flat_map("ExpandInput", move |line: String| {
                std::iter::repeat_n(line, factor as usize)
            }),
        };
        let rows = lines.try_map("ParseRow", |line: &String| line.parse::<TableRow>());
        let model_id = self.model_id.clone();
        // A record that cannot be serialized is a bug, not bad data, so it fails the bundle.
        let output = rows
            .output
            .apply("RunInference", RunInference::new(self.handler.clone()))
            .par_do_fn(
                "FormatJson",
                move |res: PredictionResult<TableRow, i64>, out| {
                    let row = &res.input;
                    out.emit(serde_json::to_string(&TablePrediction {
                        row_key: &row.id,
                        prediction: res.output as f64,
                        model_id: &model_id,
                        input_feature1: row.feature1,
                        input_feature2: row.feature2,
                        input_feature3: row.feature3,
                        input_feature4: row.feature4,
                        input_feature5: row.feature5,
                    })?)
                },
            );
        WithFailures {
            output,
            failures: rows.failures,
        }
    }
}

/// Builds the table row inference pipeline from [`Args`], run with `options`.
pub fn build_pipeline(options: &PipelineOptions, args: &Args) -> Pipeline {
    let p = Pipeline::create(options);
    let handler = OnnxModelHandler::new(OnnxConfig::from(args), RandomForestAdapter)
        .with_model_id(&args.model_path);
    let sink = textio::Write::new(&args.output).with_suffix(".jsonl");
    let sink = match args.num_shards {
        0 => sink.without_sharding(),
        n => sink.with_num_shards(n),
    };

    let WithFailures { output, failures } =
        p.apply("ReadLines", textio::Read::new(&args.input)).apply(
            "ClassifyRows",
            ClassifyRows::new(handler, &args.model_path)
                .with_expand_factor(args.input_expand_factor),
        );
    output.apply("WritePredictions", sink);
    failures
        .map("FormatFailures", |f| format!("{}\t{}", f.input, f.error))
        .apply(
            "WriteFailures",
            textio::Write::new(&args.dlq_output).without_sharding(),
        );

    p
}
