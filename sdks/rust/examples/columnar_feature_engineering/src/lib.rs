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

//! Table row inference: a RandomForest classifier over JSONL table rows, run with
//! ONNX Runtime through `RunInference`.
//!
//! Reads `table_rows_100k_benchmark.jsonl`, optionally repeats lines by
//! `--input_expand_factor`, classifies `feature1..feature5` with a RandomForest exported to
//! ONNX by `export_model.py`, and writes one JSON object per row to `<output>.jsonl`. The
//! lines must be byte-identical to the Python benchmark `table_row_inference.py` (keys,
//! key order, number formatting); the tests compare them with `python_output.jsonl`.

use std::io;

use beam::ml::onnx::{
    OnnxAdapter, OnnxConfig, OnnxDeviceOptions, OnnxModelHandler, SessionInputs, SessionOutputs,
    ort,
};
use beam::ml::{BatchBounds, PredictionResult, RunInference};
use beam::prelude::*;
use clap::Args;
use serde::{Deserialize, Serialize};
use serde_json::ser::Formatter;

/// The benchmark input: 100k rows of `{"id", "feature1".."feature5"}`.
pub const DEFAULT_INPUT: &str =
    "gs://apache-beam-ml/testing/inputs/table_rows_100k_benchmark.jsonl";
/// Output prefix; the pipeline writes the single file `<output>.jsonl`.
pub const DEFAULT_OUTPUT: &str = "/tmp/table_row_predictions";
/// Suffix appended to `--output`.
pub const OUTPUT_SUFFIX: &str = ".jsonl";

/// Model feature columns, in the order of the benchmark's `--feature_columns`.
pub const FEATURE_COLUMNS: [&str; 5] = ["feature1", "feature2", "feature3", "feature4", "feature5"];
/// Number of model input features.
pub const NUM_FEATURES: usize = FEATURE_COLUMNS.len();
/// ONNX graph input: `float32[N, NUM_FEATURES]`.
pub const ONNX_INPUT: &str = "input";
/// ONNX graph output holding the predicted class, `int64[N]` (skl2onnx with `zipmap=False`).
pub const ONNX_LABEL_OUTPUT: &str = "label";

/// One input line of the benchmark JSONL.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, BeamRow)]
pub struct TableRow {
    /// Row key, e.g. `row_0`.
    pub id: String,
    pub feature1: f64,
    pub feature2: f64,
    pub feature3: f64,
    pub feature4: f64,
    pub feature5: f64,
}

impl TableRow {
    /// Parses one JSONL line, ignoring unknown keys.
    pub fn parse(line: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(line)
    }

    /// Features in [`FEATURE_COLUMNS`] order as 32-bit floats.
    pub fn features(&self) -> [f32; NUM_FEATURES] {
        self.feature_values().map(|value| value as f32)
    }

    fn feature_values(&self) -> [f64; NUM_FEATURES] {
        [
            self.feature1,
            self.feature2,
            self.feature3,
            self.feature4,
            self.feature5,
        ]
    }
}

/// Stacks the rows' features into a row-major `N x NUM_FEATURES` buffer.
pub fn feature_matrix(rows: &[TableRow]) -> Vec<f32> {
    rows.iter().flat_map(TableRow::features).collect()
}

/// One output line. The field order sets the JSON key order of the output.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct TablePrediction {
    pub row_key: String,
    /// Predicted class as a float.
    pub prediction: f64,
    pub model_id: String,
    pub input_feature1: f64,
    pub input_feature2: f64,
    pub input_feature3: f64,
    pub input_feature4: f64,
    pub input_feature5: f64,
}

impl TablePrediction {
    /// Combines an input row with its predicted class label.
    pub fn new(row: TableRow, label: i64, model_id: &str) -> Self {
        Self {
            prediction: label as f64,
            model_id: model_id.to_string(),
            input_feature1: row.feature1,
            input_feature2: row.feature2,
            input_feature3: row.feature3,
            input_feature4: row.feature4,
            input_feature5: row.feature5,
            row_key: row.id,
        }
    }

    /// Serializes the prediction to a JSON line.
    pub fn to_json_line(&self) -> Result<String, serde_json::Error> {
        to_python_json(self)
    }
}

/// Serializes `value` byte-for-byte as Python `json.dumps` with default arguments, for
/// ASCII data: `", "` and `": "` separators and [`python_float_repr`] floats.
pub fn to_python_json<T: Serialize>(value: &T) -> Result<String, serde_json::Error> {
    let mut buffer = Vec::new();
    value.serialize(&mut serde_json::Serializer::with_formatter(
        &mut buffer,
        PythonJsonFormatter,
    ))?;
    String::from_utf8(buffer).map_err(|e| serde::ser::Error::custom(e.to_string()))
}

/// Formats a finite `f64` as Python `repr(float)`: shortest round-trip digits, scientific
/// notation when the decimal exponent is below -4 or at least 16.
pub fn python_float_repr(value: f64) -> String {
    // `{:e}` yields the shortest round-trip digits, e.g. `-1.25e-5` or `1e0`.
    let scientific = format!("{value:e}");
    let (mantissa, exponent) = scientific
        .split_once('e')
        .map(|(m, e)| (m, e.parse::<i32>().unwrap_or(0)))
        .unwrap_or((scientific.as_str(), 0));
    let (sign, mantissa) = mantissa
        .strip_prefix('-')
        .map_or(("", mantissa), |m| ("-", m));
    let digits: String = mantissa.chars().filter(char::is_ascii_digit).collect();
    let body = if (-4..16).contains(&exponent) {
        fixed_notation(&digits, exponent)
    } else {
        let (lead, rest) = digits.split_at(1);
        let fraction = if rest.is_empty() {
            String::new()
        } else {
            format!(".{rest}")
        };
        let exp_sign = if exponent < 0 { '-' } else { '+' };
        format!("{lead}{fraction}e{exp_sign}{:02}", exponent.unsigned_abs())
    };
    format!("{sign}{body}")
}

fn fixed_notation(digits: &str, exponent: i32) -> String {
    match usize::try_from(exponent) {
        Ok(point) => {
            let integer_len = point + 1;
            if digits.len() > integer_len {
                format!("{}.{}", &digits[..integer_len], &digits[integer_len..])
            } else {
                format!("{digits:0<integer_len$}.0")
            }
        }
        Err(_) => {
            let zeros = "0".repeat(exponent.unsigned_abs() as usize - 1);
            format!("0.{zeros}{digits}")
        }
    }
}

/// [`Formatter`] producing `", "` and `": "` separators with shortest round-trip floats.
#[derive(Clone, Copy, Debug, Default)]
pub struct PythonJsonFormatter;

impl Formatter for PythonJsonFormatter {
    fn write_f64<W: ?Sized + io::Write>(&mut self, writer: &mut W, value: f64) -> io::Result<()> {
        writer.write_all(python_float_repr(value).as_bytes())
    }

    fn begin_array_value<W: ?Sized + io::Write>(
        &mut self,
        writer: &mut W,
        first: bool,
    ) -> io::Result<()> {
        if first {
            Ok(())
        } else {
            writer.write_all(b", ")
        }
    }

    fn begin_object_key<W: ?Sized + io::Write>(
        &mut self,
        writer: &mut W,
        first: bool,
    ) -> io::Result<()> {
        if first {
            Ok(())
        } else {
            writer.write_all(b", ")
        }
    }

    fn begin_object_value<W: ?Sized + io::Write>(&mut self, writer: &mut W) -> io::Result<()> {
        writer.write_all(b": ")
    }
}

/// Runs the RandomForest model: `input: float32[N, 5]` in, `label: int64[N]` out.
#[derive(Clone, Copy, Debug, Default)]
pub struct RandomForestOnnxAdapter;

impl OnnxAdapter<TableRow, i64> for RandomForestOnnxAdapter {
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
        if labels.len() != batch.len() {
            return Err(format!(
                "ONNX model returned {} labels for {} rows",
                labels.len(),
                batch.len()
            )
            .into());
        }
        Ok(labels.to_vec())
    }
}

/// Command line arguments for the table row inference pipeline.
#[derive(Args, Serialize, Deserialize, Debug, Clone)]
#[command(
    name = "columnar_feature_engineering",
    about = "RandomForest table row inference with ONNX Runtime (port of Python table_row_inference)"
)]
pub struct TableRowInferenceArgs {
    /// JSONL input with `id` and `feature1`..`feature5` per line (local path or `gs://...`).
    #[arg(long, default_value = DEFAULT_INPUT)]
    pub input: String,

    /// Output prefix; predictions are written to the single file `<output>.jsonl`.
    #[arg(long, default_value = DEFAULT_OUTPUT)]
    pub output: String,

    /// ONNX RandomForest that `export_model.py` exports from
    /// `gs://apache-beam-ml/models/sklearn_table_classifier.pkl` (local path or `gs://...`;
    /// required).
    #[arg(long)]
    pub model_path: String,

    /// ONNX Runtime device: `--device`, `--device_id`, `--allow_cpu_fallback`, `--dylib_path`.
    #[command(flatten)]
    #[serde(flatten)]
    pub accelerator: OnnxDeviceOptions,

    /// Repeats each input line this many times before parsing (100 in the benchmark).
    #[arg(long, default_value_t = 1, value_parser = clap::value_parser!(u32).range(1..))]
    pub input_expand_factor: u32,

    /// Smallest inference batch.
    #[arg(long, default_value_t = 1)]
    pub min_batch_size: usize,

    /// Largest inference batch.
    #[arg(long, default_value_t = 10_000)]
    pub max_batch_size: usize,

    /// ONNX Runtime intra-op threads per session.
    #[arg(long, default_value_t = 1)]
    pub intra_op_threads: usize,

    /// Optional fixed number of output shards (0 means a single unsharded file).
    #[arg(long, default_value_t = 0)]
    pub num_shards: u32,
}

impl PipelineOptionGroup for TableRowInferenceArgs {}

impl TableRowInferenceArgs {
    /// Batch bounds for `RunInference`; batches are flushed at bundle end, with no timer.
    pub fn batch_bounds(&self) -> BatchBounds {
        BatchBounds::new(self.min_batch_size, self.max_batch_size)
    }

    /// ONNX Runtime configuration for [`Self::model_path`].
    pub fn onnx_config(&self) -> OnnxConfig {
        OnnxConfig::new(&self.model_path)
            .with_device_options(&self.accelerator)
            .with_intra_threads(self.intra_op_threads)
            .with_batch_bounds(self.batch_bounds())
    }

    /// The RandomForest model handler.
    pub fn model_handler(&self) -> OnnxModelHandler<TableRow, i64, RandomForestOnnxAdapter> {
        OnnxModelHandler::new(self.onnx_config(), RandomForestOnnxAdapter)
            .with_model_id(&self.model_path)
    }
}

/// Repeats each line `factor` times. Returns `lines` unchanged when `factor` is 1.
pub fn expand_lines(lines: &PCollection<String>, factor: u32) -> PCollection<String> {
    if factor == 1 {
        return lines.clone();
    }
    let copies = factor as usize;
    lines.flat_map("ExpandInput", move |line: String| {
        std::iter::repeat_n(line, copies)
    })
}

/// Parses JSONL lines into [`TableRow`]s. A malformed line fails the pipeline.
pub fn parse_table_rows(lines: &PCollection<String>) -> PCollection<TableRow> {
    lines.par_do_fn("ParseToTableRows", |line: String, out| {
        let row = TableRow::parse(&line).map_err(|e| format!("Invalid table row {line:?}: {e}"))?;
        out.emit(row)
    })
}

/// Formats predictions as JSON lines tagged with `model_id`.
pub fn format_predictions(
    predictions: &PCollection<PredictionResult<TableRow, i64>>,
    model_id: &str,
) -> PCollection<String> {
    let model_id = model_id.to_string();
    predictions.par_do_fn(
        "FormatOutput",
        move |result: PredictionResult<TableRow, i64>, out| {
            let line =
                TablePrediction::new(result.input, result.output, &model_id).to_json_line()?;
            out.emit(line)
        },
    )
}

/// Builds the complete pipeline from [`TableRowInferenceArgs`].
pub fn build_pipeline(options: &PipelineOptions, args: &TableRowInferenceArgs) -> Pipeline {
    let p = Pipeline::create(options);

    let sink = match args.num_shards {
        0 => textio::Write::new("WriteLines", &args.output)
            .with_suffix(OUTPUT_SUFFIX)
            .without_sharding(),
        n => textio::Write::new("WriteLines", &args.output)
            .with_suffix(OUTPUT_SUFFIX)
            .with_num_shards(n),
    };
    let lines = p
        .apply(textio::Read::new("ReadLines", &args.input))
        .reshuffle("ReshuffleInputs");
    let expanded = expand_lines(&lines, args.input_expand_factor);
    let rows = parse_table_rows(&expanded);
    let predictions = rows.apply(RunInference::new("RunInference", args.model_handler()));
    format_predictions(&predictions, &args.model_path).apply(sink);

    p
}
