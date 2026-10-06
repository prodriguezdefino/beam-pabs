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

//! Text Embedding pipeline logic demonstrating hardware-accelerated transformer inference
//! using Hugging Face Candle and Apache Beam Rust.
//!
//! Embeds text with `sentence-transformers/all-MiniLM-L6-v2`:
//!
//! * Input: text lines (default: `sentences_50k.txt`); each line is stripped and empty lines
//!   are skipped.
//! * Model: sentence-transformers semantics — batch-longest padding, truncation to 256
//!   tokens, attention-masked forward pass, mean pooling over real tokens, L2 normalization.
//! * Batching: Beam batches of 16..128 elements, embedded in length-sorted chunks of 32.
//! * Output: sharded `*.jsonl` files with one object per line, keys sorted:
//!   `{"embedding": [...], "embedding_dim": 384, "id": sha256(text), "model_name": ...,
//!   "raw_text": ...}`. Serialization matches Python `json.dumps(..., sort_keys=True)`:
//!   `", "`/`": "` separators, `ensure_ascii` escapes and float `repr` formatting. Equal
//!   embeddings produce byte-identical lines. Embedding values differ from the PyTorch model
//!   only by floating-point rounding.

use std::io;

use beam::ml::candle::{
    BertEmbeddingModelHandler, CandleConfig, CandleDeviceOptions, TextDocument, VectorEmbedding,
};
use beam::ml::{BatchBounds, PredictionResult, RunInference};
use beam::prelude::*;
use clap::Args;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub use beam::ml::candle;

/// Benchmark input (49,999 lines).
pub const DEFAULT_INPUT_SENTENCES: &str = "gs://apache-beam-ml/testing/inputs/sentences_50k.txt";
pub const DEFAULT_OUTPUT: &str = "/tmp/candle_embeddings";
pub const DEFAULT_MODEL_NAME: &str = "sentence-transformers/all-MiniLM-L6-v2";
/// Output file suffix.
pub const OUTPUT_SUFFIX: &str = ".jsonl";

/// Command line arguments for the Text Embedding Candle example pipeline.
#[derive(Args, Serialize, Deserialize, Debug, Clone)]
#[command(
    name = "text_embedding_candle",
    about = "Apache Beam Rust Text Embedding with Hugging Face Candle Example"
)]
pub struct EmbeddingArgs {
    /// Path to text file containing sentences to embed (local path or `gs://...`).
    #[arg(long, default_value = DEFAULT_INPUT_SENTENCES)]
    pub input: String,

    /// Output prefix for sharded JSONL embedding results (local path or `gs://...`).
    #[arg(long, default_value = DEFAULT_OUTPUT)]
    pub output: String,

    /// Model name recorded in each output row.
    #[arg(long, default_value = DEFAULT_MODEL_NAME)]
    pub model_name: String,

    /// Path or URI of the safetensors model weights (`model.safetensors`; required).
    #[arg(long)]
    pub weights_path: String,

    /// Path or URI of the model architecture JSON (`config.json`; required).
    #[arg(long)]
    pub config_path: String,

    /// Path or URI of the Hugging Face tokenizer JSON (`tokenizer.json`; required).
    #[arg(long)]
    pub tokenizer_path: String,

    /// Candle device: `--device`, `--device_id`, `--allow_cpu_fallback`.
    #[command(flatten)]
    #[serde(flatten)]
    pub accelerator: CandleDeviceOptions,

    /// Minimum Beam inference batch size.
    #[arg(long, default_value_t = 16)]
    pub min_batch_size: usize,

    /// Maximum Beam inference batch size.
    #[arg(long, default_value_t = 128)]
    pub max_batch_size: usize,

    /// Texts per forward pass (`SentenceTransformer.encode` batch size).
    #[arg(long, default_value_t = 32)]
    pub model_batch_size: usize,

    /// Maximum token sequence length (all-MiniLM-L6-v2 `max_seq_length`).
    #[arg(long, default_value_t = 256)]
    pub max_seq_len: usize,
}

impl PipelineOptionGroup for EmbeddingArgs {}

/// Builds the embedding inference pipeline from input TextDocuments.
pub fn build_embedding_pipeline(
    input: &PCollection<TextDocument>,
    handler: BertEmbeddingModelHandler,
) -> PCollection<PredictionResult<TextDocument, VectorEmbedding>> {
    input.apply(RunInference::new("CandleBertEmbedding", handler))
}

/// Converts an input line to a document keyed by the SHA-256 of its stripped text, or `None`
/// for blank lines.
pub fn text_to_document(line: &str) -> Option<TextDocument> {
    let text = line.trim();
    (!text.is_empty()).then(|| TextDocument {
        doc_id: Sha256::digest(text.as_bytes())
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect(),
        content: text.to_string(),
    })
}

/// One output row, with fields in sorted key order.
#[derive(Serialize)]
struct EmbeddingRecord<'a> {
    embedding: Vec<f64>,
    embedding_dim: usize,
    id: &'a str,
    model_name: &'a str,
    raw_text: &'a str,
}

/// Formats a prediction as a JSON line with sorted keys.
pub fn format_embedding_record(
    model_name: &str,
    prediction: &PredictionResult<TextDocument, VectorEmbedding>,
) -> Result<String, serde_json::Error> {
    let record = EmbeddingRecord {
        embedding: prediction
            .output
            .embedding
            .iter()
            .map(|&x| f64::from(x))
            .collect(),
        embedding_dim: prediction.output.embedding.len(),
        id: &prediction.input.doc_id,
        model_name,
        raw_text: &prediction.input.content,
    };
    let mut out = Vec::new();
    record.serialize(&mut serde_json::Serializer::with_formatter(
        &mut out,
        PythonJsonFormatter,
    ))?;
    String::from_utf8(out).map_err(|e| serde_json::Error::io(io::Error::other(e)))
}

/// `serde_json` formatter that writes the output of Python `json.dumps` with default arguments.
struct PythonJsonFormatter;

impl serde_json::ser::Formatter for PythonJsonFormatter {
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

    /// Python's `repr(float)`: shortest round-trip digits, positional for decimal exponents in
    /// `[-4, 16)` (with a trailing `.0` for integral values), otherwise `d.ddde±XX`.
    fn write_f64<W: ?Sized + io::Write>(&mut self, writer: &mut W, value: f64) -> io::Result<()> {
        if !value.is_finite() {
            return Err(io::Error::other(format!("non-finite float {value}")));
        }
        let scientific = format!("{value:e}");
        let (mantissa, exponent) = scientific
            .split_once('e')
            .ok_or_else(|| io::Error::other(format!("unexpected float format {scientific}")))?;
        let exponent: i32 = exponent.parse().map_err(io::Error::other)?;
        let python = match exponent {
            -4..=15 => match format!("{value}") {
                positional if positional.contains('.') => positional,
                integral => format!("{integral}.0"),
            },
            _ => format!(
                "{mantissa}e{}{:02}",
                if exponent < 0 { '-' } else { '+' },
                exponent.abs()
            ),
        };
        writer.write_all(python.as_bytes())
    }

    /// Python's `ensure_ascii=True`: non-ASCII characters become `\uXXXX` (UTF-16) escapes.
    fn write_string_fragment<W: ?Sized + io::Write>(
        &mut self,
        writer: &mut W,
        fragment: &str,
    ) -> io::Result<()> {
        fragment.chars().try_for_each(|c| match c.is_ascii() {
            true => writer.write_all(&[c as u8]),
            false => c
                .encode_utf16(&mut [0u16; 2])
                .iter()
                .try_for_each(|unit| write!(writer, "\\u{unit:04x}")),
        })
    }
}

/// Builds the complete Text Embedding pipeline reading from input and writing to output.
pub fn build_pipeline(options: &PipelineOptions, args: &EmbeddingArgs) -> Pipeline {
    let p = Pipeline::create(options);
    let handler =
        BertEmbeddingModelHandler::new(create_candle_config(args)).with_model_id(&args.model_name);
    let model_name = args.model_name.clone();

    p.apply(textio::Read::new("ReadLines", &args.input))
        .flat_map("ToEmbeddingRecords", |line: String| text_to_document(&line))
        .reshuffle("ReshuffleDocuments")
        .apply(RunInference::new("CandleBertEmbedding", handler))
        // A record that cannot be serialized is a bug, not bad data: fail the bundle.
        .par_do_fn("FormatOutput", move |pred, out| {
            out.emit(format_embedding_record(&model_name, &pred)?)
        })
        .apply(textio::Write::new("WriteLines", &args.output).with_suffix(OUTPUT_SUFFIX));

    p
}

/// Creates a [`CandleConfig`] from command-line arguments.
pub fn create_candle_config(args: &EmbeddingArgs) -> CandleConfig {
    CandleConfig::new(&args.weights_path, &args.config_path, &args.tokenizer_path)
        .with_device_options(&args.accelerator)
        .with_max_seq_len(args.max_seq_len)
        .with_inference_batch_size(args.model_batch_size)
        .with_batch_bounds(BatchBounds::new(args.min_batch_size, args.max_batch_size))
}
