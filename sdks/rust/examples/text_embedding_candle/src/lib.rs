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

//! Text embeddings with `sentence-transformers/all-MiniLM-L6-v2` on Hugging Face Candle.
//!
//! The pipeline trims each input line and skips empty lines. It embeds the texts in Beam
//! batches of 16 to 128 elements, with forward passes of 32 texts. It writes one JSON object
//! for each text: `{"embedding": [...], "embedding_dim": 384, "id": sha256(text),
//! "model_name": ..., "raw_text": ...}`. Texts that the model cannot embed go to a
//! dead-letter file.

use beam::ml::candle::{
    BertEmbeddingModelHandler, CandleConfig, CandleDeviceOptions, TextDocument, VectorEmbedding,
};
use beam::ml::{BatchBounds, ModelHandler, PredictionResult, RunInference};
use beam::prelude::*;
use clap::Args as ClapArgs;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// Options for this pipeline, on top of the standard Beam options.
#[derive(ClapArgs, Serialize, Deserialize, Debug, Clone)]
#[command(
    name = "text_embedding_candle",
    about = "Apache Beam text embeddings with Hugging Face Candle"
)]
pub struct Args {
    /// Text file with one text to embed for each line.
    #[arg(
        long,
        default_value = "gs://apache-beam-ml/testing/inputs/sentences_50k.txt"
    )]
    pub input: String,

    /// Prefix of the sharded `.jsonl` output files.
    #[arg(long, default_value = "/tmp/candle_embeddings")]
    pub output: String,

    /// Path of the file to write `text<TAB>error` lines to, for texts that fail to embed.
    #[arg(long, default_value = "/tmp/candle_failures.txt")]
    pub dlq_output: String,

    /// Model name written in each output object.
    #[arg(long, default_value = "sentence-transformers/all-MiniLM-L6-v2")]
    pub model_name: String,

    /// Path or URI of the safetensors model weights (`model.safetensors`).
    #[arg(long)]
    pub weights_path: String,

    /// Path or URI of the model configuration (`config.json`).
    #[arg(long)]
    pub config_path: String,

    /// Path or URI of the Hugging Face tokenizer (`tokenizer.json`).
    #[arg(long)]
    pub tokenizer_path: String,

    /// Candle device: `--device`, `--device_id`, `--allow_cpu_fallback`.
    #[command(flatten)]
    #[serde(flatten)]
    pub accelerator: CandleDeviceOptions,

    /// Minimum RunInference batch size.
    #[arg(long, default_value_t = 16)]
    pub min_batch_size: usize,

    /// Maximum RunInference batch size.
    #[arg(long, default_value_t = 128)]
    pub max_batch_size: usize,

    /// Texts for each forward pass of the model.
    #[arg(long, default_value_t = 32)]
    pub model_batch_size: usize,

    /// Maximum token sequence length (the `max_seq_length` of all-MiniLM-L6-v2).
    #[arg(long, default_value_t = 256)]
    pub max_seq_len: usize,
}

impl PipelineOptionGroup for Args {}

impl From<&Args> for CandleConfig {
    fn from(args: &Args) -> Self {
        CandleConfig::new(&args.weights_path, &args.config_path, &args.tokenizer_path)
            .with_device_options(&args.accelerator)
            .with_max_seq_len(args.max_seq_len)
            .with_inference_batch_size(args.model_batch_size)
            .with_batch_bounds(BatchBounds::new(args.min_batch_size, args.max_batch_size))
    }
}

/// The document for one input line, with the SHA-256 of its trimmed text as id, or `None`
/// for a blank line.
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

/// One output object.
#[derive(Serialize)]
struct EmbeddingRecord<'a> {
    embedding: &'a [f32],
    embedding_dim: usize,
    id: &'a str,
    model_name: &'a str,
    raw_text: &'a str,
}

/// Embeds each non-blank line and formats it as one JSON object.
///
/// The failures have one record for each text that is in a batch that the model rejects.
pub struct EmbedTexts<H> {
    handler: H,
    model_name: String,
}

impl<H: ModelHandler<TextDocument, VectorEmbedding>> EmbedTexts<H> {
    /// Embeds with `handler` and writes `model_name` in each output object.
    pub fn new(handler: H, model_name: impl Into<String>) -> Self {
        Self {
            handler,
            model_name: model_name.into(),
        }
    }
}

impl<H: ModelHandler<TextDocument, VectorEmbedding>> PTransform<PCollection<String>>
    for EmbedTexts<H>
{
    type Output = WithFailures<String, Failure<TextDocument>>;

    fn expand(&self, lines: &PCollection<String>) -> Self::Output {
        let model_name = self.model_name.clone();
        let results = lines
            .flat_map("ToDocument", |line: String| text_to_document(&line))
            .reshuffle("Reshuffle")
            .apply(
                "RunInference",
                RunInference::new(self.handler.clone()).with_exception_handling(),
            );
        // A record that cannot be serialized is a bug, not bad data, so it fails the bundle.
        let output = results.output.par_do_fn(
            "FormatJson",
            move |res: PredictionResult<TextDocument, VectorEmbedding>, out| {
                let embedding = &res.output.embedding;
                out.emit(serde_json::to_string(&EmbeddingRecord {
                    embedding,
                    embedding_dim: embedding.len(),
                    id: &res.input.doc_id,
                    model_name: &model_name,
                    raw_text: &res.input.content,
                })?)
            },
        );
        WithFailures {
            output,
            failures: results.failures,
        }
    }
}

/// Builds the embedding pipeline from [`Args`], run with `options`.
pub fn build_pipeline(options: &PipelineOptions, args: &Args) -> Pipeline {
    let p = Pipeline::create(options);
    let handler =
        BertEmbeddingModelHandler::new(CandleConfig::from(args)).with_model_id(&args.model_name);

    let WithFailures { output, failures } = p
        .apply("ReadLines", textio::Read::new(&args.input))
        .apply("EmbedTexts", EmbedTexts::new(handler, &args.model_name));
    output.apply(
        "WriteEmbeddings",
        textio::Write::new(&args.output).with_suffix(".jsonl"),
    );
    failures
        .map("FormatFailures", |f| {
            format!("{}\t{}", f.input.content, f.error)
        })
        .apply(
            "WriteFailures",
            textio::Write::new(&args.dlq_output).without_sharding(),
        );

    p
}
