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

//! Integration tests for Text Embedding Candle example pipeline.

use beam::ml::candle::{CandleDevice, TextDocument, VectorEmbedding};
use beam::ml::handler::{BatchBounds, InferenceArgs, ModelHandler};
use beam::ml::{PredictionResult, RunInference};
use beam::prelude::*;
use beam::testing::{TestPipeline, passert};
use beam::transforms::VecBatchConverter;
use text_embedding_candle::{
    DEFAULT_INPUT_SENTENCES, EmbeddingArgs, create_candle_config, format_embedding_record,
    text_to_document,
};

fn sample_sentences() -> Vec<TextDocument> {
    [
        "The quick brown fox jumps over the lazy dog.",
        "Apache Beam enables unified batch and streaming data processing at scale.",
        "Machine learning model inference with micro-batching maximizes accelerator throughput.",
        "Pure Rust transformer execution eliminates heavy C++ runtime dependencies.",
    ]
    .into_iter()
    .filter_map(text_to_document)
    .collect()
}

#[test]
fn test_candle_config_from_flags_and_defaults() {
    let raw_args = vec![
        "text_embedding_candle",
        "--weights_path=gs://bucket/weights.safetensors",
        "--config_path=cfg.json",
        "--tokenizer_path=tok.json",
        "--device=cuda",
        "--min_batch_size=8",
        "--max_batch_size=64",
        "--model_batch_size=16",
        "--max_seq_len=128",
        "--allow_cpu_fallback=true",
    ];

    let (_, args) = beam::options::parse_from::<EmbeddingArgs, _, _>(raw_args);
    let config = create_candle_config(&args);

    assert_eq!(config.weights_path, "gs://bucket/weights.safetensors");
    assert_eq!(config.config_path, "cfg.json");
    assert_eq!(config.tokenizer_path, "tok.json");
    assert_eq!(config.batch_bounds, BatchBounds::new(8, 64));
    assert_eq!(config.inference_batch_size, 16);
    assert_eq!(config.max_seq_len, 128);
    assert!(config.allow_cpu_fallback);
    assert_eq!(config.device, CandleDevice::Cuda { device_id: 0 });

    let (_, default_args) = beam::options::parse_from::<EmbeddingArgs, _, _>([
        "text_embedding_candle",
        "--weights_path=model.safetensors",
        "--config_path=config.json",
        "--tokenizer_path=tokenizer.json",
    ]);
    let default_config = create_candle_config(&default_args);

    assert_eq!(default_args.input, DEFAULT_INPUT_SENTENCES);
    assert_eq!(
        default_args.model_name,
        "sentence-transformers/all-MiniLM-L6-v2"
    );
    assert_eq!(default_config.batch_bounds, BatchBounds::new(16, 128));
    assert_eq!(default_config.inference_batch_size, 32);
    assert_eq!(default_config.max_seq_len, 256);
    assert!(!default_config.allow_cpu_fallback);
    assert_eq!(default_config.device, CandleDevice::Cpu);
}

#[test]
fn test_text_to_document_strips_and_hashes_like_python() {
    // hashlib.sha256('hello world'.encode('utf-8')).hexdigest()
    let doc = text_to_document("  hello world \n").expect("non-empty line");
    assert_eq!(doc.content, "hello world");
    assert_eq!(
        doc.doc_id,
        "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
    );
    assert_eq!(text_to_document("   \t"), None);
}

#[test]
fn test_output_record_matches_python_json_dumps() {
    let prediction = PredictionResult::new(
        TextDocument {
            doc_id: "abc".to_string(),
            content: "caf\u{e9} \u{1F600} \"q\"".to_string(),
        },
        VectorEmbedding {
            doc_id: "abc".to_string(),
            embedding: vec![0.1, -1.0, 1.5e-5, 0.0],
        },
    );

    let line = format_embedding_record("m", &prediction).expect("formatting");

    // json.dumps({...}, sort_keys=True) with the float32 values widened to float64.
    assert_eq!(
        line,
        r#"{"embedding": [0.10000000149011612, -1.0, 1.4999999621068127e-05, 0.0], "embedding_dim": 4, "id": "abc", "model_name": "m", "raw_text": "caf\u00e9 \ud83d\ude00 \"q\""}"#
    );
}

/// Mock embedding handler for verifying end-to-end pipeline wiring on Prism
/// without requiring 90MB pre-trained weights in test containers.
#[derive(Clone, Debug, Default)]
struct MockEmbeddingModelHandler;

impl ModelHandler<TextDocument, VectorEmbedding> for MockEmbeddingModelHandler {
    type Model = ();
    type Batch = Vec<TextDocument>;
    type Converter = VecBatchConverter<TextDocument>;

    fn load_model(&self) -> Result<Self::Model> {
        Ok(())
    }

    fn run_inference(
        &self,
        batch: &Self::Batch,
        _model: &Self::Model,
        _inference_args: Option<&InferenceArgs>,
    ) -> Result<Vec<VectorEmbedding>> {
        let results = batch
            .iter()
            .map(|doc| {
                // Generate a deterministic unit vector embedding (dim = 4)
                let len = doc.content.len() as f32;
                let norm = (len * len * 4.0).sqrt().max(1.0);
                VectorEmbedding {
                    doc_id: doc.doc_id.clone(),
                    embedding: vec![len / norm, -len / norm, (len * 0.5) / norm, 0.0],
                }
            })
            .collect();
        Ok(results)
    }

    fn get_batch_converter(&self) -> Self::Converter {
        VecBatchConverter::new()
    }

    fn get_batch_bounds(&self) -> BatchBounds {
        BatchBounds::new(1, 16)
    }
}

#[tokio::test]
async fn test_text_embedding_pipeline_on_prism() {
    let p = TestPipeline::new();
    let input = p.apply(Create::new("CreateSentences", sample_sentences()));

    let embeddings = input.apply(RunInference::new(
        "MockCandleEmbedding",
        MockEmbeddingModelHandler,
    ));

    let formatted = embeddings.par_do_fn("FormatOutput", |pred, out| {
        out.emit(format_embedding_record(
            "sentence-transformers/all-MiniLM-L6-v2",
            &pred,
        )?)
    });

    let mut expected: Vec<String> = sample_sentences()
        .into_iter()
        .map(|doc| {
            let doc_id = doc.doc_id.clone();
            let pred = PredictionResult::new(
                doc,
                VectorEmbedding {
                    doc_id,
                    embedding: vec![0.5, -0.5, 0.25, 0.0],
                },
            );
            format_embedding_record("sentence-transformers/all-MiniLM-L6-v2", &pred).unwrap()
        })
        .collect();
    expected.sort();

    passert::that("AssertEmbeddings", &formatted).has_count(4);
    passert::that("AssertEmbeddings", &formatted).satisfies(move |lines: &[String]| {
        let mut actual = lines.to_vec();
        actual.sort();
        if actual != expected {
            return Err(format!("Expected {expected:?}, got {actual:?}").into());
        }
        Ok(())
    });

    p.run()
        .await
        .expect("pipeline should succeed on Prism runner");
}
