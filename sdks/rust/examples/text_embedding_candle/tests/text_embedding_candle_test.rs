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

use beam::ml::candle::{CandleConfig, CandleDevice, TextDocument, VectorEmbedding};
use beam::ml::{BatchBounds, InferenceArgs, ModelHandler};
use beam::prelude::*;
use beam::testing::{TestPipeline, passert};
use beam::transforms::VecBatchConverter;
use text_embedding_candle::{Args, EmbedTexts, text_to_document};

/// Embeds a text as its length, and rejects the text `fail`. Batches have one text, so a
/// rejected text fails alone.
#[derive(Clone)]
struct LengthModel;

impl ModelHandler<TextDocument, VectorEmbedding> for LengthModel {
    type Model = ();
    type Batch = Vec<TextDocument>;
    type Converter = VecBatchConverter<TextDocument>;

    fn load_model(&self) -> Result<()> {
        Ok(())
    }

    fn run_inference(
        &self,
        batch: &Vec<TextDocument>,
        _: &(),
        _: Option<&InferenceArgs>,
    ) -> Result<Vec<VectorEmbedding>> {
        batch
            .iter()
            .map(|doc| match doc.content.as_str() {
                "fail" => Err("cannot embed".into()),
                text => Ok(VectorEmbedding {
                    doc_id: doc.doc_id.clone(),
                    embedding: vec![text.len() as f32, 0.5],
                }),
            })
            .collect()
    }

    fn get_batch_converter(&self) -> Self::Converter {
        VecBatchConverter::new()
    }

    fn get_batch_bounds(&self) -> BatchBounds {
        BatchBounds::new(1, 1)
    }
}

#[tokio::test]
async fn test_embed_texts_writes_json_and_routes_rejected_texts_to_failures() {
    let p = TestPipeline::new();
    let lines = [" abc ", "", "fail"].map(String::from);
    let WithFailures { output, failures } = p
        .apply("Create", Create::new(lines.to_vec()))
        .apply("EmbedTexts", EmbedTexts::new(LengthModel, "m"));

    passert::that("AssertEmbeddings", &output).contains_in_any_order([concat!(
        r#"{"embedding":[3.0,0.5],"embedding_dim":2,"#,
        r#""id":"ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad","#,
        r#""model_name":"m","raw_text":"abc"}"#
    )
    .to_string()]);
    passert::that(
        "AssertFailedTexts",
        &failures.map("Text", |f| f.input.content),
    )
    .contains_in_any_order(["fail".to_string()]);

    p.run().await.expect("pipeline must succeed");
}

#[test]
fn test_candle_config_from_args() {
    let (_, args) = beam::options::parse_from::<Args, _, _>([
        "text_embedding_candle",
        "--weights_path=gs://bucket/weights.safetensors",
        "--config_path=cfg.json",
        "--tokenizer_path=tok.json",
        "--device=cuda",
        "--min_batch_size=8",
        "--max_batch_size=64",
        "--model_batch_size=16",
        "--max_seq_len=128",
    ]);
    let config = CandleConfig::from(&args);

    assert_eq!(config.weights_path, "gs://bucket/weights.safetensors");
    assert_eq!(config.config_path, "cfg.json");
    assert_eq!(config.tokenizer_path, "tok.json");
    assert_eq!(config.batch_bounds, BatchBounds::new(8, 64));
    assert_eq!(config.inference_batch_size, 16);
    assert_eq!(config.max_seq_len, 128);
    assert_eq!(config.device, CandleDevice::Cuda { device_id: 0 });
}

#[test]
fn test_text_to_document_trims_and_hashes() {
    let doc = text_to_document("  hello world \n").expect("non-blank line");
    assert_eq!(doc.content, "hello world");
    assert_eq!(
        doc.doc_id,
        "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
    );
    assert_eq!(text_to_document("   \t"), None);
}
