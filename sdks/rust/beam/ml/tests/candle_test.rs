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

//! Unit and integration tests for [`BertEmbeddingModelHandler`], sentence-transformers style
//! tokenization and pooling, and device initialization with CPU fallback.

#![cfg(feature = "candle")]

use std::path::PathBuf;

use beam::coders::DefaultCoder;
use beam_ml::candle::{
    BertEmbeddingAdapter, BertEmbeddingModelHandler, CandleAdapter, CandleConfig, CandleDevice,
    CandleDeviceKind, CandleDeviceOptions, CandleModelHandler, TextDocument, VectorEmbedding,
    configure_tokenizer, masked_mean_pool_l2,
};
use beam_ml::handler::{BatchBounds, ModelHandler};
use candle_core::{DType, Device, Tensor};
use candle_nn::{VarBuilder, VarMap};
use candle_transformers::models::bert::{BertModel, Config as BertConfig};
use tokenizers::Tokenizer;

/// Tiny BERT architecture used to exercise the real handler code path.
const TINY_BERT_CONFIG: &str = r#"{
    "vocab_size": 16,
    "hidden_size": 8,
    "num_hidden_layers": 2,
    "num_attention_heads": 2,
    "intermediate_size": 16,
    "hidden_act": "gelu",
    "hidden_dropout_prob": 0.1,
    "attention_probs_dropout_prob": 0.1,
    "max_position_embeddings": 32,
    "type_vocab_size": 2,
    "initializer_range": 0.02,
    "layer_norm_eps": 1e-12,
    "pad_token_id": 0
}"#;

/// BERT WordPiece tokenizer with a fixed padding of 16, like the all-MiniLM-L6-v2
/// `tokenizer.json` (which pins `Fixed(128)`), so tests prove the handler overrides it.
const TINY_TOKENIZER: &str = r###"{
    "version": "1.0",
    "truncation": {"direction": "Right", "max_length": 16, "strategy": "LongestFirst", "stride": 0},
    "padding": {"strategy": {"Fixed": 16}, "direction": "Right", "pad_to_multiple_of": null,
                "pad_id": 0, "pad_type_id": 0, "pad_token": "[PAD]"},
    "added_tokens": [
        {"id": 0, "content": "[PAD]", "single_word": false, "lstrip": false, "rstrip": false, "normalized": false, "special": true},
        {"id": 1, "content": "[UNK]", "single_word": false, "lstrip": false, "rstrip": false, "normalized": false, "special": true},
        {"id": 2, "content": "[CLS]", "single_word": false, "lstrip": false, "rstrip": false, "normalized": false, "special": true},
        {"id": 3, "content": "[SEP]", "single_word": false, "lstrip": false, "rstrip": false, "normalized": false, "special": true}
    ],
    "normalizer": {"type": "BertNormalizer", "clean_text": true, "handle_chinese_chars": true,
                   "strip_accents": null, "lowercase": true},
    "pre_tokenizer": {"type": "BertPreTokenizer"},
    "post_processor": {"type": "BertProcessing", "sep": ["[SEP]", 3], "cls": ["[CLS]", 2]},
    "decoder": null,
    "model": {
        "type": "WordPiece", "unk_token": "[UNK]", "continuing_subword_prefix": "##",
        "max_input_chars_per_word": 100,
        "vocab": {"[PAD]": 0, "[UNK]": 1, "[CLS]": 2, "[SEP]": 3, "hello": 4, "world": 5,
                  "a": 6, "much": 7, "longer": 8, "sentence": 9, "with": 10, "many": 11,
                  "more": 12, "tokens": 13}
    }
}"###;

const SHORT: &str = "Hello world";
const LONG: &str = "a much longer sentence with many more tokens a much longer sentence";

/// Deterministic values in `[-1, 1)` from SplitMix64 seeded with `seed`.
fn fixed_values(seed: u64, len: usize) -> Vec<f32> {
    let mut state = seed;
    (0..len)
        .map(|_| {
            state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
            let mut z = state;
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            z ^= z >> 31;
            (z >> 40) as f32 / (1u32 << 23) as f32 - 1.0
        })
        .collect()
}

/// FNV-1a hash, stable across processes unlike `DefaultHasher`.
fn fnv1a(name: &str) -> u64 {
    name.bytes().fold(0xcbf2_9ce4_8422_2325, |hash, byte| {
        (hash ^ u64::from(byte)).wrapping_mul(0x0100_0000_01b3)
    })
}

/// Writes the tiny model (fixed pseudo-random weights) to a fresh directory and returns its
/// artifact paths.
fn write_tiny_model(name: &str) -> (String, String, String) {
    let dir: PathBuf =
        std::env::temp_dir().join(format!("beam_ml_candle_{name}_{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("creating temp model dir");

    let config: BertConfig = serde_json::from_str(TINY_BERT_CONFIG).expect("tiny config");
    let varmap = VarMap::new();
    BertModel::load(
        VarBuilder::from_varmap(&varmap, DType::F32, &Device::Cpu),
        &config,
    )
    .expect("building tiny BERT");
    // Seeded by variable name: VarMap iteration order varies between runs.
    for (var_name, var) in varmap.data().lock().expect("varmap lock").iter() {
        // Identity LayerNorms: a shared random bias would dominate every pooled embedding.
        let weights = if var_name.ends_with("LayerNorm.weight") {
            Tensor::ones(var.shape(), DType::F32, &Device::Cpu)
        } else if var_name.ends_with("LayerNorm.bias") {
            Tensor::zeros(var.shape(), DType::F32, &Device::Cpu)
        } else {
            let values = fixed_values(fnv1a(var_name), var.elem_count());
            Tensor::from_vec(values, var.shape(), &Device::Cpu)
        }
        .expect("weights");
        var.set(&weights).expect("setting fixed weights");
    }

    let path = |file: &str| dir.join(file).to_string_lossy().into_owned();
    varmap
        .save(dir.join("model.safetensors"))
        .expect("saving weights");
    std::fs::write(dir.join("config.json"), TINY_BERT_CONFIG).expect("writing config");
    std::fs::write(dir.join("tokenizer.json"), TINY_TOKENIZER).expect("writing tokenizer");
    (
        path("model.safetensors"),
        path("config.json"),
        path("tokenizer.json"),
    )
}

fn docs(texts: &[&str]) -> Vec<TextDocument> {
    texts
        .iter()
        .enumerate()
        .map(|(i, text)| TextDocument {
            doc_id: format!("doc_{i}"),
            content: (*text).to_string(),
        })
        .collect()
}

fn max_abs_diff(a: &[f32], b: &[f32]) -> f32 {
    a.iter()
        .zip(b)
        .map(|(x, y)| (x - y).abs())
        .fold(0.0, f32::max)
}

#[test]
fn test_text_document_wire_coder_roundtrip() {
    let doc = TextDocument {
        doc_id: "doc_123".to_string(),
        content: "Apache Beam provides unified streaming and batch.".to_string(),
    };

    let mut buf = Vec::new();
    doc.encode_element(&mut buf).expect("encoding TextDocument");

    let mut slice = buf.as_slice();
    let decoded = TextDocument::decode_element(&mut slice).expect("decoding TextDocument");

    assert_eq!(decoded, doc);
}

#[test]
fn test_vector_embedding_wire_coder_roundtrip() {
    let embedding = VectorEmbedding {
        doc_id: "emb_456".to_string(),
        embedding: vec![0.123, -0.456, 0.789, -1.0, 0.0],
    };

    let mut buf = Vec::new();
    embedding
        .encode_element(&mut buf)
        .expect("encoding VectorEmbedding");

    let mut slice = buf.as_slice();
    let decoded = VectorEmbedding::decode_element(&mut slice).expect("decoding VectorEmbedding");

    assert_eq!(decoded, embedding);
}

#[test]
fn test_candle_device_cpu_init() {
    let dev = CandleDevice::Cpu;
    let physical = dev.to_device(false).expect("CPU device should initialize");
    assert!(physical.is_cpu());
}

#[test]
fn test_candle_device_accelerator_fallback() {
    // Metal with CPU fallback allowed should successfully return CPU on non-metal machines
    let metal = CandleDevice::Metal { device_id: 0 };
    let physical = metal
        .to_device(true)
        .expect("Metal with fallback should return CPU");
    assert!(physical.is_cpu() || physical.is_metal());

    // CUDA with CPU fallback allowed should successfully return CPU when no CUDA driver exists
    let cuda = CandleDevice::Cuda { device_id: 0 };
    let physical_cuda = cuda
        .to_device(true)
        .expect("CUDA with fallback should return CPU");
    assert!(physical_cuda.is_cpu() || physical_cuda.is_cuda());
}

#[test]
#[cfg(feature = "candle-metal")]
fn test_candle_device_metal_init_and_inference() {
    let dev = CandleDevice::Metal { device_id: 0 };
    match dev.to_device(false) {
        Ok(physical) => {
            assert!(physical.is_metal());
            let (weights, config, tokenizer) = write_tiny_model("metal_inference");
            let handler = BertEmbeddingModelHandler::new(
                CandleConfig::new(weights, config, tokenizer).with_device(dev),
            );
            let model = handler.load_model().expect("loading tiny model on Metal");
            let results = handler
                .run_inference(&docs(&[SHORT]), &model, None)
                .expect("running inference on Metal");
            assert_eq!(results.len(), 1);
            assert_eq!(results[0].embedding.len(), 8);
        }
        Err(e) => {
            eprintln!("Metal not available on this host: {e}");
        }
    }
}

#[test]
fn test_candle_config_builder() {
    let config = CandleConfig::new("model.safetensors", "config.json", "tokenizer.json")
        .with_device(CandleDevice::Cpu)
        .with_cpu_fallback(true)
        .with_max_seq_len(128)
        .with_inference_batch_size(8)
        .with_batch_bounds(BatchBounds::new(2, 64));

    assert_eq!(config.max_seq_len, 128);
    assert_eq!(config.inference_batch_size, 8);
    assert!(config.allow_cpu_fallback);
    assert_eq!(config.batch_bounds.min_batch_size, 2);
    assert_eq!(config.batch_bounds.max_batch_size, 64);
}

#[test]
fn test_candle_config_defaults_match_sentence_transformers() {
    let config = CandleConfig::new("w", "c", "t");
    assert_eq!(config.max_seq_len, 256);
    assert_eq!(config.inference_batch_size, 32);
    assert!(!config.allow_cpu_fallback);
}

#[test]
fn test_masked_mean_pool_ignores_padding() {
    // Two sequences of length 3; the second has one padding position holding huge values
    // that must not leak into the mean.
    let embeddings = Tensor::new(
        &[
            [[1f32, 0.0], [3.0, 0.0], [2.0, 0.0]],
            [[0.0, 2.0], [0.0, 4.0], [1000.0, -1000.0]],
        ],
        &Device::Cpu,
    )
    .expect("embeddings");
    let mask = Tensor::new(&[[1u32, 1, 1], [1, 1, 0]], &Device::Cpu).expect("mask");

    let pooled: Vec<Vec<f32>> = masked_mean_pool_l2(&embeddings, &mask)
        .expect("pooling")
        .to_vec2()
        .expect("to_vec2");

    assert!(max_abs_diff(&pooled[0], &[1.0, 0.0]) < 1e-6, "{pooled:?}");
    assert!(max_abs_diff(&pooled[1], &[0.0, 1.0]) < 1e-6, "{pooled:?}");
}

#[test]
fn test_configure_tokenizer_pads_to_batch_longest_and_truncates() {
    let tokenizer = Tokenizer::from_bytes(TINY_TOKENIZER).expect("tokenizer");

    let configured = configure_tokenizer(tokenizer.clone(), 64).expect("configure");
    let encodings = configured
        .encode_batch(vec![SHORT, LONG], true)
        .expect("encode");
    let long_len = 2 + LONG.split_whitespace().count();
    assert_eq!(
        encodings[0].len(),
        long_len,
        "padded to batch longest, not Fixed(16)"
    );
    assert_eq!(encodings[1].len(), long_len);
    assert_eq!(encodings[0].get_attention_mask().iter().sum::<u32>(), 4);

    let truncated = configure_tokenizer(tokenizer, 5).expect("configure");
    let encodings = truncated.encode_batch(vec![LONG], true).expect("encode");
    assert_eq!(encodings[0].len(), 5);
}

#[test]
fn test_embedding_is_independent_of_batch_mates() {
    let (weights, config, tokenizer) = write_tiny_model("batch_mates");
    let handler = BertEmbeddingModelHandler::new(
        CandleConfig::new(weights, config, tokenizer).with_inference_batch_size(8),
    );
    let model = handler.load_model().expect("loading tiny model");

    let alone = handler
        .run_inference(&docs(&[SHORT]), &model, None)
        .expect("inference alone");
    let with_long = handler
        .run_inference(&docs(&[SHORT, LONG]), &model, None)
        .expect("inference with long batch-mate");
    let reordered = handler
        .run_inference(&docs(&[LONG, SHORT, SHORT]), &model, None)
        .expect("inference reordered");

    // Output order and ids follow the input order even though texts are length-sorted.
    assert_eq!(with_long[0].doc_id, "doc_0");
    assert_eq!(reordered[1].doc_id, "doc_1");

    let short = &alone[0].embedding;
    assert_eq!(short.len(), 8);
    let norm: f32 = short.iter().map(|x| x * x).sum::<f32>().sqrt();
    assert!(
        (norm - 1.0).abs() < 1e-5,
        "embedding must be unit-norm: {norm}"
    );

    assert!(max_abs_diff(short, &with_long[0].embedding) < 1e-5);
    assert!(max_abs_diff(short, &reordered[1].embedding) < 1e-5);
    assert!(max_abs_diff(short, &reordered[2].embedding) < 1e-5);
    assert!(max_abs_diff(&with_long[1].embedding, &reordered[0].embedding) < 1e-5);
    assert!(
        max_abs_diff(short, &with_long[1].embedding) > 1e-3,
        "different texts must produce different embeddings"
    );
}

#[test]
fn test_missing_artifacts_are_an_error_even_with_cpu_fallback() {
    let config = CandleConfig::new(
        "/nonexistent/model.safetensors",
        "/nonexistent/config.json",
        "/nonexistent/tokenizer.json",
    )
    .with_cpu_fallback(true);
    let err = BertEmbeddingModelHandler::new(config)
        .load_model()
        .err()
        .expect("missing artifacts must fail to load");
    assert!(err.to_string().contains("/nonexistent/"), "{err}");
}

/// A pipeline's arguments embedding the shared device options.
#[derive(clap::Args, serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
struct PipelineArgs {
    #[command(flatten)]
    #[serde(flatten)]
    accelerator: CandleDeviceOptions,
}

impl beam::options::PipelineOptionGroup for PipelineArgs {}

/// Parses `args` as a pipeline's command line, as `beam::options::parse` does.
fn parse_args(args: &[&str]) -> PipelineArgs {
    beam::options::try_parse_from::<PipelineArgs, _, _>(
        std::iter::once("pipeline").chain(args.iter().copied()),
    )
    .expect("valid arguments")
    .1
}

#[test]
fn test_device_options_configure_candle() {
    let default = parse_args(&[]).accelerator;
    assert_eq!(default, CandleDeviceOptions::default());
    let config = CandleConfig::new("w", "c", "t").with_device_options(&default);
    assert_eq!(config.device, CandleDevice::Cpu);
    assert!(!config.allow_cpu_fallback);

    let args = parse_args(&["--device=metal", "--device_id=1", "--allow_cpu_fallback"]);
    assert_eq!(args.accelerator.device, CandleDeviceKind::Metal);
    let config = CandleConfig::new("w", "c", "t").with_device_options(&args.accelerator);
    assert_eq!(config.device, CandleDevice::Metal { device_id: 1 });
    assert!(config.allow_cpu_fallback);

    let json = serde_json::to_value(&args).unwrap();
    assert_eq!(json["device"], "metal");
    assert_eq!(serde_json::from_value::<PipelineArgs>(json).unwrap(), args);
}

struct ScaleModel {
    scale: f64,
}

#[derive(Clone, Default)]
struct ScaleAdapter;

impl CandleAdapter<f64, f64> for ScaleAdapter {
    type Model = ScaleModel;

    fn load_model(
        &self,
        _device: &Device,
        vb: VarBuilder<'_>,
        _config: &CandleConfig,
    ) -> beam::Result<Self::Model> {
        let weight = vb.get(1, "weight")?;
        let scale = weight.to_vec1::<f32>()?[0] as f64;
        Ok(ScaleModel { scale })
    }

    fn run_inference(
        &self,
        model: &Self::Model,
        batch: &[f64],
        _inference_args: Option<&beam_ml::handler::InferenceArgs>,
    ) -> beam::Result<Vec<f64>> {
        Ok(batch.iter().map(|&x| x * model.scale).collect())
    }
}

#[test]
fn test_custom_candle_adapter_and_model_handler() {
    let dir = std::env::temp_dir().join(format!("beam_ml_candle_custom_{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("temp dir");
    let weights_path = dir.join("model.safetensors");

    let mut tensors = std::collections::HashMap::new();
    tensors.insert(
        "weight".to_string(),
        Tensor::new(&[3.0f32], &Device::Cpu).expect("tensor"),
    );
    candle_core::safetensors::save(&tensors, &weights_path).expect("saving weights");

    let config = CandleConfig::from_weights(weights_path.to_str().unwrap()).with_dtype(DType::F32);
    let handler = CandleModelHandler::new(config, ScaleAdapter);
    let model = handler.load_model().expect("load custom model");
    assert_eq!(model.scale, 3.0);

    let inputs = vec![1.0f64, 2.0, 4.0];
    let outputs = handler
        .run_inference(&inputs, &model, None)
        .expect("run inference");
    assert_eq!(outputs, vec![3.0f64, 6.0, 12.0]);
}

#[test]
fn test_bert_embedding_adapter_with_candle_model_handler() {
    let (weights, config, tokenizer) = write_tiny_model("adapter_test");
    let candle_config = CandleConfig::new(weights, config, tokenizer);
    let handler = CandleModelHandler::new(candle_config, BertEmbeddingAdapter);
    let model = handler.load_model().expect("loading model through adapter");
    let results = handler
        .run_inference(&docs(&[SHORT]), &model, None)
        .expect("running inference");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].embedding.len(), 8);
}

#[test]
fn test_candle_config_from_weights_and_dtype() {
    let config = CandleConfig::from_weights("path/to/weights.safetensors")
        .with_dtype(DType::F16)
        .with_config_path("path/to/config.json")
        .with_tokenizer_path("path/to/tokenizer.json");

    assert_eq!(config.weights_path, "path/to/weights.safetensors");
    assert_eq!(config.dtype, DType::F16);
    assert_eq!(config.config_path, "path/to/config.json");
    assert_eq!(config.tokenizer_path, "path/to/tokenizer.json");
}

/// Records registrations and hands out sequential ids.
#[derive(Default)]
struct RecordingRegistry {
    calls: std::sync::Mutex<Vec<(String, Vec<String>)>>,
}

impl beam::coders::CoderRegistry for RecordingRegistry {
    fn register_coder(&self, urn: &str, component_coder_ids: Vec<String>) -> String {
        let mut calls = self.calls.lock().expect("registry lock");
        calls.push((urn.to_string(), component_coder_ids));
        format!("coder_{}", calls.len())
    }
}

#[test]
fn test_text_document_coder_trait() {
    use beam::coders::{Coder, Context, URN_KV};
    let coder = TextDocument::coder();
    assert_eq!(coder.urn(), URN_KV);
    let doc = TextDocument {
        doc_id: "d1".into(),
        content: "some text".into(),
    };
    for context in [Context::Nested, Context::WholeStream] {
        let mut buf = Vec::new();
        coder.encode(&doc, &mut buf, context).expect("encode");
        assert!(!buf.is_empty());
        let decoded = coder.decode(&mut buf.as_slice(), context).expect("decode");
        assert_eq!(decoded, doc);
    }
}

#[test]
fn test_vector_embedding_coder_trait() {
    use beam::coders::{Coder, Context, URN_KV};
    let coder = VectorEmbedding::coder();
    assert_eq!(coder.urn(), URN_KV);
    let embedding = VectorEmbedding {
        doc_id: "e1".into(),
        embedding: vec![1.5, -0.25, f32::MIN_POSITIVE],
    };
    let mut buf = Vec::new();
    coder
        .encode(&embedding, &mut buf, Context::Nested)
        .expect("encode");
    assert!(!buf.is_empty());
    let decoded = coder
        .decode(&mut buf.as_slice(), Context::Nested)
        .expect("decode");
    assert_eq!(decoded, embedding);
}

#[test]
fn test_text_and_embedding_register_kv_coders() {
    use beam::coders::URN_KV;
    let registry = RecordingRegistry::default();
    let doc_id = TextDocument::register_coder(&registry);
    let emb_id = VectorEmbedding::register_coder(&registry);
    let calls = registry.calls.lock().expect("registry lock");
    let ids: Vec<String> = (1..=calls.len()).map(|i| format!("coder_{i}")).collect();
    let kv_ids: Vec<&String> = calls
        .iter()
        .zip(&ids)
        .filter(|((urn, _), _)| urn == URN_KV)
        .map(|(_, id)| id)
        .collect();
    assert_eq!(kv_ids, [&doc_id, &emb_id]);
    for (_, components) in calls.iter().filter(|(urn, _)| urn == URN_KV) {
        assert_eq!(components.len(), 2);
    }
}

#[test]
fn test_candle_model_handler_accessors_and_debug() {
    let bounds = BatchBounds::new(3, 9);
    let handler = CandleModelHandler::new(
        CandleConfig::from_weights("w.safetensors").with_batch_bounds(bounds),
        ScaleAdapter,
    );
    assert_eq!(ModelHandler::<f64, f64>::model_id(&handler), None);
    assert_eq!(ModelHandler::<f64, f64>::get_batch_bounds(&handler), bounds);
    assert_eq!(handler.config().weights_path, "w.safetensors");

    let named = handler.with_model_id("scale-v1");
    assert_eq!(
        ModelHandler::<f64, f64>::model_id(&named),
        Some("scale-v1".to_string())
    );
    let debug = format!("{named:?}");
    assert!(debug.starts_with("CandleModelHandler {"), "{debug}");
    assert!(debug.contains("model_id: Some(\"scale-v1\")"), "{debug}");
    assert!(debug.contains("w.safetensors"), "{debug}");
}

#[test]
fn test_bert_handler_accessors() {
    let bounds = BatchBounds::new(2, 5);
    let handler =
        BertEmbeddingModelHandler::new(CandleConfig::new("w", "c", "t").with_batch_bounds(bounds));
    assert_eq!(handler.get_batch_bounds(), bounds);
    assert_eq!(handler.model_id(), None);
    assert_eq!(handler.config().tokenizer_path, "t");
    let named = handler.with_model_id("minilm");
    assert_eq!(named.model_id(), Some("minilm".to_string()));
}

/// All-zero weights yield all-zero embeddings, which must be rejected.
#[test]
fn test_zero_embedding_is_rejected() {
    let (weights, config, tokenizer) = write_tiny_model("zero_weights");
    let bert: BertConfig = serde_json::from_str(TINY_BERT_CONFIG).expect("tiny config");
    let varmap = VarMap::new();
    BertModel::load(
        VarBuilder::from_varmap(&varmap, DType::F32, &Device::Cpu),
        &bert,
    )
    .expect("building tiny BERT");
    for var in varmap.all_vars() {
        let zeros = Tensor::zeros(var.shape(), DType::F32, &Device::Cpu).expect("zeros");
        var.set(&zeros).expect("zeroing weights");
    }
    varmap.save(&weights).expect("overwriting weights");

    let handler = BertEmbeddingModelHandler::new(CandleConfig::new(weights, config, tokenizer));
    let model = handler.load_model().expect("loading zero model");
    let err = handler
        .run_inference(&docs(&[SHORT]), &model, None)
        .expect_err("zero embedding must fail");
    assert!(err.to_string().contains("degenerate embedding"), "{err}");
}
