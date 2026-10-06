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

//! BERT text embeddings on Candle.
//!
//! [`BertEmbeddingModelHandler`] gives the same mean-pooled, L2-normalized embeddings as
//! `sentence-transformers`: batch-longest padding, truncation to
//! [`CandleConfig::max_seq_len`], the real attention mask, and pooling over non-padding
//! tokens only. Artifacts load through the Beam filesystem registry ([`crate::artifact`]).

use std::cmp::Reverse;
use std::sync::Arc;

use beam::coders::{Coder, CoderError, CoderRegistry, Context, DefaultCoder, URN_KV};
use beam::transforms::VecBatchConverter;
use candle_core::{Device, Tensor};
use candle_nn::VarBuilder;
use candle_transformers::models::bert::{BertModel, Config as BertConfig};
use serde::{Deserialize, Serialize};
use tokenizers::{Encoding, PaddingParams, PaddingStrategy, Tokenizer, TruncationParams};

use super::{CandleAdapter, CandleConfig, CandleModelHandler};
use crate::artifact::read_artifact;
use crate::handler::{BatchBounds, InferenceArgs, ModelHandler};

/// Input text document record consumed by embedding models.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TextDocument {
    /// Unique document or sentence identifier.
    pub doc_id: String,
    /// Text content to be embedded.
    pub content: String,
}

/// Wire coder for [`TextDocument`].
#[derive(Clone, Debug)]
pub struct TextDocumentCoder {
    id_coder: <String as DefaultCoder>::Coder,
    content_coder: <String as DefaultCoder>::Coder,
}

impl Coder<TextDocument> for TextDocumentCoder {
    fn urn(&self) -> &'static str {
        URN_KV
    }

    fn encode(
        &self,
        element: &TextDocument,
        writer: &mut dyn std::io::Write,
        context: Context,
    ) -> Result<(), CoderError> {
        self.id_coder
            .encode(&element.doc_id, writer, Context::Nested)?;
        self.content_coder
            .encode(&element.content, writer, context)?;
        Ok(())
    }

    fn decode(
        &self,
        reader: &mut dyn std::io::Read,
        context: Context,
    ) -> Result<TextDocument, CoderError> {
        let doc_id = self.id_coder.decode(reader, Context::Nested)?;
        let content = self.content_coder.decode(reader, context)?;
        Ok(TextDocument { doc_id, content })
    }
}

impl DefaultCoder for TextDocument {
    type Coder = TextDocumentCoder;

    fn coder() -> Self::Coder {
        TextDocumentCoder {
            id_coder: String::coder(),
            content_coder: String::coder(),
        }
    }

    fn encode_element(&self, writer: &mut dyn std::io::Write) -> Result<(), CoderError> {
        self.doc_id.encode_element(writer)?;
        self.content.encode_element(writer)?;
        Ok(())
    }

    fn decode_element(reader: &mut dyn std::io::Read) -> Result<Self, CoderError> {
        let doc_id = String::decode_element(reader)?;
        let content = String::decode_element(reader)?;
        Ok(Self { doc_id, content })
    }

    fn register_coder<R: CoderRegistry + ?Sized>(registry: &R) -> String {
        let str_id = String::register_coder(registry);
        registry.register_coder(URN_KV, vec![str_id.clone(), str_id])
    }
}

/// Output vector embedding produced by a transformer model.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct VectorEmbedding {
    /// Document or sentence identifier corresponding to the input.
    pub doc_id: String,
    /// Dense float vector embedding.
    pub embedding: Vec<f32>,
}

/// Wire coder for [`VectorEmbedding`].
#[derive(Clone, Debug, Default)]
pub struct VectorEmbeddingCoder;

impl Coder<VectorEmbedding> for VectorEmbeddingCoder {
    fn urn(&self) -> &'static str {
        URN_KV
    }

    fn encode(
        &self,
        element: &VectorEmbedding,
        writer: &mut dyn std::io::Write,
        _context: Context,
    ) -> Result<(), CoderError> {
        element.encode_element(writer)
    }

    fn decode(
        &self,
        reader: &mut dyn std::io::Read,
        _context: Context,
    ) -> Result<VectorEmbedding, CoderError> {
        VectorEmbedding::decode_element(reader)
    }
}

impl DefaultCoder for VectorEmbedding {
    type Coder = VectorEmbeddingCoder;

    fn coder() -> Self::Coder {
        VectorEmbeddingCoder
    }

    fn encode_element(&self, writer: &mut dyn std::io::Write) -> Result<(), CoderError> {
        self.doc_id.encode_element(writer)?;
        let bytes: Vec<u8> = self
            .embedding
            .iter()
            .flat_map(|f| f.to_le_bytes())
            .collect();
        bytes.encode_element(writer)?;
        Ok(())
    }

    fn decode_element(reader: &mut dyn std::io::Read) -> Result<Self, CoderError> {
        let doc_id = String::decode_element(reader)?;
        let bytes = Vec::<u8>::decode_element(reader)?;
        let embedding = bytes
            .as_chunks::<4>()
            .0
            .iter()
            .map(|chunk| f32::from_le_bytes(*chunk))
            .collect();
        Ok(Self { doc_id, embedding })
    }

    fn register_coder<R: CoderRegistry + ?Sized>(registry: &R) -> String {
        let str_id = String::register_coder(registry);
        let bytes_id = Vec::<u8>::register_coder(registry);
        registry.register_coder(URN_KV, vec![str_id, bytes_id])
    }
}

/// Loaded BERT embedding model and tokenizer shared across worker bundle threads.
pub struct LoadedBertEmbeddingModel {
    pub model: BertModel,
    /// Tokenizer configured for batch-longest padding and truncation to `max_seq_len`.
    pub tokenizer: Tokenizer,
    pub device: Device,
    /// Number of texts per forward pass.
    pub inference_batch_size: usize,
}

impl LoadedBertEmbeddingModel {
    /// Embeds `texts` in a single forward pass, returning one normalized vector per text.
    pub fn embed(&self, texts: &[&str]) -> beam::Result<Vec<Vec<f32>>> {
        let encodings = self.tokenizer.encode_batch(texts.to_vec(), true)?;
        let seq_len = encodings.first().map_or(0, Encoding::len);
        if let Some(bad) = encodings.iter().find(|e| e.len() != seq_len) {
            return Err(format!(
                "tokenizer produced ragged batch ({} vs {seq_len} tokens); batch-longest padding is required",
                bad.len()
            )
            .into());
        }
        let shape = (encodings.len(), seq_len);
        let tensor = |field: fn(&Encoding) -> &[u32]| {
            let flat: Vec<u32> = encodings
                .iter()
                .flat_map(|e| field(e).iter().copied())
                .collect();
            Tensor::from_vec(flat, shape, &self.device)
        };
        let input_ids = tensor(Encoding::get_ids)?;
        let token_type_ids = tensor(Encoding::get_type_ids)?;
        let attention_mask = tensor(Encoding::get_attention_mask)?;

        let token_embeddings =
            self.model
                .forward(&input_ids, &token_type_ids, Some(&attention_mask))?;
        let vectors: Vec<Vec<f32>> =
            masked_mean_pool_l2(&token_embeddings, &attention_mask)?.to_vec2()?;

        match vectors
            .iter()
            .position(|v| v.iter().any(|x| !x.is_finite()) || v.iter().all(|&x| x == 0.0))
        {
            Some(i) => Err(format!("degenerate embedding (NaN/inf/zero) for text {i}").into()),
            None => Ok(vectors),
        }
    }
}

/// Mean-pools `token_embeddings` (`[batch, seq, hidden]`) over the positions where
/// `attention_mask` (`[batch, seq]`) is non-zero, then L2-normalizes each row.
///
/// Matches sentence-transformers' `Pooling(mean)` + `Normalize` modules: token counts are
/// clamped to `1e-9` and norms to `1e-12`.
pub fn masked_mean_pool_l2(
    token_embeddings: &Tensor,
    attention_mask: &Tensor,
) -> candle_core::Result<Tensor> {
    let mask = attention_mask
        .to_dtype(token_embeddings.dtype())?
        .unsqueeze(2)?;
    let summed = token_embeddings.broadcast_mul(&mask)?.sum(1)?;
    let counts = mask.sum(1)?.clamp(1e-9f32, f32::MAX)?;
    let mean = summed.broadcast_div(&counts)?;
    let norm = mean
        .sqr()?
        .sum_keepdim(1)?
        .sqrt()?
        .clamp(1e-12f32, f32::MAX)?;
    mean.broadcast_div(&norm)
}

/// Configures `tokenizer` like sentence-transformers: pad each batch to its longest sequence
/// (overriding any fixed padding in `tokenizer.json`) and truncate to `max_seq_len` tokens.
///
/// Padding token/id and truncation strategy are kept from `tokenizer.json` when present.
pub fn configure_tokenizer(
    mut tokenizer: Tokenizer,
    max_seq_len: usize,
) -> beam::Result<Tokenizer> {
    let padding = PaddingParams {
        strategy: PaddingStrategy::BatchLongest,
        ..tokenizer.get_padding().cloned().unwrap_or_default()
    };
    let truncation = TruncationParams {
        max_length: max_seq_len,
        ..tokenizer.get_truncation().cloned().unwrap_or_default()
    };
    tokenizer.with_padding(Some(padding));
    tokenizer.with_truncation(Some(truncation))?;
    Ok(tokenizer)
}

/// Pre-packaged [`CandleAdapter`] reproducing `sentence-transformers` BERT embeddings.
#[derive(Clone, Debug, Default)]
pub struct BertEmbeddingAdapter;

impl CandleAdapter<TextDocument, VectorEmbedding> for BertEmbeddingAdapter {
    type Model = LoadedBertEmbeddingModel;

    fn load_model(
        &self,
        device: &Device,
        vb: VarBuilder<'_>,
        config: &CandleConfig,
    ) -> beam::Result<Self::Model> {
        let bert_config: BertConfig = serde_json::from_slice(&read_artifact(&config.config_path)?)?;
        let tokenizer = configure_tokenizer(
            Tokenizer::from_bytes(read_artifact(&config.tokenizer_path)?)?,
            config.max_seq_len,
        )?;
        let model = BertModel::load(vb, &bert_config)?;

        Ok(LoadedBertEmbeddingModel {
            model,
            tokenizer,
            device: device.clone(),
            inference_batch_size: config.inference_batch_size.max(1),
        })
    }

    fn run_inference(
        &self,
        model: &Self::Model,
        batch: &[TextDocument],
        _inference_args: Option<&InferenceArgs>,
    ) -> beam::Result<Vec<VectorEmbedding>> {
        let mut order: Vec<usize> = (0..batch.len()).collect();
        order.sort_by_key(|&i| Reverse(batch[i].content.chars().count()));

        let mut indexed = order
            .chunks(model.inference_batch_size)
            .map(|chunk| {
                let texts: Vec<&str> = chunk.iter().map(|&i| batch[i].content.as_str()).collect();
                model
                    .embed(&texts)
                    .map(|vectors| chunk.iter().copied().zip(vectors).collect::<Vec<_>>())
            })
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        indexed.sort_unstable_by_key(|(i, _)| *i);

        Ok(batch
            .iter()
            .zip(indexed)
            .map(|(doc, (_, embedding))| VectorEmbedding {
                doc_id: doc.doc_id.clone(),
                embedding,
            })
            .collect())
    }
}

/// A [`ModelHandler`] for transformer-based text embeddings powered by Hugging Face Candle.
///
/// This is a specialized, ready-to-use [`CandleModelHandler`] using [`BertEmbeddingAdapter`].
#[derive(Clone, Debug)]
pub struct BertEmbeddingModelHandler {
    inner: CandleModelHandler<TextDocument, VectorEmbedding, BertEmbeddingAdapter>,
}

impl BertEmbeddingModelHandler {
    /// Creates a new `BertEmbeddingModelHandler`.
    pub fn new(config: CandleConfig) -> Self {
        Self {
            inner: CandleModelHandler::new(config, BertEmbeddingAdapter),
        }
    }

    /// Sets the model identifier.
    pub fn with_model_id(mut self, id: impl Into<String>) -> Self {
        self.inner = self.inner.with_model_id(id);
        self
    }

    /// Returns a reference to the underlying Candle configuration.
    pub fn config(&self) -> &CandleConfig {
        self.inner.config()
    }
}

impl ModelHandler<TextDocument, VectorEmbedding> for BertEmbeddingModelHandler {
    type Model = Arc<LoadedBertEmbeddingModel>;
    type Batch = Vec<TextDocument>;
    type Converter = VecBatchConverter<TextDocument>;

    fn load_model(&self) -> beam::Result<Self::Model> {
        self.inner.load_model()
    }

    /// Embeds the batch like `SentenceTransformer.encode`: texts are sorted by descending
    /// length, embedded in chunks of `inference_batch_size`, and returned in input order.
    fn run_inference(
        &self,
        batch: &Self::Batch,
        model: &Self::Model,
        inference_args: Option<&InferenceArgs>,
    ) -> beam::Result<Vec<VectorEmbedding>> {
        self.inner.run_inference(batch, model, inference_args)
    }

    fn get_batch_converter(&self) -> Self::Converter {
        self.inner.get_batch_converter()
    }

    fn get_batch_bounds(&self) -> BatchBounds {
        self.inner.get_batch_bounds()
    }

    fn model_id(&self) -> Option<String> {
        self.inner.model_id()
    }
}
