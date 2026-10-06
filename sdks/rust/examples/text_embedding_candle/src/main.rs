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

//! Main binary entry point for the Text Embedding Candle example pipeline.
//!
//! To run locally on the Prism runner with CPU (artifact paths may be local or `gs://`):
//! ```text
//! cargo run -p text_embedding_candle -- --runner=prism \
//!   --input=/path/to/sentences.txt --output=/tmp/embeddings \
//!   --weights_path=/path/to/model.safetensors \
//!   --config_path=/path/to/config.json \
//!   --tokenizer_path=/path/to/tokenizer.json
//! ```
//!
//! To run with GPU acceleration (build with `--features cuda`):
//! ```text
//! cargo run -p text_embedding_candle --features cuda -- --runner=prism --device=cuda ...
//! ```

use text_embedding_candle::{EmbeddingArgs, build_pipeline};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    beam::harness::init_logging();

    let (options, args) = beam::options::parse::<EmbeddingArgs>();
    let pipeline = build_pipeline(&options, &args);

    tracing::info!(
        "Running text_embedding_candle pipeline on {}...",
        args.input
    );
    let result = pipeline.run().await?;
    tracing::info!(
        "Text Embedding pipeline completed with state: {}",
        result.state
    );

    Ok(())
}
