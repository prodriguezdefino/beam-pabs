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

//! Text embeddings with Hugging Face Candle.
//!
//! Run on Prism, the default runner, on the CPU. The model files can be local or on `gs://`:
//! ```text
//! cargo run -p text_embedding_candle -- \
//!   --input=/path/to/sentences.txt --output=/tmp/embeddings \
//!   --weights_path=/path/to/model.safetensors \
//!   --config_path=/path/to/config.json \
//!   --tokenizer_path=/path/to/tokenizer.json
//! ```
//!
//! To run on a GPU, build with `--features cuda`:
//! ```text
//! cargo run -p text_embedding_candle --features cuda -- --device=cuda ...
//! ```

use text_embedding_candle::{Args, build_pipeline};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    beam::logging::init_logging();

    let (options, args) = beam::options::parse::<Args>();
    build_pipeline(&options, &args).run().await?;

    Ok(())
}
