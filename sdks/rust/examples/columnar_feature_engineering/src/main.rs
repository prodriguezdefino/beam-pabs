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

//! Main entry point for the table row inference pipeline.
//!
//! To run locally on the Prism runner, with ONNX Runtime loaded from `ORT_DYLIB_PATH`:
//! ```text
//! ORT_DYLIB_PATH=/path/to/libonnxruntime.dylib cargo run -p columnar_feature_engineering -- \
//!   --runner=prism --input=rows.jsonl --model_path=table_row_rf.onnx --output=/tmp/predictions
//! ```

use columnar_feature_engineering::{TableRowInferenceArgs, build_pipeline};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    beam::harness::init_logging();

    let (options, args) = beam::options::parse::<TableRowInferenceArgs>();
    let pipeline = build_pipeline(&options, &args);

    tracing::info!(
        "Running table row inference on {} with model {}...",
        args.input,
        args.model_path
    );
    let result = pipeline.run().await?;
    tracing::info!(
        "Table row inference pipeline completed with state: {}",
        result.state
    );

    Ok(())
}
