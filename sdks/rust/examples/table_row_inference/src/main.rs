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

//! Table row inference with ONNX Runtime.
//!
//! Run on Prism, the default runner, with ONNX Runtime from `ORT_DYLIB_PATH`:
//!
//! ```text
//! ORT_DYLIB_PATH=/path/to/libonnxruntime.dylib cargo run -p table_row_inference -- \
//!   --input=rows.jsonl --model_path=table_row_rf.onnx --output=/tmp/predictions
//! ```

use table_row_inference::{Args, build_pipeline};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    beam::logging::init_logging();

    let (options, args) = beam::options::parse::<Args>();
    build_pipeline(&options, &args).run().await?;

    Ok(())
}
