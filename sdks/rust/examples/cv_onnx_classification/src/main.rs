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

//! Binary for the Computer Vision ONNX image classification example.
//!
//! To run locally on the Prism runner:
//! ```text
//! cargo run -p cv_onnx_classification -- --runner=prism \
//!   --input=gs://apache-beam-ml/testing/inputs/it_mobilenetv2_imagenet_validation_inputs.txt \
//!   --model_path=/path/to/mobilenet_v2_torchvision.onnx \
//!   --dylib_path=/path/to/libonnxruntime.dylib
//! ```

use cv_onnx_classification::{CvOnnxArgs, build_pipeline};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    beam::harness::init_logging();

    let (options, args) = beam::options::parse::<CvOnnxArgs>();
    let pipeline = build_pipeline(&options, &args);

    tracing::info!(
        "Running cv_onnx_classification pipeline on {}...",
        args.input
    );
    let result = pipeline.run().await?;
    tracing::info!(
        "CV ONNX Classification pipeline completed with state: {}",
        result.state
    );

    Ok(())
}
