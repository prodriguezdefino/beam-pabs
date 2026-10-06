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

//! Unit tests for [`OnnxModelHandler`], [`OnnxConfig`], and execution providers.

#![cfg(feature = "onnx")]

use std::path::PathBuf;

use beam_ml::handler::{BatchBounds, ModelHandler};
use beam_ml::onnx::{
    OnnxAdapter, OnnxConfig, OnnxDeviceKind, OnnxDeviceOptions, OnnxError, OnnxExecutionProvider,
    OnnxModelHandler,
};
use ort::session::{SessionInputs, SessionOutputs};

#[test]
fn test_onnx_config_builder() {
    let config = OnnxConfig::new("gs://bucket/models/mobilenetv2.onnx")
        .with_dylib_path("/usr/lib/libonnxruntime.so")
        .with_execution_provider(OnnxExecutionProvider::Cuda { device_id: 0 })
        .with_intra_threads(4)
        .with_cpu_fallback(true)
        .with_batch_bounds(BatchBounds::new(4, 32));

    assert_eq!(config.model_path, "gs://bucket/models/mobilenetv2.onnx");
    assert_eq!(
        config.dylib_path,
        Some(PathBuf::from("/usr/lib/libonnxruntime.so"))
    );
    assert_eq!(
        config.execution_provider,
        OnnxExecutionProvider::Cuda { device_id: 0 }
    );
    assert_eq!(config.intra_threads, 4);
    assert!(config.allow_cpu_fallback);
    assert_eq!(config.batch_bounds.min_batch_size, 4);
    assert_eq!(config.batch_bounds.max_batch_size, 32);
}

#[test]
fn test_onnx_config_defaults_disallow_cpu_fallback() {
    let config = OnnxConfig::new("model.onnx");
    assert!(!config.allow_cpu_fallback);
    assert_eq!(config.execution_provider, OnnxExecutionProvider::Cpu);
}

/// A pipeline's arguments embedding the shared device options.
#[derive(clap::Args, serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
struct PipelineArgs {
    #[arg(long, default_value = "model.onnx")]
    model_path: String,
    #[command(flatten)]
    #[serde(flatten)]
    accelerator: OnnxDeviceOptions,
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
fn test_device_options_defaults() {
    let args = parse_args(&[]);
    assert_eq!(args.accelerator, OnnxDeviceOptions::default());
    let config = OnnxConfig::new(&args.model_path).with_device_options(&args.accelerator);
    assert_eq!(config.execution_provider, OnnxExecutionProvider::Cpu);
    assert!(!config.allow_cpu_fallback);
    assert_eq!(config.dylib_path, None);
}

#[test]
fn test_device_options_flags_configure_onnx() {
    let args = parse_args(&[
        "--device=tensorrt",
        "--device_id=2",
        "--allow_cpu_fallback",
        "--dylib_path=/opt/ort/libonnxruntime.so",
    ]);
    assert_eq!(args.accelerator.device, OnnxDeviceKind::Tensorrt);
    let config = OnnxConfig::new(&args.model_path).with_device_options(&args.accelerator);
    assert_eq!(
        config.execution_provider,
        OnnxExecutionProvider::TensorRt { device_id: 2 }
    );
    assert!(config.allow_cpu_fallback);
    assert_eq!(
        config.dylib_path,
        Some(PathBuf::from("/opt/ort/libonnxruntime.so"))
    );
}

#[test]
fn test_device_options_keep_configured_dylib_when_flag_absent() {
    let config = OnnxConfig::new("model.onnx")
        .with_dylib_path("/usr/lib/libonnxruntime.so")
        .with_device_options(&parse_args(&["--device=coreml"]).accelerator);
    assert_eq!(config.execution_provider, OnnxExecutionProvider::CoreMl);
    assert_eq!(
        config.dylib_path,
        Some(PathBuf::from("/usr/lib/libonnxruntime.so"))
    );
}

#[test]
fn test_flattened_device_options_round_trip_through_json() {
    let args = parse_args(&[
        "--device=cuda",
        "--device_id=1",
        "--allow_cpu_fallback=false",
    ]);
    let json = serde_json::to_value(&args).unwrap();
    assert_eq!(json["device"], "cuda");
    assert_eq!(json["device_id"], 1);
    assert_eq!(serde_json::from_value::<PipelineArgs>(json).unwrap(), args);
}

#[derive(Clone, Debug)]
struct TestOnnxAdapter;

impl OnnxAdapter<String, String> for TestOnnxAdapter {
    fn prepare_inputs<'a>(&self, _batch: &'a [String]) -> beam::Result<SessionInputs<'a, 'a>> {
        Err("Mock prepare inputs".into())
    }

    fn parse_outputs(
        &self,
        _batch: &[String],
        _outputs: SessionOutputs<'_>,
    ) -> beam::Result<Vec<String>> {
        Ok(vec![])
    }
}

#[test]
fn test_onnx_model_handler_structure() {
    let config = OnnxConfig::new("dummy.onnx");
    let handler = OnnxModelHandler::new(config, TestOnnxAdapter).with_model_id("mobilenetv2");

    assert_eq!(handler.model_id(), Some("mobilenetv2".to_string()));
    assert_eq!(handler.get_batch_bounds().min_batch_size, 1);
    assert_eq!(handler.get_batch_bounds().max_batch_size, 32);
}

#[test]
fn test_missing_model_is_an_error_even_with_cpu_fallback() {
    for allow_cpu_fallback in [false, true] {
        let config = OnnxConfig::new("/nonexistent/dir/model.onnx")
            .with_execution_provider(OnnxExecutionProvider::Cuda { device_id: 0 })
            .with_cpu_fallback(allow_cpu_fallback);
        let handler = OnnxModelHandler::new(config, TestOnnxAdapter);
        let Err(err) = handler.load_model() else {
            panic!("a missing model must fail to load");
        };
        assert!(
            err.to_string().contains("/nonexistent/dir/model.onnx"),
            "error should name the missing model: {err}"
        );
    }
}

#[test]
fn test_unregistered_scheme_is_an_error() {
    let handler = OnnxModelHandler::new(
        OnnxConfig::new("unknownscheme://bucket/model.onnx"),
        TestOnnxAdapter,
    );
    let Err(err) = handler.load_model() else {
        panic!("an unregistered scheme must fail to load");
    };
    assert!(err.to_string().contains("unknownscheme"), "{err}");
}

#[test]
fn test_execution_provider_display() {
    let cases = [
        (OnnxExecutionProvider::Cpu, "CPU"),
        (
            OnnxExecutionProvider::Cuda { device_id: 1 },
            "CUDA(device 1)",
        ),
        (
            OnnxExecutionProvider::TensorRt { device_id: 2 },
            "TensorRT(device 2)",
        ),
        (OnnxExecutionProvider::CoreMl, "CoreML"),
    ];
    for (provider, expected) in cases {
        assert_eq!(provider.to_string(), expected);
    }
}

#[test]
fn test_cpu_uses_builtin_provider() {
    assert!(!OnnxExecutionProvider::Cpu.registers_accelerator().unwrap());
}

#[test]
fn test_gpu_ordinal_out_of_range_is_rejected() {
    for provider in [
        OnnxExecutionProvider::Cuda {
            device_id: usize::MAX,
        },
        OnnxExecutionProvider::TensorRt {
            device_id: usize::MAX,
        },
    ] {
        let err = provider
            .registers_accelerator()
            .expect_err("ordinal overflow");
        assert!(
            matches!(err, OnnxError::InvalidDeviceId(usize::MAX)),
            "{provider}: {err}"
        );
    }
}

#[cfg(not(feature = "onnx-cuda"))]
#[test]
fn test_cuda_not_compiled_is_an_error() {
    let err = OnnxExecutionProvider::Cuda { device_id: 0 }
        .registers_accelerator()
        .expect_err("CUDA provider not compiled");
    assert!(
        matches!(
            err,
            OnnxError::ProviderNotCompiled {
                provider: "CUDA",
                feature: "onnx-cuda"
            }
        ),
        "{err}"
    );
}

#[cfg(not(feature = "onnx-tensorrt"))]
#[test]
fn test_tensorrt_not_compiled_is_an_error() {
    let err = OnnxExecutionProvider::TensorRt { device_id: 0 }
        .registers_accelerator()
        .expect_err("TensorRT provider not compiled");
    assert!(err.to_string().contains("onnx-tensorrt"), "{err}");
}

#[cfg(not(feature = "onnx-coreml"))]
#[test]
fn test_coreml_not_compiled_is_an_error() {
    let err = OnnxExecutionProvider::CoreMl
        .registers_accelerator()
        .expect_err("CoreML provider not compiled");
    assert!(err.to_string().contains("onnx-coreml"), "{err}");
}

#[test]
fn test_unloadable_dylib_is_an_error() {
    let handler = OnnxModelHandler::new(
        OnnxConfig::new("model.onnx").with_dylib_path("/nonexistent/libonnxruntime.so"),
        TestOnnxAdapter,
    );
    let Err(err) = handler.load_model() else {
        panic!("a missing dylib must fail to load");
    };
    assert!(
        err.to_string()
            .contains("failed to load ONNX Runtime from \"/nonexistent/libonnxruntime.so\""),
        "{err}"
    );
}
