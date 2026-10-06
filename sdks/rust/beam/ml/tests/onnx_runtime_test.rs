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

//! [`OnnxModelHandler`] tests against a real ONNX Runtime session.
//!
//! The model is a hand-encoded one-node `y = Neg(x)` graph. Tests are skipped when no
//! `libonnxruntime` is found (`ORT_DYLIB_PATH` or a standard install location).

#![cfg(feature = "onnx")]

use std::path::PathBuf;

use beam_ml::handler::ModelHandler;
use beam_ml::onnx::{OnnxAdapter, OnnxConfig, OnnxExecutionProvider, OnnxModelHandler};
use ort::session::{SessionInputs, SessionOutputs};

fn onnxruntime_dylib() -> Option<PathBuf> {
    std::env::var_os("ORT_DYLIB_PATH")
        .map(PathBuf::from)
        .into_iter()
        .chain(
            [
                "/opt/homebrew/lib/libonnxruntime.dylib",
                "/usr/local/lib/libonnxruntime.dylib",
                "/usr/local/lib/libonnxruntime.so",
                "/usr/lib/libonnxruntime.so",
                "/usr/lib/x86_64-linux-gnu/libonnxruntime.so",
                "/usr/lib/aarch64-linux-gnu/libonnxruntime.so",
            ]
            .map(PathBuf::from),
        )
        .find(|path| path.is_file())
}

macro_rules! require_onnxruntime {
    () => {
        match onnxruntime_dylib() {
            Some(dylib) => dylib,
            None => {
                eprintln!("skipping: libonnxruntime not found");
                return;
            }
        }
    };
}

fn varint(mut value: u64, out: &mut Vec<u8>) {
    while value >= 0x80 {
        out.push((value as u8) | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
}

fn int_field(tag: u64, value: u64) -> Vec<u8> {
    let mut out = Vec::new();
    varint(tag << 3, &mut out);
    varint(value, &mut out);
    out
}

fn bytes_field(tag: u64, bytes: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    varint((tag << 3) | 2, &mut out);
    varint(bytes.len() as u64, &mut out);
    out.extend_from_slice(bytes);
    out
}

/// `ValueInfoProto` for a 1-D double tensor of dynamic length `N`.
fn double_vector(name: &str) -> Vec<u8> {
    let dim = bytes_field(2, b"N");
    let shape = bytes_field(1, &dim);
    let tensor_type = [int_field(1, 11), bytes_field(2, &shape)].concat();
    let type_proto = bytes_field(1, &tensor_type);
    [bytes_field(1, name.as_bytes()), bytes_field(2, &type_proto)].concat()
}

/// Serialized `ModelProto` (IR 8, opset 13) computing `y = Neg(x)` over doubles.
fn neg_model() -> Vec<u8> {
    let node = [
        bytes_field(1, b"x"),
        bytes_field(2, b"y"),
        bytes_field(4, b"Neg"),
    ]
    .concat();
    let graph = [
        bytes_field(1, &node),
        bytes_field(2, b"neg"),
        bytes_field(11, &double_vector("x")),
        bytes_field(12, &double_vector("y")),
    ]
    .concat();
    let opset = [bytes_field(1, b""), int_field(2, 13)].concat();
    [
        int_field(1, 8),
        bytes_field(7, &graph),
        bytes_field(8, &opset),
    ]
    .concat()
}

fn write_model(name: &str, bytes: &[u8]) -> String {
    let path =
        std::env::temp_dir().join(format!("beam_ml_onnx_{name}_{}.onnx", std::process::id()));
    std::fs::write(&path, bytes).expect("writing model");
    path.to_string_lossy().into_owned()
}

#[derive(Clone, Debug)]
struct NegAdapter;

impl OnnxAdapter<f64, f64> for NegAdapter {
    fn prepare_inputs<'a>(&self, batch: &'a [f64]) -> beam::Result<SessionInputs<'a, 'a>> {
        let tensor = ort::value::Tensor::from_array(([batch.len()], batch.to_vec()))?;
        Ok(ort::inputs!["x" => tensor].into())
    }

    fn parse_outputs(&self, _batch: &[f64], outputs: SessionOutputs<'_>) -> beam::Result<Vec<f64>> {
        let y = outputs.get("y").ok_or("model has no 'y' output")?;
        let (_, values) = y.try_extract_tensor::<f64>()?;
        Ok(values.to_vec())
    }
}

#[derive(Clone, Debug)]
struct FailingAdapter;

impl OnnxAdapter<f64, f64> for FailingAdapter {
    fn prepare_inputs<'a>(&self, _batch: &'a [f64]) -> beam::Result<SessionInputs<'a, 'a>> {
        Err("prepare failed".into())
    }

    fn parse_outputs(
        &self,
        _batch: &[f64],
        _outputs: SessionOutputs<'_>,
    ) -> beam::Result<Vec<f64>> {
        Ok(vec![])
    }
}

#[test]
fn test_cpu_session_runs_model() {
    let dylib = require_onnxruntime!();
    let mut config = OnnxConfig::new(write_model("cpu", &neg_model()))
        .with_dylib_path(dylib)
        .with_intra_threads(1);
    config.inter_threads = 1;
    let handler = OnnxModelHandler::new(config, NegAdapter);
    let model = handler.load_model().expect("loading model on CPU");

    let outputs = handler
        .run_inference(&vec![1.5, -2.0, 0.0], &model, None)
        .expect("inference");
    assert_eq!(outputs, [-1.5, 2.0, -0.0]);
    assert!(
        handler
            .run_inference(&vec![], &model, None)
            .expect("empty batch")
            .is_empty()
    );
}

#[test]
fn test_run_inference_propagates_adapter_errors() {
    let dylib = require_onnxruntime!();
    let config = OnnxConfig::new(write_model("failing", &neg_model())).with_dylib_path(dylib);
    let handler = OnnxModelHandler::new(config, FailingAdapter);
    let model = handler.load_model().expect("loading model");
    let err = handler
        .run_inference(&vec![1.0], &model, None)
        .expect_err("adapter error propagates");
    assert!(err.to_string().contains("prepare failed"), "{err}");
}

/// An unavailable accelerator falls back to CPU only when allowed.
#[cfg(not(feature = "onnx-cuda"))]
#[test]
fn test_cpu_fallback_requires_flag() {
    let dylib = require_onnxruntime!();
    let path = write_model("fallback", &neg_model());
    let config = |allow| {
        OnnxConfig::new(&path)
            .with_dylib_path(&dylib)
            .with_execution_provider(OnnxExecutionProvider::Cuda { device_id: 0 })
            .with_cpu_fallback(allow)
    };

    let handler = OnnxModelHandler::new(config(true), NegAdapter);
    let model = handler.load_model().expect("falls back to CPU");
    assert_eq!(
        handler.run_inference(&vec![4.0], &model, None).unwrap(),
        [-4.0]
    );

    let Err(err) = OnnxModelHandler::new(config(false), NegAdapter).load_model() else {
        panic!("CUDA without fallback must fail");
    };
    assert!(err.to_string().contains("'onnx-cuda' feature"), "{err}");
}

#[test]
fn test_invalid_model_reports_cpu_session_error() {
    let dylib = require_onnxruntime!();
    let config = OnnxConfig::new(write_model("invalid", b"not an onnx model"))
        .with_dylib_path(dylib)
        .with_cpu_fallback(true);
    let Err(err) = OnnxModelHandler::new(config, NegAdapter).load_model() else {
        panic!("an invalid model must fail to load");
    };
    assert!(
        err.to_string()
            .contains("failed to create ONNX Runtime session on CPU"),
        "{err}"
    );
}
