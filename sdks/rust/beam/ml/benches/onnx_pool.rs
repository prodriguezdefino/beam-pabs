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

//! Concurrent 4-thread throughput of [`OnnxSessionPool`] with 1, 2 and 4 sessions.
//!
//! Each session runs with `intra_threads = 1` and `inter_threads = 1`. Requires
//! `libonnxruntime` (`ORT_DYLIB_PATH`, a standard system install, or
//! `onnx-download-binaries`).
//!
//! ```text
//! ./gradlew :sdks:rust:harnessBenchmark -Ppkg=apache-beam-ml -Pbench=onnx_pool
//! ```

use std::hint::black_box;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;

use beam_ml::ModelHandler;
use beam_ml::onnx::{OnnxAdapter, OnnxConfig, OnnxModelHandler, OnnxSessionPool};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use ort::session::{SessionInputs, SessionOutputs};

const THREADS: usize = 4;
const BATCH_LEN: usize = 65_536;
const BATCHES_PER_THREAD: usize = 8;

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

/// Persistent worker threads synchronized per Criterion iteration to exclude thread spawn cost.
struct BenchWorkers {
    start: Arc<Barrier>,
    done: Arc<Barrier>,
    stop: Arc<AtomicBool>,
    handles: Vec<thread::JoinHandle<()>>,
}

impl BenchWorkers {
    fn spawn(
        handler: &OnnxModelHandler<f64, f64, NegAdapter>,
        model: &Arc<OnnxSessionPool>,
        batch: &[f64],
    ) -> Self {
        let start = Arc::new(Barrier::new(THREADS + 1));
        let done = Arc::new(Barrier::new(THREADS + 1));
        let stop = Arc::new(AtomicBool::new(false));
        let handles = (0..THREADS)
            .map(|_| {
                let handler = handler.clone();
                let model = Arc::clone(model);
                let batch = batch.to_vec();
                let start = Arc::clone(&start);
                let done = Arc::clone(&done);
                let stop = Arc::clone(&stop);
                thread::spawn(move || {
                    loop {
                        start.wait();
                        if stop.load(Ordering::Acquire) {
                            break;
                        }
                        for _ in 0..BATCHES_PER_THREAD {
                            let out = handler
                                .run_inference(&batch, &model, None)
                                .expect("ONNX inference");
                            black_box(out);
                        }
                        done.wait();
                    }
                })
            })
            .collect();
        Self {
            start,
            done,
            stop,
            handles,
        }
    }

    fn run_round(&self) {
        self.start.wait();
        self.done.wait();
    }
}

impl Drop for BenchWorkers {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        self.start.wait();
        for handle in self.handles.drain(..) {
            let _ = handle.join();
        }
    }
}

fn onnx_session_pool(c: &mut Criterion) {
    let model_path = write_model("bench_pool", &neg_model());
    let dylib = onnxruntime_dylib();
    let batch: Vec<f64> = (0..BATCH_LEN).map(|i| i as f64).collect();

    let mut group = c.benchmark_group("onnx_session_pool");
    group.throughput(Throughput::Elements(
        (THREADS * BATCHES_PER_THREAD * BATCH_LEN) as u64,
    ));

    for sessions in [1, 2, 4] {
        let mut config = OnnxConfig::new(&model_path)
            .with_sessions(sessions)
            .with_intra_threads(1);
        config.inter_threads = 1;
        if let Some(dylib) = &dylib {
            config = config.with_dylib_path(dylib);
        }
        let handler = OnnxModelHandler::new(config, NegAdapter);
        let model = Arc::new(
            handler
                .load_model()
                .expect("loading ONNX model (set ORT_DYLIB_PATH)"),
        );
        let workers = BenchWorkers::spawn(&handler, &model, &batch);

        group.bench_function(BenchmarkId::new("sessions", sessions), |b| {
            b.iter(|| workers.run_round());
        });
    }

    group.finish();
}

criterion_group!(benches, onnx_session_pool);
criterion_main!(benches);
