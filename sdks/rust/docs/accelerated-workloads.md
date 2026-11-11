<!--
    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.
-->

# Machine Learning and Vectorized Execution

This page explains how the Rust SDK executes machine learning inference (`beam::ml`) and vectorized batch transforms (`BatchElements`, `BatchedDoFn`, and Apache Arrow). For runnable pipelines, see the [examples catalogue](../examples/README.md).

## Architecture Overview

A Rust worker is one native process with many bundle threads:

- **Element-wise pipeline graph**: A `PCollection<T>` always carries individual elements. Each element has one timestamp, one window, and one pane.
- **Window-preserving micro-batching**: Inside a bundle, `BatchElements` buffers elements per `(window, pane)` pair. It emits a typed batch (`Vec<T>` or `ArrowRecordBatch`) when the buffer reaches a size or duration limit, or when the bundle finishes.
- **In-process models**: `RunInference` loads the model in `setup` and keeps it as an `Arc<Model>`. The model runs in the worker process, with no inter-process communication. [Process-wide sharing](#process-wide-sharing-workermodelcache) tells how many model copies a worker holds.
- **Unrolling**: After the forward pass or vectorized kernel finishes, the transform splits the batch back into individual outputs. Each output keeps the window, pane, and minimum timestamp of its batch.

```text
PCollection<In> (element, timestamp, window, pane)
   │
   ▼
BatchElements::with_converter("{name}/BatchElements")
   ├── Buffers per (encoded_window, PaneInfo)
   ├── Tracks min_timestamp across buffered elements
   └── Flushes on max_batch_size, (min_batch_size + max_batch_duration), or finish_bundle
   │
   ▼
PCollection<H::Batch> (Vec<In>, ArrowRecordBatch, ...)
   │
   ▼
RunInferenceDoFn / BatchedDoFn ("{name}/Predict")
   ├── setup(): the model of handler.model_id() from WorkerModelCache, kept as Arc<Model>
   ├── process_element(): runs handler.run_inference(&batch, &model, args)
   └── converter.explode(batch): zips inputs with predictions
   │
   ▼
PCollection<PredictionResult<In, Out>>
```

---

## Micro-Batching and Vectorized Execution

### `BatchElements` and `ExplodeBatch`

[`BatchElements`](../beam/core/src/transforms/batch_elements.rs) groups elements of a `PCollection<E>` into batches of type `B` using a [`BatchConverter<E, B>`](../beam/core/src/transforms/dofn/batch.rs).

- **Per-window and per-pane isolation**: `BatchElements` keeps a separate buffer for each `(encoded_window, PaneInfo)` key. A batch never mixes elements from different windows or trigger firings.
- **Watermark safety**: Each buffer records the minimum event timestamp (`min_timestamp`) of its elements. When `BatchElements` emits a batch, it assigns `min_timestamp` to the batch header. The batch timestamp is never later than any element in the batch.
- **Flush rules**: A buffer flushes during `process_element` when `buffer_len >= max_batch_size`. It also flushes when `buffer_len >= min_batch_size` and the time since buffer creation exceeds `max_batch_duration`. `finish_bundle` flushes all remaining open buffers.

[`ExplodeBatch`](../beam/core/src/transforms/batch_elements.rs) reverses this step. It calls `BatchConverter::explode` and emits each element with the window, pane, and timestamp of the batch.

### `BatchConverter` and Apache Arrow

[`BatchConverter<E, B>`](../beam/core/src/transforms/dofn/batch.rs) defines how elements accumulate into a batch and split back into elements:

| Method | Purpose |
|---|---|
| `create_buffer(&self) -> Self::Buffer` | Allocates an empty accumulation buffer. |
| `push(&self, &mut Buffer, E) -> Result` | Appends one element to the buffer. |
| `buffer_len(&self, &Buffer) -> usize` | Returns the number of buffered elements. |
| `finish_batch(&self, Buffer) -> Result<B>` | Converts the buffer into the batch type `B`. |
| `explode(&self, B) -> Result<Vec<E>>` | Splits a batch `B` back into `Vec<E>`. |

The SDK provides two converters:

- [`VecBatchConverter<E>`](../beam/core/src/transforms/dofn/batch.rs): collects elements into a `Vec<E>`. `BatchElements::new(min, max)` uses this converter.
- [`ArrowBeamRowBatchConverter<T>`](../beam/io/arrow/src/batch.rs) (`arrow` feature): converts `#[derive(BeamRow)]` structs into an [`ArrowRecordBatch`](../beam/io/arrow/src/batch.rs), and explodes it back into `Vec<T>`. `ArrowRecordBatch` wraps `arrow::record_batch::RecordBatch` and has a Beam coder.

> [!IMPORTANT]
> Do not use `ArrowRecordBatch` as a long-lived `PCollection` element type across shuffles or windowing transforms. Build it inside `BatchElements` or a `BatchedDoFn` for vectorized kernels, then unroll it back into typed elements.

### `BatchedDoFn`

A [`BatchedDoFn`](../beam/core/src/transforms/dofn/batch.rs) processes a full batch in one call (`process_batch`). The fluent `.par_do_batch_elementwise(name, max_batch_size, do_fn)` method chains `BatchElements`, `BatchedDoFnAdapter`, and `ExplodeBatch`:

```rust
use arrow::array::Int32Array;
use beam::io::arrow::batch::{ArrowBeamRowBatchConverter, ArrowRecordBatch};
use beam::prelude::*;
use beam::transforms::{BatchConverter, BatchedDoFn};

#[derive(Clone, Default)]
pub struct FilterOkLogs {
    converter: ArrowBeamRowBatchConverter<HttpLog>,
}

impl BatchedDoFn for FilterOkLogs {
    type InBatch = Vec<HttpLog>;
    type OutBatch = Vec<HttpLog>;

    fn process_batch(
        &mut self,
        logs: Vec<HttpLog>,
        ctx: &mut ProcessContext<'_, Vec<HttpLog>>,
    ) -> beam::Result {
        let mut buf = self.converter.create_buffer();
        for log in logs {
            self.converter.push(&mut buf, log)?;
        }
        let batch: ArrowRecordBatch = self.converter.finish_batch(buf)?;

        let status = batch
            .column_by_name("status")
            .ok_or("missing status column")?
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or("status is not Int32")?;
        let mask = arrow::compute::kernels::cmp::eq(status, &Int32Array::new_scalar(200))?;
        let filtered = arrow::compute::filter_record_batch(&batch, &mask)?;

        ctx.emit(self.converter.explode(filtered.into())?)
    }
}

let ok_logs = logs.par_do_batch_elementwise("FilterOkLogs", 1024, FilterOkLogs::default());
```

---

## `RunInference` and `ModelHandler`

### Composite Expansion

[`RunInference<In, Out, H>`](../beam/ml/src/run_inference.rs) is a composite `PTransform` over a [`ModelHandler<In, Out>`](../beam/ml/src/handler.rs). In `expand`, it reads `BatchBounds` and `H::Converter` from the handler and builds two stages:

1. `BatchElements::with_converter("{name}/BatchElements", min_batch_size, max_batch_size, converter)` (plus `max_batch_duration` when set).
2. `ParDo::new("{name}/Predict", RunInferenceDoFn)`:
   - `setup(&mut self)` calls `handler.load_model()`, or gets the model from `WorkerModelCache` when the handler has a model id, and stores `Arc<H::Model>`.
   - `process_element(&mut self, batch, ctx)` calls `handler.run_inference(&batch, &model, inference_args)` and `converter.explode(batch)`.
   - It checks that `predictions.len() == inputs.len()`, then emits one [`PredictionResult<In, Out>`](../beam/ml/src/prediction.rs) (`{ input, output }`) per element.

```rust
use beam::ml::{InferenceArgs, PredictionResult, RunInference};
use beam::prelude::*;

let predictions: PCollection<PredictionResult<In, Out>> = inputs.apply("Predict", RunInference::new(handler)
        .with_inference_args(InferenceArgs::new().with("temperature", "0.0")),
);
```

### Dead-Letter Outputs (`RunInferenceMulti`)

Calling `.with_exception_handling()` converts `RunInference` into [`RunInferenceMulti<In, Out, H>`](../beam/ml/src/run_inference.rs). Its second stage is a `ParDo` (`"{name}/PredictWithDLQ"`) with the output type `Result<PredictionResult<In, Out>, Failure<In>>`. The transform returns `WithFailures<PredictionResult<In, Out>, Failure<In>>`.

`RunInferenceMultiDoFn` calls `handler.run_inference_per_element`, which gives one result for each element. It emits each element with its own error as `Err(Failure)` to `failures`, and the other elements of the batch to `output`. If the whole batch fails, every element goes to `failures` with that error. The default `run_inference_per_element` calls `run_inference`. `RemoteModelHandler` overrides it, so one failed request does not fail the other requests of its batch. The bundle does not fail.

```rust
let result = inputs.apply("Predict", RunInference::new(handler).with_exception_handling());
result.output.apply(/* downstream transform */);
result.failures.apply(/* dead-letter sink */);
```

### The `ModelHandler` Trait and `KeyedModelHandler`

[`ModelHandler<In, Out>`](../beam/ml/src/handler.rs) is the extension point for all inference engines:

```rust
pub trait ModelHandler<In: DefaultCoder, Out: DefaultCoder>: Clone + Send + Sync + 'static {
    type Model: Send + Sync + 'static;
    type Batch: DefaultCoder;
    type Converter: BatchConverter<In, Self::Batch>;

    fn load_model(&self) -> beam::Result<Self::Model>;

    fn run_inference(
        &self,
        batch: &Self::Batch,
        model: &Self::Model,
        inference_args: Option<&InferenceArgs>,
    ) -> beam::Result<Vec<Out>>;

    fn get_batch_converter(&self) -> Self::Converter;
    fn get_batch_bounds(&self) -> BatchBounds { BatchBounds::default() }
    fn model_id(&self) -> Option<String> { None }
}
```

- [`BatchBounds`](../beam/ml/src/handler.rs): sets `min_batch_size` (default `1`), `max_batch_size` (default `64`), and `max_batch_duration` (default `Some(50ms)`).
- [`KeyedModelHandler<K, In, Out, H>`](../beam/ml/src/handler.rs): wraps a `ModelHandler<In, Out>` for `(K, In)` pairs. It runs `inner` on the values and keeps the keys. It implements `ModelHandler`, so you can give it to `RunInference`.

---

## Model Lifecycle, Caching, and Artifacts

### Process-Wide Sharing (`WorkerModelCache`)

Each bundle processor has its own copy of the `DoFn` and calls `setup` on it ([`instances.rs`](../beam/harness/src/bundle_processor/chain/instances.rs)). If `handler.model_id()` returns an id, `setup` gets the model from [`WorkerModelCache::global()`](../beam/ml/src/cache.rs), so a worker process loads one model for each id and `Model` type. If it returns `None`, each bundle processor loads its own model.

The built-in handlers give a default id: the model path for `OnnxModelHandler`, the weights path for `CandleModelHandler` and `BertEmbeddingModelHandler`, and the endpoint URL for `RemoteModelHandler`. Handlers with the same id share the model that the first one loaded, with its settings (device, threads, credentials, `max_concurrent_requests`). Call `with_model_id` to load the same artifact with other settings.

`WorkerModelCache` keeps one slot for each key in a copy-on-write map (`arc_swap::ArcSwap`). It takes no lock:

- `get_or_load(key, loader)` reads the map, and adds a slot for a new key. The slot is a `once_cell::sync::OnceCell`: concurrent calls for the same key run `loader()` once, and a load never blocks other keys. A failed load is not cached, so the next call loads again. If the slot holds a model of another type, it returns an error.
- `update_model(key, new_model)` replaces the model of `key`. Threads that hold the previous `Arc<M>` keep it.

### Loading Model Artifacts (`beam::ml::artifact`)

[`beam::ml::artifact::read_artifact(path)`](../beam/ml/src/artifact.rs) reads model weights, tokenizers, and config files through the Beam filesystem registry (`file::filesystem::get_filesystem`).

- Local paths and `file://` URIs always work.
- `gs://` URIs work when the `gcp-gcs` feature is enabled.
- Both Candle and ONNX handlers call `read_artifact` during `load_model`, so models load from local disk or Google Cloud Storage with the same configuration.

---

## Inference Backends

| Backend | Feature Flags | Accelerators | Handler Types |
|---|---|---|---|
| **Hugging Face Candle** | `ml-candle`, `ml-candle-cuda`, `ml-candle-metal` | CPU, CUDA, Metal | [`CandleModelHandler`](../beam/ml/src/candle/mod.rs), [`BertEmbeddingModelHandler`](../beam/ml/src/candle/bert.rs) |
| **ONNX Runtime (`ort`)** | `ml-onnx`, `ml-onnx-dynamic`, `ml-onnx-cuda`, `ml-onnx-tensorrt`, `ml-onnx-coreml`, `ml-onnx-download` | CPU, CUDA, TensorRT, CoreML | [`OnnxModelHandler`](../beam/ml/src/onnx.rs) |
| **Remote Endpoints** | `ml-remote` | HTTP with `reqwest` (Gemini Developer API, Vertex AI, or a custom URL) | [`RemoteModelHandler`](../beam/ml/src/remote/mod.rs), [`GeminiAdapter`](../beam/ml/src/remote/gemini.rs) |
| **Custom** | `ml` | User-defined | Any [`ModelHandler`](../beam/ml/src/handler.rs) implementation |

### Hugging Face Candle (`beam::ml::candle`)

Candle compiles into the worker binary with no Python or C++ runtime.

- [`CandleConfig`](../beam/ml/src/candle/mod.rs): holds `weights_path` (`.safetensors`), `config_path`, `tokenizer_path`, `device`, `allow_cpu_fallback`, `dtype` (`candle_core::DType`), `max_seq_len`, `inference_batch_size`, and `batch_bounds`. The `device` field is a [`CandleDevice`](../beam/ml/src/candle/mod.rs): `Cpu`, `Cuda { device_id }` or `Metal { device_id }`.
- **Device resolution**: `CandleDevice::to_device(allow_cpu_fallback)` opens the requested CUDA or Metal device. The device can fail to start, or the binary can lack the matching feature (`ml-candle-cuda` / `ml-candle-metal`). Then, if `allow_cpu_fallback` is `false`, it returns an error. If `allow_cpu_fallback` is `true`, it logs a warning and uses `Device::Cpu`.
- [`CandleAdapter<In, Out>`](../beam/ml/src/candle/mod.rs): trait with `load_model(&self, device, VarBuilder, &CandleConfig)` and `run_inference(&self, &Model, &[In], Option<&InferenceArgs>)`. Wrap an adapter in [`CandleModelHandler::new(config, adapter)`](../beam/ml/src/candle/mod.rs). Its `load_model` resolves the device, reads the weights with `read_artifact`, and calls the adapter.
- [`BertEmbeddingModelHandler`](../beam/ml/src/candle/bert.rs): built-in handler over [`BertEmbeddingAdapter`](../beam/ml/src/candle/bert.rs). It tokenizes [`TextDocument`](../beam/ml/src/candle/bert.rs) batches and runs the BERT forward pass. Then it applies mean pooling and L2 normalization, and emits [`VectorEmbedding`](../beam/ml/src/candle/bert.rs). The [`text_embedding_candle`](../examples/text_embedding_candle) example uses it.

### ONNX Runtime (`beam::ml::onnx`)

[`OnnxModelHandler<In, Out, A>`](../beam/ml/src/onnx.rs) runs `.onnx` graphs through the `ort` crate:

- [`OnnxConfig`](../beam/ml/src/onnx.rs): sets `model_path`, `execution_provider`, `allow_cpu_fallback`, `intra_threads`, `inter_threads`, `dylib_path`, and `batch_bounds`. The `execution_provider` field is an [`OnnxExecutionProvider`](../beam/ml/src/onnx.rs): `Cpu`, `Cuda { device_id }`, `TensorRt { device_id }` or `CoreMl`.
- **Dynamic library loading**: `apache-beam-ml` always builds `ort` with `load-dynamic`, so `libonnxruntime` is loaded at run time. If `dylib_path` is set, `load_model` initializes `ort` from it (`ort::init_from`). If not, `ort` reads `ORT_DYLIB_PATH`. With `ml-onnx-download`, Cargo downloads Microsoft's prebuilt `libonnxruntime` at build time.
- **Session lifecycle and thread safety**: `OnnxModelHandler::load_model` reads the model bytes with `read_artifact`. It registers the execution provider on the session builder (`error_on_failure`) and commits the session from memory. The handler model is this session, as `Arc<Mutex<ort::session::Session>>`. If the provider fails and `allow_cpu_fallback` is set, it builds a CPU session instead. `run_inference` locks the session mutex for the duration of `session.run(inputs)`.
- [`OnnxAdapter<In, Out>`](../beam/ml/src/onnx.rs): converts `&[In]` into `SessionInputs` (`prepare_inputs`) and extracts `Vec<Out>` from `SessionOutputs` (`parse_outputs`). The [`cv_onnx_classification`](../examples/cv_onnx_classification) and [`table_row_inference`](../examples/README.md#table-row-inference-table_row_inference) examples implement it.

### Device Flags from the Command Line

Flatten [`CandleDeviceOptions`](../beam/ml/src/candle/mod.rs) or [`OnnxDeviceOptions`](../beam/ml/src/onnx.rs) into a pipeline option group to expose `--device`, `--device_id`, `--allow_cpu_fallback`, and (for ONNX) `--dylib_path`:

```rust
use beam::ml::onnx::{OnnxConfig, OnnxDeviceOptions};
use beam::prelude::*;

#[derive(clap::Args, serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct Args {
    #[arg(long)]
    pub model_path: String,
    #[command(flatten)]
    #[serde(flatten)]
    pub device: OnnxDeviceOptions,
}
impl PipelineOptionGroup for Args {}
```

Both attributes are necessary. `#[command(flatten)]` registers the CLI flags with `clap`. `#[serde(flatten)]` keeps the fields in the options snapshot that goes to the workers. Apply them with `config.with_device_options(&args.device)`.

### Remote Endpoints (`beam::ml::remote`)

[`RemoteModelHandler<In, Out, A>`](../beam/ml/src/remote/mod.rs) sends micro-batches to external HTTP model servers:

- [`RemoteConfig`](../beam/ml/src/remote/mod.rs): configures `auth` ([`RemoteAuth`](../beam/ml/src/remote/auth.rs)) and `timeout` (default 60 s). It also sets `max_retries` (default 5), `initial_retry_backoff` (default 5 s), `max_concurrent_requests` (default 64), and `batch_bounds` (default 1..16). The adapter owns the endpoint, for example `GeminiEndpoint`.
- **Concurrency and retries**: `load_model` builds a `RemoteClient` with a pooled `reqwest::Client` and a `Semaphore` capped at `max_concurrent_requests`. Requests run on one process-wide Tokio runtime. `run_inference` sends one request per element concurrently. Each attempt holds a semaphore permit. Each request retries its own retryable errors, up to `max_retries` times. `RemoteInferenceError::is_retryable` marks HTTP 429, 5xx, timeouts and network errors as retryable. The delay is exponential backoff with jitter, at most `max_retry_backoff` (default 60 s), or the retry-after delay of the server, also at most `max_retry_backoff`.
- **Secret resolution**: `RemoteAuth::ApiKey(Secret)` and `RemoteAuth::BearerToken(Secret)` store a [`beam::options::Secret`](../beam/core/src/options/secret.rs) reference (`env:VAR`, `file:/path`, or `gcp:projects/P/secrets/S/versions/V`). The serialized pipeline graph carries only the reference string. The worker resolves the secret during `load_model`. `RemoteAuth::ApplicationDefault` (the default) fetches an OAuth2 token for each request from the GCP metadata server on the worker. If the worker has no credentials, `load_model` fails, and a request without a token fails with `RemoteInferenceError::Authentication`.
- [`RemoteEndpointAdapter<In, Out>`](../beam/ml/src/remote/mod.rs): trait whose `execute` returns a future for one request. [`GeminiAdapter`](../beam/ml/src/remote/gemini.rs) implements it for the Gemini Developer API, Vertex AI, and custom URLs. The [`remote_llm_inference`](../examples/remote_llm_inference) example uses it. A custom adapter must also implement `endpoint`, the URL of its requests, which is the default model id of `RemoteModelHandler`.

---

## Stage Fusion, `Reshuffle`, and Resource Hints

### Breaking Fusion Before Accelerator Stages

Runners fuse adjacent `ParDo` steps into one executable stage. A file read or network fetch can fuse with `RunInference`. Then worker threads block on I/O while they hold the bundle, and the accelerator is idle for longer.

Insert `.reshuffle(..)` before `RunInference` to force a shuffle barrier:

```rust
p.apply("ReadManifest", textio::Read::new(&args.input))
    .reshuffle("SpreadUrls")
    .par_do_fn("FetchAndDecode", fetch_and_decode)
    .reshuffle("DecoupleIoFromGpu")
    .apply("Predict", RunInference::new(handler));
```

### GPU Resource Hints and Containers

Attach [`ResourceHints`](../beam/core/src/pipeline/resources.rs) to `RunInference` (or the pipeline) so runners such as Dataflow provision GPU worker pools:

```rust
use beam::pipeline::ResourceHints;
use beam::transforms::WithResourceHintsExt;

let predictions = inputs.apply("Predict", RunInference::new(handler).with_resource_hints(
        ResourceHints::new()
            .with_accelerator("type:nvidia-tesla-t4;count:1;install-nvidia-driver")
            .with_min_ram_bytes(16 * 1024 * 1024 * 1024),
    ),
);
```

Keep CPU fallback off on production GPU jobs. Then a missing driver or a bad container fails at model load, and the job does not run on CPU. Off is the default: `with_cpu_fallback(false)` on `CandleConfig` / `OnnxConfig`, and no `--allow_cpu_fallback` flag. For CUDA worker images, see the self-contained Dockerfiles of [`cv_onnx_classification`](../examples/cv_onnx_classification/Dockerfile) (ONNX Runtime GPU) and [`text_embedding_candle`](../examples/text_embedding_candle/Dockerfile) (Candle CUDA). For a CPU ONNX Runtime image built with `prebakedImage`, see [`table_row_inference`](../examples/table_row_inference/Dockerfile). [Worker containers](containers.md) describes the image layout.
