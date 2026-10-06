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

# Table row inference (RandomForest on ONNX Runtime)

This is the Rust port of the Python batch benchmark
[`table_row_inference.py`](../../../python/apache_beam/examples/inference/table_row_inference.py),
configured by
[`beam_Inference_Python_Benchmarks_Dataflow_Table_Row_Inference_Batch.txt`](../../../../.github/workflows/load-tests-pipeline-options/beam_Inference_Python_Benchmarks_Dataflow_Table_Row_Inference_Batch.txt).
Both pipelines run the same workload for side-by-side benchmarking.
The crate name is `columnar_feature_engineering`.

| Step | Python | Rust |
|---|---|---|
| Read | `ReadFromText(--input_file)` | `textio::Read::new("ReadLines", --input)` |
| Expand | `FlatMap([line] * --input_expand_factor)` when > 1 | the same |
| Parse | `json.loads`, `beam.Row(feature1..feature5)` | `serde_json` into `TableRow` |
| Features | `np.array([...], dtype=float32)`, `--feature_columns` order | `[f64; 5] as f32`, `FEATURE_COLUMNS` order |
| Batch | `BatchElements`, dynamic 1..10000 | `RunInference` 1..`--max_batch_size` (10000), flushed per bundle |
| Model | sklearn `RandomForestClassifier.predict` (pickle) | same model exported to ONNX, ONNX Runtime CPU |
| Output | `json.dumps` of `row_key, prediction, model_id, input_*` to `<output_file>.jsonl` | byte-identical lines to `<output>.jsonl` |

The only difference in the output is `model_id`, which names the model file that each side loads.

## Inputs

- Data: `gs://apache-beam-ml/testing/inputs/table_rows_100k_benchmark.jsonl`
  (100k rows of `{"id", "feature1".."feature5"}`). The benchmark uses
  `--input_expand_factor=100` for 10M rows.
- Model: `gs://<bucket>/models/table_row_rf.onnx`, exported by
  [`export_model.py`](export_model.py) from `gs://apache-beam-ml/models/sklearn_table_classifier.pkl`.
  The model has 12 trees, a maximum depth of 8, 5 features, and classes `[0, 1]`.
  - Exported with scikit-learn 1.5.2 and skl2onnx 1.20 at opset 17 / ai.onnx.ml 3, with `zipmap=False`.
  - The graph maps `input: float32[N, 5]` to `label: int64[N]` and `probabilities: float32[N, 2]`.
  - ONNX Runtime matches `model.predict` on all 100,000 benchmark rows.

Regenerate the model and test fixtures:

```bash
python3.12 -m venv venv && . venv/bin/activate
pip install 'scikit-learn==1.5.2' 'numpy>=2,<2.5' skl2onnx onnx onnxruntime
python export_model.py --upload --fixture_dir tests/fixtures
```

## Running

The binary loads ONNX Runtime dynamically from `ORT_DYLIB_PATH`. The `onnxruntime` pip
wheel provides the library, for example at
`venv/lib/python3.12/site-packages/onnxruntime/capi/libonnxruntime.*.dylib`.

```bash
ORT_DYLIB_PATH=/path/to/libonnxruntime.dylib cargo run -p columnar_feature_engineering -- \
  --runner=prism --input=rows.jsonl --model_path=table_row_rf.onnx --output=/tmp/predictions
```

Dataflow workers also need ONNX Runtime. The [`Dockerfile`](Dockerfile) puts the CPU build
and the pipeline binary into an image:

```bash
./gradlew :sdks:rust:prebakedImage -Pexample=columnar_feature_engineering \
  -PimageName=us-central1-docker.pkg.dev/<project>/<repo>/beam_rust_table_row_inference:latest -Ppush-containers
./gradlew :sdks:rust:dataflow -Pexample=columnar_feature_engineering \
  -PsdkContainerImage=us-central1-docker.pkg.dev/<project>/<repo>/beam_rust_table_row_inference:latest \
  -PworkerBinary=none -PworkerMachineType=n1-standard-4 -PnumWorkers=10 -PmaxNumWorkers=10 \
  -Poutput=gs://<bucket>/output/rs_table_rows "-PextraArgs=--input_expand_factor=100"
```

## Tests

```bash
cargo test -p columnar_feature_engineering
# Whole pipeline with ONNX Runtime, compared with the Python output for the fixture rows:
ORT_DYLIB_PATH=/path/to/libonnxruntime.dylib TABLE_ROW_ONNX_MODEL=/path/to/table_row_rf.onnx \
  cargo test -p columnar_feature_engineering -- --ignored
```
