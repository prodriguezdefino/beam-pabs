#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
"""Exports the Python table_row_inference RandomForest to ONNX.

The Python benchmark
(apache_beam/examples/inference/table_row_inference.py, configured by
.github/workflows/load-tests-pipeline-options/
beam_Inference_Python_Benchmarks_Dataflow_Table_Row_Inference_Batch.txt)
loads a pickled scikit-learn RandomForestClassifier and calls
`model.predict(np.array(features, dtype=np.float32))`. Rust cannot load a
pickle, so this script converts that exact model with skl2onnx and checks
that ONNX Runtime reproduces `model.predict` on the benchmark rows.

The pickle was written by scikit-learn 1.5.2 with numpy 2.x, inside the
`scikit-learn<1.6.0` pin of table_row_inference_requirements.txt:

    python3.12 -m venv venv && . venv/bin/activate
    pip install 'scikit-learn==1.5.2' 'numpy>=2,<2.5' skl2onnx onnx onnxruntime
    python export_model.py --upload=gs://<bucket>/models/table_row_rf.onnx

The exported graph takes `input: float32[N, 5]` (feature1..feature5, the
order of `--feature_columns`) and, because ZipMap is disabled, returns plain
tensors `label: int64[N]` and `probabilities: float32[N, 2]`.

With --fixture_dir, a sample of input lines and the exact lines Python's
table_row_inference.py writes for them are saved for the Rust example's
tests. The sample is the first --fixture_rows lines plus the first
--fixture_small_rows lines holding a feature below 1e-4, which Python's
json.dumps writes in scientific notation.
"""

import argparse
import json
import pickle
import subprocess
import tempfile
from pathlib import Path

import numpy as np
import onnxruntime as ort
import sklearn
from skl2onnx import convert_sklearn
from skl2onnx.common.data_types import FloatTensorType

DEFAULT_PICKLE = 'gs://apache-beam-ml/models/sklearn_table_classifier.pkl'
DEFAULT_DATA = (
    'gs://apache-beam-ml/testing/inputs/table_rows_100k_benchmark.jsonl')
FEATURE_COLUMNS = ('feature1', 'feature2', 'feature3', 'feature4', 'feature5')
REQUIRED_SKLEARN = '1.5.2'
TARGET_OPSET = {'': 17, 'ai.onnx.ml': 3}


def fetch(uri: str, workdir: Path) -> Path:
  """Returns a local path for `uri`, copying gs:// objects into `workdir`."""
  if not uri.startswith('gs://'):
    return Path(uri)
  local = workdir / uri.rsplit('/', 1)[-1]
  subprocess.run(['gcloud', 'storage', 'cp', uri, str(local)], check=True)
  return local


SCIENTIFIC_BELOW = 1e-4


def read_lines(path: Path) -> list[str]:
  """Returns the non-empty lines without their CRLF/LF terminators."""
  with path.open(encoding='utf-8', newline='') as source:
    return [line.rstrip('\r\n') for line in source if line.strip()]


def features(rows: list[dict]) -> np.ndarray:
  """Mirrors TableRowModelHandler.run_inference: float() then float32."""
  return np.array([[float(row[col]) for col in FEATURE_COLUMNS]
                   for row in rows],
                  dtype=np.float32)


def convert(model) -> bytes:
  onx = convert_sklearn(
      model,
      initial_types=[('input', FloatTensorType([None, len(FEATURE_COLUMNS)]))],
      options={id(model): {
          'zipmap': False
      }},
      target_opset=TARGET_OPSET)
  return onx.SerializeToString()


def validate(model, onnx_bytes: bytes, x: np.ndarray) -> np.ndarray:
  """Compares ONNX Runtime with sklearn and returns sklearn's predictions."""
  session = ort.InferenceSession(
      onnx_bytes, providers=['CPUExecutionProvider'])
  names = [o.name for o in session.get_outputs()]
  if names != ['label', 'probabilities']:
    raise ValueError(f'unexpected ONNX outputs {names}')
  label, probabilities = session.run(None, {'input': x})
  expected = model.predict(x)
  expected_proba = model.predict_proba(x)
  agree = int(np.sum(label == expected))
  mismatches = np.flatnonzero(label != expected)
  print(f'rows: {len(x)}')
  print(f'label agreement: {agree}/{len(x)} = {100.0 * agree / len(x):.4f}%')
  print(
      'max |probabilities - predict_proba|: '
      f'{float(np.max(np.abs(probabilities - expected_proba))):.3g}')
  for i in mismatches[:10]:
    print(
        f'  mismatch row {i}: sklearn={expected[i]} onnx={label[i]} '
        f'proba={expected_proba[i]} onnx_proba={probabilities[i]}')
  return expected


def fixture_indices(rows: list[dict], first: int, small: int) -> list[int]:
  """The first `first` rows plus the first `small` rows with a tiny feature."""
  tiny = [
      i for i, row in enumerate(rows) if i >= first and
      any(abs(row[col]) < SCIENTIFIC_BELOW for col in FEATURE_COLUMNS)
  ]
  return list(range(min(first, len(rows)))) + tiny[:small]


def python_output_line(row: dict, prediction, model_id: str) -> str:
  """table_row_inference.py's FormatTableOutput followed by json.dumps."""
  return json.dumps({
      'row_key': row['id'],
      'prediction': float(prediction),
      'model_id': model_id,
      **{f'input_{col}': float(row[col])
         for col in FEATURE_COLUMNS},
  })


def write_fixtures(
    fixture_dir: Path,
    lines: list[str],
    rows: list[dict],
    expected: np.ndarray,
    indices: list[int],
    model_id: str):
  fixture_dir.mkdir(parents=True, exist_ok=True)
  (fixture_dir / 'table_rows_sample.jsonl').write_text(
      ''.join(lines[i] + '\n' for i in indices), encoding='utf-8')
  (fixture_dir / 'python_output.jsonl').write_text(
      ''.join(
          python_output_line(rows[i], expected[i], model_id) + '\n'
          for i in indices),
      encoding='utf-8')


def main():
  parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
  parser.add_argument('--pickle', default=DEFAULT_PICKLE)
  parser.add_argument('--data', default=DEFAULT_DATA)
  parser.add_argument('--output', default='table_row_rf.onnx')
  parser.add_argument(
      '--upload',
      metavar='GS_PATH',
      help='Copy the model to this gs:// path, e.g. '
      'gs://<bucket>/models/table_row_rf.onnx.')
  parser.add_argument('--fixture_dir', type=Path)
  parser.add_argument('--fixture_rows', type=int, default=32)
  parser.add_argument('--fixture_small_rows', type=int, default=8)
  args = parser.parse_args()

  if sklearn.__version__ != REQUIRED_SKLEARN:
    raise RuntimeError(
        f'scikit-learn {sklearn.__version__} found; the pickle needs '
        f'{REQUIRED_SKLEARN}')

  with tempfile.TemporaryDirectory() as tmp:
    workdir = Path(tmp)
    with fetch(args.pickle, workdir).open('rb') as f:
      model = pickle.load(f)
    print(
        f'model: {type(model).__name__}, {len(model.estimators_)} trees, '
        f'{model.n_features_in_} features, classes {model.classes_.tolist()}')
    if model.n_features_in_ != len(FEATURE_COLUMNS):
      raise ValueError(f'model expects {model.n_features_in_} features')

    data = fetch(args.data, workdir)
    lines = read_lines(data)
    rows = [json.loads(line) for line in lines]
    onnx_bytes = convert(model)
    expected = validate(model, onnx_bytes, features(rows))

    output = Path(args.output)
    output.write_bytes(onnx_bytes)
    print(f'wrote {output} ({len(onnx_bytes)} bytes)')
    if args.fixture_dir:
      indices = fixture_indices(
          rows, args.fixture_rows, args.fixture_small_rows)
      write_fixtures(
          args.fixture_dir, lines, rows, expected, indices, args.pickle)
      print(f'wrote fixtures to {args.fixture_dir}')

  if args.upload:
    subprocess.run(['gcloud', 'storage', 'cp', str(output), args.upload],
                   check=True)
    print(f'uploaded {args.upload}')


if __name__ == '__main__':
  main()
