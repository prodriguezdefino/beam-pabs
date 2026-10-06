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

"""Exports the Python benchmark's MobileNetV2 weights to ONNX for the Rust example.

The Python baseline (apache_beam/examples/inference/pytorch_image_classification.py)
runs torchvision ``mobilenet_v2(num_classes=1000)`` loaded from the state dict at
``gs://apache-beam-ml/models/imagenet_classification_mobilenet_v2.pt``. This script
exports exactly those weights with a dynamic batch axis, so the Rust
``cv_onnx_classification`` example runs the same network.

The input/output tensor names must match ``INPUT_NAME`` / ``OUTPUT_NAME`` in
``src/lib.rs``.

Usage::

    pip install torch torchvision onnx onnxruntime pillow
    gcloud storage cp gs://apache-beam-ml/models/imagenet_classification_mobilenet_v2.pt .
    python export_model.py \
        --state_dict imagenet_classification_mobilenet_v2.pt \
        --output mobilenet_v2_torchvision.onnx \
        --validate_images ILSVRC2012_val_00005001.JPEG ILSVRC2012_val_00005002.JPEG
    gcloud storage cp mobilenet_v2_torchvision.onnx gs://<bucket>/models/
"""

import argparse
import hashlib

import numpy as np
import onnxruntime
import torch
from PIL import Image
from torchvision import models
from torchvision import transforms

INPUT_NAME = 'input'
OUTPUT_NAME = 'logits'
OPSET_VERSION = 17


def load_model(state_dict_path: str) -> torch.nn.Module:
  model = models.mobilenet_v2(num_classes=1000)
  model.load_state_dict(torch.load(state_dict_path, map_location='cpu'))
  return model.eval()


def export(model: torch.nn.Module, output_path: str) -> None:
  torch.onnx.export(
      model,
      (torch.randn(1, 3, 224, 224), ),
      output_path,
      input_names=[INPUT_NAME],
      output_names=[OUTPUT_NAME],
      dynamic_axes={
          INPUT_NAME: {
              0: 'batch'
          }, OUTPUT_NAME: {
              0: 'batch'
          }
      },
      opset_version=OPSET_VERSION,
      dynamo=False)


def preprocess(path: str) -> torch.Tensor:
  """Identical to preprocess_image() in pytorch_image_classification.py."""
  transform = transforms.Compose([
      transforms.Resize((224, 224)),
      transforms.ToTensor(),
      transforms.Normalize(
          mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225]),
  ])
  return transform(Image.open(path).convert('RGB'))


def validate(model: torch.nn.Module, onnx_path: str, images: list[str]) -> None:
  """Checks ONNX Runtime against torch on random and real batched inputs."""
  session = onnxruntime.InferenceSession(
      onnx_path, providers=['CPUExecutionProvider'])

  def compare(batch: torch.Tensor) -> list[int]:
    with torch.no_grad():
      expected = model(batch).numpy()
    actual = session.run([OUTPUT_NAME], {INPUT_NAME: batch.numpy()})[0]
    max_diff = float(np.abs(expected - actual).max())
    print(f'batch={batch.shape[0]} max_abs_diff={max_diff:.2e}')
    if (expected.argmax(1) != actual.argmax(1)).any() or max_diff > 1e-3:
      raise SystemExit('ONNX export does not match torch')
    return actual.argmax(1).tolist()

  compare(torch.randn(3, 3, 224, 224))
  if images:
    predictions = compare(torch.stack([preprocess(p) for p in images]))
    for path, prediction in zip(images, predictions):
      print(f'{path},{prediction}')


def main() -> None:
  parser = argparse.ArgumentParser()
  parser.add_argument('--state_dict', required=True)
  parser.add_argument('--output', required=True)
  parser.add_argument('--validate_images', nargs='*', default=[])
  args = parser.parse_args()

  model = load_model(args.state_dict)
  export(model, args.output)
  validate(model, args.output, args.validate_images)
  with open(args.output, 'rb') as f:
    print(f'sha256={hashlib.sha256(f.read()).hexdigest()}')


if __name__ == '__main__':
  main()
