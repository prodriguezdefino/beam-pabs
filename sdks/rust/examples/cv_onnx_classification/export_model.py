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

The exported graph takes ``uint8`` RGB pixels (N x 3 x 224 x 224) and does the scaling to
``[0, 1]`` and the ImageNet normalization itself, so they run on the inference device.

The input/output tensor names must match ``INPUT_NAME`` / ``OUTPUT_NAME`` in
``src/lib.rs``.

Usage::

    pip install torch torchvision onnx onnxruntime pillow
    gcloud storage cp gs://apache-beam-ml/models/imagenet_classification_mobilenet_v2.pt .
    python export_model.py \
        --state_dict imagenet_classification_mobilenet_v2.pt \
        --output mobilenet_v2_uint8.onnx \
        --validate_images ILSVRC2012_val_00005001.JPEG ILSVRC2012_val_00005002.JPEG
    gcloud storage cp mobilenet_v2_uint8.onnx gs://<bucket>/models/
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
IMAGE_SIZE = 224
IMAGENET_MEAN = [0.485, 0.456, 0.406]
IMAGENET_STD = [0.229, 0.224, 0.225]


def load_model(state_dict_path: str) -> torch.nn.Module:
  model = models.mobilenet_v2(num_classes=1000)
  model.load_state_dict(torch.load(state_dict_path, map_location='cpu'))
  return model.eval()


class Uint8Input(torch.nn.Module):
  """The model behind ToTensor's scaling and Normalize, on uint8 RGB pixels."""
  def __init__(self, model: torch.nn.Module):
    super().__init__()
    self.model = model
    self.register_buffer('mean', torch.tensor(IMAGENET_MEAN).view(1, 3, 1, 1))
    self.register_buffer('std', torch.tensor(IMAGENET_STD).view(1, 3, 1, 1))

  def forward(self, pixels: torch.Tensor) -> torch.Tensor:
    return self.model((pixels.float() / 255.0 - self.mean) / self.std)


def export(model: torch.nn.Module, output_path: str) -> None:
  torch.onnx.export(
      Uint8Input(model).eval(),
      (torch.zeros(1, 3, IMAGE_SIZE, IMAGE_SIZE, dtype=torch.uint8), ),
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


def resize(path: str) -> torch.Tensor:
  """The uint8 CHW pixels after the Resize of preprocess_image() in
  pytorch_image_classification.py."""
  image = transforms.Resize(
      (IMAGE_SIZE, IMAGE_SIZE))(Image.open(path).convert('RGB'))
  return torch.from_numpy(np.asarray(image).copy()).permute(2, 0, 1)


def normalized(pixels: torch.Tensor) -> torch.Tensor:
  """ToTensor and Normalize of preprocess_image(), on uint8 CHW pixels."""
  return transforms.Normalize(mean=IMAGENET_MEAN,
                              std=IMAGENET_STD)(pixels.float() / 255.0)


def validate(model: torch.nn.Module, onnx_path: str, images: list[str]) -> None:
  """Checks ONNX Runtime on uint8 pixels against torch on the Python preprocessing."""
  session = onnxruntime.InferenceSession(
      onnx_path, providers=['CPUExecutionProvider'])

  def compare(pixels: torch.Tensor) -> list[int]:
    with torch.no_grad():
      expected = model(torch.stack([normalized(p) for p in pixels])).numpy()
    actual = session.run([OUTPUT_NAME], {INPUT_NAME: pixels.numpy()})[0]
    max_diff = float(np.abs(expected - actual).max())
    print(f'batch={pixels.shape[0]} max_abs_diff={max_diff:.2e}')
    if (expected.argmax(1) != actual.argmax(1)).any() or max_diff > 1e-3:
      raise SystemExit('ONNX export does not match torch')
    return actual.argmax(1).tolist()

  compare(
      torch.randint(0, 256, (3, 3, IMAGE_SIZE, IMAGE_SIZE), dtype=torch.uint8))
  if images:
    predictions = compare(torch.stack([resize(p) for p in images]))
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
