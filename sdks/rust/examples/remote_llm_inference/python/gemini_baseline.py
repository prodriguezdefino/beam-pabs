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
"""Python baseline for the Rust `remote_llm_inference` benchmark.

This is `apache_beam/examples/inference/gemini_text_classification.py` with the
changes a like-for-like comparison needs:

* prompts come from `--input`, one per line, sent verbatim (blank lines skipped),
  instead of three hard-coded prompts;
* `max_batch_size=1`. `GeminiModelHandler` sends a whole batch as one multi-part
  prompt and zips the single response with the batch, so larger batches drop
  every result but the first;
* the model and its `generationConfig` are flags, set to the Rust run's values;
* each response is one `Input: ..., Output: ...` line with all text parts
  joined, as the Rust pipeline writes it.
"""

import argparse
import logging
from collections.abc import Iterable

import apache_beam as beam
from apache_beam.ml.inference.base import PredictionResult
from apache_beam.ml.inference.base import RunInference
from apache_beam.ml.inference.gemini_inference import GeminiModelHandler
from apache_beam.ml.inference.gemini_inference import generate_from_string
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


def parse_known_args(argv):
  parser = argparse.ArgumentParser()
  parser.add_argument('--input', required=True, help='One prompt per line.')
  parser.add_argument('--output', required=True)
  parser.add_argument('--cloud_project', required=True)
  parser.add_argument('--cloud_region', default='us-central1')
  parser.add_argument('--model_name', default='gemini-2.5-flash')
  parser.add_argument('--temperature', type=float)
  parser.add_argument('--max_output_tokens', type=int)
  parser.add_argument('--thinking_budget', type=int)
  return parser.parse_known_args(argv)


def generation_config(args) -> dict:
  """The `generationConfig` fields that are set, like the Rust pipeline sends."""
  config = {
      'temperature': args.temperature,
      'max_output_tokens': args.max_output_tokens,
      'thinking_config': (
          None if args.thinking_budget is None else {
              'thinking_budget': args.thinking_budget
          }),
  }
  return {key: value for key, value in config.items() if value is not None}


class FormatPrediction(beam.DoFn):
  def process(self, element: PredictionResult) -> Iterable[str]:
    text = element.inference.text
    if not text:
      raise ValueError(f'No text in the response to: {element.example}')
    yield f'Input: {element.example}, Output: {text}'


def run(argv=None):
  args, pipeline_args = parse_known_args(argv)
  options = PipelineOptions(pipeline_args)
  options.view_as(SetupOptions).save_main_session = True

  model_handler = GeminiModelHandler(
      model_name=args.model_name,
      request_fn=generate_from_string,
      project=args.cloud_project,
      location=args.cloud_region,
      max_batch_size=1)

  with beam.Pipeline(options=options) as p:
    _ = (
        p
        | 'ReadPrompts' >> beam.io.ReadFromText(args.input)
        | 'SkipBlank' >> beam.Filter(lambda line: line.strip())
        | 'RunInference' >> RunInference(
            model_handler,
            inference_args={'config': generation_config(args)})
        | 'Format' >> beam.ParDo(FormatPrediction())
        | 'Write' >> beam.io.WriteToText(args.output))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
