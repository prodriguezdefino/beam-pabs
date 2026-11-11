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
"""Cloud Bigtable benchmark in a Python pipeline: ``bt_to_gcs`` and ``gcs_to_bt``.

Readers (``--reader``, ``bt_to_gcs``):

* ``java``: the Java ``BigtableReadSchemaTransformProvider`` on the Java GCP expansion
  service.
* ``rust``: the Rust ``BigtableReadProvider`` on the Rust GCP expansion service.

Writers (``--writer``, ``gcs_to_bt``):

* ``java``: the Java ``BigtableWriteSchemaTransformProvider`` on the Java GCP expansion
  service.
* ``rust``: the Rust ``BigtableWriteProvider`` on the Rust GCP expansion service.

``../java/BigtableBenchmark.java`` and ``../rust/src/lib.rs`` run the same shapes in Java
and Rust pipelines. ``./gradlew :sdks:rust:bigtableBenchmark`` runs this script.
"""

import argparse
import logging
import sys
import typing

import apache_beam as beam
from apache_beam.metrics import Metrics
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.transforms.external import BeamJarExpansionService
from apache_beam.transforms.external import SchemaAwareExternalTransform

JAVA_READ_URN = "beam:schematransform:org.apache.beam:bigtable_read:v1"
JAVA_WRITE_URN = "beam:schematransform:org.apache.beam:bigtable_write:v1"
RUST_READ_URN = "beam:schematransform:org.apache.beam:rust:bigtable_read:v1"
RUST_WRITE_URN = "beam:schematransform:org.apache.beam:rust:bigtable_write:v1"


class BatchedMutation(typing.NamedTuple):
  key: bytes
  mutations: typing.Sequence[typing.Mapping[str, bytes]]


beam.coders.registry.register_coder(BatchedMutation, beam.coders.RowCoder)


def read_java(args):
  service = BeamJarExpansionService(
      "sdks:java:io:google-cloud-platform:expansion-service:build")
  return SchemaAwareExternalTransform(
      JAVA_READ_URN,
      service,
      rearrange_based_on_discovery=True,
      project_id=args.bigtable_project,
      instance_id=args.bigtable_instance,
      table_id=args.bigtable_table,
      flatten=args.flatten)


def read_rust(args):
  config = {
      "project_id": args.bigtable_project,
      "instance_id": args.bigtable_instance,
      "table_id": args.bigtable_table,
      "flatten": args.flatten,
  }
  if args.app_profile:
    config["app_profile_id"] = args.app_profile
  return SchemaAwareExternalTransform(
      RUST_READ_URN,
      args.expansion_service,
      rearrange_based_on_discovery=True,
      **config)


def write_java(args):
  service = BeamJarExpansionService(
      "sdks:java:io:google-cloud-platform:expansion-service:build")
  return SchemaAwareExternalTransform(
      JAVA_WRITE_URN,
      service,
      rearrange_based_on_discovery=True,
      project_id=args.bigtable_project,
      instance_id=args.bigtable_instance,
      table_id=args.bigtable_table)


def write_rust(args):
  config = {
      "project_id": args.bigtable_project,
      "instance_id": args.bigtable_instance,
      "table_id": args.bigtable_table,
  }
  if args.app_profile:
    config["app_profile_id"] = args.app_profile
  return SchemaAwareExternalTransform(
      RUST_WRITE_URN,
      args.expansion_service,
      rearrange_based_on_discovery=True,
      **config)


READERS = {"java": read_java, "rust": read_rust}
WRITERS = {"java": write_java, "rust": write_rust}


def decode_bytes(raw):
  if isinstance(raw, (bytes, bytearray, memoryview)):
    return bytes(raw).decode("utf-8", errors="replace")
  return str(raw)


def format_row(row, flatten):
  key = decode_bytes(row.key)
  if flatten:
    family = row.family_name
    qualifier = decode_bytes(row.column_qualifier)
    cells = row.cells
    value = decode_bytes(cells[0].value) if cells else ""
    return f"{key}\t{family}:{qualifier}\t{value}"
  families = row.column_families or {}
  parts = [key]
  for family, cols in families.items():
    for qualifier, cells in (cols or {}).items():
      value = decode_bytes(cells[0].value) if cells else ""
      parts.append(f"{family}:{qualifier}={value}")
  return "\t".join(parts)


def parse_line(line):
  if not line:
    return None
  parts = line.split("\t")
  if len(parts) < 2 or not parts[0]:
    return None
  cells = []
  if len(parts) == 3 and "=" not in parts[1]:
    if ":" in parts[1]:
      family, qualifier = parts[1].split(":", 1)
      if family:
        cells.append((family, qualifier, parts[2]))
  else:
    for field in parts[1:]:
      if "=" not in field:
        continue
      col, val = field.split("=", 1)
      if ":" not in col:
        continue
      family, qualifier = col.split(":", 1)
      if family:
        cells.append((family, qualifier, val))
  if not cells:
    return None
  mutations = [{
      "type": b"SetCell",
      "family_name": family.encode("utf-8"),
      "column_qualifier": qualifier.encode("utf-8"),
      "value": value.encode("utf-8"),
  } for family, qualifier, value in cells]
  return BatchedMutation(key=parts[0].encode("utf-8"), mutations=mutations)


class Touch(beam.DoFn):
  """Counts each row in ``e2e/rows`` and formats it into a text line."""
  def __init__(self, flatten):
    self.flatten = flatten
    self.rows = Metrics.counter("e2e", "rows")

  def process(self, row):
    self.rows.inc()
    yield format_row(row, self.flatten)


class ParseMutations(beam.DoFn):
  """Parses text lines into ``BatchedMutation`` rows and counts ``e2e/rows``."""
  def __init__(self):
    self.rows = Metrics.counter("e2e", "rows")

  def process(self, line):
    parsed = parse_line(line)
    if parsed is not None:
      self.rows.inc()
      yield parsed


def parse_bool(value):
  lowered = value.strip().lower()
  if lowered in ("true", "1", "yes"):
    return True
  if lowered in ("false", "0", "no"):
    return False
  raise argparse.ArgumentTypeError(f"'{value}' is not a boolean (true or false)")


def main(argv):
  parser = argparse.ArgumentParser(description=__doc__)
  parser.add_argument("--reader", choices=sorted(READERS))
  parser.add_argument("--writer", choices=sorted(WRITERS))
  parser.add_argument(
      "--shape", choices=["bt_to_gcs", "gcs_to_bt"], default="bt_to_gcs")
  parser.add_argument("--bigtable_project", required=True)
  parser.add_argument("--bigtable_instance", required=True)
  parser.add_argument("--bigtable_table", required=True)
  parser.add_argument("--app_profile", default="")
  parser.add_argument("--flatten", type=parse_bool, default=False)
  parser.add_argument("--input")
  parser.add_argument("--output")
  parser.add_argument("--billing_project", default="")
  parser.add_argument("--expansion_service", default="localhost:8097")
  args, pipeline_args = parser.parse_known_args(argv)

  options = PipelineOptions(pipeline_args)
  p = beam.Pipeline(options=options)
  if args.shape == "bt_to_gcs":
    if not args.reader or not args.output:
      parser.error("bt_to_gcs needs --reader and --output")
    if args.app_profile and args.reader != "rust":
      parser.error("--app_profile applies to --reader=rust only in Python pipelines")
    _ = (
        p
        | "Read" >> READERS[args.reader](args)
        | "Touch" >> beam.ParDo(Touch(args.flatten)).with_output_types(str)
        | "Write" >> beam.io.WriteToText(args.output))
  else:
    if not args.writer or not args.input:
      parser.error("gcs_to_bt needs --writer and --input")
    if args.app_profile and args.writer != "rust":
      parser.error("--app_profile applies to --writer=rust only in Python pipelines")
    _ = (
        p
        | "Read" >> beam.io.ReadFromText(args.input)
        | "Touch" >>
        beam.ParDo(ParseMutations()).with_output_types(BatchedMutation)
        | "Write" >> WRITERS[args.writer](args))

  result = p.run()
  runner = (options.view_as(StandardOptions).runner or "").lower()
  if "dataflow" in runner:
    print(f"JOB_ID={result.job_id()}", flush=True)
    return
  state = result.wait_until_finish()
  if state != beam.runners.runner.PipelineState.DONE:
    raise RuntimeError(f"The job finished in state {state}")


if __name__ == "__main__":
  logging.getLogger().setLevel(logging.INFO)
  main(sys.argv[1:])
