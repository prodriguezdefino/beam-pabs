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
"""Cloud Bigtable benchmark in a Python pipeline: the ``bt_to_gcs`` shape.

Readers (``--reader``):

* ``java``: the Java ``BigtableReadSchemaTransformProvider`` on the Java GCP expansion
  service.
* ``rust``: the Rust ``BigtableReadProvider`` on the Rust GCP expansion service.

Shapes:

* ``bt_to_gcs`` reads ``--bigtable_table`` with ``--reader``, passes each row through a
  Python ``Touch`` step that counts ``e2e/rows`` and formats the row as a text line, and
  writes the lines to ``--output`` in Cloud Storage.

``../java/BigtableBenchmark.java`` and ``../rust/src/lib.rs`` run the same shape in Java
and Rust pipelines. ``./gradlew :sdks:rust:bigtableBenchmark`` runs this script.
"""

import argparse
import json
import logging
import subprocess
import sys
import time

import apache_beam as beam
from apache_beam.metrics import Metrics
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.external import BeamJarExpansionService
from apache_beam.transforms.external import SchemaAwareExternalTransform

JAVA_READ_URN = "beam:schematransform:org.apache.beam:bigtable_read:v1"
RUST_READ_URN = "beam:schematransform:org.apache.beam:rust:bigtable_read:v1"


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


READERS = {"java": read_java, "rust": read_rust}


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


class Touch(beam.DoFn):
  """Counts each row in ``e2e/rows`` and formats it into a text line."""
  def __init__(self, flatten):
    self.flatten = flatten
    self.rows = Metrics.counter("e2e", "rows")

  def process(self, row):
    self.rows.inc()
    yield format_row(row, self.flatten)


GCLOUD_ATTEMPTS = 5


def gcloud(*args):
  """Runs a gcloud command and parses its JSON output.

  A poll can fail for a moment (network, auth refresh), so the call tries again with
  backoff before it raises the last error.
  """
  for attempt in range(GCLOUD_ATTEMPTS):
    try:
      out = subprocess.run(["gcloud", *args, "--format=json"],
                           check=True,
                           capture_output=True,
                           text=True).stdout
      return json.loads(out) if out.strip() else {}
    except subprocess.CalledProcessError as err:
      if attempt + 1 == GCLOUD_ATTEMPTS:
        raise
      logging.warning("gcloud %s failed (%s), trying again", args[:3],
                      err.stderr.strip() if err.stderr else err.returncode)
      time.sleep(2**attempt)
  return {}


def committed_rows(project, region, job_id):
  metrics = gcloud("beta", "dataflow", "metrics", "list", job_id,
                   f"--project={project}", f"--region={region}")
  for metric in metrics:
    name = metric.get("name", {})
    context = name.get("context", {})
    if (name.get("name") == "rows" and context.get("namespace") == "e2e"
        and not context.get("tentative")):
      return int(float(metric.get("scalar", 0)))
  return 0


def job_state(project, region, job_id):
  job = gcloud("dataflow", "jobs", "describe", job_id, f"--project={project}",
               f"--region={region}")
  return job.get("currentState", "")


def wait_for_streaming_job(project,
                           region,
                           job_id,
                           expected_rows,
                           drain_after_seconds=None):
  """Polls Dataflow for a Rust pipeline job until ``JOB_STATE_DONE``."""
  del drain_after_seconds
  start = time.time()
  while True:
    state = job_state(project, region, job_id)
    rows = committed_rows(project, region, job_id)
    elapsed = int(time.time() - start)
    logging.info("Job %s: state=%s e2e/rows=%d elapsed=%ds", job_id, state,
                 rows, elapsed)
    if state == "JOB_STATE_DONE":
      if expected_rows is not None and rows != expected_rows:
        raise RuntimeError(
            f"Job {job_id} finished with {rows} rows, expected {expected_rows}")
      return
    if state in ("JOB_STATE_FAILED", "JOB_STATE_CANCELLED",
                 "JOB_STATE_DRAINED", "JOB_STATE_UPDATED"):
      raise RuntimeError(f"Job {job_id} ended in {state}")
    time.sleep(15)


def parse_bool(value):
  lowered = value.strip().lower()
  if lowered in ("true", "1", "yes"):
    return True
  if lowered in ("false", "0", "no"):
    return False
  raise argparse.ArgumentTypeError(f"'{value}' is not a boolean (true or false)")


def main(argv):
  parser = argparse.ArgumentParser(description=__doc__)
  parser.add_argument("--reader", choices=sorted(READERS), required=True)
  parser.add_argument("--shape", choices=["bt_to_gcs"], default="bt_to_gcs")
  parser.add_argument("--bigtable_project", required=True)
  parser.add_argument("--bigtable_instance", required=True)
  parser.add_argument("--bigtable_table", required=True)
  parser.add_argument("--app_profile", default="")
  parser.add_argument("--flatten", type=parse_bool, default=False)
  parser.add_argument("--output", required=True)
  parser.add_argument("--billing_project", default="")
  parser.add_argument("--expansion_service", default="localhost:8097")
  parser.add_argument(
      "--expected_rows",
      type=int,
      help="Fail the job unless e2e/rows equals this count.")
  args, pipeline_args = parser.parse_known_args(argv)

  if args.app_profile and args.reader != "rust":
    parser.error("--app_profile applies to --reader=rust only in Python pipelines")

  options = PipelineOptions(pipeline_args)
  p = beam.Pipeline(options=options)
  _ = (
      p
      | "Read" >> READERS[args.reader](args)
      | "Touch" >> beam.ParDo(Touch(args.flatten)).with_output_types(str)
      | "Write" >> beam.io.WriteToText(args.output))
  result = p.run()
  state = result.wait_until_finish()
  if state != beam.runners.runner.PipelineState.DONE:
    raise RuntimeError(f"The job finished in state {state}")
  if args.expected_rows is not None:
    gcloud_opts = options.view_as(GoogleCloudOptions)
    rows = committed_rows(gcloud_opts.project, gcloud_opts.region,
                          result.job_id())
    if rows != args.expected_rows:
      raise RuntimeError(
          f"Job {result.job_id()} read {rows} rows, expected {args.expected_rows}"
      )


if __name__ == "__main__":
  logging.getLogger().setLevel(logging.INFO)
  main(sys.argv[1:])
