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
"""Kafka benchmark in a Python pipeline: a batch ``load`` shape and a streaming ``copy`` shape.

Readers (``--reader``, ``copy`` only):

* ``java``: the Java provider through Managed I/O.
* ``rust``: the Rust provider through the Rust expansion service.

Writers (``--writer``):

* ``java``: the Java provider through Managed I/O.
* ``rust``: the Rust provider through the Rust expansion service.

Shapes:

* ``load`` reads ``--table`` from BigQuery with ``DIRECT_READ``, converts each dict to a Beam
  row in Python (and counts it in ``e2e/rows``), and writes the rows to ``--topic`` with
  ``--writer``. The BigQuery read and the conversion run in the Python worker, so the Rust
  worker runs only the Kafka write.
* ``copy`` streams from ``--topic`` with ``--reader``, passes each row through a Python step
  that counts ``e2e/rows`` and records ``e2e/latency_ms`` (wall clock minus the element
  timestamp, which is the Kafka record timestamp), and writes the rows to
  ``--destination_topic`` with ``--writer``. The Python step keeps a Rust reader from fusing
  with a Rust writer, so the rows cross the Fn API data plane to Python and back.

For ``copy``, ``--expected_rows`` or ``--drain_after_seconds`` polls Dataflow until the job
reaches that count or time, drains the job, and waits for ``JOB_STATE_DONE``. ``--detach``
prints the job ID and exits as soon as the job runs, so a ``load`` job can run at the same
time.

``../java/KafkaBenchmark.java`` runs the same shapes in a Java pipeline.
``./gradlew :sdks:rust:kafkaBenchmark`` runs this script.
"""

import argparse
import bisect
import json
import logging
import subprocess
import sys
import time
import typing

import apache_beam as beam
from apache_beam.metrics import Metrics
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.transforms.external import SchemaAwareExternalTransform

RUST_READ_URN = "beam:schematransform:org.apache.beam:rust:kafka_read:v1"
RUST_WRITE_URN = "beam:schematransform:org.apache.beam:rust:kafka_write:v1"

MANAGED_KAFKA_AUTH = {
    "security.protocol": "SASL_SSL",
    "sasl.mechanism": "OAUTHBEARER",
    "sasl.login.callback.handler.class": (
        "com.google.cloud.hosted.kafka.auth.GcpLoginCallbackHandler"),
    "sasl.jaas.config": (
        "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;"
    ),
}

# The JSON Schema of bigquery-public-data:samples.wikipedia. All 15 properties are in
# "required" in column order, and the 10 nullable columns set "nullable": true, so both
# JsonUtils in Java and schema_from_json_schema in Rust build the same Beam schema.
WIKIPEDIA_JSON_SCHEMA = json.dumps({
    "type": "object",
    "properties": {
        "title": {
            "type": "string"
        },
        "id": {
            "type": "integer",
            "nullable": True
        },
        "language": {
            "type": "string"
        },
        "wp_namespace": {
            "type": "integer"
        },
        "is_redirect": {
            "type": "boolean",
            "nullable": True
        },
        "revision_id": {
            "type": "integer",
            "nullable": True
        },
        "contributor_ip": {
            "type": "string",
            "nullable": True
        },
        "contributor_id": {
            "type": "integer",
            "nullable": True
        },
        "contributor_username": {
            "type": "string",
            "nullable": True
        },
        "timestamp": {
            "type": "integer"
        },
        "is_minor": {
            "type": "boolean",
            "nullable": True
        },
        "is_bot": {
            "type": "boolean",
            "nullable": True
        },
        "reversion_id": {
            "type": "integer",
            "nullable": True
        },
        "comment": {
            "type": "string",
            "nullable": True
        },
        "num_characters": {
            "type": "integer"
        },
    },
    "required": [
        "title",
        "id",
        "language",
        "wp_namespace",
        "is_redirect",
        "revision_id",
        "contributor_ip",
        "contributor_id",
        "contributor_username",
        "timestamp",
        "is_minor",
        "is_bot",
        "reversion_id",
        "comment",
        "num_characters",
    ],
})


class WikipediaRow(typing.NamedTuple):
  title: str
  id: typing.Optional[int]
  language: str
  wp_namespace: int
  is_redirect: typing.Optional[bool]
  revision_id: typing.Optional[int]
  contributor_ip: typing.Optional[str]
  contributor_id: typing.Optional[int]
  contributor_username: typing.Optional[str]
  timestamp: int
  is_minor: typing.Optional[bool]
  is_bot: typing.Optional[bool]
  reversion_id: typing.Optional[int]
  comment: typing.Optional[str]
  num_characters: int


class RawRow(typing.NamedTuple):
  payload: bytes


beam.coders.registry.register_coder(WikipediaRow, beam.coders.RowCoder)
beam.coders.registry.register_coder(RawRow, beam.coders.RowCoder)

WIKIPEDIA_FIELDS = WikipediaRow._fields


def parse_properties(raw):
  props = {}
  if not raw:
    return props
  for pair in raw.split(","):
    if "=" not in pair or pair.startswith("="):
      raise ValueError(f"'{pair}' is not a key=value pair")
    key, value = pair.split("=", 1)
    props[key] = value
  return props


def consumer_properties(args, include_java_auth):
  props = dict(MANAGED_KAFKA_AUTH) if include_java_auth else {}
  if args.group_id:
    props["group.id"] = args.group_id
  props.update(parse_properties(args.consumer_properties))
  return props


def producer_properties(args, include_java_auth):
  props = dict(MANAGED_KAFKA_AUTH) if include_java_auth else {}
  props.update(parse_properties(args.producer_properties))
  return props


def read_java(args):
  config = {
      "bootstrap_servers": args.bootstrap_servers,
      "topic": args.topic,
      "format": args.format,
      "auto_offset_reset_config": args.auto_offset_reset,
      "consumer_config_updates": consumer_properties(
          args, include_java_auth=True),
  }
  if args.format == "JSON":
    config["schema"] = WIKIPEDIA_JSON_SCHEMA
  return beam.managed.Read(beam.managed.KAFKA, config=config)


def read_rust(args):
  config = {
      "bootstrap_servers": args.bootstrap_servers,
      "topic": args.topic,
      "format": args.format,
      "auto_offset_reset_config": args.auto_offset_reset,
      "with_gcp_adc": True,
  }
  props = consumer_properties(args, include_java_auth=False)
  if props:
    config["consumer_config_updates"] = props
  if args.format == "JSON":
    config["schema"] = WIKIPEDIA_JSON_SCHEMA
  if args.max_read_time_per_call_seconds > 0:
    config[
        "max_read_time_per_call_seconds"] = args.max_read_time_per_call_seconds
  if args.max_idle_per_call_ms > 0:
    config["max_idle_per_call_ms"] = args.max_idle_per_call_ms
  return SchemaAwareExternalTransform(
      RUST_READ_URN,
      args.expansion_service,
      rearrange_based_on_discovery=True,
      **config)


READERS = {"java": read_java, "rust": read_rust}


def write_java(args, topic):
  config = {
      "bootstrap_servers": args.bootstrap_servers,
      "topic": topic,
      "format": args.format,
      "producer_config_updates": producer_properties(
          args, include_java_auth=True),
  }
  return beam.managed.Write(beam.managed.KAFKA, config=config)


def write_rust(args, topic):
  config = {
      "bootstrap_servers": args.bootstrap_servers,
      "topic": topic,
      "format": args.format,
      "with_gcp_adc": True,
  }
  props = producer_properties(args, include_java_auth=False)
  if props:
    config["producer_config_updates"] = props
  return SchemaAwareExternalTransform(
      RUST_WRITE_URN,
      args.expansion_service,
      rearrange_based_on_discovery=True,
      **config)


WRITERS = {"java": write_java, "rust": write_rust}


class TouchLoad(beam.DoFn):
  """Converts a BigQuery dict to a Beam row for the Kafka writer and counts ``e2e/rows``."""
  def __init__(self, raw):
    self.raw = raw
    self.rows = Metrics.counter("e2e", "rows")

  def process(self, record):
    self.rows.inc()
    if self.raw:
      yield RawRow(
          payload=json.dumps(record, separators=(",", ":")).encode("utf-8"))
    else:
      yield WikipediaRow(*(record.get(name) for name in WIKIPEDIA_FIELDS))


# Upper bounds (ms) of the latency buckets. ``e2e/latency_le_<bound>`` counts the rows with a
# latency above the previous bound and at most this bound; ``e2e/latency_le_inf`` counts the
# rest. Keep in sync with LATENCY_BOUNDS_MS in KafkaBenchmark.java and in io_report.py.
LATENCY_BOUNDS_MS = (
    50, 100, 250, 500, 1_000, 2_000, 5_000, 10_000, 30_000, 60_000, 120_000, 300_000)


class TouchCopy(beam.DoFn):
  """Counts ``e2e/rows`` and records ``e2e/latency_ms`` from the Kafka record timestamp.

  Each latency also goes to one ``e2e/latency_le_<ms>`` bucket counter, for percentiles.
  """
  def __init__(self):
    self.rows = Metrics.counter("e2e", "rows")
    self.latency_ms = Metrics.distribution("e2e", "latency_ms")
    self.buckets = [Metrics.counter("e2e", f"latency_le_{b}") for b in LATENCY_BOUNDS_MS
                    ] + [Metrics.counter("e2e", "latency_le_inf")]

  def process(self, row, timestamp=beam.DoFn.TimestampParam):
    self.rows.inc()
    try:
      ts_ms = int(timestamp.micros // 1000)
      now_ms = int(time.time() * 1000)
      if ts_ms > 0 and now_ms >= ts_ms:
        latency = now_ms - ts_ms
        self.latency_ms.update(latency)
        self.buckets[bisect.bisect_left(LATENCY_BOUNDS_MS, latency)].inc()
    except OverflowError:
      pass
    yield row


def gcloud(*args):
  out = subprocess.run(["gcloud", *args, "--format=json"],
                       check=True,
                       capture_output=True,
                       text=True).stdout
  return json.loads(out) if out.strip() else {}


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


def wait_for_streaming_job(project, region, job_id, expected_rows,
                           drain_after_seconds):
  start = time.time()
  drained = False
  while True:
    state = job_state(project, region, job_id)
    rows = committed_rows(project, region, job_id)
    elapsed = int(time.time() - start)
    logging.info("Job %s: state=%s e2e/rows=%d elapsed=%ds", job_id, state,
                 rows, elapsed)
    if state in ("JOB_STATE_DONE", "JOB_STATE_DRAINED", "JOB_STATE_CANCELLED"):
      if expected_rows is not None and rows < expected_rows:
        raise RuntimeError(
            f"Job {job_id} finished with {rows} rows, expected {expected_rows}")
      return
    if state in ("JOB_STATE_FAILED", "JOB_STATE_UPDATED"):
      raise RuntimeError(f"Job {job_id} ended in {state}")
    should_drain = ((expected_rows is not None and rows >= expected_rows) or
                    (drain_after_seconds is not None
                     and elapsed >= drain_after_seconds))
    if should_drain and not drained and state == "JOB_STATE_RUNNING":
      logging.info("Draining job %s at e2e/rows=%d (%ds)", job_id, rows,
                   elapsed)
      subprocess.run([
          "gcloud", "dataflow", "jobs", "drain", job_id,
          f"--project={project}", f"--region={region}"
      ],
                     check=True)
      drained = True
    time.sleep(15)


def main(argv):
  parser = argparse.ArgumentParser(description=__doc__)
  parser.add_argument(
      "--reader", choices=sorted(READERS), help="The reader of copy.")
  parser.add_argument("--writer", choices=sorted(WRITERS), required=True)
  parser.add_argument("--shape", choices=["load", "copy"], default="load")
  parser.add_argument("--format", choices=["JSON", "RAW"], default="JSON")
  parser.add_argument("--bootstrap_servers", required=True)
  parser.add_argument("--topic", required=True)
  parser.add_argument(
      "--destination_topic", help="The topic that copy writes to.")
  parser.add_argument("--table", default="bigquery-public-data:samples.wikipedia")
  parser.add_argument("--row_restriction", default="")
  parser.add_argument(
      "--auto_offset_reset", choices=["earliest", "latest"], default="earliest")
  parser.add_argument("--group_id", default="")
  parser.add_argument(
      "--consumer_properties",
      default="",
      help="More properties of the Kafka consumer, as key=value pairs.")
  parser.add_argument(
      "--producer_properties",
      default="",
      help="More properties of the Kafka producer, as key=value pairs.")
  parser.add_argument(
      "--max_read_time_per_call_seconds",
      type=int,
      default=0,
      help="For copy with the Rust reader, max seconds per read call.")
  parser.add_argument(
      "--max_idle_per_call_ms",
      type=int,
      default=0,
      help=(
          "For copy with the Rust reader, max milliseconds that a read call "
          "waits for the next record."))
  parser.add_argument("--billing_project", required=True)
  parser.add_argument("--expansion_service", default="localhost:8097")
  parser.add_argument(
      "--expected_rows",
      type=int,
      help="For copy, drain the job once e2e/rows reaches at least this count.")
  parser.add_argument(
      "--drain_after_seconds",
      type=int,
      help="For copy, drain the job after this many seconds.")
  parser.add_argument(
      "--detach",
      action="store_true",
      help="For copy, print the job ID and exit once the job is submitted.")
  args, pipeline_args = parser.parse_known_args(argv)

  if args.shape == "load":
    if args.reader:
      parser.error("--reader applies to copy only, not to load")
  else:
    if not args.reader or not args.destination_topic:
      parser.error("copy needs --reader and --destination_topic")

  options = PipelineOptions(pipeline_args)
  if args.shape == "copy":
    options.view_as(StandardOptions).streaming = True

  p = beam.Pipeline(options=options)
  if args.shape == "load":
    row_type = RawRow if args.format == "RAW" else WikipediaRow
    _ = (
        p
        | "Read" >> beam.io.ReadFromBigQuery(
            table=args.table,
            method=beam.io.ReadFromBigQuery.Method.DIRECT_READ,
            row_restriction=args.row_restriction or None)
        | "Touch" >> beam.ParDo(
            TouchLoad(raw=args.format == "RAW")).with_output_types(row_type)
        | "Write" >> WRITERS[args.writer](args, args.topic))
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
            f"Job {result.job_id()} wrote {rows} rows, expected {args.expected_rows}"
        )
  else:
    rows = p | "Read" >> READERS[args.reader](args)
    _ = (
        rows
        | "Touch" >> beam.ParDo(TouchCopy()).with_output_types(rows.element_type)
        | "Write" >> WRITERS[args.writer](args, args.destination_topic))
    result = p.run()
    job_id = result.job_id()
    print(f"JOB_ID={job_id}", flush=True)
    if args.detach:
      return
    gcloud_opts = options.view_as(GoogleCloudOptions)
    wait_for_streaming_job(gcloud_opts.project, gcloud_opts.region, job_id,
                           args.expected_rows, args.drain_after_seconds)


if __name__ == "__main__":
  logging.getLogger().setLevel(logging.INFO)
  main(sys.argv[1:])
