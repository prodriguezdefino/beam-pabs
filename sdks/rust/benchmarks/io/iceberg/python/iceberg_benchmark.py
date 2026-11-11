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
"""Iceberg benchmark in a Python pipeline: the reference job on a Kafka topic.

The pipeline is ``ingest_pipeline.py`` of the reference job (``dataflow_iceberg_lrc``) with
one change: it reads ``--topic`` with Managed Kafka (``RAW`` format) instead of Pub/Sub. The
parse, the ``DeviceMetric`` row and the Managed Iceberg write keep their configuration. A
step before the parse counts ``e2e/rows`` and records ``e2e/latency_ms``, as in the other
pipelines of the benchmark.

``./gradlew :sdks:rust:icebergBenchmark -Ppipeline=python -Pwriter=java`` runs this script;
``benchmarks/tools/job_wait.py`` waits for and drains the submitted job.
"""

import argparse
import bisect
import json
import logging
import sys
import time
from typing import NamedTuple

import apache_beam as beam
from apache_beam import managed
from apache_beam.metrics import Metrics
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.typehints import schemas
from apache_beam.utils.timestamp import Timestamp as BeamTimestamp

MANAGED_KAFKA_AUTH = {
    "security.protocol": "SASL_SSL",
    "sasl.mechanism": "OAUTHBEARER",
    "sasl.login.callback.handler.class": (
        "com.google.cloud.hosted.kafka.auth.GcpLoginCallbackHandler"),
    "sasl.jaas.config": (
        "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;"),
}

# Keep in sync with LATENCY_BOUNDS_MS of the Kafka benchmark and of io_report.py.
LATENCY_BOUNDS_MS = (
    50, 100, 250, 500, 1_000, 2_000, 5_000, 10_000, 30_000, 60_000, 120_000, 300_000)


class DeviceMetric(NamedTuple):
  """The row of the reference job. ``timestamp_ms`` is a millis instant, so the Iceberg column
  is a timestamp and ``hour()`` partitions it."""
  id: int
  timestamp_ms: schemas.MillisInstant
  device_id: str
  payload: float
  location: str


beam.coders.registry.register_coder(DeviceMetric, beam.coders.RowCoder)


class Touch(beam.DoFn):
  """Counts ``e2e/rows`` and records ``e2e/latency_ms`` from the Kafka record timestamp."""
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


@beam.typehints.with_output_types(DeviceMetric)
class ParseAndValidateMessage(beam.DoFn):
  """The parse of the reference job. A record that does not parse is logged and dropped."""
  def __init__(self):
    self.parse_errors = Metrics.counter("e2e", "parse_errors")

  def process(self, row):
    element = row.payload
    try:
      data = json.loads(element.decode("utf-8"))
      yield DeviceMetric(
          id=int(data["id"]),
          timestamp_ms=BeamTimestamp.of(float(data["timestamp"]) / 1000.0),
          device_id=str(data["device_id"]),
          payload=float(data["payload"]),
          location=str(data.get("location", "us-central1")),
      )
    except Exception as e:  # pylint: disable=broad-except
      self.parse_errors.inc()
      logging.warning("Skipping malformed message: %s | Raw element: %s", e, element)


def main(argv):
  parser = argparse.ArgumentParser(description=__doc__)
  parser.add_argument("--writer", choices=["java"], required=True)
  parser.add_argument("--shape", choices=["ingest"], default="ingest")
  parser.add_argument("--bootstrap_servers", required=True)
  parser.add_argument("--topic", required=True)
  parser.add_argument("--auto_offset_reset", choices=["earliest", "latest"], default="latest")
  parser.add_argument("--group_id", default="")
  parser.add_argument("--catalog_uri",
                      default="https://biglake.googleapis.com/iceberg/v1/restcatalog")
  parser.add_argument("--warehouse", required=True)
  parser.add_argument("--table", required=True)
  parser.add_argument("--user_project", required=True)
  parser.add_argument("--gcp_location", required=True)
  parser.add_argument("--triggering_frequency_seconds", type=int, default=60)
  parser.add_argument("--object_storage", action="store_true")
  parser.add_argument("--billing_project", required=True)
  args, pipeline_args = parser.parse_known_args(argv)

  options = PipelineOptions(pipeline_args)
  options.view_as(StandardOptions).streaming = True
  # A new group for each job, as in the Java pipeline, so a job does not resume from the
  # offsets that an earlier job committed.
  consumer = dict(MANAGED_KAFKA_AUTH)
  consumer["group.id"] = args.group_id or f"iceberg-bench-python-{int(time.time() * 1000)}"
  catalog_props = {
      "type": "rest",
      "uri": args.catalog_uri,
      "warehouse": args.warehouse,
      "gcp_location": args.gcp_location,
      "header.x-goog-user-project": args.user_project,
      "rest.auth.type": "google",
      "io-impl": "org.apache.iceberg.gcp.gcs.GCSFileIO",
      "header.X-Iceberg-Access-Delegation": "vended-credentials",
      "rest-metrics-reporting-enabled": "true",
  }
  table_props = {
      "gcp.biglake.table-management.enabled": "true",
      "gcp.biglake.bigquery-dml.enabled": "true",
      "write.parquet.compression-codec": "zstd",
  }
  if args.object_storage:
    table_props["write.object-storage.enabled"] = "true"

  p = beam.Pipeline(options=options)
  _ = (
      p
      | "Read" >> managed.Read(managed.KAFKA, config={
          "bootstrap_servers": args.bootstrap_servers,
          "topic": args.topic,
          "format": "RAW",
          "auto_offset_reset_config": args.auto_offset_reset,
          "consumer_config_updates": consumer,
      })
      | "Touch" >> beam.ParDo(Touch())
      | "ParseAndValidateMessage" >> beam.ParDo(
          ParseAndValidateMessage()).with_output_types(DeviceMetric)
      | "WriteToIcebergLakehouse" >> managed.Write(managed.ICEBERG, config={
          "table": args.table,
          "catalog_name": "biglake",
          "catalog_properties": catalog_props,
          "table_properties": table_props,
          "partition_fields": ["hour(timestamp_ms)"],
          "triggering_frequency_seconds": args.triggering_frequency_seconds,
      }))
  result = p.run()
  print(f"JOB_ID={result.job_id()}", flush=True)


if __name__ == "__main__":
  logging.getLogger().setLevel(logging.INFO)
  main(sys.argv[1:])
