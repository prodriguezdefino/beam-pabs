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
"""BigQuery benchmark in a Python pipeline: one reader and, for ``copy``, one writer.

Readers (``--reader``):

* ``python``: the native Python Storage Read source (``ReadFromBigQuery`` with
  ``DIRECT_READ``). It emits dicts.
* ``java``: the Java provider through Managed I/O.
* ``rust``: the Rust provider through the Rust expansion service.

Writers (``--writer``, ``copy`` only):

* ``java``: ``WriteToBigQuery`` with ``STORAGE_WRITE_API``. Python has no Storage Write API
  of its own, so this expands on the Java expansion service.
* ``rust``: the Rust provider through the Rust expansion service.

Shapes:

* ``count`` counts the rows. The job does almost nothing but read.
* ``per_key`` counts the rows for each value of ``--key`` and then counts the keys.
* ``copy`` reads the table, counts each row in the ``e2e/rows`` metric in a Python step, and
  writes the rows to ``--destination``. The Python step runs in the Python worker, so the rows
  of a Rust or Java reader come to Python and go back to the writer through the Fn API data
  plane.

``../java/BigQueryBenchmark.java`` runs the same shapes in a Java pipeline.
``./gradlew :sdks:rust:bigQueryBenchmark`` runs this script.
"""

import argparse
import logging
import sys

import apache_beam as beam
from apache_beam.metrics import Metrics
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.transforms.external import SchemaAwareExternalTransform

RUST_READ_URN = "beam:schematransform:org.apache.beam:rust:bigquery_storage_read:v1"
RUST_WRITE_URN = "beam:schematransform:org.apache.beam:rust:bigquery_storage_write:v1"


def read_python(args):
  source = {"query": args.query, "use_standard_sql": True} if args.query else {
      "table": args.table
  }
  return beam.io.ReadFromBigQuery(
      **source,
      method=beam.io.ReadFromBigQuery.Method.DIRECT_READ,
      selected_fields=args.selected_fields or None,
      row_restriction=args.row_restriction or None)


def read_java(args):
  config = {"query": args.query} if args.query else {"table": args.table}
  if args.selected_fields:
    config["fields"] = args.selected_fields
  if args.row_restriction:
    config["row_restriction"] = args.row_restriction
  return beam.managed.Read(beam.managed.BIGQUERY, config=config)


def read_rust(args):
  source = {"query": args.query} if args.query else {"table_spec": args.table}
  config = {**source, "billing_project": args.billing_project}
  if args.selected_fields:
    config["selected_fields"] = args.selected_fields
  if args.row_restriction:
    config["row_restriction"] = args.row_restriction
  if args.compression:
    config["compression"] = args.compression
  return SchemaAwareExternalTransform(
      RUST_READ_URN,
      args.expansion_service,
      rearrange_based_on_discovery=True,
      **config)


READERS = {"python": read_python, "java": read_java, "rust": read_rust}


def write_java(args):
  """``args.schema``, if set, is the table schema of rows that are dicts, not Beam rows."""
  table = args.destination
  table = table if ":" in table else table.replace(".", ":", 1)
  return beam.io.WriteToBigQuery(
      table=table,
      schema=args.schema,
      method=beam.io.WriteToBigQuery.Method.STORAGE_WRITE_API,
      use_at_least_once=args.semantics == "at_least_once",
      create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
      write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)


def write_rust(args):
  return SchemaAwareExternalTransform(
      RUST_WRITE_URN,
      args.expansion_service,
      rearrange_based_on_discovery=True,
      table=args.destination,
      use_at_least_once_semantics=args.semantics == "at_least_once")


WRITERS = {"java": write_java, "rust": write_rust}


def field(row, name):
  """The Python reader emits dicts; the providers emit Beam rows."""
  return row[name] if isinstance(row, dict) else getattr(row, name)


class Touch(beam.DoFn):
  """Counts each row and emits it. The rows decode in Python, as any Python step needs them."""
  def __init__(self):
    self.rows = Metrics.counter("e2e", "rows")

  def process(self, row):
    self.rows.inc()
    yield row


def table_schema(project, table):
  """The schema of ``table``, for a writer that gets dicts from the Python reader."""
  from google.cloud import bigquery
  fields = bigquery.Client(project=project).get_table(table.replace(":", ".", 1)).schema
  return {"fields": [f.to_api_repr() for f in fields]}


def main(argv):
  parser = argparse.ArgumentParser(description=__doc__)
  parser.add_argument("--reader", choices=sorted(READERS), required=True)
  parser.add_argument("--writer", choices=sorted(WRITERS), help="The writer of copy.")
  parser.add_argument("--shape", choices=["count", "per_key", "copy"], default="count")
  parser.add_argument("--table", default="bigquery-public-data:samples.wikipedia")
  parser.add_argument(
      "--query",
      default="",
      help="Read the result of this GoogleSQL query instead of --table (not with copy).")
  parser.add_argument("--selected_fields", default="")
  parser.add_argument("--row_restriction", default="")
  parser.add_argument("--key", default="language")
  parser.add_argument("--destination", help="project.dataset.table that copy creates.")
  parser.add_argument(
      "--semantics", choices=["exactly_once", "at_least_once"], default="exactly_once")
  parser.add_argument("--billing_project", required=True)
  parser.add_argument("--expansion_service", default="localhost:8097")
  parser.add_argument(
      "--compression",
      default="",
      help="Arrow buffer compression of the Rust reader: NONE, LZ4_FRAME or ZSTD.")
  parser.add_argument(
      "--expected_rows",
      type=int,
      help="Fail the job unless it reads this many rows (count shape).")
  args, pipeline_args = parser.parse_known_args(argv)
  args.selected_fields = [f for f in args.selected_fields.split(",") if f]
  if args.query and (args.selected_fields or args.row_restriction):
    parser.error("--selected_fields and --row_restriction apply to table reads only")
  if args.shape == "per_key" and args.selected_fields and (
      args.key not in args.selected_fields):
    parser.error("--selected_fields must hold --key")
  if args.shape == "copy":
    if not args.writer or not args.destination:
      parser.error("copy needs --writer and --destination")
    if args.query or args.selected_fields:
      parser.error("copy reads whole rows of --table: no --query or --selected_fields")
    if args.reader == "python" and args.writer == "rust":
      parser.error("The Python reader gives dicts, and the Rust writer takes Beam rows")
  elif args.writer:
    parser.error(f"--writer applies to copy only, not to {args.shape}")
  args.schema = (
      table_schema(args.billing_project, args.table)
      if args.shape == "copy" and args.reader == "python" else None)

  with beam.Pipeline(options=PipelineOptions(pipeline_args)) as p:
    rows = p | "Read" >> READERS[args.reader](args)
    if args.shape == "count":
      total = rows | "CountRows" >> beam.combiners.Count.Globally()
      if args.expected_rows is not None:
        assert_that(total, equal_to([args.expected_rows]), label="CheckRows")
    elif args.shape == "per_key":
      key = args.key
      (
          rows
          | "Key" >> beam.Map(lambda row: (field(row, key), 1))
          | "CountPerKey" >> beam.CombinePerKey(sum)
          | "CountKeys" >> beam.combiners.Count.Globally())
    else:
      _ = (
          rows
          | "Touch" >> beam.ParDo(Touch()).with_output_types(rows.element_type)
          | "Write" >> WRITERS[args.writer](args))


if __name__ == "__main__":
  logging.getLogger().setLevel(logging.INFO)
  main(sys.argv[1:])
