#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""Python tests of the BigQuery Storage providers of the Rust expansion service.

The providers are
beam:schematransform:org.apache.beam:rust:bigquery_storage_read:v1 and
beam:schematransform:org.apache.beam:rust:bigquery_storage_write:v1.

The tests share a table with one column for each BigQuery type, in a new
dataset that they delete at the end. The read tests check the Python type of
every value. A round trip reads the table through the Rust read, writes the rows
to an empty copy of the table, and compares the two tables in BigQuery, at least
once and exactly once. An unbounded input writes through buffered streams. Other
tests create a table and send bad rows to the error output.

Run with ./gradlew :sdks:rust:gcpExpansionServicePythonTest (Prism) or
:sdks:rust:gcpExpansionServicePythonTestDataflow (Dataflow), which start the
expansion service and set the environment:

  EXPANSION_SERVICE  host:port of the Rust expansion service
  GCP_PROJECT        project that holds the dataset and pays for the jobs
  PRISM_LOCATION     optional path of the Prism binary
  PIPELINE_OPTIONS   optional pipeline options; without them, the tests use
                     Prism with a loopback Python worker
"""

import datetime
import decimal
import os
import shlex
import time
import unittest
import uuid

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.transforms.external import SchemaAwareExternalTransform
from apache_beam.transforms.periodicsequence import PeriodicImpulse
from apache_beam.typehints.row_type import RowTypeConstraint
from apache_beam.utils.timestamp import Timestamp
from google.cloud import bigquery

READ = "beam:schematransform:org.apache.beam:rust:bigquery_storage_read:v1"
WRITE = "beam:schematransform:org.apache.beam:rust:bigquery_storage_write:v1"

# One column for each BigQuery type. The second row holds NULL in every
# nullable column, and empty arrays, because an ARRAY column cannot be NULL.
# The decimals have at most 28 significant digits: the Python DecimalCoder
# decodes in the default decimal context, which rounds longer values.
CREATE_TABLE = """
CREATE TABLE `{table}` AS
SELECT
  1 AS id,
  CAST(-42 AS INT64) AS int64_col,
  CAST(1.5 AS FLOAT64) AS float64_col,
  NUMERIC '123.456789' AS numeric_col,
  BIGNUMERIC '1234567890.123456789012345678' AS bignumeric_col,
  TRUE AS bool_col,
  'héllo' AS string_col,
  b'\\x00\\x01\\xff' AS bytes_col,
  DATE '2024-05-01' AS date_col,
  DATETIME '2024-05-01 12:30:00.000001' AS datetime_col,
  TIME '12:30:00.000001' AS time_col,
  TIMESTAMP '2024-05-01 12:30:00.000001+00' AS timestamp_col,
  ST_GEOGPOINT(1, 2) AS geography_col,
  JSON '{{"a":1}}' AS json_col,
  STRUCT(7 AS x, 'seven' AS y) AS struct_col,
  [1, 2, 3] AS array_col,
  [STRUCT(1 AS x, 'one' AS y)] AS array_struct_col
UNION ALL
SELECT
  2,
  CAST(NULL AS INT64),
  CAST(NULL AS FLOAT64),
  CAST(NULL AS NUMERIC),
  CAST(NULL AS BIGNUMERIC),
  CAST(NULL AS BOOL),
  CAST(NULL AS STRING),
  CAST(NULL AS BYTES),
  CAST(NULL AS DATE),
  CAST(NULL AS DATETIME),
  CAST(NULL AS TIME),
  CAST(NULL AS TIMESTAMP),
  CAST(NULL AS GEOGRAPHY),
  CAST(NULL AS JSON),
  CAST(NULL AS STRUCT<x INT64, y STRING>),
  CAST([] AS ARRAY<INT64>),
  CAST([] AS ARRAY<STRUCT<x INT64, y STRING>>)
"""

FULL_ROW = {
    'id': 1,
    'int64_col': -42,
    'float64_col': 1.5,
    'numeric_col': decimal.Decimal('123.456789'),
    'bignumeric_col': decimal.Decimal('1234567890.123456789012345678'),
    'bool_col': True,
    'string_col': 'héllo',
    'bytes_col': b'\x00\x01\xff',
    'date_col': datetime.date(2024, 5, 1),
    'datetime_col': '2024-05-01T12:30:00.000001',
    'time_col': '12:30:00.000001',
    'timestamp_col': Timestamp.from_rfc3339('2024-05-01T12:30:00.000001Z'),
    'geography_col': 'POINT(1 2)',
    'json_col': '{"a":1}',
    'struct_col': {
        'x': 7, 'y': 'seven'
    },
    'array_col': [1, 2, 3],
    'array_struct_col': [{
        'x': 1, 'y': 'one'
    }],
}

NULL_ROW = {
    **{name: None
       for name in FULL_ROW},
    'id': 2,
    'array_col': [],
    'array_struct_col': [],
}


def _typed_function():
  # A local function is pickled by value, so a remote Python worker does not
  # import this module to run it.
  def typed(value):
    """Pairs each leaf value with the name of its Python type.

    Python compares some values of different types as equal, for example
    Decimal('1.5') == 1.5, so the tests compare the type names too. Rows become
    dicts and arrays become lists, whatever class Python decodes them to.
    """
    if value is None:
      return None
    if hasattr(value, '_asdict'):
      value = value._asdict()
    if isinstance(value, dict):
      return {name: typed(field) for name, field in value.items()}
    if isinstance(value, (list, tuple)):
      return [typed(item) for item in value]
    return (type(value).__name__, value)

  return typed


typed = _typed_function()

# A row with the position of an impulse. The type is generated from the fields,
# so a remote Python worker does not import this module to decode the rows.
POSITION = RowTypeConstraint.from_fields([('id', int)])

# Rows as JSON text, so that two tables compare in one value per row.
ROWS_AS_JSON = "SELECT TO_JSON_STRING(t) AS j FROM `{table}` t ORDER BY id"

class BigQueryStorageTest(unittest.TestCase):
  @classmethod
  def setUpClass(cls):
    cls.service = os.environ['EXPANSION_SERVICE']
    cls.project = os.environ['GCP_PROJECT']
    cls.client = bigquery.Client(project=cls.project)
    cls.dataset = f'{cls.project}.beam_rust_it_{uuid.uuid4().hex[:12]}'
    dataset = bigquery.Dataset(cls.dataset)
    dataset.default_table_expiration_ms = 24 * 60 * 60 * 1000
    cls.client.create_dataset(dataset)
    cls.table = f'{cls.dataset}.all_types'
    cls.client.query(CREATE_TABLE.format(table=cls.table)).result()

  @classmethod
  def tearDownClass(cls):
    cls.client.delete_dataset(
        cls.dataset, delete_contents=True, not_found_ok=True)

  def pipeline(self, streaming=False):
    options = os.environ.get('PIPELINE_OPTIONS')
    if options:
      args = shlex.split(options)
      if streaming:
        args.append('--streaming')
      return beam.Pipeline(options=PipelineOptions(args))
    args = ['--runner=PrismRunner', '--environment_type=LOOPBACK']
    prism = os.environ.get('PRISM_LOCATION')
    if prism:
      args.append(f'--prism_location={prism}')
    return beam.Pipeline(options=PipelineOptions(args))

  def read(self, p, **config):
    return p | 'Read' >> SchemaAwareExternalTransform(
        READ,
        self.service,
        table_spec=self.table,
        billing_project=self.project,
        **config)

  def new_table(self, name):
    return f'{self.dataset}.{name}_{uuid.uuid4().hex[:8]}'

  def rows_as_json(self, table):
    query = ROWS_AS_JSON.format(table=table)
    return [row.j for row in self.client.query(query).result()]

  # Read.

  def test_every_column_has_the_expected_python_type(self):
    with self.pipeline() as p:
      rows = self.read(p) | beam.Map(typed)
      assert_that(rows, equal_to([typed(FULL_ROW), typed(NULL_ROW)]))

  def test_selected_fields_and_row_restriction(self):
    with self.pipeline() as p:
      rows = self.read(
          p, selected_fields=['id', 'string_col'],
          row_restriction='id = 1') | beam.Map(typed)
      assert_that(
          rows, equal_to([typed({
              'id': 1, 'string_col': 'héllo'
          })]))

  def test_query_read(self):
    with self.pipeline() as p:
      rows = (
          p | SchemaAwareExternalTransform(
              READ,
              self.service,
              query=f'SELECT id, string_col FROM `{self.table}` WHERE id = 1',
              billing_project=self.project,
              temp_dataset=self.dataset)
          | beam.Map(typed))
      assert_that(
          rows, equal_to([typed({
              'id': 1, 'string_col': 'héllo'
          })]))

  def test_snapshot_time_reads_the_table_as_it_was(self):
    target = self.new_table('snapshot')
    self.client.query(f'CREATE TABLE `{target}` AS SELECT 1 AS id').result()
    # The time of BigQuery, so the clock of this machine does not count.
    (snapshot, ) = next(iter(self.client.query(
        "SELECT FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%E6SZ', CURRENT_TIMESTAMP())"
    ).result()))
    time.sleep(1)
    self.client.query(f'INSERT INTO `{target}` VALUES (2)').result()
    with self.pipeline() as p:
      rows = (
          p
          | SchemaAwareExternalTransform(
              READ,
              self.service,
              table_spec=target,
              billing_project=self.project,
              snapshot_time=snapshot,
              preferred_min_stream_count=2)
          | beam.Map(lambda row: row.id))
      assert_that(rows, equal_to([1]))

  def test_error_handling_keeps_the_rows_in_the_main_output(self):
    # A table read converts every row of the session schema, so the error
    # output is empty. Rust tests cover the rows that do not convert.
    with self.pipeline() as p:
      outputs = self.read(p, error_handling=beam.Row(output='errors'))
      assert_that(
          outputs['output'] | beam.Map(typed),
          equal_to([typed(FULL_ROW), typed(NULL_ROW)]),
          label='Rows')
      assert_that(outputs['errors'], equal_to([]), label='Errors')

  # Write.

  def round_trip(self, **config):
    """Copies the table through the Rust read and write."""
    target = self.new_table('copy')
    self.client.query(
        f'CREATE TABLE `{target}` LIKE `{self.table}`').result()
    with self.pipeline() as p:
      _ = (
          self.read(p)
          | 'Write' >> SchemaAwareExternalTransform(
              WRITE,
              self.service,
              table=target,
              create_disposition='CREATE_NEVER',
              **config))
    self.assertEqual(
        self.rows_as_json(target), self.rows_as_json(self.table))

  def test_exactly_once_copies_every_type(self):
    self.round_trip()

  def test_at_least_once_copies_every_type(self):
    self.round_trip(use_at_least_once_semantics=True)

  def test_an_unbounded_input_writes_buffered_streams(self):
    # An unbounded input takes the buffered streams, with auto-sharding. The ids are the
    # positions of the impulses, so the table must hold 0..n once each.
    target = self.new_table('buffered')
    start = time.time()
    interval = 0.01
    with self.pipeline(streaming=True) as p:
      _ = (
          p
          | PeriodicImpulse(
              start_timestamp=start,
              stop_timestamp=start + 3,
              fire_interval=interval)
          | beam.Map(lambda at: beam.Row(id=round((float(at) - start) / interval))).
          with_output_types(POSITION)
          | SchemaAwareExternalTransform(
              WRITE, self.service, table=target, triggering_frequency_seconds=1))
    query = (
        'SELECT COUNT(*) AS n, COUNT(DISTINCT id) AS ids, MIN(id) AS low, '
        f'MAX(id) AS high FROM `{target}`')
    row = next(iter(self.client.query(query).result()))
    self.assertGreater(row.n, 0)
    self.assertEqual((row.n, row.ids, row.low), (row.high + 1, row.high + 1, 0))

  def test_a_missing_table_is_created_from_the_row_fields(self):
    target = self.new_table('created')
    with self.pipeline() as p:
      _ = (
          p
          | beam.Create([beam.Row(id=1, name='a'), beam.Row(id=2, name='b')])
          | SchemaAwareExternalTransform(WRITE, self.service, table=target))
    columns = [(f.name, f.field_type, f.mode)
               for f in self.client.get_table(target).schema]
    self.assertEqual(
        columns, [('id', 'INTEGER', 'REQUIRED'),
                  ('name', 'STRING', 'REQUIRED')])
    self.assertEqual(
        self.rows_as_json(target),
        ['{"id":1,"name":"a"}', '{"id":2,"name":"b"}'])

  def test_rows_that_do_not_convert_go_to_the_error_output(self):
    target = self.new_table('errors')
    self.client.query(
        f'CREATE TABLE `{target}` (id INT64, civil DATETIME)').result()
    with self.pipeline() as p:
      errors = (
          p
          | beam.Create([
              beam.Row(id=1, civil='2024-05-01T12:30:00'),
              beam.Row(id=2, civil='yesterday'),
          ])
          | SchemaAwareExternalTransform(
              WRITE,
              self.service,
              table=target,
              error_handling=beam.Row(output='errors')))
      # With one output tag, the transform returns that output itself.
      failed = errors | beam.Map(
          lambda error: (error.failed_row.id, 'yesterday' in error.
                         error_message))
      assert_that(failed, equal_to([(2, True)]))
    self.assertEqual(
        self.rows_as_json(target), ['{"id":1,"civil":"2024-05-01T12:30:00"}'])

  # Field selection.

  def write_rows(self, rows, **config):
    """Writes `rows` to a new table and returns the table."""
    target = self.new_table('fields')
    with self.pipeline() as p:
      _ = (
          p
          | beam.Create(rows)
          | SchemaAwareExternalTransform(
              WRITE, self.service, table=target, **config))
    return target

  def columns(self, table):
    return [f.name for f in self.client.get_table(table).schema]

  def test_keep_writes_only_the_named_fields(self):
    target = self.write_rows([
        beam.Row(id=1, name='a', extra='x'),
        beam.Row(id=2, name='b', extra='y'),
    ], keep=['id', 'name'])
    self.assertEqual(self.columns(target), ['id', 'name'])
    self.assertEqual(
        self.rows_as_json(target),
        ['{"id":1,"name":"a"}', '{"id":2,"name":"b"}'])

  def test_drop_writes_the_other_fields(self):
    target = self.write_rows([
        beam.Row(id=1, name='a', extra='x'),
        beam.Row(id=2, name='b', extra='y'),
    ], drop=['extra'])
    self.assertEqual(self.columns(target), ['id', 'name'])
    self.assertEqual(
        self.rows_as_json(target),
        ['{"id":1,"name":"a"}', '{"id":2,"name":"b"}'])

  def test_only_writes_the_fields_of_a_nested_row(self):
    target = self.write_rows([
        beam.Row(meta='m1', payload=beam.Row(id=1, name='a')),
        beam.Row(meta='m2', payload=beam.Row(id=2, name='b')),
    ], only='payload')
    self.assertEqual(self.columns(target), ['id', 'name'])
    self.assertEqual(
        self.rows_as_json(target),
        ['{"id":1,"name":"a"}', '{"id":2,"name":"b"}'])

  def test_an_error_row_of_only_holds_the_input_row(self):
    target = self.new_table('only_errors')
    self.client.query(
        f'CREATE TABLE `{target}` (id INT64, civil DATETIME)').result()
    with self.pipeline() as p:
      errors = (
          p
          | beam.Create([
              beam.Row(
                  meta='m1', payload=beam.Row(id=1, civil='2024-05-01T12:30:00')),
              beam.Row(meta='m2', payload=beam.Row(id=2, civil='yesterday')),
          ])
          | SchemaAwareExternalTransform(
              WRITE,
              self.service,
              table=target,
              create_disposition='CREATE_NEVER',
              only='payload',
              error_handling=beam.Row(output='errors')))
      failed = errors | beam.Map(
          lambda error: (error.failed_row.meta, error.failed_row.payload.id))
      assert_that(failed, equal_to([('m2', 2)]))
    self.assertEqual(
        self.rows_as_json(target), ['{"id":1,"civil":"2024-05-01T12:30:00"}'])

  def test_ignore_unknown_values_drops_the_fields_that_the_table_lacks(self):
    target = self.new_table('ignore')
    self.client.query(
        f'CREATE TABLE `{target}` (id INT64, name STRING)').result()
    rows = [
        beam.Row(id=1, name='a', unknown='x'),
        beam.Row(id=2, name='b', unknown='y'),
    ]
    with self.pipeline() as p:
      _ = (
          p
          | beam.Create(rows)
          | SchemaAwareExternalTransform(
              WRITE,
              self.service,
              table=target,
              create_disposition='CREATE_NEVER',
              ignore_unknown_values=True))
    self.assertEqual(
        self.rows_as_json(target),
        ['{"id":1,"name":"a"}', '{"id":2,"name":"b"}'])

  def test_without_ignore_unknown_values_a_missing_column_fails(self):
    target = self.new_table('unknown')
    self.client.query(
        f'CREATE TABLE `{target}` (id INT64, name STRING)').result()
    with self.assertRaisesRegex(Exception, "no column 'unknown'"):
      with self.pipeline() as p:
        _ = (
            p
            | beam.Create([beam.Row(id=1, name='a', unknown='x')])
            | SchemaAwareExternalTransform(
                WRITE,
                self.service,
                table=target,
                create_disposition='CREATE_NEVER'))

  def test_num_streams_shards_an_unbounded_write(self):
    # Python sends num_streams as INT64. The provider config takes INT32, and
    # the expansion service narrows the value.
    target = self.new_table('streams')
    start = time.time()
    interval = 0.05
    with self.pipeline(streaming=True) as p:
      _ = (
          p
          | PeriodicImpulse(
              start_timestamp=start,
              stop_timestamp=start + 2,
              fire_interval=interval)
          | beam.Map(lambda at: beam.Row(id=round((float(at) - start) / interval))).
          with_output_types(POSITION)
          | SchemaAwareExternalTransform(
              WRITE,
              self.service,
              table=target,
              triggering_frequency_seconds=1,
              num_streams=2))
    query = (
        'SELECT COUNT(*) AS n, COUNT(DISTINCT id) AS ids, MIN(id) AS low, '
        f'MAX(id) AS high FROM `{target}`')
    row = next(iter(self.client.query(query).result()))
    self.assertGreater(row.n, 0)
    self.assertEqual((row.n, row.ids, row.low), (row.high + 1, row.high + 1, 0))


if __name__ == '__main__':
  unittest.main()
