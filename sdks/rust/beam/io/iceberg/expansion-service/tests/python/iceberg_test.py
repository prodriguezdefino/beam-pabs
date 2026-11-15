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
"""Python tests of the Iceberg write provider of the Rust expansion service.

The provider is beam:schematransform:org.apache.beam:rust:iceberg_write:v1.

The tests start an in-process HTTP server bound to 127.0.0.1:0 that implements
the Iceberg REST catalog endpoints used by PrepareTableFn, WriteDataFilesFn and
CommitFn (/v1/config, namespaces, tables, credentials, commit and metrics),
backed by a temporary local warehouse directory.

Run with ./gradlew :sdks:rust:icebergExpansionServicePythonTest, which starts
the expansion service and sets the environment:

  EXPANSION_SERVICE  host:port of the Rust expansion service
  PRISM_LOCATION     optional path of the Prism binary
  PIPELINE_OPTIONS   optional pipeline options; without them, the tests use
                     Prism with a loopback Python worker
"""

import copy
import glob
import http.server
import json
import os
import shlex
import shutil
import sys
import tempfile
import threading
import time
import typing
import unittest
import urllib.parse
import uuid

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.transforms.external import SchemaAwareExternalTransform
from apache_beam.typehints import schemas
from apache_beam.utils.timestamp import Timestamp as BeamTimestamp

URN_ICEBERG_WRITE = 'beam:schematransform:org.apache.beam:rust:iceberg_write:v1'


class MetricRow(typing.NamedTuple):
  id: int
  timestamp_ms: schemas.MillisInstant
  device_id: str
  payload: float
  location: typing.Optional[str]


beam.coders.registry.register_coder(MetricRow, beam.coders.RowCoder)


def _max_field_id(node):
  """Returns the largest Iceberg field id in a JSON schema node."""
  best = 0
  if isinstance(node, dict):
    for key in ('id', 'element-id', 'key-id', 'value-id'):
      val = node.get(key)
      if isinstance(val, int) and val > best:
        best = val
    for child in node.values():
      best = max(best, _max_field_id(child))
  elif isinstance(node, list):
    for item in node:
      best = max(best, _max_field_id(item))
  return best


class _MockCatalogState:
  """Thread-safe Iceberg REST catalog state backed by a local warehouse."""
  def __init__(self, warehouse_dir):
    self.warehouse_dir = warehouse_dir
    self.lock = threading.Lock()
    self.namespaces = {}
    self.tables = {}
    self.metrics_reports = {}
    self.credential_calls = []
    self.request_headers = []

  def record_headers(self, method, path, headers):
    with self.lock:
      self.request_headers.append(
          (method, path, {k.lower(): v
                          for k, v in headers.items()}))

  def namespace_exists(self, ns):
    with self.lock:
      return tuple(ns) in self.namespaces

  def get_namespace(self, ns):
    with self.lock:
      props = self.namespaces.get(tuple(ns))
      return None if props is None else dict(props)

  def create_namespace(self, ns, properties):
    key = tuple(ns)
    with self.lock:
      if key in self.namespaces:
        return None
      self.namespaces[key] = dict(properties)
      return dict(self.namespaces[key])

  def get_table(self, ns, name):
    with self.lock:
      entry = self.tables.get((tuple(ns), name))
      return None if entry is None else copy.deepcopy(entry)

  def create_table(self, ns, body):
    name = body['name']
    key = (tuple(ns), name)
    with self.lock:
      if key in self.tables:
        return None
      schema = copy.deepcopy(body['schema'])
      schema.setdefault('type', 'struct')
      schema.setdefault('schema-id', 0)
      last_column_id = _max_field_id(schema)

      partition_spec = copy.deepcopy(
          body.get('partition-spec') or {
              'spec-id': 0, 'fields': []
          })
      partition_spec.setdefault('spec-id', 0)
      next_part_id = 1000
      fields = []
      for pf in partition_spec.get('fields', []):
        pf_copy = dict(pf)
        if pf_copy.get('field-id') is None:
          pf_copy['field-id'] = next_part_id
        next_part_id = max(next_part_id, pf_copy['field-id'] + 1)
        fields.append(pf_copy)
      partition_spec['fields'] = fields
      last_partition_id = max((pf['field-id'] for pf in fields), default=999)

      sort_order = copy.deepcopy(
          body.get('write-order') or {
              'order-id': 0, 'fields': []
          })
      sort_order.setdefault('order-id', 0)
      sort_order.setdefault('fields', [])

      table_uuid = str(uuid.uuid4())
      now_ms = int(time.time() * 1000)
      table_dir = os.path.join(self.warehouse_dir, *ns, name)
      location = f'file://{table_dir}'
      metadata = {
          'format-version': 2,
          'table-uuid': table_uuid,
          'location': location,
          'last-sequence-number': 0,
          'last-updated-ms': now_ms,
          'last-column-id': last_column_id,
          'current-schema-id': schema['schema-id'],
          'schemas': [schema],
          'default-spec-id': partition_spec['spec-id'],
          'partition-specs': [partition_spec],
          'last-partition-id': last_partition_id,
          'default-sort-order-id': sort_order['order-id'],
          'sort-orders': [sort_order],
          'properties': dict(body.get('properties') or {}),
          'current-snapshot-id': None,
          'snapshots': [],
          'snapshot-log': [],
          'metadata-log': [],
          'refs': {},
      }
      metadata_location = self._write_metadata_file(table_dir, 0, metadata)
      entry = {
          'version': 0,
          'table_dir': table_dir,
          'metadata_location': metadata_location,
          'metadata': metadata,
      }
      self.tables[key] = entry
      return copy.deepcopy(entry)

  def commit_table(self, ns, name, body):
    key = (tuple(ns), name)
    with self.lock:
      entry = self.tables.get(key)
      if entry is None:
        return 'not_found', 'Table does not exist'
      metadata = copy.deepcopy(entry['metadata'])

      for req in body.get('requirements', []):
        req_type = req.get('type')
        if req_type == 'assert-table-uuid':
          if req.get('uuid') != metadata['table-uuid']:
            return 'conflict', 'Table UUID mismatch'
        elif req_type == 'assert-ref-snapshot-id':
          ref_name = req.get('ref')
          expected_id = req.get('snapshot-id')
          actual_id = metadata['refs'].get(ref_name, {}).get('snapshot-id')
          if actual_id != expected_id:
            return (
                'conflict',
                f'Ref {ref_name} is at {actual_id}, expected {expected_id}')

      now_ms = int(time.time() * 1000)
      for update in body.get('updates', []):
        action = update.get('action')
        if action == 'add-snapshot':
          snap = update['snapshot']
          metadata['snapshots'].append(snap)
          seq = snap.get('sequence-number', 0)
          if seq > metadata['last-sequence-number']:
            metadata['last-sequence-number'] = seq
          metadata['last-updated-ms'] = max(
              metadata['last-updated-ms'], snap.get('timestamp-ms', now_ms))
        elif action == 'set-snapshot-ref':
          ref_name = update['ref-name']
          snap_id = update['snapshot-id']
          ref_obj = {
              k: v
              for k, v in update.items() if k not in ('action', 'ref-name')
          }
          metadata['refs'][ref_name] = ref_obj
          if ref_name == 'main':
            metadata['current-snapshot-id'] = snap_id
            metadata['snapshot-log'].append({
                'timestamp-ms': metadata['last-updated-ms'],
                'snapshot-id': snap_id,
            })
        elif action == 'set-properties':
          metadata['properties'].update(update.get('updates', {}))
        elif action == 'remove-properties':
          for prop in update.get('removals', []):
            metadata['properties'].pop(prop, None)

      metadata['metadata-log'].append({
          'timestamp-ms': metadata['last-updated-ms'],
          'metadata-file': entry['metadata_location'],
      })
      next_version = entry['version'] + 1
      metadata_location = self._write_metadata_file(
          entry['table_dir'], next_version, metadata)
      entry['version'] = next_version
      entry['metadata_location'] = metadata_location
      entry['metadata'] = metadata
      return 'ok', copy.deepcopy(entry)

  def record_credentials_call(self, ns, name):
    with self.lock:
      self.credential_calls.append((tuple(ns), name))

  def record_metrics(self, ns, name, report):
    key = (tuple(ns), name)
    with self.lock:
      self.metrics_reports.setdefault(key, []).append(report)

  def _write_metadata_file(self, table_dir, version, metadata):
    meta_dir = os.path.join(table_dir, 'metadata')
    os.makedirs(meta_dir, exist_ok=True)
    file_name = f'{version:05d}-{uuid.uuid4()}.metadata.json'
    local_path = os.path.join(meta_dir, file_name)
    with open(local_path, 'w', encoding='utf-8') as f:
      json.dump(metadata, f)
    return f'file://{local_path}'


def _make_handler(state):
  class CatalogHandler(http.server.BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
      del fmt, args

    def _parts(self):
      parsed = urllib.parse.urlsplit(self.path)
      return [
          urllib.parse.unquote(p) for p in parsed.path.split('/') if p
      ]

    def _read_json(self):
      length = int(self.headers.get('Content-Length', '0'))
      raw = self.rfile.read(length) if length > 0 else b'{}'
      return json.loads(raw.decode('utf-8'))

    def _send_json(self, status, payload):
      data = json.dumps(payload).encode('utf-8')
      self.send_response(status)
      self.send_header('Content-Type', 'application/json')
      self.send_header('Content-Length', str(len(data)))
      self.end_headers()
      self.wfile.write(data)

    def _send_error_json(self, status, exc_type, message):
      self._send_json(
          status,
          {'error': {
              'message': message, 'type': exc_type, 'code': status
          }})

    def _send_empty(self, status):
      self.send_response(status)
      self.send_header('Content-Length', '0')
      self.end_headers()

    def do_HEAD(self):
      state.record_headers('HEAD', self.path, self.headers)
      parts = self._parts()
      if len(parts) == 3 and parts[:2] == ['v1', 'namespaces']:
        ns = parts[2].split('\x1f')
        self._send_empty(204 if state.namespace_exists(ns) else 404)
        return
      if (len(parts) == 5 and parts[:2] == ['v1', 'namespaces'] and
          parts[3] == 'tables'):
        ns = parts[2].split('\x1f')
        entry = state.get_table(ns, parts[4])
        self._send_empty(204 if entry is not None else 404)
        return
      self._send_empty(404)

    def do_GET(self):
      state.record_headers('GET', self.path, self.headers)
      parts = self._parts()
      if parts == ['v1', 'config']:
        self._send_json(200, {'defaults': {}, 'overrides': {}})
        return
      if len(parts) == 3 and parts[:2] == ['v1', 'namespaces']:
        ns = parts[2].split('\x1f')
        props = state.get_namespace(ns)
        if props is None:
          self._send_error_json(
              404, 'NoSuchNamespaceException', f'Missing namespace {ns}')
        else:
          self._send_json(200, {'namespace': ns, 'properties': props})
        return
      if (len(parts) == 5 and parts[:2] == ['v1', 'namespaces'] and
          parts[3] == 'tables'):
        ns = parts[2].split('\x1f')
        entry = state.get_table(ns, parts[4])
        if entry is None:
          self._send_error_json(
              404, 'NoSuchTableException', f'Missing table {parts[4]}')
        else:
          self._send_json(
              200,
              {
                  'metadata-location': entry['metadata_location'],
                  'metadata': entry['metadata'],
                  'config': {},
              })
        return
      if (len(parts) == 6 and parts[:2] == ['v1', 'namespaces'] and
          parts[3] == 'tables' and parts[5] == 'credentials'):
        ns = parts[2].split('\x1f')
        entry = state.get_table(ns, parts[4])
        if entry is None:
          self._send_error_json(
              404, 'NoSuchTableException', f'Missing table {parts[4]}')
        else:
          state.record_credentials_call(ns, parts[4])
          self._send_json(
              200,
              {
                  'storage-credentials': [{
                      'prefix': entry['metadata']['location'],
                      'config': {
                          'client.region': 'local'
                      },
                  }]
              })
        return
      self._send_error_json(404, 'NotFoundException', f'Unknown path {parts}')

    def do_POST(self):
      state.record_headers('POST', self.path, self.headers)
      parts = self._parts()
      body = self._read_json()
      if parts == ['v1', 'namespaces']:
        ns = body['namespace']
        props = state.create_namespace(ns, body.get('properties') or {})
        if props is None:
          self._send_error_json(
              409, 'AlreadyExistsException', f'Namespace {ns} exists')
        else:
          self._send_json(200, {'namespace': ns, 'properties': props})
        return
      if (len(parts) == 4 and parts[:2] == ['v1', 'namespaces'] and
          parts[3] == 'tables'):
        ns = parts[2].split('\x1f')
        entry = state.create_table(ns, body)
        if entry is None:
          self._send_error_json(
              409, 'AlreadyExistsException', f'Table {body.get("name")} exists')
        else:
          self._send_json(
              200,
              {
                  'metadata-location': entry['metadata_location'],
                  'metadata': entry['metadata'],
                  'config': {},
              })
        return
      if (len(parts) == 5 and parts[:2] == ['v1', 'namespaces'] and
          parts[3] == 'tables'):
        ns = parts[2].split('\x1f')
        outcome, payload = state.commit_table(ns, parts[4], body)
        if outcome == 'not_found':
          self._send_error_json(404, 'NoSuchTableException', payload)
        elif outcome == 'conflict':
          self._send_error_json(409, 'CommitFailedException', payload)
        else:
          self._send_json(
              200,
              {
                  'metadata-location': payload['metadata_location'],
                  'metadata': payload['metadata'],
              })
        return
      if (len(parts) == 6 and parts[:2] == ['v1', 'namespaces'] and
          parts[3] == 'tables' and parts[5] == 'metrics'):
        ns = parts[2].split('\x1f')
        state.record_metrics(ns, parts[4], body)
        self._send_empty(204)
        return
      self._send_error_json(404, 'NotFoundException', f'Unknown POST {parts}')

  return CatalogHandler


class IcebergExpansionTest(unittest.TestCase):
  @classmethod
  def setUpClass(cls):
    cls.service = os.environ['EXPANSION_SERVICE']
    cls.warehouse_dir = tempfile.mkdtemp(
        prefix='beam_iceberg_warehouse_', dir='/tmp')
    cls.catalog = _MockCatalogState(cls.warehouse_dir)
    # Bind strictly to 127.0.0.1:0 (never 0.0.0.0).
    cls.httpd = http.server.ThreadingHTTPServer(('127.0.0.1', 0),
                                                _make_handler(cls.catalog))
    port = cls.httpd.server_address[1]
    default_host = (
        '127.0.0.1'
        if sys.platform.startswith('linux') else 'host.docker.internal')
    host = os.environ.get('BEAM_ICEBERG_CATALOG_HOST', default_host)
    cls.catalog_uri = f'http://{host}:{port}'
    cls.server_thread = threading.Thread(
        target=cls.httpd.serve_forever, daemon=True)
    cls.server_thread.start()

  @classmethod
  def tearDownClass(cls):
    cls.httpd.shutdown()
    cls.httpd.server_close()
    shutil.rmtree(cls.warehouse_dir, ignore_errors=True)

  def pipeline(self, streaming=False):
    options = os.environ.get('PIPELINE_OPTIONS')
    args = shlex.split(options) if options else [
        '--runner=PrismRunner', '--environment_type=LOOPBACK'
    ]
    prism = os.environ.get('PRISM_LOCATION')
    if prism and not options:
      args.append(f'--prism_location={prism}')
    if streaming:
      args.append('--streaming')
    return beam.Pipeline(options=PipelineOptions(args))

  def new_table(self, prefix):
    return f'iceberg_it.{prefix}_{uuid.uuid4().hex[:8]}'

  def test_bounded_write_creates_partitioned_table_and_emits_snapshot(self):
    target = self.new_table('metrics')
    base_sec = 1_700_000_000
    rows = [
        MetricRow(
            id=i,
            timestamp_ms=BeamTimestamp.of(base_sec + (i % 2) * 3600),
            device_id=f'dev-{i % 3}',
            payload=float(i) * 1.5,
            location='us-central1' if i % 2 == 0 else None,
        ) for i in range(40)
    ]
    catalog_props = {
        'type': 'rest',
        'uri': self.catalog_uri,
        'warehouse': f'file://{self.warehouse_dir}',
        'rest.auth.type': 'oauth2',
        'token': 'test-bearer-token',
        'io-impl': 'org.apache.iceberg.gcp.gcs.GCSFileIO',
        'header.X-Iceberg-Access-Delegation': 'vended-credentials',
        'header.x-custom-project': 'proj-test',
        'rest-metrics-reporting-enabled': 'true',
        'table-default.write.object-storage.enabled': 'true',
    }
    with self.pipeline() as p:
      snapshots = (
          p
          | beam.Create(rows).with_output_types(MetricRow)
          | SchemaAwareExternalTransform(
              URN_ICEBERG_WRITE,
              self.service,
              rearrange_based_on_discovery=True,
              table=target,
              catalog_name='local_rest',
              catalog_properties=catalog_props,
              table_properties={'write.parquet.compression-codec': 'zstd'},
              partition_fields=['hour(timestamp_ms)'],
              triggering_frequency_seconds=30,
              num_shards=2))
      summary_check = snapshots | beam.Map(
          lambda s: (
              s.sequenceNumber,
              s.operation,
              s.tableIdentifierString,
              s.summary.get('added-records'),
              int(s.summary.get('added-data-files', '0')) >= 2,
              int(s.summary.get('added-files-size', '0')) > 0,
              bool(s.summary.get('beam.commit-id')),
          ))
      assert_that(
          summary_check,
          equal_to([(1, 'append', target, '40', True, True, True)]))

    ns, tbl_name = target.split('.', 1)
    entry = self.catalog.get_table([ns], tbl_name)
    self.assertIsNotNone(entry)
    metadata = entry['metadata']
    self.assertEqual(len(metadata['snapshots']), 1)
    self.assertIsNotNone(metadata['current-snapshot-id'])
    self.assertEqual(
        metadata['snapshots'][0]['summary']['total-records'], '40')
    self.assertTrue(
        metadata['snapshots'][0]['manifest-list'].endswith('.avro'))
    self.assertEqual(
        metadata['partition-specs'][0]['fields'][0]['name'],
        'timestamp_ms_hour')
    self.assertEqual(
        metadata['partition-specs'][0]['fields'][0]['transform'], 'hour')
    self.assertEqual(
        metadata['properties'].get('write.parquet.compression-codec'), 'zstd')
    self.assertEqual(
        metadata['properties'].get('write.object-storage.enabled'), 'true')

    # Verify versioned metadata files were persisted in the warehouse directory.
    meta_files = glob.glob(
        os.path.join(entry['table_dir'], 'metadata', '*.metadata.json'))
    self.assertEqual(len(meta_files), 2)

    # If the worker shared the host filesystem, read the Parquet files directly.
    parquet_files = glob.glob(
        os.path.join(entry['table_dir'], 'data', '**', '*.parquet'),
        recursive=True)
    if parquet_files:
      import pyarrow.parquet as pq
      written_ids = sorted(
          row_id for path in parquet_files
          for row_id in pq.read_table(path, columns=['id'])['id'].to_pylist())
      self.assertEqual(written_ids, list(range(40)))

    # Verify auth token, custom headers, vended credentials and commit report.
    self.assertIn((('iceberg_it', ), tbl_name), self.catalog.credential_calls)
    reports = self.catalog.metrics_reports.get((('iceberg_it', ), tbl_name), [])
    self.assertEqual(len(reports), 1)
    self.assertEqual(reports[0]['report-type'], 'commit-report')
    self.assertEqual(reports[0]['metrics']['added-records']['value'], 40)
    self.assertGreaterEqual(
        reports[0]['metrics']['added-data-files']['value'], 2)
    self.assertGreater(
        reports[0]['metrics']['added-files-size-bytes']['value'], 0)
    seen_headers = [h for _, _, h in self.catalog.request_headers]
    self.assertTrue(
        any(h.get('authorization') == 'Bearer test-bearer-token'
            for h in seen_headers))
    self.assertTrue(
        any(h.get('x-custom-project') == 'proj-test' for h in seen_headers))

  def test_error_handling_routes_rejected_rows_and_commits_valid_rows(self):
    target = self.new_table('errors')
    # 9e15 seconds = 9e18 milliseconds: fits signed INT64 in MillisInstant,
    # and overflows INT64 microseconds when converted to an Iceberg timestamp.
    overflow_ts = BeamTimestamp(seconds=9_000_000_000_000_000)
    rows = [
        MetricRow(
            id=1,
            timestamp_ms=BeamTimestamp.of(1_700_000_000),
            device_id='ok-1',
            payload=1.0,
            location='eu'),
        MetricRow(
            id=2,
            timestamp_ms=overflow_ts,
            device_id='bad',
            payload=2.0,
            location='eu'),
        MetricRow(
            id=3,
            timestamp_ms=BeamTimestamp.of(1_700_000_060),
            device_id='ok-3',
            payload=3.0,
            location=None),
    ]
    with self.pipeline() as p:
      outputs = (
          p
          | beam.Create(rows).with_output_types(MetricRow)
          | SchemaAwareExternalTransform(
              URN_ICEBERG_WRITE,
              self.service,
              rearrange_based_on_discovery=True,
              table=target,
              catalog_properties={'uri': self.catalog_uri},
              error_handling={'output': 'bad_rows'}))
      failed = outputs['bad_rows'] | 'BadRows' >> beam.Map(
          lambda err: (err.failed_row.id, 'range' in err.error_message))
      committed = outputs['snapshots'] | 'Snapshots' >> beam.Map(
          lambda s: s.summary.get('added-records'))
      assert_that(failed, equal_to([(2, True)]), label='CheckErrors')
      assert_that(committed, equal_to(['2']), label='CheckSnapshots')

    ns, tbl_name = target.split('.', 1)
    entry = self.catalog.get_table([ns], tbl_name)
    self.assertIsNotNone(entry)
    self.assertEqual(
        entry['metadata']['snapshots'][0]['summary']['total-records'], '2')

  def test_rejected_row_without_error_handling_fails_pipeline(self):
    target = self.new_table('fail_bundle')
    overflow_ts = BeamTimestamp(seconds=9_000_000_000_000_000)
    rows = [
        MetricRow(
            id=1,
            timestamp_ms=overflow_ts,
            device_id='bad',
            payload=1.0,
            location='eu'),
    ]
    with self.assertRaisesRegex(Exception, 'error_handling'):
      with self.pipeline() as p:
        _ = (
            p
            | beam.Create(rows).with_output_types(MetricRow)
            | SchemaAwareExternalTransform(
                URN_ICEBERG_WRITE,
                self.service,
                rearrange_based_on_discovery=True,
                table=target,
                catalog_properties={'uri': self.catalog_uri}))

    ns, tbl_name = target.split('.', 1)
    entry = self.catalog.get_table([ns], tbl_name)
    if entry is not None:
      self.assertEqual(entry['metadata']['snapshots'], [])

  def test_invalid_configuration_fails_expansion(self):
    with self.assertRaisesRegex(Exception, 'merge-on-read'):
      with self.pipeline() as p:
        _ = (
            p
            | beam.Create([
                MetricRow(
                    id=1,
                    timestamp_ms=BeamTimestamp.of(1_700_000_000),
                    device_id='d',
                    payload=1.0,
                    location=None)
            ]).with_output_types(MetricRow)
            | SchemaAwareExternalTransform(
                URN_ICEBERG_WRITE,
                self.service,
                rearrange_based_on_discovery=True,
                table='iceberg_it.bad_mode',
                catalog_properties={'uri': self.catalog_uri},
                mode='merge-on-read'))


if __name__ == '__main__':
  unittest.main()
