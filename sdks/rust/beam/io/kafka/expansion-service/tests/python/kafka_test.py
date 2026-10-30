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
"""Python tests of the Kafka providers of the Rust expansion service.

The providers are beam:schematransform:org.apache.beam:rust:kafka_read:v1 and
beam:schematransform:org.apache.beam:rust:kafka_write:v1.

The tests start a single-node broker in Docker with testcontainers, and create
a new topic for each test. They write and read records with the console
producer and consumer of the broker image, so they need no Kafka client in
Python. The write is at least once, so the checks allow duplicate records and
report their number in a warning. The read tests are bounded with
max_read_time_seconds.

Run with ./gradlew :sdks:rust:kafkaExpansionServicePythonTest, which starts the
expansion service and sets the environment:

  EXPANSION_SERVICE  host:port of the Rust expansion service
  PRISM_LOCATION     optional path of the Prism binary
  PIPELINE_OPTIONS   optional pipeline options; without them, the tests use
                     Prism with a loopback Python worker
"""

import collections
import json
import os
import shlex
import typing
import unittest
import uuid
import warnings

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.transforms.external import SchemaAwareExternalTransform
from apache_beam.transforms.periodicsequence import PeriodicImpulse
from apache_beam.typehints.row_type import RowTypeConstraint
from apache_beam.utils.timestamp import Duration
from apache_beam.utils.timestamp import Timestamp
from testcontainers.community.kafka import KafkaContainer

READ = 'beam:schematransform:org.apache.beam:rust:kafka_read:v1'
WRITE = 'beam:schematransform:org.apache.beam:rust:kafka_write:v1'

# The time that a bounded read reads.
READ_SECONDS = 15

# The listener of the broker inside its container.
INTERNAL_SERVERS = 'localhost:9092'

PAYLOAD = RowTypeConstraint.from_fields([('payload', bytes)])
OPTIONAL_PAYLOAD = RowTypeConstraint.from_fields([
    ('payload', typing.Optional[bytes])
])
EVENT = RowTypeConstraint.from_fields([
    ('id', int),
    ('name', str),
    ('score', typing.Optional[float]),
    ('tags', typing.Sequence[str]),
])


class KafkaTest(unittest.TestCase):
  @classmethod
  def setUpClass(cls):
    cls.service = os.environ['EXPANSION_SERVICE']
    cls.broker = KafkaContainer().with_kraft().start()
    cls.servers = cls.broker.get_bootstrap_server()

  @classmethod
  def tearDownClass(cls):
    cls.broker.stop()

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

  def broker_command(self, *command):
    result = self.broker.exec(list(command))
    output = result.output.decode('utf-8')
    if result.exit_code != 0:
      self.fail(f'{" ".join(command)} failed: {output}')
    return output

  def new_topic(self, partitions=3):
    topic = f'beam-rust-{uuid.uuid4().hex[:12]}'
    self.broker_command(
        'kafka-topics',
        '--bootstrap-server',
        INTERNAL_SERVERS,
        '--create',
        '--topic',
        topic,
        '--partitions',
        str(partitions))
    return topic

  def record_values(self, topic, expected_count):
    """The values of the records in `topic`, read from the start.

    The consumer stops after `expected_count` records, or after ten seconds with
    no record.
    """
    output = self.broker_command(
        'kafka-console-consumer',
        '--bootstrap-server',
        INTERNAL_SERVERS,
        '--topic',
        topic,
        '--from-beginning',
        '--max-messages',
        str(expected_count),
        '--timeout-ms',
        '10000')
    # The console consumer logs its summary to the same stream.
    return [
        line for line in output.splitlines()
        if line and not line.startswith('Processed a total of')
    ]

  def assert_at_least_once(self, topic, values, expected):
    """Fails unless `values` holds each value of `expected` and no other value.

    A duplicate value does not fail the test. A warning gives their number.
    """
    counts = collections.Counter(values)
    missing = sorted(set(expected) - set(counts))
    unexpected = sorted(set(counts) - set(expected))
    if missing or unexpected:
      self.fail(
          f'{topic}: missing {missing[:10]} ({len(missing)}), '
          f'unexpected {unexpected[:10]} ({len(unexpected)})')
    duplicates = sum(n - 1 for n in counts.values())
    if duplicates:
      warnings.warn(f'{topic}: at least once wrote {duplicates} duplicates')

  def write(self, rows, **config):
    return rows | 'Write' >> SchemaAwareExternalTransform(
        WRITE, self.service, bootstrap_servers=self.servers, **config)

  def produce(self, topic, values):
    """Writes each of `values` as the value of one record of `topic`."""
    lines = ' '.join(shlex.quote(value) for value in values)
    self.broker_command(
        'bash',
        '-c',
        f'printf "%s\\n" {lines} | kafka-console-producer '
        f'--bootstrap-server {INTERNAL_SERVERS} --topic {topic}')

  def read(self, p, **config):
    """A read of the Rust provider that stops after READ_SECONDS."""
    return p | 'Read' >> SchemaAwareExternalTransform(
        READ,
        self.service,
        bootstrap_servers=self.servers,
        max_read_time_seconds=READ_SECONDS,
        **config)

  def committed_offsets(self, group, topic):
    """The committed offset of each partition of `topic` for `group`."""
    output = self.broker_command(
        'kafka-consumer-groups',
        '--bootstrap-server',
        INTERNAL_SERVERS,
        '--describe',
        '--group',
        group)
    # Columns: GROUP TOPIC PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG ...
    rows = [line.split() for line in output.splitlines()]
    return {
        int(row[2]): int(row[3])
        for row in rows if len(row) > 3 and row[1] == topic and row[3].isdigit()
    }

  # Read.

  def test_a_read_from_the_earliest_offset_gets_every_record(self):
    topic = self.new_topic()
    expected = [f'value-{i}' for i in range(300)]
    self.produce(topic, expected)
    with self.pipeline() as p:
      rows = self.read(
          p, topic=topic, format='STRING', auto_offset_reset_config='earliest')
      assert_that(rows | beam.Map(lambda row: row.payload), equal_to(expected))

  def test_two_reads_of_one_topic_each_get_every_record(self):
    topic = self.new_topic()
    expected = [f'value-{i}' for i in range(200)]
    self.produce(topic, expected)
    config = dict(
        bootstrap_servers=self.servers,
        max_read_time_seconds=READ_SECONDS,
        topic=topic,
        format='STRING',
        auto_offset_reset_config='earliest')
    with self.pipeline() as p:
      for label in ('First', 'Second'):
        rows = p | f'Read{label}' >> SchemaAwareExternalTransform(
            READ, self.service, **config)
        assert_that(
            rows | f'Payloads{label}' >> beam.Map(lambda row: row.payload),
            equal_to(expected),
            label=f'Check{label}')

  def test_a_bounded_read_that_reshuffles_its_partitions_gets_every_record(
      self):
    topic = self.new_topic()
    expected = [f'value-{i}' for i in range(300)]
    self.produce(topic, expected)
    with self.pipeline() as p:
      rows = self.read(
          p,
          topic=topic,
          format='STRING',
          auto_offset_reset_config='earliest',
          reshuffle_partitions=True)
      assert_that(rows | beam.Map(lambda row: row.payload), equal_to(expected))

  def test_a_read_with_no_end_rejects_the_reshuffle_of_its_partitions(self):
    with self.assertRaisesRegex(Exception, 'max read time'):
      with self.pipeline() as p:
        _ = p | SchemaAwareExternalTransform(
            READ,
            self.service,
            bootstrap_servers=self.servers,
            topic='unused',
            format='RAW',
            reshuffle_partitions=True)

  def test_a_read_starts_at_the_latest_offset_by_default(self):
    topic = self.new_topic()
    self.produce(topic, ['old'])
    with self.pipeline() as p:
      rows = self.read(p, topic=topic, format='RAW')
      assert_that(rows, equal_to([]))

  def test_json_records_that_do_not_convert_go_to_the_error_output(self):
    topic = self.new_topic(partitions=1)
    self.produce(topic, ['{"id": 1, "name": "one"}', 'not json'])
    schema = json.dumps({
        'type': 'object',
        'properties': {
            'id': {
                'type': 'integer'
            }, 'name': {
                'type': 'string'
            }
        },
        'required': ['id'],
    })
    with self.pipeline() as p:
      outputs = self.read(
          p,
          topic=topic,
          format='JSON',
          schema=schema,
          auto_offset_reset_config='earliest',
          error_handling=beam.Row(output='bad'))
      assert_that(
          outputs['output'] | 'Rows' >> beam.Map(lambda r: (r.id, r.name)),
          equal_to([(1, 'one')]),
          label='CheckRows')
      assert_that(
          outputs['bad'] | 'Errors' >> beam.Map(lambda e: e.failed_row),
          equal_to([b'not json']),
          label='CheckErrors')

  def test_a_consumer_group_commits_its_offsets_and_resumes_from_them(self):
    topic = self.new_topic(partitions=2)
    group = f'beam-rust-{uuid.uuid4().hex[:8]}'
    first = [f'first-{i}' for i in range(100)]
    second = [f'second-{i}' for i in range(50)]
    config = dict(
        topic=topic,
        format='STRING',
        auto_offset_reset_config='earliest',
        consumer_config_updates={'group.id': group})
    self.produce(topic, first)
    with self.pipeline() as p:
      rows = self.read(p, **config)
      assert_that(rows | beam.Map(lambda row: row.payload), equal_to(first))
    self.assertEqual(
        sum(self.committed_offsets(group, topic).values()), len(first))
    self.produce(topic, second)
    with self.pipeline() as p:
      rows = self.read(p, **config)
      assert_that(rows | beam.Map(lambda row: row.payload), equal_to(second))

  # Write.

  def test_raw_writes_each_payload_as_a_record(self):
    topic = self.new_topic()
    expected = [f'value-{i}' for i in range(500)]
    with self.pipeline() as p:
      rows = (
          p
          | beam.Create(expected)
          | beam.Map(lambda v: beam.Row(payload=v.encode('utf-8'))).
          with_output_types(PAYLOAD))
      self.write(rows, topic=topic, format='RAW')
    self.assert_at_least_once(
        topic, self.record_values(topic, len(expected)), expected)

  def test_an_unbounded_input_writes_every_record(self):
    topic = self.new_topic()
    start = Timestamp.now()
    seconds, interval = 5, 0.1
    with self.pipeline(streaming=True) as p:
      rows = (
          p
          | PeriodicImpulse(
              start_timestamp=start,
              stop_timestamp=start + Duration(seconds=seconds),
              fire_interval=interval)
          | beam.Map(
              lambda at: beam.Row(
                  payload=str(round((at - start.micros / 1e6) / interval)).
                  encode('utf-8'))).with_output_types(PAYLOAD))
      self.write(rows, topic=topic, format='RAW')
    expected = [str(i) for i in range(round(seconds / interval))]
    self.assert_at_least_once(
        topic, self.record_values(topic, len(expected)), expected)

  # JSON.

  def test_json_writes_each_row_as_an_object(self):
    topic = self.new_topic()
    events = [
        beam.Row(id=1, name='one', score=1.5, tags=['a', 'b']),
        beam.Row(id=2, name='two', score=None, tags=[]),
    ]
    with self.pipeline() as p:
      rows = p | beam.Create(events).with_output_types(EVENT)
      self.write(rows, topic=topic, format='JSON')
    objects = [json.loads(v) for v in self.record_values(topic, len(events))]
    self.assertCountEqual(
        objects,
        [
            {
                'id': 1, 'name': 'one', 'score': 1.5, 'tags': ['a', 'b']
            },
            {
                'id': 2, 'name': 'two', 'score': None, 'tags': []
            },
        ])

  # Errors.

  def test_rows_that_do_not_convert_go_to_the_error_output(self):
    topic = self.new_topic()
    with self.pipeline() as p:
      rows = (
          p
          | beam.Create([b'ok', None])
          | beam.Map(lambda v: beam.Row(payload=v)).with_output_types(
              OPTIONAL_PAYLOAD))
      errors = self.write(
          rows, topic=topic, format='RAW', error_handling=beam.Row(output='bad'))
      assert_that(
          errors | beam.Map(
              lambda e: (e.failed_row.payload, 'null' in e.error_message)),
          equal_to([(None, True)]))
    self.assertEqual(self.record_values(topic, 1), ['ok'])

  def test_a_record_that_is_too_large_goes_to_the_error_output(self):
    topic = self.new_topic()
    with self.pipeline() as p:
      rows = (
          p
          | beam.Create([b'small', b'x' * 5000])
          | beam.Map(lambda v: beam.Row(payload=v)).with_output_types(PAYLOAD))
      errors = self.write(
          rows,
          topic=topic,
          format='RAW',
          producer_config_updates={'message.max.bytes': '2000'},
          error_handling=beam.Row(output='bad'))
      assert_that(
          errors | beam.Map(lambda e: len(e.failed_row.payload)),
          equal_to([5000]))
    self.assertEqual(self.record_values(topic, 1), ['small'])

  def test_an_invalid_configuration_fails_the_expansion(self):
    with self.assertRaisesRegex(Exception, 'RAW format needs one BYTES field'):
      with self.pipeline() as p:
        rows = p | beam.Create([beam.Row(payload='text')])
        self.write(rows, topic='unused', format='RAW')


if __name__ == '__main__':
  unittest.main()
