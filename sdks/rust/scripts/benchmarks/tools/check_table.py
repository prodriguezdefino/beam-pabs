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
"""Checks the rows of a table that a benchmark wrote, and drops the table.

A query counts the rows, so the check also covers rows in the streaming buffer of the
default stream. The expected count is ``--expected_rows``, or the rows of
``--expected_table``, the table that a ``copy`` job read. ``--expected_filter`` applies the
row restriction of the read to that table.

An exactly-once write must have the expected count. An at-least-once write (``--at_least``)
can have more, and the tool reports them. With ``--distinct_column``, every value of that
column must be there once, so a missing row cannot hide behind a duplicate.
"""

import argparse
import sys

from google.cloud import bigquery


def main(argv):
  parser = argparse.ArgumentParser(description=__doc__)
  parser.add_argument("--table", required=True, help="project.dataset.table")
  parser.add_argument("--project", required=True, help="Project of the query jobs.")
  expected = parser.add_mutually_exclusive_group(required=True)
  expected.add_argument("--expected_rows", type=int)
  expected.add_argument("--expected_table", help="Expect the rows of this table.")
  parser.add_argument(
      "--expected_filter", help="A WHERE condition for the rows of --expected_table.")
  parser.add_argument("--distinct_column", help="A column with one value for each row.")
  parser.add_argument("--at_least", action="store_true")
  parser.add_argument("--drop", action="store_true", help="Drop the table after the check.")
  args = parser.parse_args(argv)
  table = args.table.replace(":", ".", 1)

  client = bigquery.Client(project=args.project)

  def count(query):
    return next(iter(client.query(query).result()))

  expected_rows = args.expected_rows
  if args.expected_table:
    source = args.expected_table.replace(":", ".", 1)
    where = f" WHERE {args.expected_filter}" if args.expected_filter else ""
    expected_rows = count(f"SELECT COUNT(*) AS n FROM `{source}`{where}").n
  distinct = (
      f", COUNT(DISTINCT {args.distinct_column}) AS distinct_values"
      if args.distinct_column else ", NULL AS distinct_values")
  row = count(f"SELECT COUNT(*) AS n{distinct} FROM `{table}`")
  print(
      f"{table}: {row.n} rows, {expected_rows} expected" +
      (f", {row.distinct_values} distinct {args.distinct_column}"
       if args.distinct_column else ""))
  if args.drop:
    client.delete_table(table, not_found_ok=True)
    print(f"Dropped {table}")

  all_rows = row.n >= expected_rows if args.at_least else row.n == expected_rows
  all_values = not args.distinct_column or row.distinct_values == expected_rows
  if not (all_rows and all_values):
    sys.exit("The table does not hold the expected rows")
  if row.n > expected_rows:
    print(f"{row.n - expected_rows} duplicate rows (at least once)")


if __name__ == "__main__":
  main(sys.argv[1:])
