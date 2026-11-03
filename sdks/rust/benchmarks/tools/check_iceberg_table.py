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
"""Checks the rows of an Iceberg table that a benchmark wrote, and drops the table.

It follows ``check_table.py``. A BigQuery query counts the rows of the BigLake table
(``project.catalog.namespace.table``). The expected count is ``--expected_rows``, or the
records of ``--kafka_topic``: the topic starts empty, and the job reads all of it. The write is
exactly once, so the counts must be equal. Dataflow counters are not the reference: a work
item that the service rejects at commit still reports its counters. With ``--distinct_column``
and ``--expected_rows``, every value of those columns must be there once.

The tool also prints the appends, their data files and the other snapshots of the table, from
the REST catalog.
``--drop`` drops the table through the catalog after the check.
"""

import argparse
import json
import subprocess
import sys
import urllib.request
from typing import Any, Dict, Optional


BIGLAKE = "https://biglake.googleapis.com/iceberg/v1/restcatalog"


def catalog_request(method: str, url: str, project: str) -> Optional[Dict[str, Any]]:
  """Sends one request to the REST catalog with Application Default Credentials."""
  token = subprocess.run(["gcloud", "auth", "application-default", "print-access-token"],
                         check=True, capture_output=True, text=True).stdout.strip()
  # A BigLake catalog in vended-credentials mode rejects table calls with no delegation header.
  request = urllib.request.Request(url, method=method, headers={
      "Authorization": f"Bearer {token}", "x-goog-user-project": project,
      "X-Iceberg-Access-Delegation": "vended-credentials"})
  with urllib.request.urlopen(request) as response:
    body = response.read()
  return json.loads(body) if body else None


def table_url(catalog_uri: str, catalog: str, project: str, table: str) -> str:
  """The REST path of `table` (`namespace.name`) in the BigLake catalog `catalog`."""
  config = catalog_request(
      "GET", f"{catalog_uri}/v1/config?warehouse=bl://projects/{project}/catalogs/{catalog}",
      project) or {}
  prefix = config.get("overrides", {}).get("prefix") or config.get("defaults", {}).get("prefix")
  namespace, name = table.rsplit(".", 1)
  return f"{catalog_uri}/v1/{prefix}/namespaces/{namespace}/tables/{name}"


def snapshot_facts(metadata: Dict[str, Any]) -> Dict[str, Any]:
  """The appends of the write and their data files, the other snapshots, and the mean time
  between two appends.

  The sums come from the `added-*` fields of each append. A snapshot of BigLake table
  management (a compaction is an `overwrite`) can have no `total-*` fields, and then the next
  appends have none either.
  """
  snapshots = sorted(metadata.get("snapshots", []), key=lambda s: s["timestamp-ms"])
  appends = [s for s in snapshots if s["summary"].get("operation") == "append"]
  added = lambda key: sum(int(s["summary"].get(key, 0)) for s in appends)
  files, size = added("added-data-files"), added("added-files-size")
  times = [s["timestamp-ms"] for s in appends]
  intervals = [b - a for a, b in zip(times, times[1:])]
  return {
      "appends": len(appends),
      "other_snapshots": len(snapshots) - len(appends),
      "added_records": added("added-records"),
      "added_data_files": files,
      "mean_file_mb": round(size / files / 1e6, 2) if files else 0,
      "mean_append_interval_s": round(sum(intervals) / len(intervals) / 1000, 1)
                                if intervals else None,
  }


def gcloud_json(*args: str) -> Any:
  out = subprocess.run(["gcloud", *args, "--format=json"], check=True, capture_output=True,
                       text=True).stdout
  return json.loads(out) if out.strip() else None


def topic_records(project: str, location: str, cluster: str, topic: str) -> int:
  """The records of a topic that started empty: for each partition, the largest offset that a
  consumer group of the cluster committed. A group that read the whole topic gives the end
  offsets. Dataflow counters are not exact (a rejected work item still reports them), so the
  topic is the reference."""
  ends: Dict[str, int] = {}
  where = [f"--project={project}", f"--location={location}"]
  for group in gcloud_json("managed-kafka", "consumer-groups", "list", cluster, *where) or []:
    name = group["name"].rsplit("/", 1)[-1]
    described = gcloud_json("managed-kafka", "consumer-groups", "describe", name,
                            f"--cluster={cluster}", *where) or {}
    for topic_name, entry in described.get("topics", {}).items():
      if topic_name.rsplit("/", 1)[-1] != topic:
        continue
      for partition, offset in entry.get("partitions", {}).items():
        ends[partition] = max(ends.get(partition, 0), int(offset.get("offset", 0)))
  if not ends:
    sys.exit(f"No consumer group of {cluster} committed offsets of {topic}")
  return sum(ends.values())


def main(argv):
  parser = argparse.ArgumentParser(description=__doc__)
  parser.add_argument("--table", required=True, help="namespace.name")
  parser.add_argument("--catalog", required=True, help="The BigLake catalog.")
  parser.add_argument("--project", required=True, help="Project of the catalog and the queries.")
  parser.add_argument("--region", default="us-central1", help="Region of the Dataflow job.")
  parser.add_argument("--catalog_uri", default=BIGLAKE)
  expected = parser.add_mutually_exclusive_group(required=True)
  expected.add_argument("--expected_rows", type=int)
  expected.add_argument(
      "--kafka_topic", help="Expect the records of this topic, as cluster/topic. The topic must "
                            "start empty, and one consumer group must read all of it.")
  parser.add_argument(
      "--distinct_column",
      help="Columns, with a comma between them, with one value for each row. The producer is "
           "at least once, so a retried record has its id again with a new timestamp: use "
           "id,timestamp_ms. The gap check uses the first column.")
  parser.add_argument("--drop", action="store_true", help="Drop the table after the check.")
  args = parser.parse_args(argv)

  from google.cloud import bigquery  # pylint: disable=import-outside-toplevel

  if args.expected_rows is not None:
    expected_rows = args.expected_rows
  else:
    cluster, topic = args.kafka_topic.split("/", 1)
    expected_rows = topic_records(args.project, args.region, cluster, topic)
  url = table_url(args.catalog_uri, args.catalog, args.project, args.table)
  facts = snapshot_facts((catalog_request("GET", url, args.project) or {}).get("metadata", {}))
  bq_table = f"{args.project}.{args.catalog}.{args.table}"
  columns = args.distinct_column.split(",") if args.distinct_column else []
  key = f"TO_JSON_STRING(STRUCT({', '.join(columns)}))" if len(columns) > 1 else "".join(columns)
  distinct = (f", COUNT(DISTINCT {key}) AS distinct_values"
              if columns else ", NULL AS distinct_values")
  client = bigquery.Client(project=args.project)
  row = next(iter(client.query(f"SELECT COUNT(*) AS n{distinct} FROM `{bq_table}`").result()))
  print(f"{bq_table}: {row.n} rows, {expected_rows} expected" +
        (f", {row.distinct_values} distinct {args.distinct_column}"
         if args.distinct_column else ""))
  print("table: " + ", ".join(f"{k}={v}" for k, v in facts.items()))
  if args.drop:
    catalog_request("DELETE", url, args.project)
    print(f"Dropped {args.table}")

  # The producer is at least once, so the topic itself can hold a record twice: against the
  # topic, the count alone decides.
  no_duplicates = args.kafka_topic or not args.distinct_column or row.distinct_values == row.n
  if row.n != expected_rows or not no_duplicates:
    sys.exit("The table does not hold the expected rows")


if __name__ == "__main__":
  main(sys.argv[1:])
