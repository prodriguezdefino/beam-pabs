<!--
    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.
-->

# WordCount mini benchmark: Java vs Python vs Rust on Dataflow

This page compares the best Dataflow WordCount run of each SDK on the same input and
worker configuration, topic by topic.

## Setup

| | |
|---|---|
| Input | `gs://apache-beam-samples/wikipedia_edits/wiki_data-0000000000[0-9][0-9].json` (100 shards, ~16.1 GB) |
| Runner | Dataflow Runner v2, batch, `us-central1` |
| Workers | `n2-standard-4`, autoscaling 1–10 workers |
| Pipeline | Read lines, then split on `[^\p{L}]+`, then count per word, then format and write. Same logic in all three SDKs. |

| SDK | Build | Job |
|---|---|---|
| Java | development build from `master` at the time of the run | `2026-09-24_14_28_23-3649551744009527463` |
| Python | 3.12 / latest release at the time of the run | `2026-09-24_23_53_01-3282675706216072510` |
| Rust | development build | `2026-10-07_14_55_55-14288964185409550418` |

## Correctness

All three runs produce the same result.

| Metric | Java | Python | Rust |
|---|---|---|---|
| Lines read | 62,762,451 | 62,762,451 | 62,762,451 |
| Words counted | 1,507,688,557 | 1,507,688,557 | 1,507,688,557 |
| Unique words written | 4,436,579 | 4,436,579 | 4,436,579 |

## Summary

| Topic | Metric | Java | Python | **Rust** |
|---|---|---|---|---|
| Compute | Total vCPU·s | 2,813 | 13,278 | **1,506** |
| Compute | Words per vCPU·s | 536k | 114k | **1,001k** |
| Memory | Memory (GB·s) | 11,255 | 53,114 | **6,026** |
| Disk | Persistent disk (GB·s) | 17,585 | 99,589 | **9,415** |
| Shuffle | Records sent to shuffle | 194.8M | 30.7M | **16.1M** |
| Shuffle | Shuffle data processed | 2.86 GB | 0.89 GB | **0.30 GB** |
| Scaling | Peak workers | 2 | 7 | **1** |
| Latency | Wall time (create to done) | 8m27s | 11m20s | **7m29s** |
| Telemetry | Total log entries | 4,920 | 15,142 | **2,431** |
| Telemetry | SDK worker log entries | 70 | 540 (28 WARN) | **30 (0 WARN)** |

**Scaling with input size.** Java and Rust also ran on 300 shards
(`wiki_data-000000000[0-2][0-9][0-9].json`): Java job `2026-10-02_13_16_15-2701778186061638886`,
Rust job `2026-10-07_15_04_37-14267515488697631015`. That input has 3× the words (4.52B)
and 1.6× the unique words (7.28M). Both jobs produced identical counts. Resource use
grew at a similar rate for both SDKs. Each used ~3.5× the vCPU·s of its 100-shard run
(Java 9,931, Rust 5,239). Java needed 1.90× Rust's compute, compared with 1.87× at 100
shards. Memory and disk kept that 1.90× ratio too (memory 39,727 vs 20,957 GB·s; disk
62,073 vs 32,745 GB·s). Words per vCPU·s fell by about 14–15% for both (Java 455k,
Rust 863k), because of the larger key space and the extra workers. Shuffle volume
followed the input: Java sent 584.5M records (8.87 GB) and Rust sent 52.3M (0.94 GB).
That is about 11× fewer records for Rust, compared with 12× at 100 shards. Java peaked
at 5 workers and took 11m32s; Rust peaked at 3 and took 9m55s.

## By topic

### Compute

| | Java | Python | Rust |
|---|---|---|---|
| Total vCPU·s | 2,813 | 13,278 | 1,506 |
| Relative to Rust | 1.87× | 8.8× | 1.00× |
| CPU utilization while processing | ~97% | ~98–99% | ~93% |

All three SDKs keep the workers CPU-bound. The difference comes from how much work
each one does per word, not from waiting on I/O.

In the Rust worker, CPU time breaks down as follows
(local `sample` profile of the same pipeline):

| Share | Category |
|---|---|
| ~49% | Splitting lines into words with `[^\p{L}]+`. This is the same regex the Java example uses. |
| ~13% | Handing elements between fused operators |
| ~10% | Combiner lifting table (`PartialCombineFn`) |
| ~10% | Allocation and copying (one `String` per word) |
| ~8% | User closures (`PairWithOne`, map) |
| ~4% | Text reading |
| < 1% | Fn API data plane, metrics, logging |

### Combining and shuffle

Each SDK's combiner lifting (the pre-shuffle partial combine) decides how many records
reach the shuffle. Fewer records means less shuffle I/O and less merging after the shuffle.

| | Java | Python | Rust |
|---|---|---|---|
| Records sent to shuffle | 194.8M | 30.7M | 16.1M |
| Reduction from 1.5B words | 7.7× | 49× | 94× |
| Table bound | 12k keys or a memory weight | 1M keys (sum/count) | 64 MiB of sampled size |
| Policy when full | Flushes everything at the key cap; evicts least-recently-used entries when over the weight | Evicts the oldest-inserted 10% | Evicts the least-recently-used 10% |
| Table hasher | JVM `hashCode` | Python `dict` | foldhash |

A replay of one shard's word stream through each policy gave the same emitted-to-input
ratios that the Dataflow jobs show:

| Policy | Records emitted / words in |
|---|---|
| Flush everything at 12k keys (Java) | 0.129 |
| Flush everything at 100k keys (previous Rust) | 0.050 |
| Evict the least-recently-used 10% at 100k keys | 0.034 |
| Bundle's unique words fit in the table (Python at 1M keys, Rust at 64 MiB) | 0.023 |

### Memory and disk

| | Java | Python | Rust |
|---|---|---|---|
| Memory (GB·s) | 11,255 | 53,114 | 6,026 |
| Persistent disk (GB·s) | 17,585 | 99,589 | 9,415 |

These metrics are allocated capacity multiplied by time, so they track worker count and
job duration. They do not measure peak usage. The 64 MiB per-bundle table budget fit
comfortably on `n2-standard-4` (16 GB).

### Scaling and latency

| | Java | Python | Rust |
|---|---|---|---|
| Peak workers | 2 | 7 | 1 |
| Wall time | 8m27s | 11m20s | 7m29s |

Rust finished the job on a single worker, and faster than Java did with two. Python
scaled to 7 workers and still took the longest.

### Telemetry and logging

| | Java | Python | Rust |
|---|---|---|---|
| Total log entries | 4,920 | 15,142 | 2,431 |
| SDK worker log entries | 70 | 540 (28 WARN) | 30 (0 WARN) |

Most entries in every job come from the Dataflow platform and runner harness, which
the SDK does not control. The Rust SDK only logs at startup. User metrics cost under
1% of CPU: a counter and a distribution updated on every line, for 125M updates in total.

## Rust: before and after the combiner changes

Replacing the 100k-key flush-everything table with one bounded by a memory budget that
evicts its least recently used entries, switching the table hasher from SipHash to
foldhash, and probing window groups with borrowed `&[u8]` slices yielded:

| | Rust before | Rust after | Change |
|---|---|---|---|
| Job | `2026-10-02_01_08_45-11703367799736881034` | `2026-10-07_14_55_55-14288964185409550418` | |
| vCPU·s | 2,851 | 1,506 | −47% |
| Records sent to shuffle | 73.4M | 16.1M | −78% |
| Shuffle data processed | 1.16 GB | 0.30 GB | −74% |
| Peak workers | 2 | 1 | |
| Wall time | 8m38s | 7m29s | −13% |

In the local profile, the changes cut CPU per word by 22%:
- hashing plus the combine table: −52%
- Fn API data plane: −86%, because fewer partial results are emitted

## Caveats

- **One run per SDK.** Dataflow run-to-run variance is a few percent, so the large
  gaps are meaningful and the small ones are not.
- **vCPU·s counts whole worker lifetimes, including boot and idle time.** A job that
  autoscales to fewer workers spends less on startup.
  - Java and the previous Rust run both peaked at 2 workers and differed by 1.4%.
  - The latest Rust run used 1 worker, because it needed less CPU overall. Part of its
    lead comes from that.
- **The runs were days apart.** The Java and Python runs date from 2026-09-24.
- **The profile is not from Dataflow.** It ran on macOS ARM with the Prism runner,
  while Dataflow workers are Linux x86. Its shares are indicative, not exact.

## Reproducing the Rust run

From the repository root:

```bash
./gradlew :sdks:rust:dataflow -Pexample=wordcount \
  -PworkerMachineType=n2-standard-4 -PnumWorkers=1 -PmaxNumWorkers=10 \
  '-PextraArgs=--input=gs://apache-beam-samples/wikipedia_edits/wiki_data-0000000000[0-9][0-9].json --output=gs://<bucket>/wordcount_wiki100/out'
```

To read the job's metrics:

```bash
gcloud beta dataflow metrics list <job-id> --region=us-central1 --source=service
gcloud beta dataflow metrics list <job-id> --region=us-central1 --source=user
```
