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

import unittest
import summary

class TestSummary(unittest.TestCase):
  def test_find_symbol(self):
    st = [
      {"rva": 100, "size": 50, "symbol": 0},
      {"rva": 200, "size": 10, "symbol": 1},
      {"rva": 300, "symbol": 2} # no size
    ]
    self.assertEqual(summary.find_symbol(st, 120), 0)
    self.assertEqual(summary.find_symbol(st, 150), None) # past end
    self.assertEqual(summary.find_symbol(st, 205), 1)
    self.assertEqual(summary.find_symbol(st, 305), 2) # no size, fallback
    self.assertEqual(summary.find_symbol(st, 50), None) # before start

  def test_classify(self):
    self.assertEqual(summary.classify("malloc"), "Allocation/memcpy")
    self.assertEqual(summary.classify("_xzm_alloc"), "Allocation/memcpy")
    self.assertEqual(summary.classify("apache_beam_harness::worker::run"), "Harness data plane")
    self.assertEqual(summary.classify("<harness::bundle_processor::chain::sink::ChainSink as apache_beam_core::transforms::handler::ElementSink>::push_value"), "Harness data plane")
    self.assertEqual(summary.classify("apache_beam_core::transforms::dofn::pardo::DoFnHandler"), "Core transforms")
    self.assertEqual(summary.classify("regex_automata::hybrid::search::find_fwd"), "User code / other crates")

  def test_classify_inclusive(self):
    stages = summary.classify_inclusive(["malloc", "apache_beam_harness::worker::run"])
    self.assertEqual(stages, {"Allocation/memcpy", "Harness data plane"})

    stages2 = summary.classify_inclusive(["regex_automata::hybrid::search::find_fwd"])
    self.assertEqual(stages2, set())

  def test_aggregate(self):
    prof = {
      "libs": [
        {"debugName": "wordcount", "codeId": "abc"}
      ],
      "threads": [
        {
          "pid": 123,
          "name": "tokio-rt-worker",
          "processName": "wordcount",
          "funcTable": {
            "name": [0, 1, 2],
            "resource": [0, 0, 0]
          },
          "stringArray": ["0x70", "0xc8", "0x12c"], # 112, 200, 300
          "frameTable": {
            "func": [0, 1, 2],
            "address": [112, 200, 300]
          },
          "stackTable": {
            "frame": [0, 1, 2],
            "prefix": [None, 0, 1] # 0 = leaf, 1 = parent, 2 = grandparent
          },
          "samples": {
            "stack": [0, 1, 2],
            "weight": [5, 10, 15],
            "threadCPUDelta": [0, 10000, 15000]
          }
        }
      ]
    }

    syms = {
      "data": [
        {
          "code_id": "abc",
          "symbol_table": [
            {"rva": 100, "size": 50, "symbol": 0},
            {"rva": 200, "size": 10, "symbol": 1},
            {"rva": 300, "size": 10, "symbol": 2}
          ]
        }
      ],
      "string_table": ["__psynch_cvwait", "apache_beam_harness::worker::run", "malloc"]
    }

    totals = summary.aggregate(prof, syms)
    # Stack 0: frame 0 -> func 0 -> address 112 -> __psynch_cvwait (idle!)
    # Stack 1: frame 1 -> func 1 -> address 200 -> apache_beam_harness::worker::run
    # Stack 2: frame 2 -> func 2 -> address 300 -> malloc

    # A sample weighs its count when it caught the thread running; the deltas give the CPU.
    self.assertEqual(totals.rust_idle_samples, 5)
    self.assertEqual(totals.rust_non_idle_samples, 25) # 10 + 15
    self.assertEqual(totals.rust_cpu_us, 25000) # 0+10000+15000

    self.assertEqual(totals.stage_self_time["Harness data plane"], 10)
    self.assertEqual(totals.stage_self_time["Allocation/memcpy"], 15)

    # A wait leaf is idle even when its delta holds the CPU of work before the wait, and a
    # sample with a zero delta is idle whatever its leaf.
    prof["threads"][0]["samples"]["threadCPUDelta"] = [40000, 0, 15000]
    totals = summary.aggregate(prof, syms)
    self.assertEqual(totals.rust_idle_samples, 15) # 5 + 10
    self.assertEqual(totals.rust_non_idle_samples, 15)
    self.assertEqual(totals.rust_cpu_us, 55000)
    self.assertEqual(dict(totals.stage_self_time), {"Allocation/memcpy": 15})

  def test_parse_time_mac(self):
    content = """        5.45 real         4.79 user         0.40 sys
           862158848  maximum resident set size
                   0  average shared memory size"""
    stats = summary.parse_time_content(content)
    self.assertIsNotNone(stats)
    self.assertEqual(stats.real_time, 5.45)
    self.assertEqual(stats.user_time, 4.79)
    self.assertEqual(stats.sys_time, 0.40)
    self.assertEqual(stats.peak_rss_mb, 862158848 / 1024.0 / 1024.0)

  def test_parse_time_gnu(self):
    content = """	Command being timed: "./target/release/wordcount"
	User time (seconds): 10.51
	System time (seconds): 2.15
	Percent of CPU this job got: 153%
	Elapsed (wall clock) time (h:mm:ss or m:ss): 0:08.23
	Average shared text size (kbytes): 0
	Average unshared data size (kbytes): 0
	Average stack size (kbytes): 0
	Average total size (kbytes): 0
	Maximum resident set size (kbytes): 123456
	Average resident set size (kbytes): 0"""
    stats = summary.parse_time_content(content)
    self.assertIsNotNone(stats)
    self.assertEqual(stats.real_time, 8.23)
    self.assertEqual(stats.user_time, 10.51)
    self.assertEqual(stats.sys_time, 2.15)
    self.assertEqual(stats.peak_rss_mb, 123456 / 1024.0)

if __name__ == "__main__":
  unittest.main()
