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

import tempfile
import unittest
from pathlib import Path

import complexity_scorecard as cs


def _write(root: Path, rel: str, content: str) -> Path:
    path = root / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content)
    return path


class TestComplexityScorecard(unittest.TestCase):
    def test_rust_benchmark_classification(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            bench_src = _write(
                root,
                "sdks/rust/benchmarks/nexmark/src/lib.rs",
                "// comment\npub fn query() -> usize {\n    1\n}\n"
                "#[cfg(test)]\nmod tests {\n    #[test]\n    fn t() {}\n}\n",
            )
            bench_test = _write(
                root,
                "sdks/rust/benchmarks/nexmark/tests/smoke.rs",
                "#[test]\nfn smoke() {\n    assert_eq!(1, 1);\n}\n",
            )
            cargo_bench = _write(
                root,
                "sdks/rust/beam/harness/benches/data_plane.rs",
                "fn bench_main() {}\n",
            )

            io_bench_src = _write(
                root,
                "sdks/rust/benchmarks/io/bigquery/rust/src/lib.rs",
                "pub fn run() {}\n",
            )
            io_bench_test = _write(
                root,
                "sdks/rust/benchmarks/io/bigquery/rust/tests/smoke.rs",
                "#[test]\nfn smoke() {}\n",
            )

            self.assertFalse(cs.is_test("rust", "benchmarks/nexmark/src/lib.rs", bench_src))
            self.assertTrue(cs.is_test("rust", "benchmarks/nexmark/tests/smoke.rs", bench_test))
            self.assertFalse(cs.is_test("rust", "benchmarks/io/bigquery/rust/src/lib.rs", io_bench_src))
            self.assertTrue(cs.is_test("rust", "benchmarks/io/bigquery/rust/tests/smoke.rs", io_bench_test))
            self.assertTrue(cs.is_test("rust", "beam/harness/benches/data_plane.rs", cargo_bench))

            self.assertEqual(cs._rust_split(str(bench_src)), (3, 5))
            self.assertEqual(cs._rust_split(str(bench_test)), (0, 4))
            self.assertEqual(cs._rust_split(str(io_bench_src)), (1, 0))
            self.assertEqual(cs._rust_split(str(io_bench_test)), (0, 2))
            self.assertEqual(cs._rust_split(str(cargo_bench)), (0, 1))

    def test_python_tree_benchmark_classification(self):
        self.assertTrue(cs._is_python_tree_test("/repo/sdks/python/apache_beam/testing/util.py"))
        self.assertFalse(
            cs._is_python_tree_test(
                "/repo/sdks/python/apache_beam/testing/benchmarks/nexmark/nexmark_launcher.py"
            )
        )
        self.assertTrue(
            cs._is_python_tree_test(
                "/repo/sdks/python/apache_beam/testing/benchmarks/nexmark/nexmark_launcher_test.py"
            )
        )


if __name__ == "__main__":
    unittest.main()
