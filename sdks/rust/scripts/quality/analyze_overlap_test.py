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

import tempfile
import unittest
from pathlib import Path

import analyze_overlap as ao

SOURCE = """\
pub fn prod() -> &'static str {
    "{"
}
#[cfg(test)]
use std::fmt;
#[cfg(test)]
#[allow(dead_code)]
mod tests {
    // A closing brace in a comment: }
    fn t() -> char {
        '}'
    }
}
pub fn after() {}
"""


class TestInlineTestRanges(unittest.TestCase):
    def test_braces_in_literals_and_comments_and_items_without_body(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "lib.rs"
            path.write_text(SOURCE)
            self.assertEqual(ao.inline_test_ranges(str(path)), [(4, 5), (6, 13)])

    def test_unreadable_file_has_no_ranges(self):
        self.assertEqual(ao.inline_test_ranges("/nonexistent/lib.rs"), [])

    def test_prod_filter_excludes_test_lines(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "lib.rs"
            path.write_text(SOURCE)
            prod = ao.ProdFilter(None, "/workspace/")
            self.assertTrue(prod.is_prod(str(path), 2))
            self.assertFalse(prod.is_prod(str(path), 11))
            self.assertTrue(prod.is_prod(str(path), 14))


if __name__ == "__main__":
    unittest.main()
