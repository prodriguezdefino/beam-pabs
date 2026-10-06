/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

//! File-based I/O connectors.

use std::fs;
use std::path::Path;

use beam::testing::{TestPipeline, passert};
use file::textio;

/// Contents of the file [`build_textio_roundtrip`] reads, and so of the file it writes.
pub const TEXTIO_ROUNDTRIP_CONTENTS: &str =
    "Apache Beam\nRust ValidatesRunner\nPortability Framework\n";

/// Builds a TextIO round trip: writes [`TEXTIO_ROUNDTRIP_CONTENTS`] to `input.txt` in
/// `temp_dir`, reads it in the pipeline, and writes it to `output.txt` unsharded.
///
/// What was read is asserted in the pipeline. The written file is for the driver to
/// read back after the run, so `temp_dir` must be a path both the workers and the
/// driver can see.
pub fn build_textio_roundtrip(p: &TestPipeline, temp_dir: &Path) {
    let in_file = temp_dir.join("input.txt");
    let out_file = temp_dir.join("output.txt");

    fs::write(&in_file, TEXTIO_ROUNDTRIP_CONTENTS).expect("write the TextIO input");

    let lines = p.apply(textio::Read::new("TextIO.Read", in_file.to_str().unwrap()));
    passert::that("AssertLines", &lines).contains_in_any_order(
        [
            "Apache Beam",
            "Rust ValidatesRunner",
            "Portability Framework",
        ]
        .map(String::from),
    );
    lines.apply(textio::Write::new("TextIO.Write", out_file.to_str().unwrap()).without_sharding());
}
