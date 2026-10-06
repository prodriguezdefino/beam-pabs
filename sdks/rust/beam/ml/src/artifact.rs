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

//! Model artifact loading through the Beam filesystem registry.
//!
//! Local paths and `file://` always work. `gs://` works when the GCP I/O crate is linked
//! (`gcs` feature of `apache-beam`). [`read_artifact`] works inside or outside Tokio.

use std::io::{self, Read};

/// Reads the full artifact at `path` into memory, for example `gs://bucket/m.onnx`.
///
/// Fails if no filesystem matches `path` or the read fails. The error names `path`.
pub fn read_artifact(path: &str) -> io::Result<Vec<u8>> {
    let with_path = |e: io::Error| io::Error::new(e.kind(), format!("reading '{path}': {e}"));
    let mut reader = file::filesystem::get_filesystem(path)
        .and_then(|fs| fs.open_read(path))
        .map_err(with_path)?;
    let mut bytes = Vec::new();
    reader.read_to_end(&mut bytes).map_err(with_path)?;
    Ok(bytes)
}
