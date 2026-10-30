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

//! Generates the gRPC clients of the vendored googleapis protos that the enabled connectors
//! call. `src/lib.rs` includes the generated module tree.

use std::path::PathBuf;

/// The cargo feature of each root proto, as cargo gives it to build scripts.
const ROOTS: &[(&str, &str)] = &[
    (
        "CARGO_FEATURE_BIGTABLE",
        "google/bigtable/v2/bigtable.proto",
    ),
    (
        "CARGO_FEATURE_BIGQUERY_STORAGE",
        "google/cloud/bigquery/storage/v1/storage.proto",
    ),
];

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_dir = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR")?).join("proto");
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed={}", proto_dir.display());
    println!(
        "cargo:rustc-env=GOOGLEAPIS_RUSTC_VERSION={}",
        rustc_version()
    );
    let protos: Vec<PathBuf> = ROOTS
        .iter()
        .filter(|(feature, _)| std::env::var_os(feature).is_some())
        .map(|(_, proto)| proto_dir.join(proto))
        .collect();
    if protos.is_empty() {
        // No feature is on: `src/lib.rs` includes an empty module tree.
        let out_dir = PathBuf::from(std::env::var("OUT_DIR")?);
        std::fs::write(out_dir.join("googleapis.rs"), "")?;
        return Ok(());
    }
    // Bytes fields share the gRPC receive buffer. The well-known types are generated too, so
    // `google.protobuf.BytesValue` (the qualifier of a Bigtable cell) also decodes to `Bytes`.
    tonic_prost_build::configure()
        .build_server(false)
        .build_client(true)
        .bytes(".")
        .compile_well_known_types(true)
        .include_file("googleapis.rs")
        .compile_protos(&protos, &[proto_dir])?;
    Ok(())
}

/// Returns the version of the Rust compiler, for example `1.90.0`, or `unknown`.
fn rustc_version() -> String {
    let rustc = std::env::var("RUSTC").unwrap_or_else(|_| "rustc".to_string());
    std::process::Command::new(rustc)
        .arg("--version")
        .output()
        .ok()
        .and_then(|output| String::from_utf8(output.stdout).ok())
        .and_then(|text| text.split_whitespace().nth(1).map(str::to_string))
        .unwrap_or_else(|| "unknown".to_string())
}
