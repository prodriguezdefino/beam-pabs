// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Resolves the Beam versions this SDK reports, so that no Beam version is hard-coded in the
//! Rust sources.
//!
//! Precedence, per variable:
//! 1. The environment (`BEAM_SDK_VERSION`, `BEAM_RELEASE_VERSION`), which Gradle sets.
//! 2. `gradle.properties` at the root of the Beam repository, for plain `cargo` builds inside
//!    the repository (`sdk_version` and `version`, the same keys Gradle reads).
//! 3. Nothing: the crate falls back to `CARGO_PKG_VERSION`, which is the case for a published
//!    crate built outside the repository.

use std::env;
use std::fs;
use std::path::{Path, PathBuf};

const SDK_VERSION_VAR: &str = "BEAM_SDK_VERSION";
const RELEASE_VERSION_VAR: &str = "BEAM_RELEASE_VERSION";

fn main() {
    println!("cargo:rerun-if-env-changed={SDK_VERSION_VAR}");
    println!("cargo:rerun-if-env-changed={RELEASE_VERSION_VAR}");

    let sdk_from_env = env::var(SDK_VERSION_VAR).ok().filter(|v| !v.is_empty());
    let release_from_env = env::var(RELEASE_VERSION_VAR).ok().filter(|v| !v.is_empty());
    if sdk_from_env.is_some() && release_from_env.is_some() {
        return;
    }

    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").expect("set by cargo"));
    let Some(properties_path) = find_gradle_properties(&manifest_dir) else {
        return;
    };
    println!("cargo:rerun-if-changed={}", properties_path.display());
    let Ok(contents) = fs::read_to_string(&properties_path) else {
        return;
    };

    if sdk_from_env.is_none()
        && let Some(sdk_version) = property(&contents, "sdk_version")
    {
        println!("cargo:rustc-env={SDK_VERSION_VAR}={sdk_version}");
    }
    if release_from_env.is_none()
        && let Some(version) = property(&contents, "version")
    {
        let release = version.strip_suffix("-SNAPSHOT").unwrap_or(version);
        println!("cargo:rustc-env={RELEASE_VERSION_VAR}={release}");
    }
}

/// Walks up from `start` to the first `gradle.properties` that defines `sdk_version`, which
/// identifies the root of the Beam repository rather than any nested Gradle project.
fn find_gradle_properties(start: &Path) -> Option<PathBuf> {
    start
        .ancestors()
        .map(|dir| dir.join("gradle.properties"))
        .find(|candidate| {
            fs::read_to_string(candidate).is_ok_and(|c| property(&c, "sdk_version").is_some())
        })
}

/// Returns the value of `key` in a Java properties file, for the simple `key=value` lines that
/// Beam's `gradle.properties` uses.
fn property<'a>(contents: &'a str, key: &str) -> Option<&'a str> {
    contents.lines().find_map(|line| {
        let (k, v) = line.trim().split_once('=')?;
        (k.trim() == key)
            .then(|| v.trim())
            .filter(|v| !v.is_empty())
    })
}
