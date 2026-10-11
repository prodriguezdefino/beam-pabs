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
//! Each variable comes from the environment that Gradle sets, else from the
//! repository's `gradle.properties`. If neither has it, the crate uses
//! `CARGO_PKG_VERSION`.

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
        let release = version.replace("-SNAPSHOT", "");
        println!("cargo:rustc-env={RELEASE_VERSION_VAR}={release}");
    }
}

/// Finds the Beam repository root that holds `sdks/rust/Cargo.toml` and `gradle.properties`.
fn find_gradle_properties(start: &Path) -> Option<PathBuf> {
    start.ancestors().find_map(|dir| {
        let candidate = dir.join("gradle.properties");
        let has_marker = dir.join("sdks/rust/Cargo.toml").is_file();
        if has_marker
            && fs::read_to_string(&candidate).is_ok_and(|c| property(&c, "sdk_version").is_some())
        {
            Some(candidate)
        } else {
            None
        }
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
