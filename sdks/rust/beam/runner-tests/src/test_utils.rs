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

//! Test fixtures for runner integration tests.

use std::fs;
use std::path::{Path, PathBuf};

/// Temporary directory fixture for tests.
///
/// Removes the directory on drop.
pub struct TempDir {
    path: PathBuf,
}

impl TempDir {
    /// Creates a temporary directory with a prefix and the process id.
    pub fn new(prefix: &str) -> Self {
        let path = std::env::temp_dir().join(format!("beam_test_{prefix}_{}", std::process::id()));
        let _ = fs::remove_dir_all(&path);
        fs::create_dir_all(&path).expect("create temp test directory");
        Self { path }
    }

    /// Returns a reference to the directory path.
    pub fn as_path(&self) -> &Path {
        &self.path
    }

    /// Joins a child path component to the directory path.
    pub fn join(&self, name: &str) -> String {
        self.path.join(name).to_string_lossy().into_owned()
    }

    /// Creates a file in the directory with the given content.
    pub fn create_file(&self, name: &str, content: &str) -> PathBuf {
        let file_path = self.path.join(name);
        fs::write(&file_path, content).expect("write temp test file");
        file_path
    }

    /// Returns sorted paths of all files that end with suffix.
    pub fn files_with_suffix(&self, suffix: &str) -> Vec<String> {
        let mut files: Vec<String> = fs::read_dir(&self.path)
            .expect("read temp dir")
            .map(|entry| {
                entry
                    .expect("dir entry")
                    .path()
                    .to_string_lossy()
                    .into_owned()
            })
            .filter(|p| p.ends_with(suffix))
            .collect();
        files.sort();
        files
    }

    /// Returns sorted names of all directory entries.
    pub fn entries(&self) -> Vec<String> {
        let mut names: Vec<String> = fs::read_dir(&self.path)
            .expect("read temp dir")
            .map(|entry| {
                entry
                    .expect("dir entry")
                    .file_name()
                    .to_string_lossy()
                    .into_owned()
            })
            .collect();
        names.sort();
        names
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}
