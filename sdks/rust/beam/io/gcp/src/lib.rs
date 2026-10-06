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

//! Google Cloud Platform file systems and I/O connectors.
//!
//! Linking this crate registers the `"gs"` scheme with the global file system registry
//! through [`inventory`]. To register explicitly:
//! ```no_run
//! use gcp::register;
//!
//! register().unwrap();
//! ```

pub mod bigquery;
pub mod gcs;
pub mod options;
pub mod runtime;
pub mod secret_manager;

use std::sync::Arc;

use file::filesystem::{FileSystemRegistration, register_filesystem};

pub use bigquery::{
    BigQueryRead, BigQueryWrite, CreateDisposition, URN_BIGQUERY_FILELOADS,
    URN_BIGQUERY_STORAGE_READ, URN_BIGQUERY_STORAGE_WRITE, URN_BIGQUERY_WRITE, WriteDisposition,
    WriteMethod,
};
pub use gcs::{GcsFileSystem, parse_gcs_uri};
pub use options::GcpOptions;

inventory::submit! {
    FileSystemRegistration {
        scheme: "gs",
        factory: || Arc::new(GcsFileSystem::new()),
    }
}

/// Registers `GcsFileSystem` with the default configuration.
pub fn register() -> std::io::Result<()> {
    register_filesystem("gs", Arc::new(GcsFileSystem::new()))
}
