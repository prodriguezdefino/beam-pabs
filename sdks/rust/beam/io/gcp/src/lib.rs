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
//! Each connector is a feature: `gcs`, `bigquery`, `bigtable` and `pubsub` (all on by
//! default; the `beam` facade enables them with `gcp-gcs`, `gcp-bigquery`, `gcp-bigtable`
//! and `gcp-pubsub`).
//!
//! With `gcs`, linking this crate registers the `"gs"` scheme with the global file system registry
//! through [`inventory`]. To register explicitly:
//! ```no_run
//! use gcp::register;
//!
//! register().unwrap();
//! ```

pub mod auth;
#[cfg(feature = "bigquery")]
pub mod bigquery;
#[cfg(feature = "bigtable")]
pub mod bigtable;
#[cfg(feature = "gcs")]
pub mod gcs;
pub mod options;
#[cfg(feature = "pubsub")]
pub mod pubsub;
pub mod runtime;
pub mod secret_manager;

#[cfg(feature = "gcs")]
use std::sync::Arc;

#[cfg(feature = "gcs")]
use file::{FileSystemRegistration, register_filesystem};

/// Default expansion service for Google Cloud Platform transforms.
pub const DEFAULT_EXPANSION_SERVICE: &str =
    "autojava::sdks:java:io:google-cloud-platform:expansion-service:runExpansionService";

#[cfg(feature = "gcs")]
inventory::submit! {
    FileSystemRegistration {
        scheme: "gs",
        factory: || Arc::new(gcs::GcsFileSystem::new()),
    }
}

/// Registers `GcsFileSystem` with the default configuration.
#[cfg(feature = "gcs")]
pub fn register() -> std::io::Result<()> {
    register_filesystem("gs", Arc::new(gcs::GcsFileSystem::new()))
}
