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

//! Cross-language transform support for remote Beam transforms (Java, Python, Go)
//! through the Beam Portability Expansion Service.

mod artifact;
mod client;
mod error;
mod payload;
mod service;
mod splicing;
mod transform;

pub use artifact::{
    AutoServiceError, GCP_EXPANSION_SERVICE_TARGET, IO_EXPANSION_SERVICE_TARGET,
    JavaExpansionArtifact, SCHEMAIO_EXPANSION_SERVICE_TARGET,
};
#[doc(hidden)]
pub use artifact::{
    automated_java_target, beam_jar_cache_dir, is_automated_expansion_service,
    resolve_beam_jar_cache_dir,
};
pub use client::ExpansionClient;
pub use error::ExpansionError;
pub use payload::{
    JavaBuilderMethodCall, encode_external_configuration_payload, encode_java_class_lookup_payload,
    encode_schema_transform_payload,
};
pub use service::{JavaExpansionServer, find_java_executable};
#[doc(hidden)]
pub use service::{
    MAX_STARTUP_ATTEMPTS, Readiness, StartupAttempt, drive_startup, drive_startup_blocking,
};
#[doc(hidden)]
pub use splicing::{extract_input_components, splice_expansion_response};
pub use transform::{
    ExternalOutputs, ExternalSink, ExternalSource, ExternalTransform, MultiOutputSink,
    MultiOutputSource,
};
