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

//! gRPC clients on the transport of the Google Cloud client libraries, for RPCs that no
//! client library exposes directly.

use std::io;
use std::sync::{Arc, LazyLock};

use google_cloud_auth::credentials::anonymous;
use google_cloud_gax::retry_policy::NeverRetry;
use google_cloud_gax_internal::api_header::{GCCL, XGoogApiClient};
use google_cloud_gax_internal::grpc::Client;
use google_cloud_gax_internal::options::ClientConfig;

/// Value of the `x-goog-api-client` header reporting the Rust compiler, transport and crate.
pub static API_CLIENT: LazyLock<String> = LazyLock::new(|| {
    XGoogApiClient {
        name: env!("CARGO_PKG_NAME"),
        version: env!("CARGO_PKG_VERSION"),
        library_type: GCCL,
    }
    .grpc_header_value()
});

/// Connects to `endpoint` over `subchannels` HTTP/2 connections. `default_endpoint` is the
/// production Google endpoint of the service. Call on [`crate::runtime::client_runtime`] only.
///
/// Plaintext `http://` endpoints (such as emulators) connect with anonymous credentials;
/// `https://` endpoints use Application Default Credentials. Callers manage retries.
pub async fn connect(
    endpoint: &str,
    default_endpoint: &str,
    subchannels: usize,
) -> io::Result<Client> {
    let mut config = ClientConfig::default();
    config.endpoint = Some(endpoint.to_string());
    config.cred = endpoint
        .starts_with("http://")
        .then(|| anonymous::Builder::new().build());
    config.retry_policy = Some(Arc::new(NeverRetry));
    config.grpc_subchannel_count = Some(subchannels);
    Client::new(config, default_endpoint)
        .await
        .map_err(|e| io::Error::other(format!("Cannot connect to {endpoint}: {e}")))
}
