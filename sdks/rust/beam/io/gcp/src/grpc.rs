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

//! gRPC clients on the transport of the Google Cloud client libraries, for the calls that no
//! client library makes. The transport adds the credentials, the `x-goog-api-client` header
//! and the `x-goog-request-params` routing header.

use std::io;
use std::sync::{Arc, LazyLock};

use google_cloud_auth::credentials::anonymous;
use google_cloud_gax::retry_policy::NeverRetry;
use google_cloud_gax_internal::api_header::{GCCL, XGoogApiClient};
use google_cloud_gax_internal::grpc::Client;
use google_cloud_gax_internal::options::ClientConfig;

/// The `x-goog-api-client` header value: the Rust compiler, the transport and this crate.
pub(crate) static API_CLIENT: LazyLock<String> = LazyLock::new(|| {
    XGoogApiClient {
        name: env!("CARGO_PKG_NAME"),
        version: env!("CARGO_PKG_VERSION"),
        library_type: GCCL,
    }
    .grpc_header_value()
});

/// Connects to `endpoint` over `subchannels` HTTP/2 connections. `default_endpoint` is the
/// Google endpoint of the service. Call it on the client runtime only.
///
/// An `http://` endpoint, such as an emulator, gets no credentials. Other endpoints use the
/// Application Default Credentials. The client does not retry: the callers retry.
pub(crate) async fn connect(
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
