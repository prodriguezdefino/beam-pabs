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

//! Shared dialing rules for Beam's gRPC services.

use std::time::Duration;

use thiserror::Error;
use tonic::transport::Channel;

/// Ceiling for one gRPC message body, in bytes. Beam often exceeds gRPC's 4 MiB default: an
/// artifact service may return a worker binary as one chunk, and a descriptor grows with
/// the pipeline.
pub const MAX_MESSAGE_BYTES: usize = i32::MAX as usize;

#[derive(Error, Debug)]
pub enum ChannelError {
    #[error("Invalid endpoint URI '{endpoint}': {source}")]
    Uri {
        endpoint: String,
        source: tonic::codegen::http::uri::InvalidUri,
    },
    #[error("Failed to connect to '{endpoint}': {source}")]
    Connect {
        endpoint: String,
        source: tonic::transport::Error,
    },
}

/// Prefixes a bare `host:port` endpoint with the scheme gRPC needs; runners send both forms.
pub fn with_scheme(endpoint: &str) -> String {
    if endpoint.starts_with("http://") || endpoint.starts_with("https://") {
        endpoint.to_string()
    } else {
        format!("http://{endpoint}")
    }
}

/// Maximum duration allowed for establishing a gRPC connection.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(60);

/// HTTP/2 ping interval and timeout while calls are open, ensuring half-open connections
/// fail rather than hang.
const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(20);
const KEEPALIVE_TIMEOUT: Duration = Duration::from_secs(300);

/// Opens a channel to `endpoint`, which may be given with or without a scheme.
pub async fn channel(endpoint: &str) -> Result<Channel, ChannelError> {
    let uri = with_scheme(endpoint);
    Channel::from_shared(uri.clone())
        .map_err(|source| ChannelError::Uri {
            endpoint: uri.clone(),
            source,
        })?
        .connect_timeout(CONNECT_TIMEOUT)
        .tcp_keepalive(Some(KEEPALIVE_INTERVAL))
        .http2_keep_alive_interval(KEEPALIVE_INTERVAL)
        .keep_alive_timeout(KEEPALIVE_TIMEOUT)
        .connect()
        .await
        .map_err(|source| ChannelError::Connect {
            endpoint: uri,
            source,
        })
}

/// Tags an outbound gRPC request with the given metadata key and value.
pub fn attach_header<T>(request: &mut tonic::Request<T>, key: &'static str, value: &str) {
    match value.parse() {
        Ok(val) => {
            request.metadata_mut().insert(key, val);
        }
        Err(e) => tracing::warn!("Header '{key}' value '{value}' is not valid gRPC metadata: {e}"),
    }
}

/// Tags an outbound gRPC request with the `worker_id` metadata the runner expects.
pub fn attach_worker_id<T>(request: &mut tonic::Request<T>, worker_id: &str) {
    attach_header(request, "worker_id", worker_id);
}
