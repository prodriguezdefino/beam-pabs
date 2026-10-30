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

//! gRPC client for `google.bigtable.v2.Bigtable` on the transport of the Google Cloud client
//! libraries.

use std::collections::{BTreeMap, HashMap};
use std::io;
use std::sync::{Arc, LazyLock, Mutex};

use google_cloud_gax::error::Error;
use google_cloud_gax::options::RequestOptions;
use google_cloud_gax::options::internal::RequestOptionsExt as _;
use google_cloud_gax_internal::grpc::Client;
use tokio::sync::OnceCell;
use tonic::codegen::http::HeaderMap;
use tonic::codegen::http::uri::PathAndQuery;
use tonic::metadata::{KeyAndValueRef, MetadataKey, MetadataMap, MetadataValue};
use tonic::{Extensions, GrpcMethod, Status, Streaming};

use super::proto::{
    MutateRowsRequest, MutateRowsResponse, ReadRowsRequest, ReadRowsResponse, SampleRowKeysRequest,
    SampleRowKeysResponse,
};
use crate::grpc::{API_CLIENT, connect};

/// Default Bigtable data API endpoint.
pub const DEFAULT_ENDPOINT: &str = "https://bigtable.googleapis.com";

/// Environment variable that overrides the endpoint with a local emulator (`host:port`).
pub const EMULATOR_ENV: &str = "BIGTABLE_EMULATOR_HOST";

/// Number of HTTP/2 connections of each client.
const POOL_CHANNELS: usize = 4;

const SERVICE: &str = "google.bigtable.v2.Bigtable";

const ROUTING_COOKIE_PREFIX: &str = "x-goog-cbt-cookie";

/// One method of the service: its name and its HTTP/2 path.
type Method = (&'static str, &'static str);

const SAMPLE_ROW_KEYS: Method = (
    "SampleRowKeys",
    "/google.bigtable.v2.Bigtable/SampleRowKeys",
);
const READ_ROWS: Method = ("ReadRows", "/google.bigtable.v2.Bigtable/ReadRows");
const MUTATE_ROWS: Method = ("MutateRows", "/google.bigtable.v2.Bigtable/MutateRows");

/// Stores `x-goog-cbt-cookie*` metadata headers returned by Bigtable across retries.
#[derive(Clone, Debug, Default)]
pub struct RoutingCookies {
    entries: BTreeMap<String, String>,
}

impl RoutingCookies {
    /// Extracts any `x-goog-cbt-cookie*` ASCII headers from `metadata`.
    pub fn extract(&mut self, metadata: &MetadataMap) {
        for (key, value) in metadata.iter().filter_map(|kv| match kv {
            KeyAndValueRef::Ascii(k, v) => Some((k, v)),
            KeyAndValueRef::Binary(_, _) => None,
        }) {
            if key.as_str().starts_with(ROUTING_COOKIE_PREFIX)
                && let Ok(val_str) = value.to_str()
            {
                self.entries
                    .insert(key.as_str().to_string(), val_str.to_string());
            }
        }
    }

    /// Injects stored `x-goog-cbt-cookie*` headers into `metadata`.
    pub fn inject(&self, metadata: &mut MetadataMap) {
        for (name, value) in &self.entries {
            if let (Ok(key), Ok(val)) = (
                MetadataKey::from_bytes(name.as_bytes()),
                MetadataValue::try_from(value.as_str()),
            ) {
                metadata.insert(key, val);
            }
        }
    }
}

/// The clients of the process by endpoint URI. The scheme keeps an emulator apart from TLS.
static CLIENTS: LazyLock<Mutex<HashMap<String, Arc<OnceCell<Client>>>>> =
    LazyLock::new(Mutex::default);

/// Normalizes an endpoint string or `host:port` into a URI. A `host:port` is plaintext.
pub fn resolve_endpoint(explicit: Option<&str>) -> String {
    let raw = explicit
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .or_else(|| std::env::var(EMULATOR_ENV).ok().filter(|s| !s.is_empty()));
    match raw {
        None => DEFAULT_ENDPOINT.to_string(),
        Some(ep) if ep.starts_with("https://") || ep.starts_with("http://") => ep,
        Some(host_port) => format!("http://{host_port}"),
    }
}

/// Client for `google.bigtable.v2.Bigtable`. Cloning shares the connections.
#[derive(Clone, Debug)]
pub struct BigtableClient {
    inner: Client,
}

impl BigtableClient {
    /// Connects to `endpoint` (or [`EMULATOR_ENV`] / [`DEFAULT_ENDPOINT`] when `None`). All
    /// calls for the same endpoint share one client. Call on
    /// [`crate::runtime::client_runtime`] only.
    pub async fn connect(endpoint: Option<&str>) -> io::Result<Self> {
        let uri = resolve_endpoint(endpoint);
        let cell = CLIENTS
            .lock()
            .map(|mut clients| Arc::clone(clients.entry(uri.clone()).or_default()))
            .map_err(|_| io::Error::other("The Bigtable client cache is poisoned"))?;
        let inner = cell
            .get_or_try_init(|| connect(&uri, DEFAULT_ENDPOINT, POOL_CHANNELS))
            .await?;
        Ok(Self {
            inner: inner.clone(),
        })
    }

    /// Calls `SampleRowKeys` and returns the response stream.
    pub async fn sample_row_keys(
        &self,
        request: SampleRowKeysRequest,
        routing_header: &str,
        cookies: &mut RoutingCookies,
    ) -> Result<Streaming<SampleRowKeysResponse>, Status> {
        self.server_streaming(SAMPLE_ROW_KEYS, request, routing_header, cookies)
            .await
    }

    /// Calls `ReadRows` and returns the response stream.
    pub async fn read_rows(
        &self,
        request: ReadRowsRequest,
        routing_header: &str,
        cookies: &mut RoutingCookies,
    ) -> Result<Streaming<ReadRowsResponse>, Status> {
        self.server_streaming(READ_ROWS, request, routing_header, cookies)
            .await
    }

    /// Calls `MutateRows` and returns the response stream.
    pub async fn mutate_rows(
        &self,
        request: MutateRowsRequest,
        routing_header: &str,
        cookies: &mut RoutingCookies,
    ) -> Result<Streaming<MutateRowsResponse>, Status> {
        self.server_streaming(MUTATE_ROWS, request, routing_header, cookies)
            .await
    }

    /// Opens the stream of `method` and stores the routing cookies of the response or error.
    async fn server_streaming<Req, Resp>(
        &self,
        (method, path): Method,
        request: Req,
        routing_header: &str,
        cookies: &mut RoutingCookies,
    ) -> Result<Streaming<Resp>, Status>
    where
        Req: prost::Message + Clone + 'static,
        Resp: prost::Message + Default + 'static,
    {
        let mut extensions = Extensions::new();
        extensions.insert(GrpcMethod::new(SERVICE, method));
        let options = RequestOptions::default().insert_extension(request_headers(cookies)?);
        let result = self
            .inner
            .server_streaming(
                extensions,
                PathAndQuery::from_static(path),
                request,
                options,
                &API_CLIENT,
                routing_header,
            )
            .await;
        match result {
            Ok(response) => {
                cookies.extract(response.metadata());
                Ok(response.into_inner())
            }
            Err(error) => {
                let status = to_status(&error);
                cookies.extract(status.metadata());
                Err(status)
            }
        }
    }
}

/// The headers of each call: the Bigtable features and the routing cookies.
fn request_headers(cookies: &RoutingCookies) -> Result<HeaderMap, Status> {
    let features = MetadataValue::try_from(FEATURE_FLAGS.as_str())
        .map_err(|e| Status::internal(e.to_string()))?;
    let mut metadata = MetadataMap::new();
    metadata.insert("bigtable-features", features);
    cookies.inject(&mut metadata);
    Ok(metadata.into_headers())
}

/// Returns the gRPC status of a failed call. An error without a status, such as a connection
/// that is not ready, is `UNAVAILABLE`, so the callers retry it.
fn to_status(error: &Error) -> Status {
    let status = std::iter::successors(std::error::Error::source(error), |&cause| cause.source())
        .find_map(|cause| cause.downcast_ref::<Status>());
    match status {
        Some(status) => status.clone(),
        None if error.is_authentication() => Status::unauthenticated(error.to_string()),
        None if error.is_serialization() => Status::invalid_argument(error.to_string()),
        None => Status::unavailable(error.to_string()),
    }
}

/// `bigtable-features` value. Each flag names a feature that this client implements:
/// - `mutate_rows_rate_limit*`: the writer waits for the `RateLimitInfo` of `MutateRows`.
/// - `last_scanned_row_responses`: the reader claims scanned-row heartbeats.
/// - `routing_cookie`: retries send the `x-goog-cbt-cookie*` headers back.
/// - `retry_info`: retries wait for the delay of the `RetryInfo` error detail.
static FEATURE_FLAGS: LazyLock<String> = LazyLock::new(|| {
    use prost::Message as _;
    let flags = super::proto::FeatureFlags {
        mutate_rows_rate_limit: true,
        mutate_rows_rate_limit2: true,
        last_scanned_row_responses: true,
        routing_cookie: true,
        retry_info: true,
        ..Default::default()
    };
    url_safe_base64(&flags.encode_to_vec())
});

fn url_safe_base64(input: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut out = String::with_capacity(input.len().div_ceil(3) * 4);
    for chunk in input.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = chunk.get(1).copied().unwrap_or(0) as u32;
        let b2 = chunk.get(2).copied().unwrap_or(0) as u32;
        let triple = (b0 << 16) | (b1 << 8) | b2;
        out.push(ALPHABET[((triple >> 18) & 0x3F) as usize] as char);
        out.push(ALPHABET[((triple >> 12) & 0x3F) as usize] as char);
        if chunk.len() > 1 {
            out.push(ALPHABET[((triple >> 6) & 0x3F) as usize] as char);
        } else {
            out.push('=');
        }
        if chunk.len() > 2 {
            out.push(ALPHABET[(triple & 0x3F) as usize] as char);
        } else {
            out.push('=');
        }
    }
    out
}
