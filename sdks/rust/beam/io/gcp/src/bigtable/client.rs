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

//! Pooled gRPC client for `google.bigtable.v2.Bigtable`.

use std::collections::BTreeMap;
use std::io;
use std::sync::LazyLock;
use std::time::Duration;

use tokio::sync::OnceCell;
use tonic::body::Body;
use tonic::codegen::{Service, http};
use tonic::metadata::{KeyAndValueRef, MetadataKey, MetadataMap, MetadataValue};
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};
use tonic::{Request, Response, Status, Streaming};

use super::proto::bigtable_client::BigtableClient as GrpcClient;
use super::proto::{
    MutateRowsRequest, MutateRowsResponse, ReadRowsRequest, ReadRowsResponse, SampleRowKeysRequest,
    SampleRowKeysResponse,
};
pub use super::retry::{DEFAULT_RETRY, RetryPolicy, is_transient, is_transient_code};

/// Default Bigtable data API endpoint.
pub const DEFAULT_ENDPOINT: &str = "https://bigtable.googleapis.com";

/// Environment variable that overrides the endpoint with a local emulator (`host:port`).
pub const EMULATOR_ENV: &str = "BIGTABLE_EMULATOR_HOST";

/// Number of HTTP/2 connections in the default worker channel pool.
const POOL_CHANNELS: usize = 4;

const ROUTING_COOKIE_PREFIX: &str = "x-goog-cbt-cookie";

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

/// Process-wide balanced channel pool for [`DEFAULT_ENDPOINT`].
static DEFAULT_POOL: OnceCell<Channel> = OnceCell::const_new();

async fn default_pool() -> io::Result<Channel> {
    DEFAULT_POOL
        .get_or_try_init(|| async { build_pool(DEFAULT_ENDPOINT, true) })
        .await
        .cloned()
}

fn build_pool(uri: &str, tls: bool) -> io::Result<Channel> {
    let endpoints = (0..POOL_CHANNELS)
        .map(|_| {
            let ep = Endpoint::from_shared(uri.to_string())
                .map_err(io::Error::other)?
                .tcp_keepalive(Some(Duration::from_secs(30)))
                .http2_keep_alive_interval(Duration::from_secs(30))
                .keep_alive_while_idle(true);
            if tls {
                ep.tls_config(ClientTlsConfig::new().with_native_roots())
                    .map_err(io::Error::other)
            } else {
                Ok(ep)
            }
        })
        .collect::<io::Result<Vec<_>>>()?;
    Ok(Channel::balance_list(endpoints.into_iter()))
}

/// Normalizes an endpoint string or `host:port` into `(uri, uses_tls)`.
pub fn resolve_endpoint(explicit: Option<&str>) -> (String, bool) {
    let raw = explicit
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .or_else(|| std::env::var(EMULATOR_ENV).ok().filter(|s| !s.is_empty()));
    match raw {
        None => (DEFAULT_ENDPOINT.to_string(), true),
        Some(ep) if ep.starts_with("https://") => (ep, true),
        Some(ep) if ep.starts_with("http://") => (ep, false),
        Some(host_port) => (format!("http://{host_port}"), false),
    }
}

/// Client for `google.bigtable.v2.Bigtable`. Cloning shares the underlying channel pool.
#[derive(Clone, Debug)]
pub struct BigtableClient {
    channel: Channel,
    authenticate: bool,
}

impl BigtableClient {
    /// Connects to `endpoint` (or [`EMULATOR_ENV`] / [`DEFAULT_ENDPOINT`] when `None`).
    /// Call on [`crate::runtime::client_runtime`] only.
    pub async fn connect(endpoint: Option<&str>) -> io::Result<Self> {
        let (uri, tls) = resolve_endpoint(endpoint);
        let channel = if uri == DEFAULT_ENDPOINT && tls {
            default_pool().await?
        } else {
            build_pool(&uri, tls)?
        };
        Ok(Self {
            channel,
            authenticate: tls,
        })
    }

    /// Creates a client over an existing `channel` without TLS authentication, for tests.
    pub fn from_channel(channel: Channel) -> Self {
        Self {
            channel,
            authenticate: false,
        }
    }

    /// Calls `SampleRowKeys` and returns the response stream.
    pub async fn sample_row_keys(
        &self,
        request: SampleRowKeysRequest,
        routing_header: &str,
        cookies: &mut RoutingCookies,
    ) -> Result<Streaming<SampleRowKeysResponse>, Status> {
        let (mut client, request) = self.prepare(request, routing_header, cookies).await?;
        with_cookies(client.sample_row_keys(request).await, cookies)
    }

    /// Calls `ReadRows` and returns the response stream.
    pub async fn read_rows(
        &self,
        request: ReadRowsRequest,
        routing_header: &str,
        cookies: &mut RoutingCookies,
    ) -> Result<Streaming<ReadRowsResponse>, Status> {
        let (mut client, request) = self.prepare(request, routing_header, cookies).await?;
        with_cookies(client.read_rows(request).await, cookies)
    }

    /// Calls `MutateRows` and returns the response stream.
    pub async fn mutate_rows(
        &self,
        request: MutateRowsRequest,
        routing_header: &str,
        cookies: &mut RoutingCookies,
    ) -> Result<Streaming<MutateRowsResponse>, Status> {
        let (mut client, request) = self.prepare(request, routing_header, cookies).await?;
        with_cookies(client.mutate_rows(request).await, cookies)
    }

    /// Builds the request with its metadata and a generated client on a ready channel.
    async fn prepare<Req>(
        &self,
        message: Req,
        routing_header: &str,
        cookies: &RoutingCookies,
    ) -> Result<(GrpcClient<Channel>, Request<Req>), Status> {
        let mut metadata = self.build_metadata(routing_header).await?;
        cookies.inject(&mut metadata);
        let mut request = Request::new(message);
        *request.metadata_mut() = metadata;
        // The generated client reports a channel that is not ready as `Unknown`, which is not
        // transient. Wait for the channel here, so this case stays `Unavailable` and retries.
        let mut channel = self.channel.clone();
        std::future::poll_fn(|cx| Service::<http::Request<Body>>::poll_ready(&mut channel, cx))
            .await
            .map_err(|e| Status::unavailable(e.to_string()))?;
        Ok((GrpcClient::new(channel), request))
    }

    async fn build_metadata(&self, routing_header: &str) -> Result<MetadataMap, Status> {
        let mut metadata = if self.authenticate {
            crate::auth::headers()
                .await
                .map_err(|e| Status::unauthenticated(e.to_string()))?
                .into_iter()
                .try_fold(MetadataMap::new(), |mut map, (name, value)| {
                    let key =
                        MetadataKey::from_bytes(name.as_bytes()).map_err(|e| e.to_string())?;
                    let val = MetadataValue::try_from(value).map_err(|e| e.to_string())?;
                    map.insert(key, val);
                    Ok::<_, String>(map)
                })
                .map_err(Status::unauthenticated)?
        } else {
            MetadataMap::new()
        };
        if !routing_header.is_empty() {
            let routing = MetadataValue::try_from(routing_header)
                .map_err(|e| Status::invalid_argument(e.to_string()))?;
            metadata.insert("x-goog-request-params", routing);
        }
        metadata.insert(
            "x-goog-api-client",
            MetadataValue::try_from(API_CLIENT.as_str())
                .map_err(|e| Status::internal(e.to_string()))?,
        );
        metadata.insert(
            "bigtable-features",
            MetadataValue::try_from(FEATURE_FLAGS.as_str())
                .map_err(|e| Status::internal(e.to_string()))?,
        );
        Ok(metadata)
    }
}

/// Stores the routing cookies of the response or of the error, then returns the stream.
fn with_cookies<Resp>(
    result: Result<Response<Streaming<Resp>>, Status>,
    cookies: &mut RoutingCookies,
) -> Result<Streaming<Resp>, Status> {
    match result {
        Ok(response) => {
            cookies.extract(response.metadata());
            Ok(response.into_inner())
        }
        Err(status) => {
            cookies.extract(status.metadata());
            Err(status)
        }
    }
}

/// `x-goog-api-client` value: the Rust compiler and the version of this crate.
static API_CLIENT: LazyLock<String> = LazyLock::new(|| {
    format!(
        "gl-rust/{} gccl/{}",
        googleapis::RUSTC_VERSION,
        env!("CARGO_PKG_VERSION")
    )
});

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
