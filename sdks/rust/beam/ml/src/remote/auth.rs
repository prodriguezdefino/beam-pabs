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

//! Credentials for remote endpoints.

use std::time::Duration;

use beam::options::{Secret, SecretError, SecretValue};
use serde::Deserialize;

/// How requests to a remote endpoint authenticate.
///
/// Credentials are [`Secret`] references, resolved on the worker when the model is loaded,
/// so they never appear in pipeline options or in the pipeline graph.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum RemoteAuth {
    /// No credentials, for example a model server on a private network.
    None,
    /// An API key, sent in the `x-goog-api-key` header.
    ApiKey(Secret),
    /// An OAuth 2.0 access token, sent as `Authorization: Bearer`.
    BearerToken(Secret),
    /// An access token for the worker's Google Cloud service account, from the metadata
    /// server.
    #[default]
    ApplicationDefault,
}

impl RemoteAuth {
    /// Fetches the credentials this refers to.
    pub async fn resolve(&self) -> Result<ResolvedAuth, SecretError> {
        Ok(match self {
            Self::None => ResolvedAuth::None,
            Self::ApiKey(secret) => ResolvedAuth::ApiKey(secret.resolve().await?),
            Self::BearerToken(secret) => ResolvedAuth::BearerToken(secret.resolve().await?),
            Self::ApplicationDefault => ResolvedAuth::ApplicationDefault,
        })
    }
}

/// Credentials of a [`RemoteAuth`], ready to authenticate requests.
#[derive(Clone, Debug)]
pub enum ResolvedAuth {
    /// No credentials.
    None,
    /// An API key.
    ApiKey(SecretValue),
    /// An OAuth 2.0 access token.
    BearerToken(SecretValue),
    /// A metadata server token, fetched per request because it expires.
    ApplicationDefault,
}

impl ResolvedAuth {
    /// Adds these credentials to `request`.
    pub async fn authenticate(&self, request: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        match self {
            Self::None => request,
            Self::ApiKey(key) => request.header("x-goog-api-key", key.expose()),
            Self::BearerToken(token) => request.bearer_auth(token.expose()),
            Self::ApplicationDefault => match fetch_gcp_access_token().await {
                Some(token) => request.bearer_auth(token),
                None => request,
            },
        }
    }
}

/// Fetches an OAuth 2.0 access token for the worker's service account from the Google
/// Compute Engine metadata server, or `None` off Google Cloud.
pub async fn fetch_gcp_access_token() -> Option<String> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_millis(1500))
        .build()
        .ok()?;
    let resp = client
        .get("http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token")
        .header("Metadata-Flavor", "Google")
        .send()
        .await
        .ok()?;

    if resp.status().is_success() {
        #[derive(Deserialize)]
        struct TokenResponse {
            access_token: String,
        }
        let token_resp: TokenResponse = resp.json().await.ok()?;
        Some(token_resp.access_token)
    } else {
        None
    }
}
