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

use beam::options::{Secret, SecretValue};

use super::RemoteInferenceError;

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
    /// The Google Application Default Credentials of the worker: its service account on
    /// Google Cloud, or the credentials of `gcloud auth application-default login`.
    #[default]
    ApplicationDefault,
}

impl RemoteAuth {
    /// Fetches the credentials this refers to.
    ///
    /// # Errors
    ///
    /// Fails if the worker cannot resolve a secret or has no Application Default Credentials.
    pub async fn resolve(&self) -> Result<ResolvedAuth, RemoteInferenceError> {
        Ok(match self {
            Self::None => ResolvedAuth::None,
            Self::ApiKey(secret) => ResolvedAuth::ApiKey(secret.resolve().await?),
            Self::BearerToken(secret) => ResolvedAuth::BearerToken(secret.resolve().await?),
            Self::ApplicationDefault => {
                beam_gcp_core::auth::authorization()
                    .await
                    .map_err(|e| RemoteInferenceError::Authentication(e.to_string()))?;
                ResolvedAuth::ApplicationDefault
            }
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
    /// The Application Default Credentials, asked for each request because their token
    /// expires.
    ApplicationDefault,
}

impl ResolvedAuth {
    /// Adds these credentials to `request`.
    ///
    /// # Errors
    ///
    /// Fails if the Application Default Credentials give no token.
    pub async fn authenticate(
        &self,
        request: reqwest::RequestBuilder,
    ) -> Result<reqwest::RequestBuilder, RemoteInferenceError> {
        Ok(match self {
            Self::None => request,
            Self::ApiKey(key) => request.header("x-goog-api-key", key.expose()),
            Self::BearerToken(token) => request.bearer_auth(token.expose()),
            Self::ApplicationDefault => {
                let authorization = beam_gcp_core::auth::authorization()
                    .await
                    .map_err(|e| RemoteInferenceError::Authentication(e.to_string()))?;
                request.header("authorization", authorization)
            }
        })
    }
}
