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

//! The Google Application Default Credentials of this process, for the clients that do not
//! get credentials from a Google Cloud client library: REST and gRPC calls that the SDK
//! makes itself, and clients of other protocols, such as Kafka with SASL/OAUTHBEARER.
//!
//! The credentials load once, with the `cloud-platform` scope, and keep their access token
//! fresh on [`client_runtime`](crate::runtime::client_runtime). Call the functions from any
//! runtime.

use std::io;
use std::sync::OnceLock;

use google_cloud_auth::credentials::{
    AccessTokenCredentials, Builder, CacheableResource, CredentialsProvider,
};

use crate::runtime::client_runtime;

/// The scope of the credentials.
const SCOPE: &str = "https://www.googleapis.com/auth/cloud-platform";

/// The environment variable that names the principal of credentials that have no service
/// account, for example the credentials of a user. The Google client libraries of Managed
/// Service for Apache Kafka read the same variable.
pub const PRINCIPAL_VARIABLE: &str = "GOOGLE_MANAGED_KAFKA_AUTH_PRINCIPAL";

static CREDENTIALS: OnceLock<Result<AccessTokenCredentials, String>> = OnceLock::new();

/// The credentials of this process. They load on the client runtime, because the task that
/// refreshes their token runs on the runtime that built them.
fn credentials() -> io::Result<&'static AccessTokenCredentials> {
    CREDENTIALS
        .get_or_init(|| {
            let _runtime = client_runtime().enter();
            Builder::default()
                .with_scopes([SCOPE])
                .build_access_token_credentials()
                .map_err(|e| format!("Cannot load the Google Application Default Credentials: {e}"))
        })
        .as_ref()
        .map_err(|e| io::Error::other(e.clone()))
}

/// The request headers of the credentials, as names and values: `authorization`, and the
/// quota project of user credentials.
pub async fn headers() -> io::Result<Vec<(String, String)>> {
    let headers = match credentials()?.headers(Default::default()).await {
        Ok(CacheableResource::New { data, .. }) => data,
        Ok(CacheableResource::NotModified) => {
            return Err(io::Error::other(
                "The Google credentials returned no headers",
            ));
        }
        Err(e) => {
            return Err(io::Error::other(format!(
                "Cannot get the Google credential headers: {e}"
            )));
        }
    };
    headers
        .iter()
        .map(|(name, value)| {
            value
                .to_str()
                .map(|value| (name.to_string(), value.to_string()))
                .map_err(io::Error::other)
        })
        .collect()
}

/// The value of the `authorization` header of the credentials: `Bearer <access token>`.
pub async fn authorization() -> io::Result<String> {
    headers()
        .await?
        .into_iter()
        .find_map(|(name, value)| (name == "authorization").then_some(value))
        .ok_or_else(|| io::Error::other("The Google credentials gave no authorization header"))
}

/// An access token of the credentials.
pub async fn access_token() -> io::Result<String> {
    credentials()?
        .access_token()
        .await
        .map(|token| token.token)
        .map_err(|e| io::Error::other(format!("Cannot get a Google access token: {e}")))
}

/// The principal of the credentials: [`PRINCIPAL_VARIABLE`] if it is set, else the email of
/// the service account of the credentials (a key file, impersonation, or the metadata server
/// on Google Cloud).
pub async fn principal() -> io::Result<String> {
    if let Ok(principal) = std::env::var(PRINCIPAL_VARIABLE)
        && !principal.is_empty()
    {
        return Ok(principal);
    }
    let signer = {
        let _runtime = client_runtime().enter();
        Builder::default().build_signer().map_err(|e| {
            io::Error::other(format!(
                "The Google credentials name no service account ({e}). Set \
                 {PRINCIPAL_VARIABLE} to the principal of the credentials"
            ))
        })?
    };
    signer.client_email().await.map_err(|e| {
        io::Error::other(format!(
            "Cannot read the service account of the Google credentials: {e}"
        ))
    })
}
