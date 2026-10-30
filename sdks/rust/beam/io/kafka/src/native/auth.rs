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

//! SASL/OAUTHBEARER tokens for Google Cloud Managed Service for Apache Kafka, from the
//! Application Default Credentials of [`gcp::auth`].
//!
//! The broker takes a token of three base64url parts: a header with the algorithm
//! `GOOG_OAUTH2_TOKEN`, claims that name the principal, and a Google access token in place of
//! a signature. This is the format of the Google client libraries for the service.

use std::sync::OnceLock;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use gcp::auth::{access_token, principal};
use gcp::runtime::block_on_async;
use rdkafka::client::OAuthToken;
use serde_json::json;

/// The lifetime that a token claims. librdkafka asks for a new token after 80% of it. The
/// credentials refresh an access token 4 minutes before it expires, so a token never outlives
/// its access token.
pub const TOKEN_LIFETIME: Duration = Duration::from_secs(180);

/// The principal of the tokens of this process.
static PRINCIPAL: OnceLock<Result<String, String>> = OnceLock::new();

/// A token for the principal of the credentials, valid for [`TOKEN_LIFETIME`].
pub(super) fn google_token() -> Result<OAuthToken, String> {
    let principal = PRINCIPAL
        .get_or_init(|| block_on_async(principal()).map_err(|e| e.to_string()))
        .clone()?;
    let access_token = block_on_async(access_token()).map_err(|e| e.to_string())?;
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| e.to_string())?;
    let expiry = now + TOKEN_LIFETIME;
    Ok(OAuthToken {
        token: managed_kafka_token(&principal, now, expiry, &access_token),
        lifetime_ms: i64::try_from(expiry.as_millis()).map_err(|e| e.to_string())?,
        principal_name: principal,
    })
}

/// The token of `principal` with the access token `access_token`, valid from `issued` to
/// `expiry` (since the Unix epoch).
#[doc(hidden)]
pub fn managed_kafka_token(
    principal: &str,
    issued: Duration,
    expiry: Duration,
    access_token: &str,
) -> String {
    let header = json!({"typ": "JWT", "alg": "GOOG_OAUTH2_TOKEN"});
    let claims = json!({
        "exp": expiry.as_secs(),
        "iat": issued.as_secs(),
        "iss": "Google",
        "scope": "kafka",
        "sub": principal,
    });
    [
        header.to_string().as_str(),
        claims.to_string().as_str(),
        access_token,
    ]
    .iter()
    .map(|part| URL_SAFE_NO_PAD.encode(part))
    .collect::<Vec<_>>()
    .join(".")
}
