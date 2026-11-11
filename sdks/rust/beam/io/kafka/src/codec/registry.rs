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

//! A client of a Confluent Schema Registry ([`SchemaRegistry`]), for the [`Avro`] codec.
//!
//! The client uses the REST API of the registry: `GET /subjects/{subject}/versions/latest`,
//! `GET /schemas/ids/{id}` and `POST /subjects/{subject}/versions`. It accepts only Avro
//! schemas. Its calls block the thread, so the reader of a read calls it only when it finds a
//! schema ID that the codec does not have yet.
//!
//! [`Avro`]: super::Avro

use std::fmt;
use std::time::Duration;

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use beam_gcp_core::runtime::block_on_async;
use serde_json::{Value as Json, json};

/// The media type of the requests and the responses of the registry.
const MEDIA_TYPE: &str = "application/vnd.schemaregistry.v1+json";

/// The longest time of one call to the registry when the pipeline is built.
const CALL_TIMEOUT: Duration = Duration::from_secs(30);

/// The longest time of one call to get a schema by ID. The reader waits for this call.
const READ_TIMEOUT: Duration = Duration::from_secs(5);

/// How the client authenticates to the registry.
#[derive(Clone)]
enum Auth {
    None,
    /// HTTP basic authentication.
    Basic {
        user: String,
        password: String,
    },
    /// A bearer token of the Google Application Default Credentials.
    GoogleAdc,
}

/// A client of a Confluent Schema Registry.
#[derive(Clone)]
pub struct SchemaRegistry {
    url: String,
    auth: Auth,
    agent: ureq::Agent,
}

/// A schema of the registry and its ID.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RegisteredSchema {
    pub id: u32,
    /// The Avro schema, in JSON.
    pub schema: String,
}

impl fmt::Debug for SchemaRegistry {
    /// Shows the kind of authentication, not the password.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let auth = match self.auth {
            Auth::None => "none",
            Auth::Basic { .. } => "basic",
            Auth::GoogleAdc => "google",
        };
        f.debug_struct("SchemaRegistry")
            .field("url", &self.url)
            .field("auth", &auth)
            .finish_non_exhaustive()
    }
}

impl SchemaRegistry {
    /// The registry at `url`, for example `http://registry:8081`, with no authentication.
    pub fn new(url: impl Into<String>) -> Self {
        let url = url.into().trim_end_matches('/').to_string();
        // The client reads the status of a failed call, to give the message of the registry.
        let config = ureq::Agent::config_builder()
            .http_status_as_error(false)
            .timeout_global(Some(CALL_TIMEOUT));
        // A registry on this machine needs no proxy of the environment.
        let local = ["http://127.0.0.1", "http://localhost", "http://[::1]"]
            .iter()
            .any(|prefix| url.starts_with(prefix));
        let config = if local { config.proxy(None) } else { config };
        Self {
            url,
            auth: Auth::None,
            agent: config.build().into(),
        }
    }

    /// The same registry, with HTTP basic authentication.
    pub fn with_basic_auth(mut self, user: impl Into<String>, password: impl Into<String>) -> Self {
        self.auth = Auth::Basic {
            user: user.into(),
            password: password.into(),
        };
        self
    }

    /// The same registry, with a bearer token of the Google Application Default Credentials,
    /// for the schema registry of Google Cloud Managed Service for Apache Kafka.
    pub fn with_google_auth(mut self) -> Self {
        self.auth = Auth::GoogleAdc;
        self
    }

    pub fn url(&self) -> &str {
        &self.url
    }

    /// The latest version of the schema of `subject`.
    ///
    /// # Errors
    ///
    /// Fails if the call fails, or the schema is not an Avro schema.
    pub fn latest(&self, subject: &str) -> beam::Result<RegisteredSchema> {
        let response = self.call(
            &format!("/subjects/{}/versions/latest", path_segment(subject)),
            None,
            CALL_TIMEOUT,
        )?;
        Ok(RegisteredSchema {
            id: schema_id(&response)?,
            schema: avro_schema(&response)?,
        })
    }

    /// The schema with the ID `id`. The call stops after 5 seconds, so a reader does not wait
    /// long for a registry that does not answer.
    ///
    /// # Errors
    ///
    /// Fails if the call fails, or the schema is not an Avro schema.
    pub fn schema(&self, id: u32) -> beam::Result<String> {
        avro_schema(&self.call(&format!("/schemas/ids/{id}"), None, READ_TIMEOUT)?)
    }

    /// Registers the Avro schema `schema` under `subject`, and returns its ID. For a schema
    /// that the subject has already, the registry returns the ID that it has.
    ///
    /// # Errors
    ///
    /// Fails if the call fails, for example if the schema is not compatible with the subject.
    pub fn register(&self, subject: &str, schema: &str) -> beam::Result<u32> {
        let body = json!({ "schema": schema }).to_string();
        let response = self.call(
            &format!("/subjects/{}/versions", path_segment(subject)),
            Some(&body),
            CALL_TIMEOUT,
        )?;
        schema_id(&response)
    }

    /// Sends a GET, or a POST of `body`, to `path`, and returns the JSON of the response.
    fn call(&self, path: &str, body: Option<&str>, timeout: Duration) -> beam::Result<Json> {
        let url = format!("{}{path}", self.url);
        let fail = |reason: String| beam::Error::msg(format!("Schema registry {url}: {reason}"));
        let authorization = self.authorization().map_err(fail)?;
        let auth = authorization.as_deref();
        let response = match body {
            None => prepare(self.agent.get(&url), auth, timeout).call(),
            Some(body) => prepare(self.agent.post(&url), auth, timeout)
                .header("Content-Type", MEDIA_TYPE)
                .send(body),
        };
        let mut response = response.map_err(|e| fail(e.to_string()))?;
        let status = response.status();
        let text = response
            .body_mut()
            .read_to_string()
            .map_err(|e| fail(e.to_string()))?;
        if status.is_success() {
            serde_json::from_str(&text).map_err(|e| fail(format!("not JSON: {e}")))
        } else {
            Err(fail(format!("status {status}: {text}")))
        }
    }

    /// The value of the `Authorization` header, if the registry needs one.
    fn authorization(&self) -> Result<Option<String>, String> {
        match &self.auth {
            Auth::None => Ok(None),
            Auth::Basic { user, password } => Ok(Some(format!(
                "Basic {}",
                STANDARD.encode(format!("{user}:{password}"))
            ))),
            Auth::GoogleAdc => block_on_async(beam_gcp_core::auth::authorization())
                .map(Some)
                .map_err(|e| format!("no Google credentials: {e}")),
        }
    }
}

/// `request` with the `Accept` header, the `Authorization` header if `auth` has one, and
/// `timeout`.
fn prepare<B>(
    request: ureq::RequestBuilder<B>,
    auth: Option<&str>,
    timeout: Duration,
) -> ureq::RequestBuilder<B> {
    let request = request
        .header("Accept", MEDIA_TYPE)
        .config()
        .timeout_global(Some(timeout))
        .build();
    match auth {
        Some(auth) => request.header("Authorization", auth),
        None => request,
    }
}

fn schema_id(response: &Json) -> beam::Result<u32> {
    response
        .get("id")
        .and_then(Json::as_u64)
        .and_then(|id| u32::try_from(id).ok())
        .ok_or_else(|| beam::Error::msg(format!("The registry gave no schema ID: {response}")))
}

/// The Avro schema of a response. A response with no `schemaType` has an Avro schema.
fn avro_schema(response: &Json) -> beam::Result<String> {
    match response.get("schemaType").and_then(Json::as_str) {
        None | Some("AVRO") => response
            .get("schema")
            .and_then(Json::as_str)
            .map(str::to_string)
            .ok_or_else(|| beam::Error::msg(format!("The registry gave no schema: {response}"))),
        Some(other) => Err(beam::Error::msg(format!(
            "The registry schema is {other}, not AVRO"
        ))),
    }
}

/// `segment` with the bytes that a URL path segment cannot hold in percent encoding.
fn path_segment(segment: &str) -> String {
    segment
        .bytes()
        .map(|byte| match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                char::from(byte).to_string()
            }
            other => format!("%{other:02X}"),
        })
        .collect()
}
