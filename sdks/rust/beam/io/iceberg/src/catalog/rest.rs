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

//! The REST catalog client, and the vended storage credentials of its tables.
//!
//! The `iceberg` REST client reads storage credentials only from the `config` of a table
//! response. A catalog can send them in `storage-credentials` instead, so the connection
//! reads them from the `credentials` endpoint of the table.

use std::collections::HashMap;
use std::error::Error as StdError;
use std::fmt::{self, Display};
use std::sync::{Arc, LazyLock, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use beam::metrics::{ApiRequest, Metrics, http_status};
use beam::retry::{Backoff, Decision, RetryWaits, with_retry_waits, with_throttled_retries};
use iceberg::table::Table;
use iceberg::{
    CatalogBuilder, ErrorKind, Namespace, NamespaceIdent, TableCommit, TableCreation, TableIdent,
};
use iceberg_catalog_rest::{
    AuthManager, AuthSession, HttpClient, HttpRequest, RestCatalog, RestCatalogBuilder,
};
use reqwest::header::{AUTHORIZATION, HeaderMap, HeaderName, HeaderValue, RETRY_AFTER};
use serde::Deserialize;
use tokio::sync::OnceCell;

use super::file_io::with_storage_config;
use super::{Auth, Catalog, RedactedKeys};
use crate::METRICS;
use crate::storage::FileSystemStorageFactory;

/// The header that asks a REST catalog for vended storage credentials.
pub const ACCESS_DELEGATION: &str = "X-Iceberg-Access-Delegation";
const VENDED_CREDENTIALS: &str = "vended-credentials";

/// The service of the catalog calls in the I/O request metrics.
pub const SERVICE: &str = "IcebergRestCatalog";

/// Retries for REST catalog HTTP calls (4 retries, 0.25 s to 10 s).
const CATALOG_RETRY: Backoff = Backoff {
    attempts: 5,
    initial: Duration::from_millis(250),
    max: Duration::from_secs(10),
};

/// The status name of a failed catalog call for the I/O request metrics.
pub fn catalog_status(error: &iceberg::Error) -> &'static str {
    match error.kind() {
        ErrorKind::TableNotFound | ErrorKind::NamespaceNotFound => "not_found",
        ErrorKind::TableAlreadyExists | ErrorKind::NamespaceAlreadyExists => "already_exists",
        ErrorKind::CatalogCommitConflicts => "aborted",
        ErrorKind::PreconditionFailed => "failed_precondition",
        ErrorKind::DataInvalid => "invalid_argument",
        ErrorKind::FeatureUnsupported => "unimplemented",
        _ if is_throttled_catalog_error(error) => "resource_exhausted",
        _ => "unknown",
    }
}

/// Returns how to retry a failed Iceberg catalog call.
pub fn catalog_decision(error: &iceberg::Error) -> Decision {
    match error.kind() {
        ErrorKind::CatalogCommitConflicts => Decision::Backoff,
        ErrorKind::TableNotFound
        | ErrorKind::NamespaceNotFound
        | ErrorKind::TableAlreadyExists
        | ErrorKind::NamespaceAlreadyExists
        | ErrorKind::PreconditionFailed
        | ErrorKind::DataInvalid
        | ErrorKind::FeatureUnsupported => Decision::Stop,
        _ => {
            if is_throttled_catalog_error(error) {
                return Decision::Throttled;
            }
            if let Some(req) = reqwest_source(error) {
                if let Some(status) = req.status() {
                    if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
                        return Decision::Throttled;
                    }
                    return if is_transient_http(status.as_u16()) {
                        Decision::Backoff
                    } else {
                        Decision::Stop
                    };
                }
                if !req.is_builder() && !req.is_decode() {
                    return Decision::Backoff;
                }
            }
            if error.retryable() || is_transient_catalog_text(&format!("{error:?}")) {
                Decision::Backoff
            } else {
                Decision::Stop
            }
        }
    }
}

/// Parses a numeric delta-seconds `Retry-After` header.
pub fn parse_retry_after(headers: &HeaderMap) -> Option<Duration> {
    let seconds: u64 = headers
        .get(RETRY_AFTER)?
        .to_str()
        .ok()?
        .trim()
        .parse()
        .ok()?;
    Some(Duration::from_secs(seconds))
}

fn is_transient_http(code: u16) -> bool {
    matches!(code, 408 | 429 | 500 | 502 | 503 | 504)
}

fn reqwest_source<'a>(error: &'a (dyn StdError + 'static)) -> Option<&'a reqwest::Error> {
    let mut current = Some(error);
    while let Some(err) = current {
        if let Some(req) = err.downcast_ref::<reqwest::Error>() {
            return Some(req);
        }
        current = err.source();
    }
    None
}

/// Returns whether `error` reflects HTTP 429 or server rate limiting.
fn is_throttled_catalog_error(error: &iceberg::Error) -> bool {
    if reqwest_source(error)
        .is_some_and(|req| req.status() == Some(reqwest::StatusCode::TOO_MANY_REQUESTS))
    {
        return true;
    }
    let text = format!("{error:?}").to_ascii_lowercase();
    text.contains("429 too many")
        || text.contains("status: 429")
        || text.contains("http 429")
        || text.contains("code: 429")
        || text.contains("\"code\":429")
        || text.contains("\"code\": 429")
        || text.contains("too many requests")
        || text.contains("rate limit")
        || text.contains("resource_exhausted")
}

/// Returns whether a formatted `iceberg::Error` names a transient HTTP status.
fn is_transient_catalog_text(debug: &str) -> bool {
    let text = debug.to_ascii_lowercase();
    [
        "status: 408",
        "status: 500",
        "status: 502",
        "status: 503",
        "status: 504",
        "http 408",
        "http 500",
        "http 502",
        "http 503",
        "http 504",
        "408 request timeout",
        "500 internal server error",
        "502 bad gateway",
        "503 service unavailable",
        "504 gateway timeout",
    ]
    .iter()
    .any(|needle| text.contains(needle))
}

/// Failure of one HTTP attempt of [`Connection`].
enum SendError {
    Http {
        status: reqwest::StatusCode,
        retry_after: Option<Duration>,
        message: String,
    },
    Transport(reqwest::Error),
    Other(beam::Error),
}

impl From<SendError> for beam::Error {
    fn from(error: SendError) -> Self {
        match error {
            SendError::Http { message, .. } => Self::msg(message),
            SendError::Transport(err) => err.into(),
            SendError::Other(err) => err,
        }
    }
}

fn send_decision(error: &SendError) -> Decision {
    match error {
        SendError::Http {
            status,
            retry_after,
            ..
        } => match (status.as_u16(), *retry_after) {
            (429 | 503, Some(delay)) => Decision::Wait(delay),
            (429, None) => Decision::Throttled,
            (408 | 500 | 502 | 503 | 504, _) => Decision::Backoff,
            _ => Decision::Stop,
        },
        SendError::Transport(err) if !err.is_builder() && !err.is_decode() => Decision::Backoff,
        SendError::Transport(_) | SendError::Other(_) => Decision::Stop,
    }
}

/// The props of the `iceberg` REST client for `catalog`. The bearer token is not a prop:
/// each request takes it again.
pub fn rest_props(catalog: &Catalog) -> HashMap<String, String> {
    let Catalog {
        uri,
        warehouse,
        properties,
        ..
    } = catalog;
    let headers = request_headers(catalog)
        .into_iter()
        .map(|(name, value)| (format!("header.{name}"), value));
    properties
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .chain(headers)
        .chain([("uri".to_string(), uri.clone())])
        .chain(
            warehouse
                .iter()
                .map(|w| ("warehouse".to_string(), w.clone())),
        )
        .collect()
}

/// The headers of each catalog request, without the bearer token.
fn request_headers(catalog: &Catalog) -> Vec<(String, String)> {
    catalog
        .headers
        .iter()
        .map(|(name, value)| (name.clone(), value.clone()))
        .chain(catalog.vended_credentials.then(|| {
            (
                ACCESS_DELEGATION.to_string(),
                VENDED_CREDENTIALS.to_string(),
            )
        }))
        .collect()
}

async fn token(auth: &Auth) -> beam::Result<Option<String>> {
    match auth {
        Auth::None => Ok(None),
        Auth::Token(token) => Ok(Some(token.clone())),
        #[cfg(feature = "gcp")]
        Auth::Google => Ok(Some(beam_gcp_core::auth::access_token().await?)),
    }
}

/// The `Authorization` header of `auth`, if it has a token. A Google token comes from the
/// credentials cache, which renews it before it expires.
async fn bearer(auth: &Auth) -> beam::Result<Option<HeaderValue>> {
    let Some(token) = token(auth).await? else {
        return Ok(None);
    };
    let mut value = HeaderValue::try_from(format!("Bearer {token}"))?;
    value.set_sensitive(true);
    Ok(Some(value))
}

/// Adds the bearer token of `auth` to each request of the `iceberg` REST client.
#[derive(Debug)]
struct BearerAuth(Auth);

#[async_trait::async_trait]
impl AuthManager for BearerAuth {
    async fn init_session(
        &self,
        _client: &HttpClient,
        _props: &HashMap<String, String>,
    ) -> iceberg::Result<Box<dyn AuthSession>> {
        Ok(Box::new(BearerAuth(self.0.clone())))
    }

    async fn catalog_session(
        &self,
        _client: &HttpClient,
        _props: &HashMap<String, String>,
    ) -> iceberg::Result<Arc<dyn AuthSession>> {
        Ok(Arc::new(BearerAuth(self.0.clone())))
    }
}

#[async_trait::async_trait]
impl AuthSession for BearerAuth {
    async fn authenticate(&self, request: &mut HttpRequest) -> iceberg::Result<()> {
        let value = bearer(&self.0)
            .await
            .map_err(|e| iceberg::Error::new(ErrorKind::Unexpected, e.to_string()))?;
        if let Some(value) = value {
            request.headers_mut().insert(AUTHORIZATION, value);
        }
        Ok(())
    }
}

pub(super) fn runtime() -> beam::Result<iceberg::Runtime> {
    Ok(iceberg::Runtime::new(beam::internals::io_runtime()?))
}

/// The connections of this process, one for each catalog config. The lock is held only to
/// read or update the map, never across a catalog call.
static CONNECTIONS: LazyLock<Mutex<HashMap<Catalog, Arc<Connection>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// A REST catalog client, and the HTTP client and headers for the requests that the
/// `iceberg` client does not make.
///
/// It is an [`iceberg::Catalog`]. Its `load_table` adds the vended credentials of the table,
/// so a transaction that reloads the table still writes its files with them.
pub struct Connection {
    catalog: RestCatalog,
    client: reqwest::Client,
    uri: String,
    warehouse: Option<String>,
    prefix: OnceCell<String>,
    headers: HeaderMap,
    auth: Auth,
    vended_credentials: bool,
    metrics_reporting: bool,
}

/// Hides header values and internal catalog config, which can hold credentials.
impl fmt::Debug for Connection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Connection")
            .field("uri", &self.uri)
            .field("warehouse", &self.warehouse)
            .field("prefix", &self.prefix.get())
            .field(
                "headers",
                // `http` header keys do not clone, so collect them.
                &RedactedKeys(
                    self.headers
                        .keys()
                        .map(HeaderName::as_str)
                        .collect::<Vec<_>>()
                        .into_iter(),
                ),
            )
            .field("auth", &self.auth)
            .field("vended_credentials", &self.vended_credentials)
            .field("metrics_reporting", &self.metrics_reporting)
            .finish_non_exhaustive()
    }
}

/// The connection to `catalog`. Every catalog call of the write goes through this function.
/// A connection serves all the steps of the process. Each request takes the bearer token
/// again, so a long write does not use an expired token.
///
/// # Errors
///
/// Fails if a header is not valid or the client cannot start.
pub async fn connect(catalog: &Catalog) -> beam::Result<Arc<Connection>> {
    let cached = connections().get(catalog).map(Arc::clone);
    if let Some(connection) = cached {
        return Ok(connection);
    }
    let connection = Arc::new(open(catalog).await?);
    connections().insert(catalog.clone(), Arc::clone(&connection));
    Ok(connection)
}

fn connections() -> MutexGuard<'static, HashMap<Catalog, Arc<Connection>>> {
    CONNECTIONS.lock().unwrap_or_else(PoisonError::into_inner)
}

async fn open(catalog: &Catalog) -> beam::Result<Connection> {
    let client = reqwest::Client::builder().build()?;
    let rest = RestCatalogBuilder::default()
        .with_client(client.clone())
        .with_auth_manager(BearerAuth(catalog.auth.clone()))
        .with_storage_factory(Arc::new(FileSystemStorageFactory))
        .with_runtime(runtime()?)
        .load("beam", rest_props(catalog))
        .await?;
    let headers = request_headers(catalog)
        .into_iter()
        .map(|(name, value)| {
            let mut value = HeaderValue::try_from(value.as_str())?;
            value.set_sensitive(true);
            Ok((HeaderName::try_from(name.as_str())?, value))
        })
        .collect::<beam::Result<HeaderMap>>()?;
    Ok(Connection {
        catalog: rest,
        client,
        uri: catalog.uri.trim_end_matches('/').to_string(),
        warehouse: catalog.warehouse.clone(),
        prefix: OnceCell::new(),
        headers,
        auth: catalog.auth.clone(),
        vended_credentials: catalog.vended_credentials,
        metrics_reporting: catalog.metrics_reporting,
    })
}

/// The catalog of a write: the REST catalog of the options, or a client that the program
/// gives, for example a memory catalog in a test.
#[derive(Clone, Debug)]
pub enum CatalogClient {
    Rest(Catalog),
    Given(Arc<dyn iceberg::Catalog>),
}

impl CatalogClient {
    /// The client. A REST catalog goes through [`connect`].
    ///
    /// # Errors
    ///
    /// Fails if the connection cannot open.
    pub async fn catalog(&self) -> beam::Result<Arc<dyn iceberg::Catalog>> {
        match self {
            Self::Rest(catalog) => Ok(connect(catalog).await?),
            Self::Given(catalog) => Ok(Arc::clone(catalog)),
        }
    }

    /// Sends `report` as [`Connection::report_metrics`] does. A given client sends nothing.
    pub async fn report_metrics(&self, ident: &TableIdent, report: &serde_json::Value) -> bool {
        match self {
            Self::Rest(catalog) if catalog.metrics_reporting => match connect(catalog).await {
                Ok(connection) => connection.report_metrics(ident, report).await,
                Err(error) => {
                    Metrics::counter(METRICS, "metrics_report_failures").inc();
                    tracing::warn!(
                        "The metrics report of the Iceberg table {ident} was not sent: {error}"
                    );
                    false
                }
            },
            Self::Rest(_) | Self::Given(_) => true,
        }
    }
}

#[derive(Deserialize)]
struct ConfigResponse {
    #[serde(default)]
    overrides: HashMap<String, String>,
    #[serde(default)]
    defaults: HashMap<String, String>,
}

#[derive(Deserialize)]
struct CredentialsResponse {
    #[serde(rename = "storage-credentials", default)]
    storage_credentials: Vec<StorageCredential>,
}

/// A storage credential of a REST catalog: the config for the paths under `prefix`.
#[derive(Deserialize)]
pub struct StorageCredential {
    pub prefix: String,
    pub config: HashMap<String, String>,
}

/// Hides credential config values, which hold short-lived storage tokens and keys.
impl fmt::Debug for StorageCredential {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StorageCredential")
            .field("prefix", &self.prefix)
            .field("config", &RedactedKeys(self.config.keys()))
            .finish()
    }
}

/// The config of the credential with the longest prefix that holds `location`. A prefix can
/// end with `/` when the location does not.
pub fn credential_for(
    credentials: Vec<StorageCredential>,
    location: &str,
) -> Option<HashMap<String, String>> {
    let location = location.trim_end_matches('/');
    credentials
        .into_iter()
        .filter(|credential| {
            let prefix = credential.prefix.trim_end_matches('/');
            location
                .strip_prefix(prefix)
                .is_some_and(|rest| rest.is_empty() || rest.starts_with('/'))
        })
        .max_by_key(|credential| credential.prefix.len())
        .map(|credential| credential.config)
}

impl Connection {
    /// The storage config of the credential with the longest prefix that holds `location`,
    /// from the `credentials` endpoint of the table.
    ///
    /// # Errors
    ///
    /// Fails if the request fails or no credential holds `location`.
    pub async fn credentials(
        &self,
        ident: &TableIdent,
        location: &str,
    ) -> beam::Result<HashMap<String, String>> {
        let url = self.table_url(ident, "credentials").await?;
        let response: CredentialsResponse = self
            .send("LoadCredentials", &url, || self.client.get(&url))
            .await?;
        credential_for(response.storage_credentials, location).ok_or_else(|| {
            beam::Error::msg(format!(
                "The catalog gave no storage credentials for '{location}'"
            ))
        })
    }

    /// Sends `report` to the `metrics` endpoint of the table, if the catalog has
    /// [`Catalog::with_metrics_reporting`]. Retries transient failures, and counts and logs a
    /// report that still fails.
    pub async fn report_metrics(&self, ident: &TableIdent, report: &serde_json::Value) -> bool {
        if !self.metrics_reporting {
            return true;
        }
        let waits = RetryWaits::default();
        let sent = self.send_metrics(ident, report, &waits).await;
        waits.record(METRICS);
        match sent {
            Ok(()) => true,
            Err(error) => {
                Metrics::counter(METRICS, "metrics_report_failures").inc();
                tracing::warn!(
                    "The metrics report of the Iceberg table {ident} was not sent: {error}"
                );
                false
            }
        }
    }

    /// `{uri}/v1/{prefix}/namespaces/{namespace}/tables/{table}/{suffix}`.
    async fn table_url(&self, ident: &TableIdent, suffix: &str) -> beam::Result<String> {
        let prefix = self.prefix().await?;
        let mut url = reqwest::Url::parse(&self.uri)?;
        let namespace = ident.namespace().as_ref().join("\x1f");
        {
            let mut segments = url.path_segments_mut().map_err(|()| {
                beam::Error::msg(format!(
                    "The catalog URI '{}' cannot hold path segments",
                    self.uri
                ))
            })?;
            segments.pop_if_empty();
            segments.push("v1");
            segments.extend(prefix.split('/').filter(|segment| !segment.is_empty()));
            segments.extend([
                "namespaces",
                namespace.as_str(),
                "tables",
                ident.name(),
                suffix,
            ]);
        }
        Ok(url.into())
    }

    /// The path prefix of the catalog, fetched once from its `config` endpoint.
    async fn prefix(&self) -> beam::Result<&str> {
        let prefix = self
            .prefix
            .get_or_try_init(|| async {
                let url = format!("{}/v1/config", self.uri);
                let query: Vec<(&str, &str)> = self
                    .warehouse
                    .iter()
                    .map(|warehouse| ("warehouse", warehouse.as_str()))
                    .collect();
                let ConfigResponse {
                    mut overrides,
                    mut defaults,
                } = self
                    .send("GetConfig", &url, || self.client.get(&url).query(&query))
                    .await?;
                Ok::<String, beam::Error>(
                    overrides
                        .remove("prefix")
                        .or_else(|| defaults.remove("prefix"))
                        .unwrap_or_default(),
                )
            })
            .await?;
        Ok(prefix.as_str())
    }

    /// `request` with the catalog headers and the bearer token.
    async fn authorized(
        &self,
        request: reqwest::RequestBuilder,
    ) -> beam::Result<reqwest::RequestBuilder> {
        let request = request.headers(self.headers.clone());
        Ok(match bearer(&self.auth).await? {
            Some(value) => request.header(AUTHORIZATION, value),
            None => request,
        })
    }

    async fn send_metrics(
        &self,
        ident: &TableIdent,
        report: &serde_json::Value,
        waits: &RetryWaits,
    ) -> beam::Result<()> {
        let url = self.table_url(ident, "metrics").await?;
        with_retry_waits(CATALOG_RETRY, waits, send_decision, || async {
            let request = self.client.post(&url).json(report);
            let response = self.send_once("ReportMetrics", &url, request).await?;
            let _ = response.bytes().await;
            Ok(())
        })
        .await
        .map_err(beam::Error::from)
    }

    async fn send_once(
        &self,
        method: &'static str,
        url: &str,
        request: reqwest::RequestBuilder,
    ) -> Result<reqwest::Response, SendError> {
        let started = Instant::now();
        let request = self.authorized(request).await.map_err(SendError::Other)?;
        let response = request.send().await;
        record_response(method, url, &response, started);
        let response = response.map_err(SendError::Transport)?;
        let status = response.status();
        if status.is_success() {
            Ok(response)
        } else {
            let retry_after = parse_retry_after(response.headers());
            let body = response.text().await.unwrap_or_default();
            Err(SendError::Http {
                status,
                retry_after,
                message: format!("The catalog request '{url}' failed with HTTP {status}: {body}"),
            })
        }
    }

    async fn send<T: serde::de::DeserializeOwned>(
        &self,
        method: &'static str,
        url: &str,
        build: impl Fn() -> reqwest::RequestBuilder,
    ) -> beam::Result<T> {
        with_throttled_retries(CATALOG_RETRY, METRICS, send_decision, || async {
            self.send_once(method, url, build())
                .await?
                .json()
                .await
                .map_err(|err| SendError::Other(err.into()))
        })
        .await
        .map_err(beam::Error::from)
    }

    /// The I/O request metric of a call to `method` of the catalog about `resource`.
    fn request(&self, method: &'static str, resource: impl Display) -> ApiRequest {
        Metrics::api_request(SERVICE, method, format!("{}/{resource}", self.uri))
    }

    /// `table` with the vended credentials of its location.
    async fn with_credentials(&self, table: Table) -> beam::Result<Table> {
        let config = self
            .credentials(table.identifier(), table.metadata().location())
            .await?;
        with_storage_config(&table, config)
    }
}

#[async_trait::async_trait]
impl iceberg::Catalog for Connection {
    async fn list_namespaces(
        &self,
        parent: Option<&NamespaceIdent>,
    ) -> iceberg::Result<Vec<NamespaceIdent>> {
        self.catalog.list_namespaces(parent).await
    }

    async fn create_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> iceberg::Result<Namespace> {
        self.catalog.create_namespace(namespace, properties).await
    }

    async fn get_namespace(&self, namespace: &NamespaceIdent) -> iceberg::Result<Namespace> {
        self.catalog.get_namespace(namespace).await
    }

    async fn namespace_exists(&self, namespace: &NamespaceIdent) -> iceberg::Result<bool> {
        self.catalog.namespace_exists(namespace).await
    }

    async fn update_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> iceberg::Result<()> {
        self.catalog.update_namespace(namespace, properties).await
    }

    async fn drop_namespace(&self, namespace: &NamespaceIdent) -> iceberg::Result<()> {
        self.catalog.drop_namespace(namespace).await
    }

    async fn list_tables(&self, namespace: &NamespaceIdent) -> iceberg::Result<Vec<TableIdent>> {
        self.catalog.list_tables(namespace).await
    }

    async fn create_table(
        &self,
        namespace: &NamespaceIdent,
        creation: TableCreation,
    ) -> iceberg::Result<Table> {
        let resource = format!("{}.{}", namespace.as_ref().join("."), creation.name);
        self.request("CreateTable", resource)
            .time(
                self.catalog.create_table(namespace, creation),
                catalog_status,
            )
            .await
    }

    /// Loads the table. With vended credentials, its files use the credentials of its location.
    async fn load_table(&self, ident: &TableIdent) -> iceberg::Result<Table> {
        let table = self
            .request("LoadTable", ident)
            .time(self.catalog.load_table(ident), catalog_status)
            .await?;
        if self.vended_credentials {
            self.with_credentials(table).await.map_err(|e| {
                iceberg::Error::new(
                    ErrorKind::Unexpected,
                    format!("Cannot get the storage credentials of the table {ident}: {e}"),
                )
            })
        } else {
            Ok(table)
        }
    }

    async fn drop_table(&self, ident: &TableIdent) -> iceberg::Result<()> {
        self.catalog.drop_table(ident).await
    }

    async fn purge_table(&self, ident: &TableIdent) -> iceberg::Result<()> {
        self.catalog.purge_table(ident).await
    }

    async fn table_exists(&self, ident: &TableIdent) -> iceberg::Result<bool> {
        self.request("TableExists", ident)
            .time(self.catalog.table_exists(ident), catalog_status)
            .await
    }

    async fn rename_table(&self, src: &TableIdent, dest: &TableIdent) -> iceberg::Result<()> {
        self.catalog.rename_table(src, dest).await
    }

    async fn register_table(
        &self,
        ident: &TableIdent,
        metadata_location: String,
    ) -> iceberg::Result<Table> {
        self.catalog.register_table(ident, metadata_location).await
    }

    async fn update_table(&self, commit: TableCommit) -> iceberg::Result<Table> {
        let request = self.request("CommitTable", commit.identifier());
        request
            .time(self.catalog.update_table(commit), catalog_status)
            .await
    }
}

/// Records an HTTP call to `method` of the catalog at `url` that started at `started`.
fn record_response(
    method: &'static str,
    url: &str,
    response: &reqwest::Result<reqwest::Response>,
    started: Instant,
) {
    let status = match response {
        Ok(response) => http_status(response.status().as_u16()),
        Err(error) if error.is_timeout() => "deadline_exceeded",
        Err(_) => "unavailable",
    };
    Metrics::api_request(SERVICE, method, url).record(status, started.elapsed());
}
