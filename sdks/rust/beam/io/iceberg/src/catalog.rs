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

//! The REST catalog client of a write, and the vended storage credentials of its tables.
//!
//! The `iceberg` REST client reads storage credentials only from the `config` of a table
//! response. A catalog can send them in `storage-credentials` instead, so the connection
//! reads them from the `credentials` endpoint of the table.

use std::collections::HashMap;
use std::sync::{Arc, LazyLock, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use iceberg::io::{FileIO, FileIOBuilder};
use iceberg::table::Table;
use iceberg::{
    CatalogBuilder, ErrorKind, Namespace, NamespaceIdent, TableCommit, TableCreation, TableIdent,
};
use iceberg_catalog_rest::{RestCatalog, RestCatalogBuilder};
use reqwest::header::{AUTHORIZATION, HeaderMap, HeaderName, HeaderValue};
use serde::Deserialize;

use crate::options::{Auth, Catalog};
use crate::storage::FileSystemStorageFactory;

/// The header that asks a REST catalog for vended storage credentials.
pub const ACCESS_DELEGATION: &str = "X-Iceberg-Access-Delegation";
const VENDED_CREDENTIALS: &str = "vended-credentials";

/// The props of the `iceberg` REST client for `catalog`, with the bearer `token`.
pub fn rest_props(catalog: &Catalog, token: Option<&str>) -> HashMap<String, String> {
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
        .chain(token.map(|t| ("token".to_string(), t.to_string())))
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
        Auth::Google => Ok(Some(gcp::auth::access_token().await?)),
    }
}

fn runtime() -> beam::Result<iceberg::Runtime> {
    Ok(iceberg::Runtime::new(beam::internals::io_runtime()?))
}

/// How long a connection with Google auth serves before the next [`connect`] makes a new one.
/// An access token of the Application Default Credentials lives about one hour.
const GOOGLE_CONNECTION_LIFETIME: Duration = Duration::from_secs(50 * 60);

/// The connections of this process, one for each catalog config. The lock is held only to
/// read or update the map, never across a catalog call.
type Connections = HashMap<Catalog, (Arc<Connection>, Instant)>;

static CONNECTIONS: LazyLock<Mutex<Connections>> = LazyLock::new(|| Mutex::new(HashMap::new()));

/// A REST catalog client, and the HTTP client and headers for the requests that the
/// `iceberg` client does not make.
///
/// It is an [`iceberg::Catalog`]. Its `load_table` adds the vended credentials of the table,
/// so a transaction that reloads the table still writes its files with them.
#[derive(Debug)]
pub struct Connection {
    catalog: RestCatalog,
    client: reqwest::Client,
    uri: String,
    warehouse: Option<String>,
    headers: HeaderMap,
    vended_credentials: bool,
    metrics_reporting: bool,
}

/// The connection to `catalog`. Every catalog call of the write goes through this function.
/// A connection serves all the steps of the process. With Google auth, a connection older
/// than 50 minutes is replaced, so its token does not expire.
///
/// # Errors
///
/// Fails if the token cannot be taken, a header is not valid, or the client cannot start.
pub async fn connect(catalog: &Catalog) -> beam::Result<Arc<Connection>> {
    let cached = connections()
        .get(catalog)
        .filter(|(_, opened)| fresh(&catalog.auth, *opened))
        .map(|(connection, _)| Arc::clone(connection));
    if let Some(connection) = cached {
        return Ok(connection);
    }
    let connection = Arc::new(open(catalog).await?);
    connections().insert(catalog.clone(), (Arc::clone(&connection), Instant::now()));
    Ok(connection)
}

fn connections() -> MutexGuard<'static, Connections> {
    CONNECTIONS.lock().unwrap_or_else(PoisonError::into_inner)
}

fn fresh(auth: &Auth, opened: Instant) -> bool {
    match auth {
        Auth::None | Auth::Token(_) => true,
        #[cfg(feature = "gcp")]
        Auth::Google => opened.elapsed() < GOOGLE_CONNECTION_LIFETIME,
    }
}

async fn open(catalog: &Catalog) -> beam::Result<Connection> {
    let token = token(&catalog.auth).await?;
    let client = reqwest::Client::builder().build()?;
    let rest = RestCatalogBuilder::default()
        .with_client(client.clone())
        .with_storage_factory(Arc::new(FileSystemStorageFactory))
        .with_runtime(runtime()?)
        .load("beam", rest_props(catalog, token.as_deref()))
        .await?;
    let headers = request_headers(catalog)
        .into_iter()
        .map(|(name, value)| {
            Ok((
                HeaderName::try_from(name.as_str())?,
                HeaderValue::try_from(value.as_str())?,
            ))
        })
        .chain(token.map(|token| {
            let mut value = HeaderValue::try_from(format!("Bearer {token}"))?;
            value.set_sensitive(true);
            Ok((AUTHORIZATION, value))
        }))
        .collect::<beam::Result<HeaderMap>>()?;
    Ok(Connection {
        catalog: rest,
        client,
        uri: catalog.uri.trim_end_matches('/').to_string(),
        warehouse: catalog.warehouse.clone(),
        headers,
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
    pub async fn report_metrics(&self, ident: &TableIdent, report: &serde_json::Value) {
        match self {
            Self::Rest(catalog) => match connect(catalog).await {
                Ok(connection) => connection.report_metrics(ident, report).await,
                Err(e) => tracing::warn!(
                    "The metrics report of the Iceberg table {ident} was not sent: {e}"
                ),
            },
            Self::Given(_) => {}
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
#[derive(Debug, Deserialize)]
pub struct StorageCredential {
    pub prefix: String,
    pub config: HashMap<String, String>,
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
        let response: CredentialsResponse = self.send(self.client.get(&url), &url).await?;
        credential_for(response.storage_credentials, location).ok_or_else(|| {
            beam::Error::msg(format!(
                "The catalog gave no storage credentials for '{location}'"
            ))
        })
    }

    /// Sends `report` to the `metrics` endpoint of the table, if the catalog has
    /// [`Catalog::with_metrics_reporting`]. A failed send is logged, not returned.
    pub async fn report_metrics(&self, ident: &TableIdent, report: &serde_json::Value) {
        if !self.metrics_reporting {
            return;
        }
        let sent = async {
            let url = self.table_url(ident, "metrics").await?;
            let response = self
                .client
                .post(&url)
                .headers(self.headers.clone())
                .json(report)
                .send()
                .await?;
            match response.status().is_success() {
                true => Ok(()),
                false => Err(beam::Error::msg(format!(
                    "HTTP {}: {}",
                    response.status(),
                    response.text().await.unwrap_or_default()
                ))),
            }
        };
        if let Err(e) = sent.await {
            tracing::warn!("The metrics report of the Iceberg table {ident} was not sent: {e}");
        }
    }

    /// `{uri}/v1/{prefix}/namespaces/{namespace}/tables/{table}/{suffix}`.
    async fn table_url(&self, ident: &TableIdent, suffix: &str) -> beam::Result<String> {
        let prefix = self.prefix().await?;
        Ok([
            self.uri.as_str(),
            "v1",
            prefix.as_str(),
            "namespaces",
            &ident.namespace().to_url_string(),
            "tables",
            ident.name(),
            suffix,
        ]
        .into_iter()
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>()
        .join("/"))
    }

    /// The path prefix of the catalog, from its `config` endpoint.
    async fn prefix(&self) -> beam::Result<String> {
        let url = format!("{}/v1/config", self.uri);
        let query: Vec<(&str, &str)> = self
            .warehouse
            .iter()
            .map(|warehouse| ("warehouse", warehouse.as_str()))
            .collect();
        let ConfigResponse {
            mut overrides,
            mut defaults,
        } = self.send(self.client.get(&url).query(&query), &url).await?;
        Ok(overrides
            .remove("prefix")
            .or_else(|| defaults.remove("prefix"))
            .unwrap_or_default())
    }

    async fn send<T: serde::de::DeserializeOwned>(
        &self,
        request: reqwest::RequestBuilder,
        url: &str,
    ) -> beam::Result<T> {
        let response = request.headers(self.headers.clone()).send().await?;
        let status = response.status();
        match status.is_success() {
            true => Ok(response.json().await?),
            false => Err(beam::Error::msg(format!(
                "The catalog request '{url}' failed with HTTP {status}: {}",
                response.text().await.unwrap_or_default()
            ))),
        }
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
        self.catalog.create_table(namespace, creation).await
    }

    /// Loads the table. With vended credentials, its files use the credentials of its location.
    async fn load_table(&self, ident: &TableIdent) -> iceberg::Result<Table> {
        let table = self.catalog.load_table(ident).await?;
        match self.vended_credentials {
            false => Ok(table),
            true => self.with_credentials(table).await.map_err(|e| {
                iceberg::Error::new(
                    ErrorKind::Unexpected,
                    format!("Cannot get the storage credentials of the table {ident}: {e}"),
                )
            }),
        }
    }

    async fn drop_table(&self, ident: &TableIdent) -> iceberg::Result<()> {
        self.catalog.drop_table(ident).await
    }

    async fn purge_table(&self, ident: &TableIdent) -> iceberg::Result<()> {
        self.catalog.purge_table(ident).await
    }

    async fn table_exists(&self, ident: &TableIdent) -> iceberg::Result<bool> {
        self.catalog.table_exists(ident).await
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
        self.catalog.update_table(commit).await
    }
}

/// The prop of a vended credential that holds its expiry, in milliseconds since the epoch.
pub const TOKEN_EXPIRES_AT: &str = "gcs.oauth2.token-expires-at";

/// A file IO on the SDK file systems with the storage `config`.
pub fn file_io(config: impl IntoIterator<Item = (String, String)>) -> FileIO {
    FileIOBuilder::new(Arc::new(FileSystemStorageFactory))
        .with_props(config)
        .build()
}

/// `table` with a file IO that has its props and `config`.
fn with_storage_config(table: &Table, config: HashMap<String, String>) -> beam::Result<Table> {
    let props = table
        .file_io()
        .config()
        .props()
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .chain(config);
    let file_io = file_io(props);
    let builder = Table::builder()
        .identifier(table.identifier().clone())
        .file_io(file_io)
        .metadata(table.metadata_ref())
        .runtime(runtime()?);
    let builder = match table.metadata_location() {
        Some(location) => builder.metadata_location(location),
        None => builder,
    };
    Ok(builder.build()?)
}
