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

//! The REST catalog of a read or a write: its config, its client and the file IO of its
//! tables.

mod file_io;
mod rest;

use std::collections::BTreeMap;
use std::fmt;

pub use file_io::{TOKEN_EXPIRES_AT, TableFileIo, file_io};
pub use rest::{
    ACCESS_DELEGATION, CatalogClient, Connection, SERVICE, StorageCredential, catalog_decision,
    catalog_status, connect, credential_for, parse_retry_after, rest_props,
};

/// An Iceberg REST catalog and how to reach it.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct Catalog {
    pub(crate) uri: String,
    pub(crate) warehouse: Option<String>,
    pub(crate) headers: BTreeMap<String, String>,
    pub(crate) properties: BTreeMap<String, String>,
    pub(crate) auth: Auth,
    pub(crate) vended_credentials: bool,
    pub(crate) metrics_reporting: bool,
}

/// Formats the keys of a map with every value redacted as `".."`.
pub(crate) struct RedactedKeys<I>(pub(crate) I);

impl<I, K> fmt::Debug for RedactedKeys<I>
where
    I: Iterator<Item = K> + Clone,
    K: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_map()
            .entries(self.0.clone().map(|key| (key, "..")))
            .finish()
    }
}

/// Hides header and property values, which can hold credentials.
impl fmt::Debug for Catalog {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Catalog")
            .field("uri", &self.uri)
            .field("warehouse", &self.warehouse)
            .field("headers", &RedactedKeys(self.headers.keys()))
            .field("properties", &RedactedKeys(self.properties.keys()))
            .field("auth", &self.auth)
            .field("vended_credentials", &self.vended_credentials)
            .field("metrics_reporting", &self.metrics_reporting)
            .finish()
    }
}

/// How the catalog client authenticates.
#[derive(Clone, PartialEq, Eq, Hash)]
pub(crate) enum Auth {
    None,
    Token(String),
    /// An access token of the Application Default Credentials, taken for each catalog request.
    #[cfg(feature = "gcp")]
    Google,
}

/// Hides the token, so that logs and errors do not show it.
impl fmt::Debug for Auth {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::None => f.write_str("None"),
            Self::Token(_) => f.write_str("Token(..)"),
            #[cfg(feature = "gcp")]
            Self::Google => f.write_str("Google"),
        }
    }
}

impl Catalog {
    /// A REST catalog at `uri`, for example `https://biglake.googleapis.com/iceberg/v1/restcatalog`.
    pub fn rest(uri: impl Into<String>) -> Self {
        Self {
            uri: uri.into(),
            warehouse: None,
            headers: BTreeMap::new(),
            properties: BTreeMap::new(),
            auth: Auth::None,
            vended_credentials: false,
            metrics_reporting: false,
        }
    }

    /// The warehouse that the catalog serves, for example `bl://projects/<p>/catalogs/<c>`.
    pub fn with_warehouse(mut self, warehouse: impl Into<String>) -> Self {
        self.warehouse = Some(warehouse.into());
        self
    }

    /// Sends the HTTP header `name: value` with each catalog request.
    pub fn with_header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.insert(name.into(), value.into());
        self
    }

    /// Sends the catalog property `key = value` to the catalog client.
    pub fn with_property(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.properties.insert(key.into(), value.into());
        self
    }

    /// Authenticates with a static bearer token.
    pub fn with_token(mut self, token: impl Into<String>) -> Self {
        self.auth = Auth::Token(token.into());
        self
    }

    /// Authenticates with an access token of the Google Application Default Credentials, as
    /// the BigLake catalog needs.
    #[cfg(feature = "gcp")]
    pub fn with_google_auth(mut self) -> Self {
        self.auth = Auth::Google;
        self
    }

    /// Asks the catalog for short-lived storage credentials of each table, and writes the
    /// table files with them. A catalog in vended-credentials mode rejects requests without
    /// this option.
    pub fn with_vended_credentials(mut self) -> Self {
        self.vended_credentials = true;
        self
    }

    /// Sends a commit report to the `metrics` endpoint of the table after each commit. A
    /// report that fails is logged and does not fail the commit.
    pub fn with_metrics_reporting(mut self) -> Self {
        self.metrics_reporting = true;
        self
    }
}
