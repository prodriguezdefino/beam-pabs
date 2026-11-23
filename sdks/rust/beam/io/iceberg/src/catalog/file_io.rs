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

//! The file IO of a table, with the vended credentials of its location.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use iceberg::TableIdent;
use iceberg::io::{FileIO, FileIOBuilder};
use iceberg::table::Table;

use super::Catalog;
use super::rest::{connect, runtime};
use crate::storage::FileSystemStorageFactory;

/// The prop of a vended credential that holds its expiry, in milliseconds since the epoch.
pub const TOKEN_EXPIRES_AT: &str = "gcs.oauth2.token-expires-at";

/// A copy gets new vended credentials when its token expires in less than this.
const REFRESH_MARGIN: Duration = Duration::from_secs(5 * 60);

/// The file IO of one table for one DoFn copy. With vended credentials, it asks the catalog
/// for new ones before the token expires.
#[derive(Clone, Debug)]
pub struct TableFileIo {
    catalog: Catalog,
    ident: TableIdent,
    current: Option<(FileIO, Option<SystemTime>)>,
}

impl TableFileIo {
    pub fn new(catalog: Catalog, ident: TableIdent) -> Self {
        Self {
            catalog,
            ident,
            current: None,
        }
    }

    /// The file IO for the files under `location`, with fresh vended credentials when enabled.
    ///
    /// # Errors
    ///
    /// Fails if the catalog does not give credentials.
    pub fn get(&mut self, location: &str) -> beam::Result<FileIO> {
        let fresh = |refresh_after: &Option<SystemTime>| {
            refresh_after.is_none_or(|refresh_after| SystemTime::now() < refresh_after)
        };
        if let Some((file_io, _)) = self.current.as_ref().filter(|(_, r)| fresh(r)) {
            return Ok(file_io.clone());
        }
        let current = if self.catalog.vended_credentials {
            let (catalog, ident) = (&self.catalog, &self.ident);
            let config = beam::internals::block_on_io(async {
                connect(catalog).await?.credentials(ident, location).await
            })??;
            let now = SystemTime::now();
            let refresh_after = config
                .get(TOKEN_EXPIRES_AT)
                .and_then(|millis| millis.parse::<u64>().ok())
                .and_then(|millis| refresh_at(now, millis));
            (file_io(config), refresh_after)
        } else {
            (file_io([]), None)
        };
        let file_io = current.0.clone();
        self.current = Some(current);
        Ok(file_io)
    }
}

/// Computes when to refresh a token that expires at `millis` since the Unix epoch. Keeps at
/// least half of the remaining lifetime so short-lived tokens stay cached.
fn refresh_at(now: SystemTime, millis: u64) -> Option<SystemTime> {
    let expires_at = UNIX_EPOCH.checked_add(Duration::from_millis(millis))?;
    let remaining = expires_at.duration_since(now).unwrap_or_default();
    let margin = REFRESH_MARGIN.min(remaining / 2);
    Some(expires_at.checked_sub(margin).unwrap_or(UNIX_EPOCH))
}

/// A file IO on the SDK file systems with the storage `config`.
pub fn file_io(config: impl IntoIterator<Item = (String, String)>) -> FileIO {
    FileIOBuilder::new(Arc::new(FileSystemStorageFactory))
        .with_props(config)
        .build()
}

/// `table` with a file IO that has its props and `config`.
pub(super) fn with_storage_config(
    table: &Table,
    config: HashMap<String, String>,
) -> beam::Result<Table> {
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
