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

//! `gs://` storage on the async GCS client, with a shared client for each vended token.

use std::fmt;
use std::ops::Range;
use std::sync::{Arc, LazyLock};

use beam::metrics::Metrics;
use beam_io_gcp::gcs::GcsFileSystem;
use bytes::Bytes;
use google_cloud_auth::credentials::{CacheableResource, CredentialsProvider, EntityTag};
use google_cloud_auth::errors::CredentialsError;
use http::header::{AUTHORIZATION, HeaderValue};
use http::{Extensions, HeaderMap};
use iceberg::io::{FileMetadata, FileRead, FileWrite};
use iceberg::{Error, ErrorKind};
use moka::sync::Cache;

use super::{io_error, tail_cache};

/// The count of vended tokens that keep a cached GCS client.
const MAX_CACHED_GCS_TOKENS: u64 = 16;

/// The client of `gs://` paths with no vended token.
pub(super) static DEFAULT_GCS: LazyLock<Arc<GcsFileSystem>> =
    LazyLock::new(|| Arc::new(GcsFileSystem::new()));

static TOKEN_GCS_CACHE: LazyLock<Cache<String, Arc<GcsFileSystem>, foldhash::fast::RandomState>> =
    LazyLock::new(|| {
        Cache::builder()
            .max_capacity(MAX_CACHED_GCS_TOKENS)
            .build_with_hasher(foldhash::fast::RandomState::default())
    });

/// The shared client of `token`. Counts the cache hits and misses, so a catalog that vends a
/// new token for each call shows as misses.
pub(super) fn cached_gcs_with_token(token: &str) -> iceberg::Result<Arc<GcsFileSystem>> {
    if let Some(fs) = TOKEN_GCS_CACHE.get(token) {
        Metrics::counter(crate::METRICS, "gcs_client_cache_hits").inc();
        return Ok(fs);
    }
    let entry = TOKEN_GCS_CACHE
        .entry_by_ref(token)
        .or_try_insert_with(|| {
            let credentials = BearerToken::new(token)?;
            Ok::<_, Error>(Arc::new(GcsFileSystem::with_credentials(
                credentials.into(),
            )))
        })
        .map_err(|err| match Arc::try_unwrap(err) {
            Ok(inner) => inner,
            Err(shared) => Error::new(shared.kind(), shared.message()),
        })?;
    let name = if entry.is_fresh() {
        "gcs_client_cache_misses"
    } else {
        "gcs_client_cache_hits"
    };
    Metrics::counter(crate::METRICS, name).inc();
    Ok(entry.into_value())
}

pub(super) async fn remove_gcs(gcs: &GcsFileSystem, file: String) -> iceberg::Result<()> {
    gcs.remove_async(&file)
        .await
        .map_err(|e| io_error(&file, e))
}

pub(super) struct GcsFileRead {
    pub(super) gcs: Arc<GcsFileSystem>,
    pub(super) path: String,
}

#[async_trait::async_trait]
impl FileRead for GcsFileRead {
    async fn read(&self, range: Range<u64>) -> iceberg::Result<Bytes> {
        let length = range.end.saturating_sub(range.start);
        match length {
            0 => Ok(Bytes::new()),
            _ => self
                .gcs
                .read_range(&self.path, range.start, length)
                .await
                .map_err(|e| io_error(&self.path, e)),
        }
    }
}

pub(super) struct GcsFileWrite {
    pub(super) writer: beam_io_gcp::gcs::writer::GcsAsyncWriter,
    pub(super) path: String,
}

#[async_trait::async_trait]
impl FileWrite for GcsFileWrite {
    async fn write(&mut self, bs: Bytes) -> iceberg::Result<()> {
        self.writer
            .write(&bs)
            .await
            .map_err(|e| io_error(&self.path, e))
    }

    async fn close(&mut self) -> iceberg::Result<FileMetadata> {
        let size = self
            .writer
            .close()
            .await
            .map_err(|e| io_error(&self.path, e))?;
        tail_cache::invalidate(&self.path);
        Ok(FileMetadata { size })
    }
}

/// Sends a fixed access token as `Authorization: Bearer <token>`.
struct BearerToken {
    headers: HeaderMap,
    entity_tag: EntityTag,
}

impl BearerToken {
    /// # Errors
    ///
    /// Fails if the token is not a valid HTTP header value.
    fn new(token: &str) -> iceberg::Result<Self> {
        let mut value = HeaderValue::from_str(&format!("Bearer {token}")).map_err(|e| {
            Error::new(ErrorKind::DataInvalid, "The vended GCS token is not valid").with_source(e)
        })?;
        value.set_sensitive(true);
        Ok(Self {
            headers: HeaderMap::from_iter([(AUTHORIZATION, value)]),
            entity_tag: EntityTag::new(),
        })
    }
}

/// Hides the token, so that logs and errors do not show it.
impl fmt::Debug for BearerToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("BearerToken(..)")
    }
}

impl CredentialsProvider for BearerToken {
    async fn headers(
        &self,
        extensions: Extensions,
    ) -> Result<CacheableResource<HeaderMap>, CredentialsError> {
        match extensions.get::<EntityTag>() {
            Some(tag) if *tag == self.entity_tag => Ok(CacheableResource::NotModified),
            _ => Ok(CacheableResource::New {
                entity_tag: self.entity_tag.clone(),
                data: self.headers.clone(),
            }),
        }
    }

    async fn universe_domain(&self) -> Option<String> {
        None
    }
}
