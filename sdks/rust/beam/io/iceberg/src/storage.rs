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

//! Iceberg storage on the file systems of the SDK.
//!
//! With the `gcp` feature, `gs://` paths use the async GCS client and do not block a thread.
//! Each vended token has one shared client. With no vended token, `gs://` paths use one client
//! with Application Default Credentials, not the file system that the pipeline registers for
//! `gs`. Other schemes use `beam_io_file::get_filesystem` on a blocking thread.

#[cfg(feature = "gcp")]
use std::collections::VecDeque;
use std::io::{self, Read, Write};
use std::ops::Range;
use std::sync::Arc;
#[cfg(feature = "gcp")]
use std::sync::{LazyLock, Mutex, PoisonError};

#[cfg(feature = "gcp")]
use beam::metrics::Metrics;
use beam_io_file::FileSystem;
#[cfg(feature = "gcp")]
use beam_io_gcp::gcs::GcsFileSystem;
use bytes::Bytes;
use futures::stream::{self, BoxStream};
use futures::{StreamExt, TryStreamExt};
use iceberg::io::{
    FileMetadata, FileRead, FileWrite, InputFile, OutputFile, Storage, StorageConfig,
    StorageFactory,
};
use iceberg::{Error, ErrorKind};
use serde::{Deserialize, Serialize};

/// The prop of a vended GCS access token.
pub const GCS_TOKEN: &str = "gcs.oauth2.token";

/// The count of vended tokens that keep a cached GCS client.
#[cfg(feature = "gcp")]
const MAX_CACHED_GCS_TOKENS: usize = 16;

/// The count of deletes that run at the same time.
const DELETE_CONCURRENCY: usize = 16;

/// The client of `gs://` paths with no vended token.
#[cfg(feature = "gcp")]
static DEFAULT_GCS: LazyLock<Arc<GcsFileSystem>> = LazyLock::new(|| Arc::new(GcsFileSystem::new()));

/// Vended tokens and their clients, from the least recently used.
#[cfg(feature = "gcp")]
type TokenGcsCache = VecDeque<(String, Arc<GcsFileSystem>)>;

#[cfg(feature = "gcp")]
static TOKEN_GCS_CACHE: LazyLock<Mutex<TokenGcsCache>> = LazyLock::new(Mutex::default);

/// Builds a [`FileSystemStorage`] for the props of a table.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize)]
pub struct FileSystemStorageFactory;

#[typetag::serde]
impl StorageFactory for FileSystemStorageFactory {
    fn build(&self, config: &StorageConfig) -> iceberg::Result<Arc<dyn Storage>> {
        Ok(Arc::new(FileSystemStorage::new(
            config.get(GCS_TOKEN).map(String::as_str),
        )?))
    }
}

/// Iceberg storage on the SDK file systems.
///
/// A serialized storage keeps no token: after a round trip, `gs://` paths use the default
/// GCS file system.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct FileSystemStorage {
    #[cfg(feature = "gcp")]
    #[serde(skip)]
    gcs: Option<Arc<GcsFileSystem>>,
}

impl FileSystemStorage {
    /// A storage that reads and writes `gs://` paths with `gcs_token`, if it is set.
    ///
    /// # Errors
    ///
    /// Fails if the token is not a valid HTTP header value.
    pub fn new(gcs_token: Option<&str>) -> iceberg::Result<Self> {
        #[cfg(feature = "gcp")]
        {
            Ok(Self {
                gcs: gcs_token.map(cached_gcs_with_token).transpose()?,
            })
        }
        #[cfg(not(feature = "gcp"))]
        {
            let _ = gcs_token;
            Ok(Self {})
        }
    }

    /// Whether `gs://` paths use a vended token.
    pub fn has_gcs_token(&self) -> bool {
        #[cfg(feature = "gcp")]
        {
            self.gcs.is_some()
        }
        #[cfg(not(feature = "gcp"))]
        {
            false
        }
    }

    /// Whether `self` and `other` share one client for a vended token.
    #[cfg(feature = "gcp")]
    #[doc(hidden)]
    pub fn shares_vended_gcs(&self, other: &Self) -> bool {
        matches!((&self.gcs, &other.gcs), (Some(a), Some(b)) if Arc::ptr_eq(a, b))
    }

    #[cfg(feature = "gcp")]
    fn gcs_for(&self, path: &str) -> Option<Arc<GcsFileSystem>> {
        path.starts_with("gs://").then(|| match &self.gcs {
            Some(gcs) => Arc::clone(gcs),
            None => Arc::clone(&DEFAULT_GCS),
        })
    }

    fn file_system(&self, path: &str) -> iceberg::Result<Arc<dyn FileSystem>> {
        beam_io_file::get_filesystem(path).map_err(|e| io_error(path, e))
    }
}

/// The shared client of `token`. Counts the cache hits and misses, so a catalog that vends a
/// new token for each call shows as misses.
#[cfg(feature = "gcp")]
fn cached_gcs_with_token(token: &str) -> iceberg::Result<Arc<GcsFileSystem>> {
    let (fs, name) = {
        let mut cache = TOKEN_GCS_CACHE
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let pos = cache.iter().position(|(cached, _)| cached == token);
        match pos.and_then(|pos| cache.remove(pos)) {
            Some(entry) => {
                let fs = Arc::clone(&entry.1);
                cache.push_back(entry);
                (fs, "gcs_client_cache_hits")
            }
            None => {
                let credentials = crate::credentials::BearerToken::new(token)?;
                let fs = Arc::new(GcsFileSystem::with_credentials(credentials.into()));
                if cache.len() >= MAX_CACHED_GCS_TOKENS {
                    cache.pop_front();
                }
                cache.push_back((token.to_string(), Arc::clone(&fs)));
                (fs, "gcs_client_cache_misses")
            }
        }
    };
    Metrics::counter(crate::METRICS, name).inc();
    Ok(fs)
}

#[cfg(feature = "gcp")]
async fn remove_gcs(gcs: &GcsFileSystem, file: String) -> iceberg::Result<()> {
    gcs.remove_async(&file)
        .await
        .map_err(|e| io_error(&file, e))
}

fn io_error(path: &str, error: io::Error) -> Error {
    let kind = match error.kind() {
        io::ErrorKind::NotFound => ErrorKind::DataInvalid,
        _ => ErrorKind::Unexpected,
    };
    Error::new(kind, format!("File system call on '{path}' failed")).with_source(error)
}

/// Runs a synchronous file system call on a blocking thread.
async fn blocking<T: Send + 'static>(
    path: &str,
    call: impl FnOnce() -> io::Result<T> + Send + 'static,
) -> iceberg::Result<T> {
    tokio::task::spawn_blocking(call)
        .await
        .map_err(|e| {
            Error::new(
                ErrorKind::Unexpected,
                format!("The file system task of '{path}' failed"),
            )
            .with_source(e)
        })?
        .map_err(|e| io_error(path, e))
}

fn read_all(mut reader: impl Read) -> io::Result<Bytes> {
    let mut buffer = Vec::new();
    reader.read_to_end(&mut buffer)?;
    Ok(Bytes::from(buffer))
}

#[async_trait::async_trait]
#[typetag::serde]
impl Storage for FileSystemStorage {
    async fn exists(&self, path: &str) -> iceberg::Result<bool> {
        #[cfg(feature = "gcp")]
        if let Some(gcs) = self.gcs_for(path) {
            return gcs.exists_async(path).await.map_err(|e| io_error(path, e));
        }
        let (fs, owned) = (self.file_system(path)?, path.to_string());
        blocking(path, move || fs.exists(&owned)).await
    }

    async fn metadata(&self, path: &str) -> iceberg::Result<FileMetadata> {
        #[cfg(feature = "gcp")]
        if let Some(gcs) = self.gcs_for(path) {
            let size = gcs.size_async(path).await.map_err(|e| io_error(path, e))?;
            return Ok(FileMetadata { size });
        }
        let (fs, owned) = (self.file_system(path)?, path.to_string());
        let size = blocking(path, move || fs.size(&owned)).await?;
        Ok(FileMetadata { size })
    }

    async fn read(&self, path: &str) -> iceberg::Result<Bytes> {
        #[cfg(feature = "gcp")]
        if let Some(gcs) = self.gcs_for(path) {
            return gcs.read(path).await.map_err(|e| io_error(path, e));
        }
        let (fs, owned) = (self.file_system(path)?, path.to_string());
        blocking(path, move || read_all(fs.open_read(&owned)?)).await
    }

    async fn reader(&self, path: &str) -> iceberg::Result<Box<dyn FileRead>> {
        #[cfg(feature = "gcp")]
        if let Some(gcs) = self.gcs_for(path) {
            return Ok(Box::new(GcsFileRead {
                gcs,
                path: path.to_string(),
            }));
        }
        Ok(Box::new(FileSystemRead {
            fs: self.file_system(path)?,
            path: path.to_string(),
        }))
    }

    async fn write(&self, path: &str, bs: Bytes) -> iceberg::Result<()> {
        #[cfg(feature = "gcp")]
        if let Some(gcs) = self.gcs_for(path) {
            return gcs.write(path, bs).await.map_err(|e| io_error(path, e));
        }
        let (fs, owned) = (self.file_system(path)?, path.to_string());
        blocking(path, move || {
            let mut writer = fs.open_write(&owned)?;
            writer.write_all(&bs)?;
            writer.flush()
        })
        .await
    }

    async fn writer(&self, path: &str) -> iceberg::Result<Box<dyn FileWrite>> {
        #[cfg(feature = "gcp")]
        if let Some(gcs) = self.gcs_for(path) {
            let writer = gcs.writer(path).await.map_err(|e| io_error(path, e))?;
            return Ok(Box::new(GcsFileWrite {
                writer,
                path: path.to_string(),
            }));
        }
        let (fs, owned) = (self.file_system(path)?, path.to_string());
        let writer = blocking(path, move || fs.open_write(&owned)).await?;
        Ok(Box::new(FileSystemWrite {
            writer: Some(writer),
            path: path.to_string(),
            written: 0,
        }))
    }

    async fn delete(&self, path: &str) -> iceberg::Result<()> {
        #[cfg(feature = "gcp")]
        if let Some(gcs) = self.gcs_for(path) {
            return gcs.remove_async(path).await.map_err(|e| io_error(path, e));
        }
        let (fs, owned) = (self.file_system(path)?, path.to_string());
        blocking(path, move || fs.remove(&owned)).await
    }

    async fn delete_prefix(&self, path: &str) -> iceberg::Result<()> {
        let pattern = if path.ends_with('/') {
            format!("{path}**")
        } else {
            format!("{path}/**")
        };
        #[cfg(feature = "gcp")]
        if let Some(gcs) = self.gcs_for(path) {
            let files = gcs
                .match_files_async(&pattern)
                .await
                .map_err(|e| io_error(path, e))?;
            return stream::iter(files)
                .map(|file| remove_gcs(&gcs, file))
                .buffer_unordered(DELETE_CONCURRENCY)
                .try_collect()
                .await;
        }
        let fs = self.file_system(path)?;
        blocking(path, move || {
            fs.match_files(&pattern)?
                .iter()
                .try_for_each(|file| fs.remove(file))
        })
        .await
    }

    async fn delete_stream(&self, paths: BoxStream<'static, String>) -> iceberg::Result<()> {
        paths
            .map(|path| async move { self.delete(&path).await })
            .buffer_unordered(DELETE_CONCURRENCY)
            .try_collect()
            .await
    }

    fn new_input(&self, path: &str) -> iceberg::Result<InputFile> {
        Ok(InputFile::new(Arc::new(self.clone()), path.to_string()))
    }

    fn new_output(&self, path: &str) -> iceberg::Result<OutputFile> {
        Ok(OutputFile::new(Arc::new(self.clone()), path.to_string()))
    }
}

#[cfg(feature = "gcp")]
struct GcsFileRead {
    gcs: Arc<GcsFileSystem>,
    path: String,
}

#[cfg(feature = "gcp")]
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

#[cfg(feature = "gcp")]
struct GcsFileWrite {
    writer: beam_io_gcp::gcs::writer::GcsAsyncWriter,
    path: String,
}

#[cfg(feature = "gcp")]
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
        Ok(FileMetadata { size })
    }
}

/// Reads byte ranges of one non-GCS file.
struct FileSystemRead {
    fs: Arc<dyn FileSystem>,
    path: String,
}

#[async_trait::async_trait]
impl FileRead for FileSystemRead {
    async fn read(&self, range: Range<u64>) -> iceberg::Result<Bytes> {
        let (fs, path) = (Arc::clone(&self.fs), self.path.clone());
        let length = range.end.saturating_sub(range.start);
        match length {
            0 => Ok(Bytes::new()),
            _ => {
                blocking(&self.path, move || {
                    read_all(fs.open_read_range(&path, range.start, length)?)
                })
                .await
            }
        }
    }
}

/// Writes one non-GCS file. The file system writer moves to a blocking thread for each call and
/// back, so one writer is never used by two threads at a time.
struct FileSystemWrite {
    writer: Option<Box<dyn Write + Send>>,
    path: String,
    written: u64,
}

impl FileSystemWrite {
    fn take(&mut self) -> iceberg::Result<Box<dyn Write + Send>> {
        self.writer.take().ok_or_else(|| {
            Error::new(
                ErrorKind::Unexpected,
                format!("The file '{}' is closed or failed", self.path),
            )
        })
    }
}

#[async_trait::async_trait]
impl FileWrite for FileSystemWrite {
    async fn write(&mut self, bs: Bytes) -> iceberg::Result<()> {
        let mut writer = self.take()?;
        let length = bs.len() as u64;
        let writer = blocking(&self.path, move || {
            writer.write_all(&bs)?;
            Ok(writer)
        })
        .await?;
        self.writer = Some(writer);
        self.written += length;
        Ok(())
    }

    async fn close(&mut self) -> iceberg::Result<FileMetadata> {
        let mut writer = self.take()?;
        blocking(&self.path, move || writer.flush()).await?;
        Ok(FileMetadata { size: self.written })
    }
}
