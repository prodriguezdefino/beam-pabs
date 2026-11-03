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
//! A path goes to the file system that `beam_io_file::get_filesystem` registers for its scheme. When
//! the table props hold a vended GCS token, `gs://` paths use a GCS file system with that
//! token. The file systems are synchronous, so each call runs in `spawn_blocking`.

use std::io::{self, Read, Write};
use std::ops::Range;
use std::sync::Arc;

use bytes::Bytes;
use beam_io_file::FileSystem;
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use iceberg::io::{
    FileMetadata, FileRead, FileWrite, InputFile, OutputFile, Storage, StorageConfig,
    StorageFactory,
};
use iceberg::{Error, ErrorKind};
use serde::{Deserialize, Serialize};

/// The prop of a vended GCS access token.
pub const GCS_TOKEN: &str = "gcs.oauth2.token";

/// The count of deletes that run at the same time.
const DELETE_CONCURRENCY: usize = 16;

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
    #[serde(skip)]
    gcs: Option<Arc<dyn FileSystem>>,
}

impl FileSystemStorage {
    /// A storage that reads and writes `gs://` paths with `gcs_token`, if it is set.
    ///
    /// # Errors
    ///
    /// Fails if the token is not a valid HTTP header value.
    pub fn new(gcs_token: Option<&str>) -> iceberg::Result<Self> {
        Ok(Self {
            gcs: gcs_token.map(gcs_with_token).transpose()?.flatten(),
        })
    }

    /// Whether `gs://` paths use a vended token.
    pub fn has_gcs_token(&self) -> bool {
        self.gcs.is_some()
    }

    fn file_system(&self, path: &str) -> iceberg::Result<Arc<dyn FileSystem>> {
        match &self.gcs {
            Some(gcs) if path.starts_with("gs://") => Ok(Arc::clone(gcs)),
            _ => beam_io_file::get_filesystem(path).map_err(|e| io_error(path, e)),
        }
    }
}

#[cfg(feature = "gcp")]
fn gcs_with_token(token: &str) -> iceberg::Result<Option<Arc<dyn FileSystem>>> {
    let credentials = crate::credentials::BearerToken::new(token)?;
    Ok(Some(Arc::new(beam_io_gcp::gcs::GcsFileSystem::with_credentials(
        credentials.into(),
    ))))
}

/// Without the `gcp` feature, `gs://` paths use the registered file system, if any.
#[cfg(not(feature = "gcp"))]
fn gcs_with_token(_token: &str) -> iceberg::Result<Option<Arc<dyn FileSystem>>> {
    Ok(None)
}

fn io_error(path: &str, error: io::Error) -> Error {
    let kind = match error.kind() {
        io::ErrorKind::NotFound => ErrorKind::DataInvalid,
        _ => ErrorKind::Unexpected,
    };
    Error::new(kind, format!("File system call on '{path}' failed")).with_source(error)
}

/// Runs a file system call on a blocking thread, so that it does not block the runtime.
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
        let (fs, owned) = (self.file_system(path)?, path.to_string());
        blocking(path, move || fs.exists(&owned)).await
    }

    async fn metadata(&self, path: &str) -> iceberg::Result<FileMetadata> {
        let (fs, owned) = (self.file_system(path)?, path.to_string());
        let size = blocking(path, move || fs.size(&owned)).await?;
        Ok(FileMetadata { size })
    }

    async fn read(&self, path: &str) -> iceberg::Result<Bytes> {
        let (fs, owned) = (self.file_system(path)?, path.to_string());
        blocking(path, move || read_all(fs.open_read(&owned)?)).await
    }

    async fn reader(&self, path: &str) -> iceberg::Result<Box<dyn FileRead>> {
        Ok(Box::new(FileSystemRead {
            fs: self.file_system(path)?,
            path: path.to_string(),
        }))
    }

    async fn write(&self, path: &str, bs: Bytes) -> iceberg::Result<()> {
        let (fs, owned) = (self.file_system(path)?, path.to_string());
        blocking(path, move || {
            let mut writer = fs.open_write(&owned)?;
            writer.write_all(&bs)?;
            writer.flush()
        })
        .await
    }

    async fn writer(&self, path: &str) -> iceberg::Result<Box<dyn FileWrite>> {
        let (fs, owned) = (self.file_system(path)?, path.to_string());
        let writer = blocking(path, move || fs.open_write(&owned)).await?;
        Ok(Box::new(FileSystemWrite {
            writer: Some(writer),
            path: path.to_string(),
            written: 0,
        }))
    }

    async fn delete(&self, path: &str) -> iceberg::Result<()> {
        let (fs, owned) = (self.file_system(path)?, path.to_string());
        blocking(path, move || fs.remove(&owned)).await
    }

    async fn delete_prefix(&self, path: &str) -> iceberg::Result<()> {
        let fs = self.file_system(path)?;
        let pattern = match path.ends_with('/') {
            true => format!("{path}**"),
            false => format!("{path}/**"),
        };
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

/// Reads byte ranges of one file.
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

/// Writes one file. The file system writer moves to a blocking thread for each call and
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

    /// Flushes the file and returns the bytes written. A GCS writer uploads the object on
    /// flush.
    async fn close(&mut self) -> iceberg::Result<FileMetadata> {
        let mut writer = self.take()?;
        blocking(&self.path, move || writer.flush()).await?;
        Ok(FileMetadata { size: self.written })
    }
}
