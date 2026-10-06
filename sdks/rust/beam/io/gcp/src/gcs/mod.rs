/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

//! Google Cloud Storage file system for `gs://<bucket>/<object>` paths, used by I/O such as
//! `textio`.
//!
//! Uses the `google-cloud-storage` crate with Application Default Credentials. To test
//! against an emulator, set `STORAGE_EMULATOR_HOST`, such as `http://localhost:9023`.

use std::io::{self, Read, Write};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, SystemTime};

use beam_io_file::{FileSystem, GlobMatcher, find_wildcard};
use bytes::{Bytes, BytesMut};
use futures::{Stream, TryStreamExt, stream};
use google_cloud_gax::paginator::ItemPaginator;
use tokio::sync::{OnceCell, mpsc};

pub mod reader;
pub mod writer;

use crate::runtime::{block_on_async, client_runtime};
use reader::GcsStreamReader;
use writer::{GcsAsyncWriter, GcsWriter, upload_bytes};

/// Splits `gs://bucket/object` into `(bucket, object)`. Returns
/// [`io::ErrorKind::InvalidInput`] if the scheme is not `gs://` or the bucket is empty.
pub fn parse_gcs_uri(uri: &str) -> io::Result<(&str, &str)> {
    let raw = uri.strip_prefix("gs://").ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("Invalid GCS URI: expected 'gs://' scheme, got '{uri}'"),
        )
    })?;

    let (bucket, object) = raw.split_once('/').unwrap_or((raw, ""));

    if bucket.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("Invalid GCS URI: bucket name cannot be empty in '{uri}'"),
        ));
    }

    Ok((bucket, object))
}

/// Maps a typed GCS error to an [`io::Error`].
#[doc(hidden)]
pub fn map_gcs_error(err: google_cloud_gax::error::Error) -> io::Error {
    let kind = if let Some(status) = err.status() {
        match status.code {
            google_cloud_gax::error::rpc::Code::NotFound => io::ErrorKind::NotFound,
            google_cloud_gax::error::rpc::Code::PermissionDenied
            | google_cloud_gax::error::rpc::Code::Unauthenticated => {
                io::ErrorKind::PermissionDenied
            }
            google_cloud_gax::error::rpc::Code::AlreadyExists => io::ErrorKind::AlreadyExists,
            _ => io::ErrorKind::Other,
        }
    } else if let Some(code) = err.http_status_code() {
        match code {
            404 => io::ErrorKind::NotFound,
            401 | 403 => io::ErrorKind::PermissionDenied,
            409 => io::ErrorKind::AlreadyExists,
            _ => io::ErrorKind::Other,
        }
    } else {
        io::ErrorKind::Other
    };
    io::Error::new(kind, err)
}

/// Literal prefix before the first glob metacharacter, cut back to the last `/`, or `None`
/// without a wildcard. Listing only this prefix scans fewer objects.
#[doc(hidden)]
pub fn glob_prefix(object_pattern: &str) -> Option<&str> {
    find_wildcard(object_pattern).map(|wildcard| {
        object_pattern[..wildcard]
            .rfind('/')
            .map_or("", |slash| &object_pattern[..=slash])
    })
}

/// Returns an empty match only if the object is absent. Other errors propagate so that auth
/// or network failures do not look like empty input.
#[doc(hidden)]
pub fn literal_match(uri: String, exists: io::Result<bool>) -> io::Result<Vec<String>> {
    exists.map(|found| found.then_some(uri).into_iter().collect())
}

/// Collects object names from all pages of a listing, stopping at the first error.
#[doc(hidden)]
pub async fn collect_object_names(
    names: impl Stream<Item = Result<String, google_cloud_gax::error::Error>>,
) -> io::Result<Vec<String>> {
    names.map_err(map_gcs_error).try_collect().await
}

/// Returns the names accepted by `matcher` as sorted `gs://bucket/name` URIs.
#[doc(hidden)]
pub fn to_sorted_uris(bucket: &str, matcher: &GlobMatcher, names: Vec<String>) -> Vec<String> {
    let mut uris: Vec<String> = names
        .into_iter()
        .filter(|name| matcher.is_match(name))
        .map(|name| format!("gs://{bucket}/{name}"))
        .collect();
    uris.sort_unstable();
    uris
}

/// Adds `http://` to a `STORAGE_EMULATOR_HOST` value that has no scheme.
#[doc(hidden)]
pub fn emulator_endpoint(host: Option<String>) -> Option<String> {
    host.map(|h| {
        let host = h.trim();
        if host.starts_with("http://") || host.starts_with("https://") {
            host.to_string()
        } else {
            format!("http://{host}")
        }
    })
}

fn read_range_spec(start_offset: u64, length: u64) -> google_cloud_storage::model_ext::ReadRange {
    match (start_offset, length) {
        (0, 0) => google_cloud_storage::model_ext::ReadRange::all(),
        (offset, 0) => google_cloud_storage::model_ext::ReadRange::offset(offset),
        (offset, len) => google_cloud_storage::model_ext::ReadRange::segment(offset, len),
    }
}

#[derive(Clone, Debug)]
struct GcsClientPair {
    storage: Arc<google_cloud_storage::client::Storage>,
    control: Arc<google_cloud_storage::client::StorageControl>,
}

/// Google Cloud Storage client and [`FileSystem`] implementation.
///
/// Async callers, such as Iceberg storage, await the inherent `async` methods. An async
/// method with the name of a [`FileSystem`] method ends in `_async`. The [`FileSystem`]
/// methods wait on the async methods with [`block_on_async`].
#[derive(Clone, Debug, Default)]
pub struct GcsFileSystem {
    clients: Arc<OnceCell<GcsClientPair>>,
    credentials: Option<google_cloud_auth::credentials::Credentials>,
}

impl GcsFileSystem {
    /// Uses Application Default Credentials, or `STORAGE_EMULATOR_HOST` if set.
    pub fn new() -> Self {
        Self::default()
    }

    /// Uses `credentials` instead of Application Default Credentials.
    pub fn with_credentials(credentials: google_cloud_auth::credentials::Credentials) -> Self {
        Self {
            clients: Arc::new(OnceCell::new()),
            credentials: Some(credentials),
        }
    }

    async fn clients(&self) -> io::Result<&GcsClientPair> {
        self.clients
            .get_or_try_init(|| async {
                let endpoint = emulator_endpoint(std::env::var("STORAGE_EMULATOR_HOST").ok());
                let credentials = self.credentials.clone();
                // Build on the shared client runtime so background token-refresh tasks outlive
                // any short-lived caller runtime.
                client_runtime()?
                    .spawn(async move {
                        let mut storage_builder = google_cloud_storage::client::Storage::builder();
                        let mut control_builder =
                            google_cloud_storage::client::StorageControl::builder();

                        if let Some(ref ep) = endpoint {
                            storage_builder = storage_builder.with_endpoint(ep);
                            control_builder = control_builder.with_endpoint(ep);
                        }
                        if let Some(credentials) = credentials {
                            storage_builder = storage_builder.with_credentials(credentials.clone());
                            control_builder = control_builder.with_credentials(credentials);
                        }

                        let storage =
                            Arc::new(storage_builder.build().await.map_err(io::Error::other)?);
                        let control =
                            Arc::new(control_builder.build().await.map_err(io::Error::other)?);
                        Ok::<_, io::Error>(GcsClientPair { storage, control })
                    })
                    .await
                    .map_err(|e| io::Error::other(format!("GCS client init task failed: {e}")))?
            })
            .await
    }

    /// Returns whether `path` exists in GCS.
    pub async fn exists_async(&self, path: &str) -> io::Result<bool> {
        match self.size_async(path).await {
            Ok(_) => Ok(true),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(false),
            Err(e) => Err(e),
        }
    }

    /// Returns the byte length of `path`.
    pub async fn size_async(&self, path: &str) -> io::Result<u64> {
        let (bucket, object) = parse_gcs_uri(path)?;
        let clients = self.clients().await?;
        let parent = format!("projects/_/buckets/{bucket}");
        let obj = clients
            .control
            .get_object()
            .set_bucket(&parent)
            .set_object(object)
            .send()
            .await
            .map_err(map_gcs_error)?;
        Ok(obj.size.max(0) as u64)
    }

    /// Returns the last-modified timestamp of `path`.
    pub async fn last_modified_async(&self, path: &str) -> io::Result<SystemTime> {
        let (bucket, object) = parse_gcs_uri(path)?;
        let clients = self.clients().await?;
        let parent = format!("projects/_/buckets/{bucket}");
        let obj = clients
            .control
            .get_object()
            .set_bucket(&parent)
            .set_object(object)
            .send()
            .await
            .map_err(map_gcs_error)?;
        let ts = obj.update_time.or(obj.create_time).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("object {path} has neither update_time nor create_time"),
            )
        })?;
        Ok(SystemTime::UNIX_EPOCH
            + Duration::from_secs(ts.seconds().max(0) as u64)
            + Duration::from_nanos(ts.nanos().max(0) as u64))
    }

    /// Lists all `gs://` URIs matching `pattern`, sorted lexicographically.
    pub async fn match_files_async(&self, pattern: &str) -> io::Result<Vec<String>> {
        let (bucket, object_pattern) = parse_gcs_uri(pattern)?;
        let Some(prefix) = glob_prefix(object_pattern) else {
            return literal_match(
                format!("gs://{bucket}/{object_pattern}"),
                self.exists_async(pattern).await,
            );
        };
        let matcher = GlobMatcher::new(object_pattern)?;
        let clients = self.clients().await?;
        let parent = format!("projects/_/buckets/{bucket}");
        let items = clients
            .control
            .list_objects()
            .set_parent(parent)
            .set_prefix(prefix)
            .by_item();
        let names = collect_object_names(
            stream::unfold(items, |mut items| async move {
                items.next().await.map(|item| (item, items))
            })
            .map_ok(|object| object.name),
        )
        .await?;
        Ok(to_sorted_uris(bucket, &matcher, names))
    }

    /// Reads the entire object at `path` into memory.
    pub async fn read(&self, path: &str) -> io::Result<Bytes> {
        self.read_range(path, 0, 0).await
    }

    /// Reads `length` bytes (or to EOF when `length == 0`) starting at `start_offset`.
    pub async fn read_range(
        &self,
        path: &str,
        start_offset: u64,
        length: u64,
    ) -> io::Result<Bytes> {
        let (bucket, object) = parse_gcs_uri(path)?;
        let clients = self.clients().await?;
        let parent = format!("projects/_/buckets/{bucket}");
        let mut resp = clients
            .storage
            .read_object(&parent, object)
            .set_read_range(read_range_spec(start_offset, length))
            .send()
            .await
            .map_err(map_gcs_error)?;
        let mut first_chunk: Option<Bytes> = None;
        let mut combined: Option<BytesMut> = None;
        while let Some(chunk) = resp.next().await {
            let chunk = chunk.map_err(map_gcs_error)?;
            if let Some(buf) = &mut combined {
                buf.extend_from_slice(&chunk);
            } else if let Some(prev) = first_chunk.take() {
                let cap = usize::try_from(length)
                    .ok()
                    .filter(|&n| n > 0)
                    .unwrap_or(prev.len() + chunk.len());
                let mut buf = BytesMut::with_capacity(cap);
                buf.extend_from_slice(&prev);
                buf.extend_from_slice(&chunk);
                combined = Some(buf);
            } else {
                first_chunk = Some(chunk);
            }
        }
        Ok(combined.map_or_else(|| first_chunk.unwrap_or_default(), BytesMut::freeze))
    }

    /// Uploads `payload` to `path` with idempotent retries.
    pub async fn write(&self, path: &str, payload: Bytes) -> io::Result<()> {
        let (bucket, object) = parse_gcs_uri(path)?;
        let clients = self.clients().await?;
        upload_bytes(&clients.storage, bucket, object, payload).await
    }

    /// Opens an async writer for `path`.
    #[doc(hidden)]
    pub async fn writer(&self, path: &str) -> io::Result<GcsAsyncWriter> {
        let (bucket, object) = parse_gcs_uri(path)?;
        let clients = self.clients().await?;
        Ok(GcsAsyncWriter::new(
            Arc::clone(&clients.storage),
            bucket.to_string(),
            object.to_string(),
        ))
    }

    /// Deletes the object at `path`.
    pub async fn remove_async(&self, path: &str) -> io::Result<()> {
        let (bucket, object) = parse_gcs_uri(path)?;
        let clients = self.clients().await?;
        let parent = format!("projects/_/buckets/{bucket}");
        clients
            .control
            .delete_object()
            .set_bucket(&parent)
            .set_object(object)
            .send()
            .await
            .map_err(map_gcs_error)?;
        Ok(())
    }

    /// Copies `from` to `to` using server-side `rewrite_object`.
    pub async fn copy_async(&self, from: &str, to: &str) -> io::Result<()> {
        let (src_bucket, src_object) = parse_gcs_uri(from)?;
        let (dst_bucket, dst_object) = parse_gcs_uri(to)?;
        let clients = self.clients().await?;
        let src_parent = format!("projects/_/buckets/{src_bucket}");
        let dst_parent = format!("projects/_/buckets/{dst_bucket}");
        let mut rewrite_token: Option<String> = None;
        loop {
            let mut req = clients
                .control
                .rewrite_object()
                .set_source_bucket(&src_parent)
                .set_source_object(src_object)
                .set_destination_bucket(&dst_parent)
                .set_destination_name(dst_object);
            if let Some(token) = rewrite_token {
                req = req.set_rewrite_token(token);
            }
            let resp = req.send().await.map_err(map_gcs_error)?;
            if resp.done {
                return Ok(());
            }
            rewrite_token = Some(resp.rewrite_token);
        }
    }

    /// Copies `old_path` to `new_path` and removes `old_path`. Does nothing when both name the
    /// same object, so that the remove cannot delete it.
    pub async fn rename_async(&self, old_path: &str, new_path: &str) -> io::Result<()> {
        if parse_gcs_uri(old_path)? == parse_gcs_uri(new_path)? {
            return Ok(());
        }
        self.copy_async(old_path, new_path).await?;
        self.remove_async(old_path).await
    }
}

impl FileSystem for GcsFileSystem {
    fn match_files(&self, pattern: &str) -> io::Result<Vec<String>> {
        block_on_async(Self::match_files_async(self, pattern))
    }

    fn open_read(&self, path: &str) -> io::Result<Box<dyn Read + Send>> {
        FileSystem::open_read_range(self, path, 0, 0)
    }

    fn open_read_range(
        &self,
        path: &str,
        start_offset: u64,
        length: u64,
    ) -> io::Result<Box<dyn Read + Send>> {
        let (bucket, object) = parse_gcs_uri(path)?;
        let clients = block_on_async(self.clients())?;
        let parent = format!("projects/_/buckets/{bucket}");
        let object = object.to_string();
        let storage = Arc::clone(&clients.storage);

        let (tx, rx) = mpsc::channel(128);
        let completed = Arc::new(AtomicBool::new(false));
        let task_completed = Arc::clone(&completed);
        let rt = client_runtime()?;
        rt.spawn(async move {
            let req = storage
                .read_object(&parent, object)
                .set_read_range(read_range_spec(start_offset, length));
            match req.send().await {
                Ok(mut resp) => {
                    let mut clean = true;
                    while let Some(chunk) = resp.next().await {
                        let res = chunk.map_err(map_gcs_error);
                        if tx.send(res).await.is_err() {
                            clean = false;
                            break; // The reader was dropped.
                        }
                    }
                    if clean {
                        task_completed.store(true, Ordering::Release);
                    }
                }
                Err(err) => {
                    let _ = tx.send(Err(map_gcs_error(err))).await;
                }
            }
        });

        Ok(Box::new(GcsStreamReader::with_completion(rx, completed)))
    }

    fn open_write(&self, path: &str) -> io::Result<Box<dyn Write + Send>> {
        let (bucket, object) = parse_gcs_uri(path)?;
        let clients = block_on_async(self.clients())?;
        Ok(Box::new(GcsWriter::new(
            Arc::clone(&clients.storage),
            bucket.to_string(),
            object.to_string(),
        )))
    }

    fn open_append(&self, path: &str) -> io::Result<Box<dyn Write + Send>> {
        let (bucket, object) = parse_gcs_uri(path)?;
        let clients = block_on_async(self.clients())?;
        let existing = match block_on_async(self.read(path)) {
            Ok(bytes) => Vec::from(bytes),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Vec::new(),
            Err(e) => return Err(e),
        };

        Ok(Box::new(GcsWriter::with_initial(
            Arc::clone(&clients.storage),
            bucket.to_string(),
            object.to_string(),
            existing,
        )))
    }

    fn size(&self, path: &str) -> io::Result<u64> {
        block_on_async(Self::size_async(self, path))
    }

    fn remove(&self, path: &str) -> io::Result<()> {
        block_on_async(Self::remove_async(self, path))
    }

    fn rename(&self, old_path: &str, new_path: &str) -> io::Result<()> {
        block_on_async(Self::rename_async(self, old_path, new_path))
    }

    fn copy(&self, from: &str, to: &str) -> io::Result<()> {
        block_on_async(Self::copy_async(self, from, to))
    }

    fn last_modified(&self, path: &str) -> io::Result<SystemTime> {
        block_on_async(Self::last_modified_async(self, path))
    }
}
