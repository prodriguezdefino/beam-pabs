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

use std::fmt;
use std::io::{self, Read, Write};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, SystemTime};

use file::filesystem::{FileSystem, GlobMatcher};
use futures::{Stream, TryStreamExt, stream};
use google_cloud_gax::paginator::ItemPaginator;
use tokio::sync::mpsc;

pub mod reader;
pub mod writer;

use crate::runtime::{block_on_async, client_runtime};
use reader::GcsStreamReader;
use writer::GcsWriter;

/// Splits `gs://bucket/object` into `(bucket, object)`. Returns
/// [`io::ErrorKind::InvalidInput`] if the scheme is not `gs://` or the bucket is empty.
pub fn parse_gcs_uri(uri: &str) -> io::Result<(String, String)> {
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

    Ok((bucket.to_string(), object.to_string()))
}

/// Maps a GCS error message to an [`io::ErrorKind`], case-insensitively. The default
/// `exists()` maps `NotFound` from `size()` to `Ok(false)`, so an unmatched not-found
/// message becomes a hard error.
#[doc(hidden)]
pub fn map_gcs_error(err: impl fmt::Display) -> io::Error {
    const NOT_FOUND: [&str; 5] = [
        "404",
        "notfound",
        "not found",
        "not_found",
        "no such object",
    ];
    const PERMISSION_DENIED: [&str; 4] = ["401", "403", "permissiondenied", "permission_denied"];

    let msg = err.to_string();
    let lower = msg.to_lowercase();
    let matches_any = |needles: &[&str]| needles.iter().any(|needle| lower.contains(needle));

    if matches_any(&NOT_FOUND) {
        io::Error::new(io::ErrorKind::NotFound, msg)
    } else if matches_any(&PERMISSION_DENIED) {
        io::Error::new(io::ErrorKind::PermissionDenied, msg)
    } else {
        io::Error::other(msg)
    }
}

/// Literal prefix before the first glob metacharacter, cut back to the last `/`, or `None`
/// without a wildcard. Listing only this prefix scans fewer objects.
pub fn glob_prefix(object_pattern: &str) -> Option<&str> {
    object_pattern.find(['*', '?', '[']).map(|wildcard| {
        object_pattern[..wildcard]
            .rfind('/')
            .map_or("", |slash| &object_pattern[..=slash])
    })
}

/// Returns an empty match only if the object is absent. Other errors propagate so that auth
/// or network failures do not look like empty input.
pub fn literal_match(uri: String, exists: io::Result<bool>) -> io::Result<Vec<String>> {
    exists.map(|found| found.then_some(uri).into_iter().collect())
}

/// Collects object names from all pages of a listing, stopping at the first error.
pub async fn collect_object_names<E: fmt::Display>(
    names: impl Stream<Item = Result<String, E>>,
) -> io::Result<Vec<String>> {
    names.map_err(map_gcs_error).try_collect().await
}

/// Returns the names accepted by `matcher` as sorted `gs://bucket/name` URIs.
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

#[derive(Clone, Debug)]
struct GcsClientPair {
    storage: Arc<google_cloud_storage::client::Storage>,
    control: Arc<google_cloud_storage::client::StorageControl>,
}

/// Google Cloud Storage implementation of [`FileSystem`].
#[derive(Clone, Debug, Default)]
pub struct GcsFileSystem {
    clients: Arc<OnceLock<GcsClientPair>>,
}

impl GcsFileSystem {
    /// Uses Application Default Credentials, or `STORAGE_EMULATOR_HOST` if set.
    pub fn new() -> Self {
        Self {
            clients: Arc::new(OnceLock::new()),
        }
    }

    fn get_or_init_clients(&self) -> io::Result<&GcsClientPair> {
        if let Some(clients) = self.clients.get() {
            return Ok(clients);
        }

        let endpoint = emulator_endpoint(std::env::var("STORAGE_EMULATOR_HOST").ok());

        let clients = block_on_async(async move {
            let mut storage_builder = google_cloud_storage::client::Storage::builder();
            let mut control_builder = google_cloud_storage::client::StorageControl::builder();

            if let Some(ref ep) = endpoint {
                storage_builder = storage_builder.with_endpoint(ep);
                control_builder = control_builder.with_endpoint(ep);
            }

            let storage = Arc::new(storage_builder.build().await.map_err(map_gcs_error)?);
            let control = Arc::new(control_builder.build().await.map_err(map_gcs_error)?);
            Ok(GcsClientPair { storage, control })
        })?;

        let _ = self.clients.set(clients);
        Ok(self.clients.get().expect("Initialized GCS clients"))
    }
}

impl FileSystem for GcsFileSystem {
    fn match_files(&self, pattern: &str) -> io::Result<Vec<String>> {
        let (bucket, object_pattern) = parse_gcs_uri(pattern)?;
        let Some(prefix) = glob_prefix(&object_pattern) else {
            return literal_match(
                format!("gs://{bucket}/{object_pattern}"),
                self.exists(pattern),
            );
        };

        let clients = self.get_or_init_clients()?;
        let control = Arc::clone(&clients.control);
        let parent = format!("projects/_/buckets/{bucket}");
        let prefix = prefix.to_string();
        let names = block_on_async(async move {
            let items = control
                .list_objects()
                .set_parent(parent)
                .set_prefix(prefix)
                .by_item();
            collect_object_names(
                stream::unfold(items, |mut items| async move {
                    items.next().await.map(|item| (item, items))
                })
                .map_ok(|object| object.name),
            )
            .await
        })?;

        let matcher = GlobMatcher::new(&object_pattern)?;
        Ok(to_sorted_uris(&bucket, &matcher, names))
    }

    fn open_read(&self, path: &str) -> io::Result<Box<dyn Read + Send>> {
        self.open_read_range(path, 0, 0)
    }

    fn open_read_range(
        &self,
        path: &str,
        start_offset: u64,
        length: u64,
    ) -> io::Result<Box<dyn Read + Send>> {
        let (bucket, object) = parse_gcs_uri(path)?;
        let clients = self.get_or_init_clients()?;
        let parent = format!("projects/_/buckets/{bucket}");
        let storage = Arc::clone(&clients.storage);

        let (tx, rx) = mpsc::channel(128);
        let rt = client_runtime();
        rt.spawn(async move {
            let range = match (start_offset, length) {
                (0, 0) => google_cloud_storage::model_ext::ReadRange::all(),
                (offset, 0) => google_cloud_storage::model_ext::ReadRange::offset(offset),
                (offset, len) => google_cloud_storage::model_ext::ReadRange::segment(offset, len),
            };
            let req = storage.read_object(&parent, &object).set_read_range(range);
            match req.send().await {
                Ok(mut resp) => {
                    while let Some(chunk) = resp.next().await {
                        let res = chunk.map_err(map_gcs_error);
                        if tx.send(res).await.is_err() {
                            break; // The reader was dropped.
                        }
                    }
                }
                Err(err) => {
                    let _ = tx.send(Err(map_gcs_error(err))).await;
                }
            }
        });

        Ok(Box::new(GcsStreamReader::new(rx)))
    }

    fn open_write(&self, path: &str) -> io::Result<Box<dyn Write + Send>> {
        let (bucket, object) = parse_gcs_uri(path)?;
        let clients = self.get_or_init_clients()?;
        Ok(Box::new(GcsWriter::new(
            Arc::clone(&clients.storage),
            bucket,
            object,
        )))
    }

    fn open_append(&self, path: &str) -> io::Result<Box<dyn Write + Send>> {
        let (bucket, object) = parse_gcs_uri(path)?;
        let clients = self.get_or_init_clients()?;
        let mut existing = Vec::new();
        if let Ok(mut reader) = self.open_read(path) {
            let _ = reader.read_to_end(&mut existing);
        }

        Ok(Box::new(GcsWriter::with_initial(
            Arc::clone(&clients.storage),
            bucket,
            object,
            existing,
        )))
    }

    fn size(&self, path: &str) -> io::Result<u64> {
        let (bucket, object) = parse_gcs_uri(path)?;
        let clients = self.get_or_init_clients()?;
        let parent = format!("projects/_/buckets/{bucket}");
        let control = Arc::clone(&clients.control);
        block_on_async(async move {
            let obj = control
                .get_object()
                .set_bucket(&parent)
                .set_object(&object)
                .send()
                .await
                .map_err(map_gcs_error)?;
            Ok(obj.size.max(0) as u64)
        })
    }

    fn remove(&self, path: &str) -> io::Result<()> {
        let (bucket, object) = parse_gcs_uri(path)?;
        let clients = self.get_or_init_clients()?;
        let parent = format!("projects/_/buckets/{bucket}");
        let control = Arc::clone(&clients.control);
        block_on_async(async move {
            control
                .delete_object()
                .set_bucket(&parent)
                .set_object(&object)
                .send()
                .await
                .map_err(map_gcs_error)?;
            Ok(())
        })
    }

    fn rename(&self, old_path: &str, new_path: &str) -> io::Result<()> {
        self.copy(old_path, new_path)?;
        self.remove(old_path)
    }

    fn copy(&self, from: &str, to: &str) -> io::Result<()> {
        let (src_bucket, src_object) = parse_gcs_uri(from)?;
        let (dst_bucket, dst_object) = parse_gcs_uri(to)?;
        let clients = self.get_or_init_clients()?;
        let src_parent = format!("projects/_/buckets/{src_bucket}");
        let dst_parent = format!("projects/_/buckets/{dst_bucket}");
        let control = Arc::clone(&clients.control);
        block_on_async(async move {
            control
                .rewrite_object()
                .set_source_bucket(&src_parent)
                .set_source_object(&src_object)
                .set_destination_bucket(&dst_parent)
                .set_destination_name(&dst_object)
                .send()
                .await
                .map_err(map_gcs_error)?;
            Ok(())
        })
    }

    fn last_modified(&self, path: &str) -> io::Result<SystemTime> {
        let (bucket, object) = parse_gcs_uri(path)?;
        let clients = self.get_or_init_clients()?;
        let parent = format!("projects/_/buckets/{bucket}");
        let control = Arc::clone(&clients.control);
        block_on_async(async move {
            let obj = control
                .get_object()
                .set_bucket(&parent)
                .set_object(&object)
                .send()
                .await
                .map_err(map_gcs_error)?;
            let updated = obj
                .update_time
                .or(obj.create_time)
                .map(|ts| {
                    SystemTime::UNIX_EPOCH
                        + Duration::from_secs(ts.seconds().max(0) as u64)
                        + Duration::from_nanos(ts.nanos().max(0) as u64)
                })
                .unwrap_or_else(SystemTime::now);
            Ok(updated)
        })
    }
}
