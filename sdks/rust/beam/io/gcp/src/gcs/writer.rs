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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

//! Buffered upload writer for Google Cloud Storage objects.

use std::io::{self, Write};
use std::sync::Arc;

use bytes::Bytes;
use google_cloud_gax::error::Error as GaxError;
use google_cloud_gax::error::rpc::Code;
use google_cloud_storage::client::Storage;
use google_cloud_storage::stub::{self, DefaultStorage};

use super::map_gcs_error;
use crate::runtime::block_on_async;

/// Buffered writer that uploads with `write_object`. Generic over the storage stub so tests
/// can inject a fake with [`Storage::from_stub`].
///
/// Callers must call [`Write::flush`] when writes complete to finish the upload. If dropped
/// with dirty bytes, [`Drop`] calls flush, but any upload error is logged with
/// `tracing::error!` and cannot return to the caller.
#[doc(hidden)]
pub struct GcsWriter<S = DefaultStorage>
where
    S: stub::Storage + 'static,
{
    storage: Arc<Storage<S>>,
    bucket: String,
    object: String,
    buffer: Vec<u8>,
    dirty: bool,
}

impl<S> GcsWriter<S>
where
    S: stub::Storage + 'static,
{
    pub fn new(storage: Arc<Storage<S>>, bucket: String, object: String) -> Self {
        Self {
            storage,
            bucket,
            object,
            buffer: Vec::new(),
            dirty: true,
        }
    }

    pub fn with_initial(
        storage: Arc<Storage<S>>,
        bucket: String,
        object: String,
        initial: Vec<u8>,
    ) -> Self {
        let dirty = !initial.is_empty();
        Self {
            storage,
            bucket,
            object,
            buffer: initial,
            dirty,
        }
    }
}

#[doc(hidden)]
pub fn is_rate_limit_or_retryable(err: &GaxError) -> bool {
    if let Some(status) = err.status() {
        matches!(
            status.code,
            Code::ResourceExhausted | Code::Unavailable | Code::DeadlineExceeded
        )
    } else if let Some(code) = err.http_status_code() {
        code == 429 || code == 503 || code == 504
    } else {
        false
    }
}

const MAX_ATTEMPTS: usize = 5;

fn backoff(attempt: usize) -> std::time::Duration {
    let millis = 250_u64.saturating_mul(1 << attempt).min(5000);
    std::time::Duration::from_millis(millis)
}

impl<S> Write for GcsWriter<S>
where
    S: stub::Storage + 'static,
{
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.buffer.extend_from_slice(buf);
        self.dirty = true;
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        if !self.dirty {
            return Ok(());
        }
        let parent = format!("projects/_/buckets/{}", self.bucket);
        let object = self.object.clone();
        // Take ownership of the buffer with zero copy.
        let payload = Bytes::from(std::mem::take(&mut self.buffer));
        let storage = Arc::clone(&self.storage);
        let (upload_res, payload) = block_on_async(async move {
            for attempt in 0..MAX_ATTEMPTS {
                match storage
                    .write_object(&parent, &object, payload.clone())
                    .send_buffered()
                    .await
                {
                    Ok(_) => return Ok((Ok(()), payload)),
                    Err(err) => {
                        if attempt + 1 < MAX_ATTEMPTS && is_rate_limit_or_retryable(&err) {
                            tokio::time::sleep(backoff(attempt)).await;
                            continue;
                        }
                        return Ok((Err(map_gcs_error(err)), payload));
                    }
                }
            }
            Ok((
                Err(io::Error::other("max retry attempts exhausted")),
                payload,
            ))
        })?;
        // Reclaim the buffer without copy when the upload future drops its clone.
        self.buffer = Vec::from(payload);
        upload_res?;
        self.dirty = false;
        Ok(())
    }
}

impl<S> Drop for GcsWriter<S>
where
    S: stub::Storage + 'static,
{
    fn drop(&mut self) {
        if self.dirty
            && let Err(e) = self.flush()
        {
            tracing::error!(
                "Failed to flush GcsWriter for gs://{}/{} on drop: {e}",
                self.bucket,
                self.object
            );
        }
    }
}
