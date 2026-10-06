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
use google_cloud_storage::client::Storage;
use google_cloud_storage::stub::{self, DefaultStorage};

use super::map_gcs_error;
use crate::runtime::block_on_async;

/// Buffered writer that uploads with `write_object`. Generic over the storage stub so tests
/// can inject a fake with [`Storage::from_stub`].
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
pub fn is_rate_limit_or_retryable(err_str: &str) -> bool {
    let lower = err_str.to_lowercase();
    const RETRYABLE_PATTERNS: &[&str] = &[
        "429",
        "rate limit",
        "exceeded the rate limit",
        "503",
        "service unavailable",
        "resource exhausted",
        "resource_exhausted",
        "unavailable",
    ];
    RETRYABLE_PATTERNS.iter().any(|&pat| lower.contains(pat))
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
        let payload = Bytes::copy_from_slice(&self.buffer);
        let storage = Arc::clone(&self.storage);
        block_on_async(async move {
            let mut attempts = 0;
            let mut backoff_ms = 250_u64;
            loop {
                match storage
                    .write_object(&parent, &object, payload.clone())
                    .send_buffered()
                    .await
                {
                    Ok(_) => return Ok(()),
                    Err(err) => {
                        attempts += 1;
                        let err_msg = err.to_string();
                        if attempts < 5 && is_rate_limit_or_retryable(&err_msg) {
                            tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)).await;
                            backoff_ms = (backoff_ms * 2).min(5000);
                            continue;
                        }
                        return Err(map_gcs_error(err));
                    }
                }
            }
        })?;
        self.dirty = false;
        Ok(())
    }
}

impl<S> Drop for GcsWriter<S>
where
    S: stub::Storage + 'static,
{
    fn drop(&mut self) {
        if self.dirty {
            let _ = self.flush();
        }
    }
}
