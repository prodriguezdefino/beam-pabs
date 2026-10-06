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
use std::sync::atomic::{AtomicBool, Ordering};

use bytes::Bytes;
use google_cloud_gax::error::Error as GaxError;
use google_cloud_gax::error::rpc::Code;
use google_cloud_storage::client::Storage;
use google_cloud_storage::streaming_source::StreamingSource;
use google_cloud_storage::stub::{self, DefaultStorage};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use super::map_gcs_error;
use crate::runtime::{block_on_async, client_runtime, send_blocking};

/// Chunk size (`8 MiB`) above which [`GcsWriter`] switches from an in-memory buffer to a
/// background streaming upload so open bundle files stay bounded in memory.
pub const STREAM_CHUNK_BYTES: usize = 8 * 1024 * 1024;

const STREAM_CHANNEL_CAPACITY: usize = 2;

/// Upload source that reads chunks from the writer. It records when the writer closed the
/// channel and the upload read all chunks.
struct ChannelSource {
    rx: mpsc::Receiver<Bytes>,
    drained: Arc<AtomicBool>,
}

impl StreamingSource for ChannelSource {
    type Error = io::Error;

    async fn next(&mut self) -> Option<Result<Bytes, Self::Error>> {
        let next = self.rx.recv().await;
        if next.is_none() {
            self.drained.store(true, Ordering::Release);
        }
        next.map(Ok)
    }
}

/// Background upload of one object, fed chunk by chunk through a bounded channel.
struct ActiveStream {
    tx: mpsc::Sender<Bytes>,
    handle: JoinHandle<io::Result<()>>,
    drained: Arc<AtomicBool>,
}

impl ActiveStream {
    fn start<S>(storage: &Arc<Storage<S>>, bucket: &str, object: &str) -> io::Result<Self>
    where
        S: stub::Storage + 'static,
    {
        let (tx, rx) = mpsc::channel(STREAM_CHANNEL_CAPACITY);
        let drained = Arc::new(AtomicBool::new(false));
        let source = ChannelSource {
            rx,
            drained: Arc::clone(&drained),
        };
        let storage = Arc::clone(storage);
        let parent = format!("projects/_/buckets/{bucket}");
        let object = object.to_string();
        let handle = client_runtime()?.spawn(async move {
            storage
                .write_object(&parent, &object, source)
                .send_buffered()
                .await
                .map(|_| ())
                .map_err(map_gcs_error)
        });
        Ok(Self {
            tx,
            handle,
            drained,
        })
    }

    /// Sends one chunk. If the upload cannot take it, closes the upload and returns an error.
    fn send(self, chunk: Vec<u8>) -> io::Result<Self> {
        match send_blocking(&self.tx, Bytes::from(chunk)) {
            Ok(()) => Ok(self),
            Err(e) => Err(self.abort(&e.to_string())),
        }
    }

    /// Sends the last chunk, closes the channel and waits for the upload. A chunk that the
    /// upload did not read is an error, also when the upload reports success.
    fn finish(self, tail: Vec<u8>) -> io::Result<()> {
        let sent = match tail.is_empty() {
            true => Ok(()),
            false => send_blocking(&self.tx, Bytes::from(tail)),
        };
        match sent {
            Err(e) => Err(self.abort(&e.to_string())),
            Ok(()) => match self.join()? {
                true => Ok(()),
                false => Err(io::Error::other(
                    "GCS streaming upload ended before it read all chunks",
                )),
            },
        }
    }

    /// Closes the upload after a chunk was not delivered. Returns the upload error, or else an
    /// error with `reason`.
    fn abort(self, reason: &str) -> io::Error {
        match self.join() {
            Err(e) => e,
            Ok(_) => io::Error::other(format!(
                "GCS streaming upload did not take a chunk: {reason}"
            )),
        }
    }

    /// Closes the channel and waits for the upload task. Returns whether the upload read all
    /// chunks.
    fn join(self) -> io::Result<bool> {
        let Self {
            tx,
            handle,
            drained,
        } = self;
        drop(tx);
        block_on_async(async move {
            handle
                .await
                .map_err(|e| io::Error::other(format!("GCS upload task failed: {e}")))?
        })?;
        Ok(drained.load(Ordering::Acquire))
    }
}

/// Lifecycle of a [`GcsWriter`].
enum State {
    /// The object is in memory. Each flush of a dirty buffer uploads the whole object again.
    Buffered { buffer: Vec<u8>, dirty: bool },
    /// A background upload takes full chunks. `buffer` holds the bytes of the next chunk.
    Streaming {
        buffer: Vec<u8>,
        stream: ActiveStream,
    },
    /// The streamed upload completed. The object cannot take more bytes.
    Finished,
    /// An upload failed and the writer lost bytes. All later calls fail.
    Failed,
}

/// Buffered writer that uploads with `write_object`. Generic over the storage stub so tests
/// can inject a fake with [`Storage::from_stub`].
///
/// Callers must call [`Write::flush`] when writes complete to finish the upload. If dropped
/// with dirty bytes, [`Drop`] calls flush, but any upload error is logged with
/// `tracing::error!` and cannot return to the caller.
///
/// An object of less than [`STREAM_CHUNK_BYTES`] stays in memory. Each flush uploads it
/// again, and retries rate-limit and unavailable errors with backoff. A larger object streams
/// in chunks, and the writer keeps only the next chunk in memory. So a streamed upload cannot
/// restart: if it fails, writes and flush return the error, and the runner retries the bundle.
/// The flush of a streamed object completes it, and later writes return an error.
#[doc(hidden)]
pub struct GcsWriter<S = DefaultStorage>
where
    S: stub::Storage + 'static,
{
    storage: Arc<Storage<S>>,
    bucket: String,
    object: String,
    state: State,
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
            state: State::Buffered {
                buffer: Vec::new(),
                dirty: true,
            },
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
            state: State::Buffered {
                buffer: initial,
                dirty,
            },
        }
    }

    fn closed_error(&self, reason: &str) -> io::Error {
        io::Error::other(format!("gs://{}/{}: {reason}", self.bucket, self.object))
    }

    /// Hands a full buffer to the streamed upload, and starts the upload if necessary.
    /// The state stays [`State::Failed`] if this returns an error.
    fn send_chunk(&mut self) -> io::Result<()> {
        let stream_and_chunk = match std::mem::replace(&mut self.state, State::Failed) {
            State::Buffered { buffer, .. } => {
                ActiveStream::start(&self.storage, &self.bucket, &self.object)
                    .map(|stream| (stream, buffer))
            }
            State::Streaming { buffer, stream } => Ok((stream, buffer)),
            state @ (State::Finished | State::Failed) => {
                self.state = state;
                return Err(self.closed_error("the writer is closed"));
            }
        };
        let (stream, chunk) = stream_and_chunk?;
        let stream = stream.send(chunk)?;
        self.state = State::Streaming {
            buffer: Vec::with_capacity(STREAM_CHUNK_BYTES),
            stream,
        };
        Ok(())
    }

    /// Copies a prefix of `buf` into the buffer, up to [`STREAM_CHUNK_BYTES`]. Sends a full
    /// buffer first, so the count is above zero for a non-empty `buf`.
    fn write_some(&mut self, buf: &[u8]) -> io::Result<usize> {
        let full = match &self.state {
            State::Buffered { buffer, .. } | State::Streaming { buffer, .. } => {
                buffer.len() >= STREAM_CHUNK_BYTES
            }
            State::Finished => {
                return Err(self.closed_error(
                    "the streamed upload is complete, and a GCS object cannot take more bytes",
                ));
            }
            State::Failed => return Err(self.closed_error("an earlier upload failed")),
        };
        if full {
            self.send_chunk()?;
        }
        let buffer = match &mut self.state {
            State::Buffered { buffer, dirty } => {
                *dirty = true;
                buffer
            }
            State::Streaming { buffer, .. } => buffer,
            State::Finished | State::Failed => {
                return Err(self.closed_error("the writer is closed"));
            }
        };
        let room = STREAM_CHUNK_BYTES
            .saturating_sub(buffer.len())
            .min(buf.len());
        buffer.extend_from_slice(&buf[..room]);
        Ok(room)
    }

    /// Uploads the whole buffer, with retries. A failed upload keeps the buffer dirty.
    fn upload_buffered(&mut self, buffer: Vec<u8>) -> io::Result<()> {
        let parent = format!("projects/_/buckets/{}", self.bucket);
        let object = self.object.clone();
        // Take ownership of the buffer with zero copy.
        let payload = Bytes::from(buffer);
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
        self.state = State::Buffered {
            buffer: Vec::from(payload),
            dirty: upload_res.is_err(),
        };
        upload_res
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
        let written = self.write_some(buf)?;
        match &buf[written..] {
            [] => Ok(written),
            rest => self.write(rest).map(|more| written + more),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match std::mem::replace(&mut self.state, State::Failed) {
            State::Buffered {
                buffer,
                dirty: true,
            } => self.upload_buffered(buffer),
            state @ (State::Buffered { dirty: false, .. } | State::Finished) => {
                self.state = state;
                Ok(())
            }
            State::Streaming { buffer, stream } => {
                stream.finish(buffer)?;
                self.state = State::Finished;
                Ok(())
            }
            State::Failed => Err(self.closed_error("an earlier upload failed")),
        }
    }
}

impl<S> Drop for GcsWriter<S>
where
    S: stub::Storage + 'static,
{
    fn drop(&mut self) {
        let pending = match &self.state {
            State::Buffered { dirty, .. } => *dirty,
            State::Streaming { .. } => true,
            State::Finished | State::Failed => false,
        };
        if pending && let Err(e) = self.flush() {
            tracing::error!(
                "Failed to flush GcsWriter for gs://{}/{} on drop: {e}",
                self.bucket,
                self.object
            );
        }
    }
}
