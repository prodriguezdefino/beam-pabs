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

//! Upload writers for Google Cloud Storage objects.

use std::io::{self, Write};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use bytes::Bytes;
use google_cloud_gax::exponential_backoff::{ExponentialBackoff, ExponentialBackoffBuilder};
use google_cloud_gax::retry_policy::RetryPolicyExt;
use google_cloud_storage::builder::storage::WriteObject;
use google_cloud_storage::client::Storage;
use google_cloud_storage::retry_policy::RetryableErrors;
use google_cloud_storage::streaming_source::StreamingSource;
use google_cloud_storage::stub::{self, DefaultStorage};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use super::map_gcs_error;

use crate::runtime::{block_on_async, client_runtime};

/// Chunk size (`8 MiB`) above which a writer stops the in-memory buffer and streams the
/// object, so open bundle files stay bounded in memory.
pub const STREAM_CHUNK_BYTES: usize = 8 * 1024 * 1024;

const STREAM_CHANNEL_CAPACITY: usize = 2;

/// Calls of an upload: 4 retries.
const UPLOAD_ATTEMPTS: u32 = 5;

/// Waits between the calls of an upload: from 0.25 seconds, at most 5 seconds.
fn upload_backoff() -> ExponentialBackoff {
    ExponentialBackoffBuilder::new()
        .with_initial_delay(Duration::from_millis(250))
        .with_maximum_delay(Duration::from_secs(5))
        .clamp()
}

/// Sets the retries of a buffered upload. A retry writes the same object name again, so it
/// is idempotent.
fn with_upload_retries<T, S>(request: WriteObject<T, S>) -> WriteObject<T, S>
where
    S: stub::Storage + 'static,
{
    request
        .with_idempotency(true)
        .with_retry_policy(RetryableErrors.with_attempt_limit(UPLOAD_ATTEMPTS))
        .with_backoff_policy(upload_backoff())
}

/// Marks a streamed upload as idempotent. It keeps the retry time limit (300 seconds) and the
/// backoff of the client, because a retry resumes from the unconfirmed bytes of a large object.
fn with_stream_retries<T, S>(request: WriteObject<T, S>) -> WriteObject<T, S>
where
    S: stub::Storage + 'static,
{
    request.with_idempotency(true)
}

/// Uploads `payload` to `gs://{bucket}/{object}` with idempotent retries.
pub(super) async fn upload_bytes<S>(
    storage: &Storage<S>,
    bucket: &str,
    object: &str,
    payload: Bytes,
) -> io::Result<()>
where
    S: stub::Storage + 'static,
{
    let parent = format!("projects/_/buckets/{bucket}");
    with_upload_retries(storage.write_object(&parent, object, payload))
        .send_buffered()
        .await
        .map(drop)
        .map_err(map_gcs_error)
}

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
            with_stream_retries(storage.write_object(&parent, &object, source))
                .send_buffered()
                .await
                .map(drop)
                .map_err(map_gcs_error)
        });
        Ok(Self {
            tx,
            handle,
            drained,
        })
    }

    /// Sends one chunk. If the upload cannot take it, closes the upload and returns an error.
    async fn send(self, chunk: Vec<u8>) -> io::Result<Self> {
        match self.tx.send(Bytes::from(chunk)).await {
            Ok(()) => Ok(self),
            Err(e) => Err(self.abort(&e.to_string()).await),
        }
    }

    /// Sends the last chunk, closes the channel and waits for the upload. A chunk that the
    /// upload did not read is an error, also when the upload reports success.
    async fn finish(self, tail: Vec<u8>) -> io::Result<()> {
        if !tail.is_empty()
            && let Err(e) = self.tx.send(Bytes::from(tail)).await
        {
            return Err(self.abort(&e.to_string()).await);
        }
        if self.join().await? {
            Ok(())
        } else {
            Err(io::Error::other(
                "GCS streaming upload ended before it read all chunks",
            ))
        }
    }

    /// Closes the upload after a chunk was not delivered. Returns the upload error, or else an
    /// error with `reason`.
    async fn abort(self, reason: &str) -> io::Error {
        match self.join().await {
            Err(e) => e,
            Ok(_) => io::Error::other(format!(
                "GCS streaming upload did not take a chunk: {reason}"
            )),
        }
    }

    /// Closes the channel and waits for the upload task. Returns whether the upload read all
    /// chunks.
    async fn join(self) -> io::Result<bool> {
        let Self {
            tx,
            handle,
            drained,
        } = self;
        drop(tx);
        handle
            .await
            .map_err(|e| io::Error::other(format!("GCS upload task failed: {e}")))??;
        Ok(drained.load(Ordering::Acquire))
    }
}

/// Lifecycle of a [`GcsAsyncWriter`].
enum State {
    /// The object is in memory. Each flush of a dirty buffer uploads the whole object again.
    Buffered { buffer: Vec<u8>, dirty: bool },
    /// A background upload takes full chunks. `buffer` holds the bytes of the next chunk.
    Streaming {
        buffer: Vec<u8>,
        stream: ActiveStream,
    },
    /// The object is complete. It cannot take more bytes.
    Finished,
    /// An upload failed and the writer lost bytes. All later calls fail.
    Failed,
}

/// The result of one copy into the buffer.
enum Copied {
    /// The count of bytes that the buffer took.
    Bytes(usize),
    /// The buffer is full. Send it with [`GcsAsyncWriter::send_chunk`], then copy again.
    Full,
}

/// Async writer of one Google Cloud Storage object. Generic over the storage stub so tests
/// can inject a fake with [`Storage::from_stub`].
///
/// An object of less than [`STREAM_CHUNK_BYTES`] stays in memory and uploads in one call,
/// with the retries of the Storage client. A larger object streams in chunks, and the writer
/// keeps only the next chunk in memory. The Storage client keeps the bytes that the service
/// did not confirm and resumes from them. If the streamed upload fails, writes and
/// [`Self::close`] return the error, and the runner retries the bundle.
#[doc(hidden)]
pub struct GcsAsyncWriter<S = DefaultStorage>
where
    S: stub::Storage + 'static,
{
    storage: Arc<Storage<S>>,
    bucket: String,
    object: String,
    state: State,
    written: u64,
}

impl<S> GcsAsyncWriter<S>
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
            written: 0,
        }
    }

    /// Starts the object with `initial`. The writer uploads `initial` also with no writes.
    fn with_initial(
        storage: Arc<Storage<S>>,
        bucket: String,
        object: String,
        initial: Vec<u8>,
    ) -> Self {
        let (dirty, written) = (!initial.is_empty(), initial.len() as u64);
        Self {
            storage,
            bucket,
            object,
            state: State::Buffered {
                buffer: initial,
                dirty,
            },
            written,
        }
    }

    fn closed_error(&self, reason: &str) -> io::Error {
        io::Error::other(format!("gs://{}/{}: {reason}", self.bucket, self.object))
    }

    /// Copies a prefix of `buf` into the buffer, up to [`STREAM_CHUNK_BYTES`]. Does no I/O.
    fn copy_some(&mut self, buf: &[u8]) -> io::Result<Copied> {
        let buffer = match &mut self.state {
            State::Buffered { buffer, .. } | State::Streaming { buffer, .. }
                if buffer.len() >= STREAM_CHUNK_BYTES =>
            {
                return Ok(Copied::Full);
            }
            State::Buffered { buffer, dirty } => {
                *dirty = true;
                buffer
            }
            State::Streaming { buffer, .. } => buffer,
            State::Finished => {
                return Err(self.closed_error(
                    "the object is complete, and a GCS object cannot take more bytes",
                ));
            }
            State::Failed => return Err(self.closed_error("an earlier upload failed")),
        };
        let room = (STREAM_CHUNK_BYTES - buffer.len()).min(buf.len());
        buffer.extend_from_slice(&buf[..room]);
        self.written += room as u64;
        Ok(Copied::Bytes(room))
    }

    /// Sends a full buffer to the streamed upload, and starts the upload if necessary.
    /// The state stays [`State::Failed`] if this returns an error.
    async fn send_chunk(&mut self) -> io::Result<()> {
        let (stream, chunk) = match std::mem::replace(&mut self.state, State::Failed) {
            State::Buffered { buffer, .. } => (
                ActiveStream::start(&self.storage, &self.bucket, &self.object)?,
                buffer,
            ),
            State::Streaming { buffer, stream } => (stream, buffer),
            state @ (State::Finished | State::Failed) => {
                self.state = state;
                return Err(self.closed_error("the writer is closed"));
            }
        };
        let stream = stream.send(chunk).await?;
        self.state = State::Streaming {
            buffer: Vec::with_capacity(STREAM_CHUNK_BYTES),
            stream,
        };
        Ok(())
    }

    /// Appends `chunk` to the object. Sends each full buffer to the streamed upload.
    pub async fn write(&mut self, chunk: &[u8]) -> io::Result<()> {
        let mut remaining = chunk;
        while !remaining.is_empty() {
            match self.copy_some(remaining)? {
                Copied::Bytes(count) => remaining = &remaining[count..],
                Copied::Full => self.send_chunk().await?,
            }
        }
        Ok(())
    }

    /// Uploads a dirty buffer, or completes a streamed upload. A buffered object can take
    /// more bytes after this, and the next flush uploads the whole object again.
    async fn flush(&mut self) -> io::Result<()> {
        match std::mem::replace(&mut self.state, State::Failed) {
            State::Buffered {
                buffer,
                dirty: true,
            } => self.upload_buffered(buffer).await,
            state @ (State::Buffered { dirty: false, .. } | State::Finished) => {
                self.state = state;
                Ok(())
            }
            State::Streaming { buffer, stream } => {
                stream.finish(buffer).await?;
                self.state = State::Finished;
                Ok(())
            }
            State::Failed => Err(self.closed_error("an earlier upload failed")),
        }
    }

    /// Uploads the whole buffer. A failed upload keeps the buffer dirty.
    async fn upload_buffered(&mut self, buffer: Vec<u8>) -> io::Result<()> {
        // A clone of `Bytes` shares the buffer; the retries of the client need one.
        let payload = Bytes::from(buffer);
        let result = upload_bytes(&self.storage, &self.bucket, &self.object, payload.clone()).await;
        // The upload dropped its clone, so this takes the buffer back with no copy.
        self.state = State::Buffered {
            buffer: Vec::from(payload),
            dirty: result.is_err(),
        };
        result
    }

    /// Whether a flush has bytes to upload.
    fn pending(&self) -> bool {
        match &self.state {
            State::Buffered { dirty, .. } => *dirty,
            State::Streaming { .. } => true,
            State::Finished | State::Failed => false,
        }
    }

    /// Completes the object and returns its size. The object cannot take more bytes.
    pub async fn close(&mut self) -> io::Result<u64> {
        self.flush().await?;
        self.state = State::Finished;
        Ok(self.written)
    }
}

/// Sync [`Write`] over a [`GcsAsyncWriter`]. A write that fits in the buffer does no I/O; a
/// full buffer and [`Write::flush`] wait on the async writer.
///
/// Call [`Write::flush`] when writes complete. If dropped with bytes to upload, [`Drop`]
/// calls flush, but it can only log an upload error with `tracing::error!`. A buffered
/// object can take more bytes after a flush, and each flush uploads it again. The flush of
/// a streamed object completes it, and later writes return an error.
#[doc(hidden)]
pub struct GcsWriter<S = DefaultStorage>
where
    S: stub::Storage + 'static,
{
    inner: GcsAsyncWriter<S>,
}

impl<S> GcsWriter<S>
where
    S: stub::Storage + 'static,
{
    pub fn new(storage: Arc<Storage<S>>, bucket: String, object: String) -> Self {
        Self {
            inner: GcsAsyncWriter::new(storage, bucket, object),
        }
    }

    pub fn with_initial(
        storage: Arc<Storage<S>>,
        bucket: String,
        object: String,
        initial: Vec<u8>,
    ) -> Self {
        Self {
            inner: GcsAsyncWriter::with_initial(storage, bucket, object, initial),
        }
    }
}

impl<S> Write for GcsWriter<S>
where
    S: stub::Storage + 'static,
{
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let written = match self.inner.copy_some(buf)? {
            Copied::Bytes(count) => count,
            Copied::Full => {
                block_on_async(self.inner.send_chunk())?;
                0
            }
        };
        match &buf[written..] {
            [] => Ok(written),
            rest => self.write(rest).map(|more| written + more),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        block_on_async(self.inner.flush())
    }
}

impl<S> Drop for GcsWriter<S>
where
    S: stub::Storage + 'static,
{
    fn drop(&mut self) {
        if self.inner.pending()
            && let Err(e) = self.flush()
        {
            tracing::error!(
                "Failed to flush GcsWriter for gs://{}/{} on drop: {e}",
                self.inner.bucket,
                self.inner.object
            );
        }
    }
}
