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

//! Offline tests for `GcsWriter`. A fake `google-cloud-storage` stub, injected through
//! `Storage::from_stub`, replaces the GCS backend.

use std::collections::VecDeque;
use std::io::{self, Write};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use gcp::gcs::writer::{GcsWriter, is_rate_limit_or_retryable};
use google_cloud_gax::error::Error as GaxError;
use google_cloud_gax::error::rpc::{Code, Status};
use google_cloud_storage::client::Storage;
use google_cloud_storage::model::Object;
use google_cloud_storage::model_ext::WriteObjectRequest;
use google_cloud_storage::request_options::RequestOptions;
use google_cloud_storage::streaming_source::StreamingSource;
use google_cloud_storage::stub;

/// One recorded `write_object` call: (bucket, object, payload).
type Upload = (String, String, Vec<u8>);

/// Fake storage stub: records every buffered upload and fails the first calls with the
/// scripted errors (one per call, in order). With `read_limit`, an upload reads at most that
/// many chunks of its source, then returns and drops the source.
#[derive(Debug, Default)]
struct FakeStorage {
    uploads: Mutex<Vec<Upload>>,
    failures: Mutex<VecDeque<(Code, &'static str)>>,
    read_limit: Option<usize>,
}

impl stub::Storage for FakeStorage {
    async fn write_object_buffered<P>(
        &self,
        mut payload: P,
        req: WriteObjectRequest,
        _options: RequestOptions,
    ) -> google_cloud_storage::Result<Object>
    where
        P: StreamingSource + Send + Sync + 'static,
    {
        let mut data = Vec::new();
        let mut remaining = self.read_limit.unwrap_or(usize::MAX);
        while remaining > 0
            && let Some(chunk) = payload.next().await
        {
            data.extend_from_slice(&chunk.expect("payload chunk"));
            remaining -= 1;
        }
        let resource = req.spec.resource.expect("write spec has a resource");
        self.uploads.lock().expect("lock").push((
            resource.bucket.clone(),
            resource.name.clone(),
            data,
        ));
        if let Some((code, msg)) = self.failures.lock().expect("lock").pop_front() {
            return Err(GaxError::service(
                Status::default().set_code(code).set_message(msg),
            ));
        }
        Ok(resource)
    }
}

fn fake(failures: &[(Code, &'static str)]) -> (Arc<FakeStorage>, Arc<Storage<FakeStorage>>) {
    fake_with_read_limit(failures, None)
}

fn fake_with_read_limit(
    failures: &[(Code, &'static str)],
    read_limit: Option<usize>,
) -> (Arc<FakeStorage>, Arc<Storage<FakeStorage>>) {
    let stub = Arc::new(FakeStorage {
        failures: Mutex::new(failures.iter().cloned().collect()),
        read_limit,
        ..FakeStorage::default()
    });
    let client = Arc::new(Storage::from_stub(Arc::clone(&stub)));
    (stub, client)
}

fn uploads(stub: &FakeStorage) -> Vec<Upload> {
    stub.uploads.lock().expect("lock").clone()
}

fn upload(bucket: &str, object: &str, data: &[u8]) -> Upload {
    (
        format!("projects/_/buckets/{bucket}"),
        object.to_string(),
        data.to_vec(),
    )
}

#[test]
fn write_buffers_until_flush_then_uploads_whole_buffer_once() {
    let (stub, client) = fake(&[]);
    let mut writer = GcsWriter::new(client, "bkt".into(), "dir/obj.txt".into());

    assert_eq!(writer.write(b"hello ").unwrap(), 6);
    writer.write_all(b"world").unwrap();
    assert!(uploads(&stub).is_empty(), "write() must not upload");

    writer.flush().unwrap();
    assert_eq!(
        uploads(&stub),
        [upload("bkt", "dir/obj.txt", b"hello world")]
    );

    // A clean writer does not upload again on flush or drop.
    writer.flush().unwrap();
    drop(writer);
    assert_eq!(uploads(&stub).len(), 1);
}

#[test]
fn flush_after_more_writes_reuploads_full_object() {
    // GCS objects are immutable: every flush rewrites the whole object.
    let (stub, client) = fake(&[]);
    let mut writer = GcsWriter::new(client, "b".into(), "o".into());
    writer.write_all(b"a").unwrap();
    writer.flush().unwrap();
    writer.write_all(b"b").unwrap();
    writer.flush().unwrap();
    assert_eq!(
        uploads(&stub),
        [upload("b", "o", b"a"), upload("b", "o", b"ab")]
    );
}

#[test]
fn drop_flushes_unflushed_bytes() {
    let (stub, client) = fake(&[]);
    {
        let mut writer = GcsWriter::new(client, "b".into(), "o".into());
        writer.write_all(b"data").unwrap();
    }
    assert_eq!(uploads(&stub), [upload("b", "o", b"data")]);
}

#[test]
fn with_initial_prepends_existing_bytes_and_uploads_them_even_without_writes() {
    let (stub, client) = fake(&[]);
    drop(GcsWriter::with_initial(
        Arc::clone(&client),
        "b".into(),
        "o".into(),
        b"old\n".to_vec(),
    ));
    assert_eq!(uploads(&stub), [upload("b", "o", b"old\n")]);

    let mut writer = GcsWriter::with_initial(client, "b".into(), "o".into(), b"old\n".to_vec());
    writer.write_all(b"new\n").unwrap();
    writer.flush().unwrap();
    assert_eq!(uploads(&stub)[1], upload("b", "o", b"old\nnew\n"));
}

#[test]
fn with_empty_initial_is_clean() {
    let (stub, client) = fake(&[]);
    drop(GcsWriter::with_initial(
        client,
        "b".into(),
        "o".into(),
        Vec::new(),
    ));
    assert!(uploads(&stub).is_empty());
}

#[test]
fn flush_with_no_writes_creates_empty_object() {
    let (stub, client) = fake(&[]);
    let mut writer = GcsWriter::new(client, "b".into(), "empty".into());
    writer.flush().unwrap();
    drop(writer);
    assert_eq!(uploads(&stub), [upload("b", "empty", b"")]);
}

#[test]
fn flush_retries_rate_limited_uploads_then_succeeds() {
    let (stub, client) = fake(&[
        (Code::ResourceExhausted, "429 Too Many Requests"),
        (Code::Unavailable, "backend busy"),
    ]);
    let mut writer = GcsWriter::new(client, "b".into(), "o".into());
    writer.write_all(b"x").unwrap();
    writer.flush().unwrap();
    assert_eq!(uploads(&stub), vec![upload("b", "o", b"x"); 3]);
    // The writer is clean. Drop does not upload again.
    drop(writer);
    assert_eq!(uploads(&stub).len(), 3);
}

#[test]
fn flush_backoff_doubles_between_retries() {
    // Two retries sleep 250 ms then 500 ms; constant backoff sleeps less.
    let (stub, client) = fake(&[
        (Code::ResourceExhausted, "429 Too Many Requests"),
        (Code::ResourceExhausted, "429 Too Many Requests"),
    ]);
    let mut writer = GcsWriter::new(client, "b".into(), "o".into());
    writer.write_all(b"x").unwrap();
    let started = Instant::now();
    writer.flush().unwrap();
    let elapsed = started.elapsed();
    assert_eq!(uploads(&stub).len(), 3);
    assert!(elapsed >= Duration::from_millis(740), "{elapsed:?}");
}

#[test]
fn flush_does_not_retry_non_retryable_errors_and_stays_dirty() {
    let (stub, client) = fake(&[(Code::PermissionDenied, "403 Forbidden")]);
    let mut writer = GcsWriter::new(client, "b".into(), "o".into());
    writer.write_all(b"x").unwrap();
    let err = writer.flush().unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::PermissionDenied);
    assert!(err.to_string().contains("403 Forbidden"), "{err}");
    assert_eq!(uploads(&stub).len(), 1, "no retry for a 403");

    // The failed upload leaves the writer dirty, so the next call retries the upload.
    writer.flush().unwrap();
    assert_eq!(uploads(&stub).len(), 2);
}

#[test]
fn flush_gives_up_after_five_attempts() {
    // Takes ~3.75s of real backoff (250 + 500 + 1000 + 2000 ms).
    let (stub, client) = fake(&[(Code::ResourceExhausted, "429 rate limit"); 6]);
    let mut writer = GcsWriter::new(client, "b".into(), "o".into());
    writer.write_all(b"x").unwrap();
    let err = writer.flush().unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::Other);
    assert!(err.to_string().contains("429 rate limit"), "{err}");
    assert_eq!(uploads(&stub).len(), 5);
    // Avoid a further upload (and its backoff) from Drop.
    std::mem::forget(writer);
}

#[test]
fn drop_with_error_does_not_panic() {
    let (stub, client) = fake(&[(Code::PermissionDenied, "403 Forbidden")]);
    {
        let mut writer = GcsWriter::new(client, "b".into(), "o".into());
        writer.write_all(b"dirty bytes").unwrap();
        // Drop attempts flush, fails with 403, logs error, and does not panic.
    }
    assert_eq!(uploads(&stub).len(), 1);
}

#[test]
fn retryable_classification() {
    for code in [
        Code::ResourceExhausted,
        Code::Unavailable,
        Code::DeadlineExceeded,
    ] {
        let err = GaxError::service(Status::default().set_code(code).set_message("retryable"));
        assert!(
            is_rate_limit_or_retryable(&err),
            "{code:?} should be retryable"
        );
    }
    for code in [
        Code::NotFound,
        Code::PermissionDenied,
        Code::InvalidArgument,
        Code::Internal,
    ] {
        let err = GaxError::service(
            Status::default()
                .set_code(code)
                .set_message("not retryable"),
        );
        assert!(
            !is_rate_limit_or_retryable(&err),
            "{code:?} should not be retryable"
        );
    }
}

#[test]
fn write_streams_chunks_above_threshold_and_completes_on_flush() {
    use gcp::gcs::writer::STREAM_CHUNK_BYTES;

    let (stub, client) = fake(&[]);
    let mut writer = GcsWriter::new(client, "bkt".into(), "dir/large.bin".into());

    let chunk = vec![b'z'; STREAM_CHUNK_BYTES / 2];
    // Write 5 half-chunks (2.5 * STREAM_CHUNK_BYTES) so two full chunks stream before flush
    // and the last half-chunk flushes on close.
    for _ in 0..5 {
        writer.write_all(&chunk).unwrap();
    }
    writer.flush().unwrap();

    let recorded = uploads(&stub);
    assert_eq!(recorded.len(), 1);
    assert_eq!(recorded[0].0, "projects/_/buckets/bkt");
    assert_eq!(recorded[0].1, "dir/large.bin");
    assert_eq!(recorded[0].2.len(), chunk.len() * 5);
    assert!(recorded[0].2.iter().all(|&b| b == b'z'));
}

#[test]
fn streamed_upload_keeps_byte_order_across_chunks_and_tail() {
    use gcp::gcs::writer::STREAM_CHUNK_BYTES;

    let (stub, client) = fake(&[]);
    let mut writer = GcsWriter::new(client, "b".into(), "o".into());
    let data: Vec<u8> = (0..STREAM_CHUNK_BYTES * 2 + 17)
        .map(|i| (i % 251) as u8)
        .collect();
    // One large write spans two full chunks and a short tail.
    writer.write_all(&data).unwrap();
    writer.flush().unwrap();
    let recorded = uploads(&stub);
    assert_eq!(recorded.len(), 1);
    assert!(
        recorded[0].2 == data,
        "streamed bytes differ from the written bytes"
    );
}

#[test]
fn write_after_streamed_flush_returns_error_and_keeps_object() {
    use gcp::gcs::writer::STREAM_CHUNK_BYTES;

    let (stub, client) = fake(&[]);
    let mut writer = GcsWriter::new(client, "b".into(), "o".into());
    writer
        .write_all(&vec![b'a'; STREAM_CHUNK_BYTES + 1])
        .unwrap();
    writer.flush().unwrap();

    let err = writer.write_all(b"more").unwrap_err();
    assert!(err.to_string().contains("gs://b/o"), "{err}");
    // A flush after the completed upload does not upload again, and neither does drop.
    writer.flush().unwrap();
    drop(writer);
    let recorded = uploads(&stub);
    assert_eq!(recorded.len(), 1);
    assert_eq!(recorded[0].2.len(), STREAM_CHUNK_BYTES + 1);
}

#[test]
fn streamed_upload_that_fails_early_returns_its_error() {
    use gcp::gcs::writer::STREAM_CHUNK_BYTES;

    let (stub, client) =
        fake_with_read_limit(&[(Code::PermissionDenied, "403 Forbidden")], Some(0));
    let mut writer = GcsWriter::new(client, "b".into(), "o".into());
    // Four chunks fill the channel, so a write or the flush sees the closed upload.
    let err = writer
        .write_all(&vec![b'a'; STREAM_CHUNK_BYTES * 4])
        .and_then(|()| writer.flush())
        .unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::PermissionDenied, "{err}");

    // The writer stays failed, and drop does not start a new upload.
    assert!(writer.write_all(b"x").is_err());
    assert!(writer.flush().is_err());
    drop(writer);
    assert_eq!(uploads(&stub).len(), 1);
}

#[test]
fn streamed_upload_that_succeeds_without_reading_all_chunks_is_an_error() {
    use gcp::gcs::writer::STREAM_CHUNK_BYTES;

    // With one chunk, the chunk and the tail fit in the channel, so only the flush sees the
    // lost bytes. With four chunks, a send also fails.
    for chunks in [1, 4] {
        let (stub, client) = fake_with_read_limit(&[], Some(0));
        let mut writer = GcsWriter::new(client, "b".into(), "o".into());
        let result = writer
            .write_all(&vec![b'a'; STREAM_CHUNK_BYTES * chunks + 1])
            .and_then(|()| writer.flush());
        assert!(result.is_err(), "{chunks} chunks: lost bytes returned Ok");
        drop(writer);
        assert_eq!(uploads(&stub).len(), 1, "{chunks} chunks");
    }
}
