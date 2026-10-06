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

//! Offline tests for the GCS reader and helpers. An in-memory channel replaces the network
//! task of `open_read_range`.

use std::io::{self, BufRead, Read};
use std::time::Duration;

use bytes::Bytes;
use gcp::gcs::reader::GcsStreamReader;
use gcp::gcs::{emulator_endpoint, map_gcs_error};
use gcp::runtime::block_on_async;
use google_cloud_gax::error::Error as GaxError;
use google_cloud_gax::error::rpc::{Code, Status};
use tokio::sync::mpsc;

/// A reader over a closed channel pre-filled with `chunks` (`Err(msg)` = stream error).
fn reader(chunks: &[Result<&str, &str>]) -> GcsStreamReader {
    let (tx, rx) = mpsc::channel(chunks.len().max(1));
    for chunk in chunks {
        let item = match chunk {
            Ok(data) => Ok(Bytes::copy_from_slice(data.as_bytes())),
            Err(msg) => Err(io::Error::other(msg.to_string())),
        };
        tx.try_send(item).expect("channel has capacity");
    }
    GcsStreamReader::new(rx)
}

/// Calls `read` with a `buf_len` buffer until EOF, returning each non-empty read.
fn reads(r: &mut GcsStreamReader, buf_len: usize) -> Vec<String> {
    let mut out = Vec::new();
    let mut buf = vec![0; buf_len];
    loop {
        let n = r.read(&mut buf).expect("read");
        if n == 0 {
            return out;
        }
        out.push(String::from_utf8(buf[..n].to_vec()).expect("utf8"));
    }
}

fn lines(r: GcsStreamReader) -> Vec<String> {
    r.lines().collect::<io::Result<_>>().expect("lines")
}

#[test]
fn read_with_tiny_buffer_spans_chunks() {
    let mut r = reader(&[Ok("hel"), Ok("lo "), Ok("world")]);
    assert_eq!(reads(&mut r, 2), ["he", "ll", "o ", "wo", "rl", "d"]);
    // EOF is sticky.
    assert_eq!(r.read(&mut [0; 4]).unwrap(), 0);
}

#[test]
fn read_with_large_buffer_coalesces_queued_chunks() {
    let mut r = reader(&[Ok("hel"), Ok("lo "), Ok("world")]);
    assert_eq!(reads(&mut r, 64), ["hello world"]);
}

#[test]
fn read_into_empty_buffer_consumes_nothing() {
    let mut r = reader(&[Ok("abc")]);
    assert_eq!(r.read(&mut []).unwrap(), 0);
    assert_eq!(reads(&mut r, 8), ["abc"]);
}

#[test]
fn read_skips_zero_length_chunks() {
    let mut r = reader(&[Ok(""), Ok("ab"), Ok(""), Ok(""), Ok("cd"), Ok("")]);
    let mut s = String::new();
    r.read_to_string(&mut s).unwrap();
    assert_eq!(s, "abcd");

    let mut r = reader(&[Ok("ab"), Ok(""), Ok("cd")]);
    assert_eq!(reads(&mut r, 1), ["a", "b", "c", "d"]);
}

#[test]
fn clean_eof_on_empty_stream() {
    let mut r = reader(&[]);
    assert_eq!(r.read(&mut [0; 8]).unwrap(), 0);
    assert_eq!(r.fill_buf().unwrap(), b"");

    let mut r = reader(&[Ok(""), Ok("")]);
    assert_eq!(r.fill_buf().unwrap(), b"");
    assert_eq!(r.read(&mut [0; 8]).unwrap(), 0);
}

#[test]
fn read_error_as_first_item_is_returned() {
    let mut r = reader(&[Err("404 no such object")]);
    let err = r.read(&mut [0; 8]).unwrap_err();
    assert_eq!(err.to_string(), "404 no such object");
}

#[test]
fn read_error_mid_stream_keeps_already_copied_bytes() {
    // `Err` from `read` means nothing was read, so copied bytes come first, the error next.
    let mut r = reader(&[Ok("abc"), Err("boom"), Ok("never")]);
    let mut buf = [0; 64];
    assert_eq!(r.read(&mut buf).unwrap(), 3);
    assert_eq!(&buf[..3], b"abc");
    let err = r.read(&mut buf).unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::Other);
    assert_eq!(err.to_string(), "boom");
}

#[test]
fn read_to_end_surfaces_mid_stream_error_after_prefix() {
    let mut r = reader(&[Ok("abc"), Ok("def"), Err("boom")]);
    let mut out = Vec::new();
    let err = r.read_to_end(&mut out).unwrap_err();
    assert_eq!(err.to_string(), "boom");
    assert_eq!(out, b"abcdef");
}

#[test]
fn fill_buf_and_consume_walk_chunks() {
    let mut r = reader(&[Ok("line1\nli"), Ok("ne2\n")]);
    assert_eq!(r.fill_buf().unwrap(), b"line1\nli");
    r.consume(3);
    assert_eq!(r.fill_buf().unwrap(), b"e1\nli");
    r.consume(5);
    assert_eq!(r.fill_buf().unwrap(), b"ne2\n");
    r.consume(4);
    assert_eq!(r.fill_buf().unwrap(), b"");
}

#[test]
fn lines_span_chunk_boundaries() {
    let r = reader(&[Ok("line1\nli"), Ok("ne2\nline"), Ok("3")]);
    assert_eq!(lines(r), ["line1", "line2", "line3"]);
}

#[test]
fn fill_buf_skips_zero_length_chunks() {
    // An empty chunk is not EOF. The reader must keep reading.
    let r = reader(&[Ok("a\n"), Ok(""), Ok("b\n")]);
    assert_eq!(lines(r), ["a", "b"]);

    let mut r = reader(&[Ok(""), Ok(""), Ok("x\n")]);
    let mut line = String::new();
    assert_eq!(r.read_line(&mut line).unwrap(), 2);
    assert_eq!(line, "x\n");
}

#[test]
fn fill_buf_propagates_stream_error() {
    let mut r = reader(&[Ok("ok"), Err("403 denied")]);
    assert_eq!(r.fill_buf().unwrap(), b"ok");
    r.consume(2);
    assert_eq!(r.fill_buf().unwrap_err().to_string(), "403 denied");
}

#[test]
fn fill_buf_reports_error_deferred_by_read() {
    let mut r = reader(&[Ok("abc"), Err("boom")]);
    assert_eq!(r.read(&mut [0; 64]).unwrap(), 3);
    assert_eq!(r.fill_buf().unwrap_err().to_string(), "boom");
}

/// Chunks sent from another thread after a delay force the blocking slow path.
fn delayed_reader() -> GcsStreamReader {
    let (tx, rx) = mpsc::channel(1);
    std::thread::spawn(move || {
        for chunk in ["slow ", "", "path"] {
            std::thread::sleep(Duration::from_millis(20));
            tx.blocking_send(Ok(Bytes::from_static(chunk.as_bytes())))
                .expect("reader alive");
        }
    });
    GcsStreamReader::new(rx)
}

#[test]
fn blocking_path_outside_runtime() {
    let mut s = String::new();
    delayed_reader().read_to_string(&mut s).unwrap();
    assert_eq!(s, "slow path");
    assert_eq!(lines(delayed_reader()), ["slow path"]);
}

#[test]
fn blocking_path_inside_current_thread_runtime() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let s = rt.block_on(async {
        let mut s = String::new();
        delayed_reader().read_to_string(&mut s).unwrap();
        s
    });
    assert_eq!(s, "slow path");
}

#[test]
fn blocking_path_inside_multi_thread_runtime() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let s = rt
        .block_on(rt.spawn(async {
            let mut s = String::new();
            delayed_reader().read_to_string(&mut s).unwrap();
            s
        }))
        .unwrap();
    assert_eq!(s, "slow path");
}

#[test]
fn block_on_async_works_from_every_context() {
    assert_eq!(block_on_async(async { Ok(7) }).unwrap(), 7);

    let current = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    assert_eq!(
        current
            .block_on(async { block_on_async(async { Ok(8) }) })
            .unwrap(),
        8
    );

    let multi = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .build()
        .unwrap();
    let joined = multi.block_on(multi.spawn(async { block_on_async(async { Ok(9) }) }));
    assert_eq!(joined.unwrap().unwrap(), 9);

    let err = block_on_async::<_, ()>(async { Err(io::Error::other("x")) }).unwrap_err();
    assert_eq!(err.to_string(), "x");
}

#[test]
fn map_gcs_error_classifies_not_found() {
    for msg in [
        "HTTP 404",
        "NotFound",
        "object Not Found",
        "code NOT_FOUND",
        "No such object: b/o",
    ] {
        let err = map_gcs_error(msg);
        assert_eq!(err.kind(), io::ErrorKind::NotFound, "{msg:?}");
        assert_eq!(err.to_string(), msg);
    }
}

#[test]
fn map_gcs_error_classifies_permission_denied() {
    for msg in ["401 Unauthorized", "403 Forbidden", "PermissionDenied"] {
        let err = map_gcs_error(msg);
        assert_eq!(err.kind(), io::ErrorKind::PermissionDenied, "{msg:?}");
        assert_eq!(err.to_string(), msg);
    }
}

fn service_error(code: Code, msg: &str) -> GaxError {
    GaxError::service(Status::default().set_code(code).set_message(msg))
}

#[test]
fn map_gcs_error_classifies_real_gax_service_errors() {
    assert_eq!(
        map_gcs_error(service_error(Code::NotFound, "gone")).kind(),
        io::ErrorKind::NotFound
    );
    // GAX `PERMISSION_DENIED` maps to `io::ErrorKind::PermissionDenied`.
    let err = map_gcs_error(service_error(Code::PermissionDenied, "caller lacks access"));
    assert_eq!(err.kind(), io::ErrorKind::PermissionDenied);
    assert!(err.to_string().contains("caller lacks access"), "{err}");
    assert_eq!(
        map_gcs_error(service_error(Code::Internal, "oops")).kind(),
        io::ErrorKind::Other
    );
}

#[test]
fn map_gcs_error_prefers_not_found_and_falls_back_to_other() {
    assert_eq!(
        map_gcs_error("403 then 404").kind(),
        io::ErrorKind::NotFound
    );
    let err = map_gcs_error("500 internal");
    assert_eq!(err.kind(), io::ErrorKind::Other);
    assert_eq!(err.to_string(), "500 internal");
}

#[test]
fn emulator_endpoint_normalises_scheme() {
    assert_eq!(emulator_endpoint(None), None);
    for (input, expected) in [
        ("localhost:9023", "http://localhost:9023"),
        ("  localhost:9023\n", "http://localhost:9023"),
        ("http://h:1", "http://h:1"),
        ("https://h:1", "https://h:1"),
    ] {
        assert_eq!(
            emulator_endpoint(Some(input.to_string())).as_deref(),
            Some(expected),
            "{input:?}"
        );
    }
}
