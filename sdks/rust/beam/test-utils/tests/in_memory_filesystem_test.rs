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

//! Tests for the `InMemoryFileSystem` paths that the `io/file` tests do not cover:
//! range reads and not-found errors. All IO tests built on this mock depend on it.

use std::io::{self, Read, Write};

use file::filesystem::FileSystem;
use testutils::InMemoryFileSystem;

fn read_range(fs: &InMemoryFileSystem, path: &str, start: u64, length: u64) -> Vec<u8> {
    let mut out = Vec::new();
    fs.open_read_range(path, start, length)
        .expect("range read")
        .read_to_end(&mut out)
        .expect("read");
    out
}

fn with_file() -> InMemoryFileSystem {
    let fs = InMemoryFileSystem::new();
    fs.insert_file("mem://f", b"0123456789".to_vec());
    fs
}

#[test]
fn range_reads_return_exactly_the_requested_bytes() {
    let fs = with_file();
    assert_eq!(read_range(&fs, "mem://f", 2, 3), b"234");
    assert_eq!(read_range(&fs, "mem://f", 0, 10), b"0123456789");
    // Length 0 reads to the end of the file, as in the local file system.
    assert_eq!(read_range(&fs, "mem://f", 7, 0), b"789");
    // Ranges past the end are clamped.
    assert_eq!(read_range(&fs, "mem://f", 8, 100), b"89");
    assert_eq!(read_range(&fs, "mem://f", 20, 5), b"");
}

#[test]
fn range_reads_accept_huge_lengths() {
    // `start + length` must not overflow and panic for a large length.
    let fs = with_file();
    assert_eq!(read_range(&fs, "mem://f", 4, u64::MAX), b"456789");
    assert_eq!(read_range(&fs, "mem://f", u64::MAX, u64::MAX), b"");
}

fn assert_not_found<T: std::fmt::Debug>(result: io::Result<T>, path: &str) {
    let err = result.expect_err("expected a not-found error");
    assert_eq!(err.kind(), io::ErrorKind::NotFound);
    assert_eq!(err.to_string(), format!("file not found: {path}"));
}

#[test]
fn missing_files_report_not_found() {
    let fs = with_file();
    assert_not_found(fs.open_read("mem://none").map(|_| ()), "mem://none");
    assert_not_found(
        fs.open_read_range("mem://none", 0, 1).map(|_| ()),
        "mem://none",
    );
    assert_not_found(fs.size("mem://none"), "mem://none");
    assert_not_found(fs.remove("mem://none"), "mem://none");
    assert_not_found(fs.rename("mem://none", "mem://g"), "mem://none");
    assert_not_found(fs.copy("mem://none", "mem://g"), "mem://none");
    assert_not_found(fs.last_modified("mem://none"), "mem://none");
    assert!(!fs.exists("mem://none").unwrap());
    // Failed operations must not create files.
    assert_eq!(fs.all_paths(), vec!["mem://f".to_string()]);
}

#[test]
fn writes_become_visible_on_flush_and_append_extends() {
    let fs = InMemoryFileSystem::new();
    let mut writer = fs.open_write("mem://w").unwrap();
    writer.write_all(b"ab").unwrap();
    // Unlike the local file system, the file does not exist until the writer flushes.
    assert!(!fs.contains_file("mem://w"));
    writer.flush().unwrap();
    assert_eq!(fs.get_file("mem://w").as_deref(), Some(&b"ab"[..]));
    drop(writer);

    let mut appender = fs.open_append("mem://w").unwrap();
    appender.write_all(b"cd").unwrap();
    drop(appender);
    assert_eq!(fs.get_file("mem://w").as_deref(), Some(&b"abcd"[..]));
    assert_eq!(fs.size("mem://w").unwrap(), 4);
}
