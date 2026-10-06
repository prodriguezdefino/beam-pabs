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

//! `ReadableFile`, `FileMetadata` and the `FormatSink` adapter, exercised without a runner.

use std::io::Read;

use beam::coders::DefaultCoder;
use file::{FileMetadata, FileSink, FormatSink, ReadableFile, TextFormat};

struct TempDir(std::path::PathBuf);

impl TempDir {
    fn new(name: &str) -> Self {
        let dir = std::env::temp_dir().join(format!("beam_{name}_{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("create temp dir");
        Self(dir)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[test]
fn readable_file_reads_local_files() {
    let dir = TempDir::new("readable_file");
    let path = dir.0.join("hello.txt");
    std::fs::write(&path, "hello world").expect("write fixture");
    let path = path.to_string_lossy().into_owned();

    let file = ReadableFile::new(FileMetadata::of(&path).expect("stat"));
    assert_eq!(file.path(), path);
    assert_eq!(file.size_bytes(), 11);
    assert!(file.metadata.last_modified_millis > 0);
    assert_eq!(
        file.read_fully_as_utf8_string().expect("read"),
        "hello world"
    );

    let mut tail = String::new();
    file.open_range(6, 0)
        .expect("open range")
        .read_to_string(&mut tail)
        .expect("read range");
    assert_eq!(tail, "world");
}

#[test]
fn metadata_of_missing_file_is_an_error() {
    assert!(FileMetadata::of("/definitely/not/here.txt").is_err());
}

#[test]
fn readable_file_round_trips_through_its_row_coder() {
    let file = ReadableFile::new(FileMetadata {
        path: "gs://bucket/object".to_string(),
        size_bytes: 42,
        last_modified_millis: 7,
    });
    let bytes = file.encode().expect("encode");
    assert_eq!(ReadableFile::decode(&bytes).expect("decode"), file);
}

/// A `Write` that appends into a shared buffer, so the test can inspect it after
/// the sink has consumed the boxed writer.
#[derive(Clone, Default)]
struct SharedBuf(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);

impl std::io::Write for SharedBuf {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().expect("lock").extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[test]
fn format_sink_writes_header_elements_and_footer() {
    struct Csv;
    impl file::FileFormat<String> for Csv {
        fn write_element(&self, e: &String, w: &mut dyn std::io::Write) -> beam::Result {
            Ok(writeln!(w, "{e}")?)
        }
        fn write_header(&self, w: &mut dyn std::io::Write) -> beam::Result {
            Ok(writeln!(w, "h")?)
        }
        fn write_footer(&self, w: &mut dyn std::io::Write) -> beam::Result {
            Ok(writeln!(w, "f")?)
        }
    }

    let buf = SharedBuf::default();
    let mut writer = FormatSink::new(Csv)
        .open(Box::new(buf.clone()))
        .expect("open");
    writer.write(&"a".to_string()).expect("write");
    writer.write(&"b".to_string()).expect("write");
    writer.finish().expect("finish");
    assert_eq!(buf.0.lock().expect("lock").as_slice(), b"h\na\nb\nf\n");

    let empty = SharedBuf::default();
    FormatSink::<String, _>::new(TextFormat)
        .open(Box::new(empty.clone()))
        .expect("open")
        .finish()
        .expect("finish");
    assert!(empty.0.lock().expect("lock").is_empty());
}
