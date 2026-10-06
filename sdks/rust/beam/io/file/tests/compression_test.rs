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

//! `Compression`, its stream decorators and `FileOutput`, without a runner.

use std::io::{self, Cursor, Read, Write};
use std::sync::{Arc, Mutex};

use file::{Compression, FileOutput};

/// Every write mode: uncompressed and each codec.
fn all_modes() -> impl Iterator<Item = Compression> {
    std::iter::once(Compression::Uncompressed).chain(Compression::CODECS)
}

fn decode(codec: Compression, bytes: Vec<u8>) -> io::Result<Vec<u8>> {
    let mut out = Vec::new();
    codec
        .reader(Box::new(Cursor::new(bytes)))?
        .read_to_end(&mut out)?;
    Ok(out)
}

/// A `Write` into a shared buffer, readable after the writer is consumed.
#[derive(Clone, Default)]
struct SharedBuf(Arc<Mutex<Vec<u8>>>);

impl SharedBuf {
    fn take(&self) -> Vec<u8> {
        std::mem::take(&mut *self.0.lock().expect("lock"))
    }
}

impl Write for SharedBuf {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.lock().expect("lock").extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// A `Write` whose every call fails.
struct Broken;

impl Write for Broken {
    fn write(&mut self, _: &[u8]) -> io::Result<usize> {
        Err(io::Error::other("disk full"))
    }

    fn flush(&mut self) -> io::Result<()> {
        Err(io::Error::other("disk full"))
    }
}

#[test]
fn detect_maps_extensions_to_codecs() {
    for (path, expected) in [
        ("a/b/file.txt.gz", Compression::Gzip),
        ("a/b/FILE.GZIP", Compression::Gzip),
        ("a/b/file.zst", Compression::Zstd),
        ("a/b/file.zstd", Compression::Zstd),
        ("a/b/file.deflate", Compression::Deflate),
        ("a/b/file.zlib", Compression::Deflate),
        ("a/b/file.sz", Compression::Snappy),
        ("a/b/file.snappy", Compression::Uncompressed),
        ("a/b/file.txt", Compression::Uncompressed),
        ("a/b/noext", Compression::Uncompressed),
        ("gz", Compression::Uncompressed),
    ] {
        assert_eq!(Compression::detect(path), expected, "{path}");
        assert_eq!(Compression::Auto.resolve(path), expected, "{path}");
        assert_eq!(
            Compression::Auto.is_compressed(path),
            expected != Compression::Uncompressed,
            "{path}"
        );
        assert!(!Compression::Auto.matches(path), "{path}");
        assert!(!Compression::Uncompressed.matches(path), "{path}");
    }
    assert_eq!(Compression::Gzip.resolve("data.bin"), Compression::Gzip);
}

#[test]
fn codecs_have_names_and_suffixes() {
    for (codec, name, suffix) in [
        (Compression::Auto, "AUTO", None),
        (Compression::Uncompressed, "UNCOMPRESSED", None),
        (Compression::Gzip, "GZIP", Some(".gz")),
        (Compression::Zstd, "ZSTD", Some(".zst")),
        (Compression::Deflate, "DEFLATE", Some(".deflate")),
        (Compression::Snappy, "SNAPPY", Some(".sz")),
    ] {
        assert_eq!(codec.to_string(), name);
        assert_eq!(codec.extensions().first().copied(), suffix, "{codec}");
        let path = "/out/part-00000.txt";
        let expected = format!("{path}{}", suffix.unwrap_or(""));
        assert_eq!(codec.append_suffix(path), expected, "{codec}");
        assert_eq!(codec.append_suffix(expected.as_str()), expected, "{codec}");
    }
}

#[test]
fn auto_cannot_write_or_read() {
    let write = Compression::Auto.writer(Vec::new()).map(drop);
    assert_eq!(
        write.expect_err("auto write").kind(),
        io::ErrorKind::InvalidInput
    );
    let read = Compression::Auto.reader(Box::new(io::empty())).map(drop);
    assert_eq!(
        read.expect_err("auto read").kind(),
        io::ErrorKind::InvalidInput
    );
    let output = FileOutput::new(Box::new(io::sink()), Compression::Auto).map(drop);
    assert_eq!(
        output.expect_err("auto output").kind(),
        io::ErrorKind::InvalidInput
    );
}

#[test]
fn writes_after_flush_reach_the_stream() {
    for codec in all_modes() {
        let mut writer = codec.writer(Vec::new()).expect("writer");
        writer.write_all(b"before flush\n").expect("write");
        writer.flush().expect("flush");
        writer
            .write_all(b"after flush\n")
            .expect("write after flush");
        writer.flush().expect("second flush");
        let bytes = writer.finish().expect("finish");
        assert_eq!(
            decode(codec, bytes).expect("decode"),
            b"before flush\nafter flush\n",
            "{codec}"
        );
    }
}

#[test]
fn file_output_round_trips_every_codec() {
    let lines: Vec<String> = (0..2000).map(|i| format!("record-{i:05}\n")).collect();
    for codec in all_modes() {
        let buf = SharedBuf::default();
        let mut out = FileOutput::new(Box::new(buf.clone()), codec).expect("output");
        lines.iter().enumerate().for_each(|(i, line)| {
            out.write_all(line.as_bytes()).expect("write");
            if i == 1000 {
                out.flush().expect("flush");
            }
        });
        out.close().expect("close");
        assert_eq!(
            decode(codec, buf.take()).expect("decode"),
            lines.concat().into_bytes(),
            "{codec}"
        );
    }
}

#[test]
fn file_output_close_surfaces_errors() {
    for codec in all_modes() {
        let mut out = FileOutput::new(Box::new(Broken), codec).expect("output");
        out.write_all(b"buffered").expect("buffered write");
        assert_eq!(
            out.close().expect_err("close must fail").to_string(),
            "disk full",
            "{codec}"
        );
    }
}

#[test]
fn empty_inputs_read_as_no_bytes() {
    for codec in all_modes() {
        assert_eq!(
            decode(codec, Vec::new()).expect("zero bytes"),
            b"",
            "{codec}"
        );

        let buf = SharedBuf::default();
        FileOutput::new(Box::new(buf.clone()), codec)
            .expect("output")
            .close()
            .expect("close");
        assert_eq!(
            decode(codec, buf.take()).expect("empty stream"),
            b"",
            "{codec}"
        );
    }
}

#[test]
fn gzip_reads_concatenated_members() {
    let member = |text: &[u8]| {
        let mut writer = Compression::Gzip.writer(Vec::new()).expect("writer");
        writer.write_all(text).expect("write");
        writer.finish().expect("finish")
    };
    let bytes = [member(b"first\n"), member(b"second\n")].concat();
    assert_eq!(
        decode(Compression::Gzip, bytes).expect("decode"),
        b"first\nsecond\n"
    );
}
