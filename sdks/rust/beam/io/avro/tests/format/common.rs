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

//! Shared helpers: an independent Avro encoder / container parser and fixtures.

use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

use arrow_io::RowCodec;
use avro_io::avroio::{AvroRecordReader, AvroSink};
use beam::schema::BeamRow;
use beam::transforms::sdf::{OffsetRange, OffsetRangeTracker};
use chrono::{DateTime, NaiveDate, Utc};
use file::filesystem::LocalFileSystem;
use file::sink::FileSink;
use std::collections::BTreeMap;

// ---------------------------------------------------------------------------
// Independent Avro binary encoding / container parsing helpers
// ---------------------------------------------------------------------------

/// Avro `long`/`int`: zig-zag then base-128 varint.
pub(crate) fn long(v: i64) -> Vec<u8> {
    let mut n = ((v << 1) ^ (v >> 63)) as u64;
    let mut out = Vec::new();
    loop {
        let byte = (n & 0x7f) as u8;
        n >>= 7;
        if n == 0 {
            out.push(byte);
            return out;
        }
        out.push(byte | 0x80);
    }
}

/// Avro `bytes`/`string`: length as a long, then the raw bytes.
pub(crate) fn bytes(b: &[u8]) -> Vec<u8> {
    let mut out = long(b.len() as i64);
    out.extend_from_slice(b);
    out
}

pub(crate) fn read_long(buf: &[u8], pos: &mut usize) -> i64 {
    let mut n: u64 = 0;
    let mut shift = 0;
    loop {
        let byte = buf[*pos];
        *pos += 1;
        n |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return ((n >> 1) as i64) ^ -((n & 1) as i64);
        }
        shift += 7;
    }
}

pub(crate) fn read_bytes<'a>(buf: &'a [u8], pos: &mut usize) -> &'a [u8] {
    let len = usize::try_from(read_long(buf, pos)).unwrap();
    let out = &buf[*pos..*pos + len];
    *pos += len;
    out
}

pub(crate) struct OcfHeader {
    pub(crate) metadata: BTreeMap<String, Vec<u8>>,
    pub(crate) sync: [u8; 16],
    pub(crate) len: usize,
}

impl OcfHeader {
    pub(crate) fn meta(&self, key: &str) -> &str {
        std::str::from_utf8(&self.metadata[key]).unwrap()
    }
}

/// Parses an object container header per the Avro 1.11 specification.
pub(crate) fn parse_header(buf: &[u8]) -> OcfHeader {
    assert_eq!(&buf[..4], b"Obj\x01", "bad magic");
    let mut pos = 4;
    let mut metadata = BTreeMap::new();
    loop {
        let mut count = read_long(buf, &mut pos);
        if count == 0 {
            break;
        }
        if count < 0 {
            count = -count;
            let _block_bytes = read_long(buf, &mut pos);
        }
        for _ in 0..count {
            let key = String::from_utf8(read_bytes(buf, &mut pos).to_vec()).unwrap();
            let value = read_bytes(buf, &mut pos).to_vec();
            metadata.insert(key, value);
        }
    }
    let sync: [u8; 16] = buf[pos..pos + 16].try_into().unwrap();
    OcfHeader {
        metadata,
        sync,
        len: pos + 16,
    }
}

/// Splits the data blocks following the header into `(count, data)` pairs,
/// checking every block ends with the header's sync marker.
pub(crate) fn parse_blocks(buf: &[u8], header: &OcfHeader) -> Vec<(i64, Vec<u8>)> {
    let mut pos = header.len;
    let mut blocks = Vec::new();
    while pos < buf.len() {
        let count = read_long(buf, &mut pos);
        let data = read_bytes(buf, &mut pos).to_vec();
        assert_eq!(&buf[pos..pos + 16], &header.sync, "block sync marker");
        pos += 16;
        blocks.push((count, data));
    }
    blocks
}

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

pub(crate) struct TempDir(PathBuf);

impl TempDir {
    pub(crate) fn new(name: &str) -> Self {
        static COUNTER: AtomicU32 = AtomicU32::new(0);
        let path = std::env::temp_dir().join(format!(
            "beam_avro_format_{name}_{}_{}",
            std::process::id(),
            COUNTER.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir_all(&path).unwrap();
        Self(path)
    }

    pub(crate) fn file(&self, name: &str) -> String {
        self.0.join(name).to_str().unwrap().to_string()
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

/// An in-memory output whose bytes remain readable after the writer is dropped.
#[derive(Clone, Default)]
pub(crate) struct SharedBuf(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for SharedBuf {
    fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(b);
        Ok(b.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

pub(crate) fn write_to_bytes<T: 'static>(sink: &AvroSink<T>, items: &[T]) -> Vec<u8> {
    let buf = SharedBuf::default();
    let mut writer = sink.open(Box::new(buf.clone())).unwrap();
    for item in items {
        writer.write(item).unwrap();
    }
    writer.finish().unwrap();
    buf.0.lock().unwrap().clone()
}

pub(crate) fn read_all<T: 'static>(codec: Arc<dyn RowCodec<T>>, path: &str) -> Vec<T> {
    let len = i64::try_from(fs::metadata(path).unwrap().len()).unwrap();
    let reader = AvroRecordReader::new(codec, 100);
    let tracker = OffsetRangeTracker::new(OffsetRange::new(0, len));
    let mut out = Vec::new();
    reader
        .read_with_tracker(&LocalFileSystem::new(), path, &tracker, |item| {
            out.push(item);
            Ok(())
        })
        .unwrap();
    tracker.check_done().unwrap();
    out
}

#[derive(Debug, Clone, PartialEq, BeamRow)]
pub(crate) struct Simple {
    pub(crate) id: i64,
    pub(crate) name: String,
    pub(crate) score: Option<f64>,
    pub(crate) day: NaiveDate,
    pub(crate) at: DateTime<Utc>,
}

pub(crate) fn simple() -> Simple {
    Simple {
        id: 1,
        name: "a".into(),
        score: None,
        day: NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
        at: DateTime::from_timestamp(0, 0).unwrap(),
    }
}
