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

//! The reads that each split makes for the container header.

use std::fs;
use std::io::{self, Read};
use std::sync::{Arc, Mutex};
use std::time::SystemTime;

use beam::schema::BeamRow;
use beam::transforms::sdf::{OffsetRange, OffsetRangeTracker, RestrictionTracker};
use beam_io_arrow::BeamRowCodec;
use beam_io_avro::avroio::{AvroRecordReader, AvroSink};
use beam_io_file::{FileSystem, LocalFileSystem};

use crate::common::{TempDir, bytes, long, write_file};

/// A local file system that records the `(start, length)` of each ranged read.
#[derive(Debug, Default)]
struct CountingFileSystem {
    reads: Mutex<Vec<(u64, u64)>>,
}

impl CountingFileSystem {
    /// The number of reads that start at the beginning of the file.
    fn header_reads(&self) -> usize {
        self.reads
            .lock()
            .unwrap()
            .iter()
            .filter(|(start, _)| *start == 0)
            .count()
    }
}

impl FileSystem for CountingFileSystem {
    fn match_files(&self, pattern: &str) -> io::Result<Vec<String>> {
        LocalFileSystem::new().match_files(pattern)
    }
    fn open_read(&self, path: &str) -> io::Result<Box<dyn Read + Send>> {
        self.open_read_range(path, 0, 0)
    }
    fn open_read_range(
        &self,
        path: &str,
        start: u64,
        length: u64,
    ) -> io::Result<Box<dyn Read + Send>> {
        self.reads.lock().unwrap().push((start, length));
        LocalFileSystem::new().open_read_range(path, start, length)
    }
    fn open_write(&self, path: &str) -> io::Result<Box<dyn io::Write + Send>> {
        LocalFileSystem::new().open_write(path)
    }
    fn open_append(&self, path: &str) -> io::Result<Box<dyn io::Write + Send>> {
        LocalFileSystem::new().open_append(path)
    }
    fn size(&self, path: &str) -> io::Result<u64> {
        LocalFileSystem::new().size(path)
    }
    fn remove(&self, path: &str) -> io::Result<()> {
        LocalFileSystem::new().remove(path)
    }
    fn rename(&self, old_path: &str, new_path: &str) -> io::Result<()> {
        LocalFileSystem::new().rename(old_path, new_path)
    }
    fn copy(&self, from: &str, to: &str) -> io::Result<()> {
        LocalFileSystem::new().copy(from, to)
    }
    fn last_modified(&self, path: &str) -> io::Result<SystemTime> {
        LocalFileSystem::new().last_modified(path)
    }
}

#[derive(Debug, Clone, PartialEq, BeamRow)]
struct Id {
    id: i64,
}

/// Reads `path` in `splits` equal splits through `fs` and returns the ids in order.
fn read_in_splits(fs: &CountingFileSystem, path: &str, splits: i64) -> Vec<i64> {
    let len = i64::try_from(fs::metadata(path).unwrap().len()).unwrap();
    let step = len / splits + 1;
    let reader = AvroRecordReader::new(Arc::new(BeamRowCodec::<Id>::new()), 100);
    let mut ids = Vec::new();
    for i in 0..splits {
        let tracker =
            OffsetRangeTracker::new(OffsetRange::new(i * step, ((i + 1) * step).min(len)));
        reader
            .read_with_tracker(fs, path, &tracker, |Id { id }| {
                ids.push(id);
                Ok(())
            })
            .unwrap();
        tracker.check_done().unwrap();
    }
    ids
}

#[test]
fn each_split_reads_the_header_once() {
    let dir = TempDir::new("header_reads");
    let path = dir.file("ids.avro");
    let items: Vec<Id> = (0..2_000).map(|id| Id { id }).collect();
    write_file(&AvroSink::<Id>::new().with_block_size(100), &path, &items);
    let fs = CountingFileSystem::default();
    assert_eq!(
        read_in_splits(&fs, &path, 4),
        (0..2_000).collect::<Vec<_>>()
    );
    assert_eq!(fs.header_reads(), 4);
}

/// A container whose header is longer than the first read: a 100 KiB metadata entry.
fn container_with_large_header(ids: &[i64]) -> Vec<u8> {
    let schema = r#"{"type":"record","name":"Id","fields":[{"name":"id","type":"long"}]}"#;
    let sync = [7u8; 16];
    let mut out = b"Obj\x01".to_vec();
    out.extend(long(3));
    out.extend(bytes(b"avro.schema"));
    out.extend(bytes(schema.as_bytes()));
    out.extend(bytes(b"avro.codec"));
    out.extend(bytes(b"null"));
    out.extend(bytes(b"padding"));
    out.extend(bytes(&vec![b'x'; 100 * 1024]));
    out.push(0x00);
    out.extend(sync);
    let data: Vec<u8> = ids.iter().flat_map(|id| long(*id)).collect();
    out.extend(long(i64::try_from(ids.len()).unwrap()));
    out.extend(bytes(&data));
    out.extend(sync);
    out
}

#[test]
fn a_header_longer_than_the_first_read_is_read_whole() {
    let dir = TempDir::new("large_header");
    let path = dir.file("ids.avro");
    fs::write(&path, container_with_large_header(&[3, 1, 2])).unwrap();
    let fs = CountingFileSystem::default();
    assert_eq!(read_in_splits(&fs, &path, 1), [3, 1, 2]);
}
