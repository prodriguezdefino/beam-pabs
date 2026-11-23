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

//! Worker-level row-group and tail-range caches for Iceberg Parquet reads.
//!
//! The caches are shared by the whole process. This binary isolates cache tests from other
//! reads so no unrelated test evicts entries while read counts are checked.

mod common;

use std::fs;
use std::io::{Read, Seek, SeekFrom};
use std::ops::Range;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use beam::transforms::sdf::{OffsetRange, OffsetRangeTracker, RestrictionTracker};
use beam_io_arrow::FieldSelection;
use beam_io_iceberg::catalog::file_io;
use beam_io_iceberg::read::plan::plan_tasks;
use beam_io_iceberg::read::reader::{read_task_with_tracker, row_groups};
use beam_io_iceberg::storage::CachedTailRead;
use bytes::Bytes;
use futures::stream::BoxStream;
use iceberg::io::{
    FileIOBuilder, FileMetadata, FileRead, FileWrite, InputFile, OutputFile, Storage,
    StorageConfig, StorageFactory,
};
use iceberg::{Error, ErrorKind};
use serde::{Deserialize, Serialize};
use tempfile::TempDir;

use common::{CountingFileSystem, Fixture, batch_ids, footer_reads, options};

fn unsupported() -> Error {
    Error::new(ErrorKind::FeatureUnsupported, "unused in test")
}

/// Local `FileRead` that records every requested byte range.
struct CountingFileRead {
    path: PathBuf,
    reads: Arc<Mutex<Vec<Range<u64>>>>,
}

#[async_trait::async_trait]
impl FileRead for CountingFileRead {
    async fn read(&self, range: Range<u64>) -> iceberg::Result<Bytes> {
        self.reads.lock().unwrap().push(range.clone());
        let mut file = fs::File::open(&self.path)
            .map_err(|e| Error::new(ErrorKind::Unexpected, e.to_string()))?;
        file.seek(SeekFrom::Start(range.start))
            .map_err(|e| Error::new(ErrorKind::Unexpected, e.to_string()))?;
        let len = usize::try_from(range.end.saturating_sub(range.start)).unwrap();
        let mut buf = vec![0u8; len];
        file.read_exact(&mut buf)
            .map_err(|e| Error::new(ErrorKind::Unexpected, e.to_string()))?;
        Ok(Bytes::from(buf))
    }
}

/// Local `FileRead` that records each range and returns the bytes that the file has, which
/// can be fewer than the range asks for.
struct ShortFileRead {
    path: PathBuf,
    reads: Arc<Mutex<Vec<Range<u64>>>>,
}

#[async_trait::async_trait]
impl FileRead for ShortFileRead {
    async fn read(&self, range: Range<u64>) -> iceberg::Result<Bytes> {
        self.reads.lock().unwrap().push(range.clone());
        let bytes =
            fs::read(&self.path).map_err(|e| Error::new(ErrorKind::Unexpected, e.to_string()))?;
        let len = bytes.len() as u64;
        let (start, end) = (range.start.min(len), range.end.min(len));
        Ok(Bytes::copy_from_slice(&bytes[start as usize..end as usize]))
    }
}

/// Direct local storage without `CachedTailRead`, to isolate row-group cache hits.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct DirectCountingStorage {
    #[serde(skip)]
    reads: Arc<AtomicUsize>,
}

struct DirectCountingRead {
    path: PathBuf,
    reads: Arc<AtomicUsize>,
}

#[async_trait::async_trait]
impl FileRead for DirectCountingRead {
    async fn read(&self, range: Range<u64>) -> iceberg::Result<Bytes> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        let mut file = fs::File::open(&self.path)
            .map_err(|e| Error::new(ErrorKind::Unexpected, e.to_string()))?;
        file.seek(SeekFrom::Start(range.start))
            .map_err(|e| Error::new(ErrorKind::Unexpected, e.to_string()))?;
        let len = usize::try_from(range.end.saturating_sub(range.start)).unwrap();
        let mut buf = vec![0u8; len];
        file.read_exact(&mut buf)
            .map_err(|e| Error::new(ErrorKind::Unexpected, e.to_string()))?;
        Ok(Bytes::from(buf))
    }
}

#[async_trait::async_trait]
#[typetag::serde]
impl Storage for DirectCountingStorage {
    async fn exists(&self, path: &str) -> iceberg::Result<bool> {
        Ok(PathBuf::from(path).exists())
    }

    async fn metadata(&self, path: &str) -> iceberg::Result<FileMetadata> {
        let size = fs::metadata(path)
            .map_err(|e| Error::new(ErrorKind::Unexpected, e.to_string()))?
            .len();
        Ok(FileMetadata { size })
    }

    async fn read(&self, path: &str) -> iceberg::Result<Bytes> {
        let bytes = fs::read(path).map_err(|e| Error::new(ErrorKind::Unexpected, e.to_string()))?;
        Ok(Bytes::from(bytes))
    }

    async fn reader(&self, path: &str) -> iceberg::Result<Box<dyn FileRead>> {
        Ok(Box::new(DirectCountingRead {
            path: PathBuf::from(path),
            reads: Arc::clone(&self.reads),
        }))
    }

    async fn write(&self, _path: &str, _bs: Bytes) -> iceberg::Result<()> {
        Err(unsupported())
    }

    async fn writer(&self, _path: &str) -> iceberg::Result<Box<dyn FileWrite>> {
        Err(unsupported())
    }

    async fn delete(&self, _path: &str) -> iceberg::Result<()> {
        Err(unsupported())
    }

    async fn delete_prefix(&self, _path: &str) -> iceberg::Result<()> {
        Err(unsupported())
    }

    async fn delete_stream(&self, _paths: BoxStream<'static, String>) -> iceberg::Result<()> {
        Err(unsupported())
    }

    fn new_input(&self, path: &str) -> iceberg::Result<InputFile> {
        Ok(InputFile::new(Arc::new(self.clone()), path.to_string()))
    }

    fn new_output(&self, path: &str) -> iceberg::Result<OutputFile> {
        Ok(OutputFile::new(Arc::new(self.clone()), path.to_string()))
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct DirectCountingStorageFactory {
    #[serde(skip)]
    reads: Arc<AtomicUsize>,
}

#[typetag::serde]
impl StorageFactory for DirectCountingStorageFactory {
    fn build(&self, _config: &StorageConfig) -> iceberg::Result<Arc<dyn Storage>> {
        Ok(Arc::new(DirectCountingStorage {
            reads: Arc::clone(&self.reads),
        }))
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn splits_of_a_multi_row_group_file_match_rows_and_read_the_footer_once() {
    let fixture = Fixture::new(50_000).await;
    let files = fixture
        .append_batches((0..8).map(|g| (g * 1_000)..((g + 1) * 1_000)), 36)
        .await;
    let [data_file] = files.as_slice() else {
        panic!("expected one data file: {files:?}");
    };
    let uri = data_file.file_path();
    let local_path = CountingFileSystem::local(uri);
    let file_size = data_file.file_size_in_bytes();
    assert!(
        file_size > 64 * 1024,
        "test file must exceed 64 KiB tail window: {file_size}"
    );

    let tasks = plan_tasks(&fixture.catalog, &options(), &FieldSelection::All)
        .await
        .unwrap();
    let [task] = tasks.as_slice() else {
        panic!("expected one task, got {}", tasks.len());
    };

    let io = file_io([]);
    let counting = CountingFileSystem::get();
    let first_groups = row_groups(&io, uri, file_size).await.unwrap();
    assert!(
        first_groups.len() >= 8,
        "expected >= 8 row groups, got {}",
        first_groups.len()
    );
    assert_eq!(footer_reads(local_path, &counting.reads(uri)), 1);

    let end = i64::try_from(file_size).unwrap();
    let step = end / 5 + 1;
    let mut all_ids = Vec::new();
    let mut total_rows = 0usize;
    let mut batch_count = 0usize;
    for i in 0..5 {
        let split_start = i * step;
        let split_end = ((i + 1) * step).min(end);
        let tracker = OffsetRangeTracker::new(OffsetRange::new(split_start, split_end));
        let (rows, _) = read_task_with_tracker(&io, task, &tracker, 512, |batch| {
            batch_count += 1;
            all_ids.extend(batch_ids(&batch));
            Ok(())
        })
        .await
        .unwrap();
        tracker.check_done().unwrap();
        total_rows += rows;
    }

    all_ids.sort_unstable();
    assert_eq!(all_ids, (0..8_000).collect::<Vec<_>>());
    assert_eq!(total_rows, 8_000);
    assert!(
        batch_count > first_groups.len(),
        "expected streamed batches: {batch_count}"
    );
    // Across row_groups() and all row groups across all 5 splits, only 1 footer/tail read runs.
    assert_eq!(footer_reads(local_path, &counting.reads(uri)), 1);

    // A split with no row-group start performs zero storage reads.
    let reads_before = counting.reads(uri).len();
    let empty_tracker = OffsetRangeTracker::new(OffsetRange::new(end - 8, end));
    let (empty_rows, empty_bytes) = read_task_with_tracker(&io, task, &empty_tracker, 512, |_| {
        panic!("empty split must emit no batches");
    })
    .await
    .unwrap();
    empty_tracker.check_done().unwrap();
    assert_eq!((empty_rows, empty_bytes), (0, 0));
    assert_eq!(counting.reads(uri).len(), reads_before);

    // Row-group discovery hits the worker cache and returns the same Arc without reading storage.
    let cached_groups = row_groups(&io, uri, file_size).await.unwrap();
    assert!(Arc::ptr_eq(&first_groups, &cached_groups));
    assert_eq!(counting.reads(uri).len(), reads_before);
}

#[tokio::test(flavor = "multi_thread")]
async fn row_group_cache_hits_across_splits_and_reloads_when_file_size_changes() {
    let fixture_a = Fixture::new(50_000).await;
    let files_a = fixture_a
        .append_batches((0..4).map(|g| (g * 1_000)..((g + 1) * 1_000)), 36)
        .await;
    let fixture_b = Fixture::new(50_000).await;
    let files_b = fixture_b
        .append_batches((0..7).map(|g| (g * 1_000)..((g + 1) * 1_000)), 36)
        .await;

    let dir = TempDir::new().unwrap();
    let target = dir.path().join("reused.parquet");
    let target_str = target.to_str().unwrap().to_string();

    fs::copy(CountingFileSystem::local(files_a[0].file_path()), &target).unwrap();
    let size_a = fs::metadata(&target).unwrap().len();

    let reads = Arc::new(AtomicUsize::new(0));
    let io = FileIOBuilder::new(Arc::new(DirectCountingStorageFactory {
        reads: Arc::clone(&reads),
    }))
    .build();

    let first = row_groups(&io, &target_str, size_a).await.unwrap();
    assert!(first.len() >= 4);
    assert_eq!(reads.load(Ordering::SeqCst), 1);

    let second = row_groups(&io, &target_str, size_a).await.unwrap();
    assert!(Arc::ptr_eq(&first, &second));
    assert_eq!(reads.load(Ordering::SeqCst), 1);

    // Overwriting the same path with a different file size misses the cache and decodes again.
    fs::copy(CountingFileSystem::local(files_b[0].file_path()), &target).unwrap();
    let size_b = fs::metadata(&target).unwrap().len();
    assert_ne!(size_a, size_b);

    let third = row_groups(&io, &target_str, size_b).await.unwrap();
    assert!(!Arc::ptr_eq(&first, &third));
    assert!(third.len() > first.len());
    assert_eq!(reads.load(Ordering::SeqCst), 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn tail_cache_serves_subranges_and_extends_on_wider_tail() {
    let fixture = Fixture::new(50_000).await;
    let files = fixture
        .append_batches((0..4).map(|g| (g * 1_000)..((g + 1) * 1_000)), 36)
        .await;
    let local_path = PathBuf::from(CountingFileSystem::local(files[0].file_path()));
    let all_bytes = fs::read(&local_path).unwrap();
    let size = all_bytes.len() as u64;
    assert!(size > 64 * 1024 + 2048);

    let trailer: [u8; 4] = all_bytes[all_bytes.len() - 8..all_bytes.len() - 4]
        .try_into()
        .unwrap();
    let footer_len = u64::from(u32::from_le_bytes(trailer));
    let footer_start = size - 8 - footer_len;
    let tail_start = size - 64 * 1024;

    // Row-group discovery knows the file size, so its 64 KiB tail read fills the cache.
    let path = files[0].file_path().to_string();
    row_groups(&file_io([]), &path, size).await.unwrap();

    let recorded = Arc::new(Mutex::new(Vec::new()));
    let reader = CachedTailRead::new(
        path,
        CountingFileRead {
            path: local_path,
            reads: Arc::clone(&recorded),
        },
    );

    // Sub-ranges inside the cached tail serve straight from cached Bytes with zero I/O.
    let tail = reader.read(tail_start..size).await.unwrap();
    assert_eq!(&tail[..], &all_bytes[tail_start as usize..]);
    let magic = reader.read((size - 8)..size).await.unwrap();
    assert_eq!(&magic[..], &all_bytes[(size - 8) as usize..]);
    let footer = reader.read(footer_start..(size - 8)).await.unwrap();
    assert_eq!(
        &footer[..],
        &all_bytes[footer_start as usize..(size - 8) as usize]
    );
    assert!(recorded.lock().unwrap().is_empty());

    // A read outside the cached tail goes to the underlying FileRead, and does not fill the
    // cache: the reader does not know the file size.
    let head = reader.read(0..1024).await.unwrap();
    assert_eq!(&head[..], &all_bytes[..1024]);
    assert_eq!(recorded.lock().unwrap().len(), 1);

    // A wider tail fetches only the missing prefix and caches the extended tail.
    let wider_start = tail_start - 1024;
    let wider = reader.read(wider_start..size).await.unwrap();
    assert_eq!(&wider[..], &all_bytes[wider_start as usize..]);
    assert_eq!(recorded.lock().unwrap().len(), 2);
    assert_eq!(recorded.lock().unwrap()[1], wider_start..tail_start);

    let sub_wider = reader.read((tail_start - 512)..size).await.unwrap();
    assert_eq!(&sub_wider[..], &all_bytes[(tail_start - 512) as usize..]);
    assert_eq!(recorded.lock().unwrap().len(), 2);
}

/// A file that is shorter than its cached tail says fails the read with an error, and the
/// stale tail goes, so the next read goes to the file.
#[tokio::test(flavor = "multi_thread")]
async fn a_short_read_before_a_cached_tail_fails_and_drops_the_tail() {
    let fixture = Fixture::new(50_000).await;
    let files = fixture
        .append_batches((0..4).map(|g| (g * 1_000)..((g + 1) * 1_000)), 36)
        .await;
    let local_path = PathBuf::from(CountingFileSystem::local(files[0].file_path()));
    let size = fs::metadata(&local_path).unwrap().len();
    let path = files[0].file_path().to_string();
    row_groups(&file_io([]), &path, size).await.unwrap();

    // The file is now 4 KiB shorter: a read below the cached tail comes back short.
    let tail_start = size - 64 * 1024;
    let all_bytes = fs::read(&local_path).unwrap();
    let dir = TempDir::new().unwrap();
    let truncated = dir.path().join("truncated.parquet");
    fs::write(&truncated, &all_bytes[..(tail_start - 4096) as usize]).unwrap();
    let recorded = Arc::new(Mutex::new(Vec::new()));
    let reader = CachedTailRead::new(
        path,
        ShortFileRead {
            path: truncated,
            reads: Arc::clone(&recorded),
        },
    );
    let error = reader.read((tail_start - 1024)..size).await.unwrap_err();
    assert!(
        error.to_string().contains("before the cached tail"),
        "{error}"
    );

    // The tail is gone, so a read inside it goes to the file.
    let _ = reader.read((size - 8)..size).await;
    assert_eq!(recorded.lock().unwrap().len(), 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn overwriting_a_file_with_another_size_reads_the_new_rows() {
    let fixture_a = Fixture::new(50_000).await;
    let files_a = fixture_a.append_batches(std::iter::once(0..200), 36).await;
    let tasks_a = plan_tasks(&fixture_a.catalog, &options(), &FieldSelection::All)
        .await
        .unwrap();
    let io = file_io([]);
    let end_a = i64::try_from(files_a[0].file_size_in_bytes()).unwrap();
    let tracker_a = OffsetRangeTracker::new(OffsetRange::new(0, end_a));
    let mut ids_a = Vec::new();
    read_task_with_tracker(&io, &tasks_a[0], &tracker_a, 256, |batch| {
        ids_a.extend(batch_ids(&batch));
        Ok(())
    })
    .await
    .unwrap();
    tracker_a.check_done().unwrap();
    ids_a.sort_unstable();
    assert_eq!(ids_a, (0..200).collect::<Vec<_>>());

    // Write a larger file and overwrite file A's path on disk, updating the task metadata.
    let fixture_b = Fixture::new(50_000).await;
    let files_b = fixture_b
        .append_batches((0..4).map(|g| (g * 500)..((g + 1) * 500)), 36)
        .await;
    let tasks_b = plan_tasks(&fixture_b.catalog, &options(), &FieldSelection::All)
        .await
        .unwrap();
    assert_ne!(
        files_a[0].file_size_in_bytes(),
        files_b[0].file_size_in_bytes()
    );
    fs::copy(
        CountingFileSystem::local(files_b[0].file_path()),
        CountingFileSystem::local(files_a[0].file_path()),
    )
    .unwrap();
    let mut patched: serde_json::Value = serde_json::from_slice(&tasks_b[0]).unwrap();
    patched["data_file_path"] = serde_json::Value::String(files_a[0].file_path().to_string());
    let patched_bytes = serde_json::to_vec(&patched).unwrap();

    let end_b = i64::try_from(files_b[0].file_size_in_bytes()).unwrap();
    let tracker_b = OffsetRangeTracker::new(OffsetRange::new(0, end_b));
    let mut ids_b = Vec::new();
    read_task_with_tracker(&io, &patched_bytes, &tracker_b, 256, |batch| {
        ids_b.extend(batch_ids(&batch));
        Ok(())
    })
    .await
    .unwrap();
    tracker_b.check_done().unwrap();
    ids_b.sort_unstable();
    assert_eq!(ids_b, (0..2_000).collect::<Vec<_>>());
}
