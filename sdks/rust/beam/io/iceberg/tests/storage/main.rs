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

//! Iceberg storage on the SDK file systems, on a local temp directory.

use std::sync::Arc;

use bytes::Bytes;
use iceberg::io::{FileIO, FileIOBuilder, StorageConfig, StorageFactory};
use iceberg_io::storage::{FileSystemStorage, FileSystemStorageFactory, GCS_TOKEN};
use tempfile::TempDir;

fn file_io() -> FileIO {
    FileIOBuilder::new(Arc::new(FileSystemStorageFactory)).build()
}

fn path(dir: &TempDir, name: &str) -> String {
    format!("file://{}/{name}", dir.path().display())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_written_file_reads_back_with_its_size() {
    let dir = TempDir::new().expect("a temp dir");
    let (io, file) = (file_io(), path(&dir, "data/a.bin"));
    std::fs::create_dir_all(dir.path().join("data")).expect("the data dir");
    io.new_output(&file)
        .expect("an output file")
        .write(Bytes::from_static(b"hello iceberg"))
        .await
        .expect("the write succeeds");
    let input = io.new_input(&file).expect("an input file");
    assert!(input.exists().await.expect("exists succeeds"));
    assert_eq!(input.metadata().await.expect("metadata").size, 13);
    assert_eq!(
        input.read().await.expect("read"),
        Bytes::from_static(b"hello iceberg")
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_streamed_file_reads_back_by_range() {
    let dir = TempDir::new().expect("a temp dir");
    let (io, file) = (file_io(), path(&dir, "b.bin"));
    let mut writer = io
        .new_output(&file)
        .expect("an output file")
        .writer()
        .await
        .expect("a writer");
    writer
        .write(Bytes::from_static(b"0123"))
        .await
        .expect("first part");
    writer
        .write(Bytes::from_static(b"4567"))
        .await
        .expect("second part");
    writer.close().await.expect("close");
    writer
        .write(Bytes::from_static(b"8"))
        .await
        .expect_err("a closed file takes no more bytes");

    let reader = io
        .new_input(&file)
        .expect("an input file")
        .reader()
        .await
        .expect("a reader");
    assert_eq!(
        reader.read(2..6).await.expect("a range"),
        Bytes::from_static(b"2345")
    );
    assert_eq!(
        reader.read(3..3).await.expect("an empty range"),
        Bytes::new()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn delete_removes_one_file_and_delete_prefix_removes_a_tree() {
    let dir = TempDir::new().expect("a temp dir");
    let io = file_io();
    std::fs::create_dir_all(dir.path().join("tree/sub")).expect("the tree");
    let files = ["keep.bin", "tree/a.bin", "tree/sub/b.bin"].map(|name| path(&dir, name));
    for file in &files {
        io.new_output(file)
            .expect("an output file")
            .write(Bytes::from_static(b"x"))
            .await
            .expect("the write succeeds");
    }

    io.delete(&files[0]).await.expect("delete");
    assert!(!io.exists(&files[0]).await.expect("exists"));

    io.delete_prefix(path(&dir, "tree"))
        .await
        .expect("delete prefix");
    for file in &files[1..] {
        assert!(!io.exists(file).await.expect("exists"), "{file}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_missing_file_does_not_exist() {
    let dir = TempDir::new().expect("a temp dir");
    assert!(
        !file_io()
            .exists(path(&dir, "missing"))
            .await
            .expect("exists")
    );
}

#[test]
fn the_factory_uses_the_vended_gcs_token_when_the_props_hold_one() {
    let plain = StorageConfig::new();
    let vended = StorageConfig::new().with_prop(GCS_TOKEN, "token-value");
    let storage_without = FileSystemStorageFactory.build(&plain).expect("a storage");
    let storage_with = FileSystemStorageFactory.build(&vended).expect("a storage");
    assert!(!format!("{storage_without:?}").contains("Gcs"));
    assert!(format!("{storage_with:?}").contains("GcsFileSystem"));
    assert!(!format!("{storage_with:?}").contains("token-value"));
    assert!(
        FileSystemStorage::new(Some("token-value"))
            .expect("a storage")
            .has_gcs_token()
    );
}

#[test]
fn a_token_that_is_not_a_header_value_is_rejected() {
    FileSystemStorage::new(Some("bad\ntoken")).expect_err("a newline is not allowed");
}
