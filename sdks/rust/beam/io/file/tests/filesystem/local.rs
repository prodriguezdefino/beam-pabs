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

#![expect(
    clippy::unwrap_used,
    reason = "test helpers; a failure is a test failure"
)]

use file::filesystem::{
    FileSystem, LocalFileSystem, exists, read_to_bytes, read_to_string, write_bytes,
};
use std::fs;
use std::io::{Read, Write};
use std::path::Path;
use std::time::{Duration, SystemTime};

#[test]
fn test_local_filesystem_list_no_matches() {
    let fs = LocalFileSystem::new();
    let temp_dir = std::env::temp_dir().join(format!("beam_no_matches_{}", std::process::id()));
    fs::create_dir_all(&temp_dir).unwrap();

    let pattern = format!("{}/non_existent_*.txt", temp_dir.to_str().unwrap());
    let matches = fs.match_files(&pattern).unwrap();
    assert!(
        matches.is_empty(),
        "Expected empty matches for non-existent glob"
    );

    let _ = fs::remove_dir_all(&temp_dir);
}

#[test]
fn test_local_filesystem_glob_matching() {
    let temp_dir = std::env::temp_dir().join(format!("beam_glob_test_{}", std::process::id()));
    let sub_dir = temp_dir.join("subdir");
    fs::create_dir_all(&sub_dir).unwrap();

    fs::write(temp_dir.join("file1.txt"), "1").unwrap();
    fs::write(temp_dir.join("file2.txt"), "2").unwrap();
    fs::write(temp_dir.join("file3.log"), "3").unwrap();
    fs::write(sub_dir.join("subfile.txt"), "sub").unwrap();

    let fs = LocalFileSystem::new();

    let pattern = format!("{}/file*.txt", temp_dir.to_str().unwrap());
    let mut matches = fs.match_files(&pattern).unwrap();
    matches.sort();
    assert_eq!(matches.len(), 2);
    assert!(matches[0].ends_with("file1.txt"));
    assert!(matches[1].ends_with("file2.txt"));

    let pattern_recursive = format!("{}/**/*.txt", temp_dir.to_str().unwrap());
    let matches_rec = fs.match_files(&pattern_recursive).unwrap();
    let base = temp_dir.to_str().unwrap();
    assert_eq!(
        matches_rec,
        vec![
            format!("{base}/file1.txt"),
            format!("{base}/file2.txt"),
            format!("{base}/subdir/subfile.txt"),
        ]
    );

    let pattern_q = format!("{}/file?.log", temp_dir.to_str().unwrap());
    let matches_q = fs.match_files(&pattern_q).unwrap();
    assert_eq!(matches_q.len(), 1);
    assert!(matches_q[0].ends_with("file3.log"));

    let _ = fs::remove_dir_all(&temp_dir);
}

#[test]
fn test_local_filesystem_last_modified() {
    let fs = LocalFileSystem::new();
    let temp_dir = std::env::temp_dir().join(format!("beam_mod_test_{}", std::process::id()));
    let file_path = temp_dir.join("timestamp.txt");
    let file_str = file_path.to_str().unwrap();

    fs::create_dir_all(&temp_dir).unwrap();
    let before = SystemTime::now() - Duration::from_secs(2);
    fs.open_write(file_str)
        .unwrap()
        .write_all(b"content")
        .unwrap();
    let after = SystemTime::now() + Duration::from_secs(2);

    let mod_time = fs.last_modified(file_str).unwrap();
    assert!(
        mod_time >= before && mod_time <= after,
        "Modified time out of bounds"
    );

    let _ = fs::remove_dir_all(&temp_dir);
}

#[test]
fn test_local_filesystem_rename() {
    let fs = LocalFileSystem::new();
    let temp_dir = std::env::temp_dir().join(format!("beam_rename_test_{}", std::process::id()));
    let old_path = temp_dir.join("old_name.txt");
    let new_path = temp_dir.join("new_name.txt");
    let old_str = old_path.to_str().unwrap();
    let new_str = new_path.to_str().unwrap();

    fs::create_dir_all(&temp_dir).unwrap();
    fs.open_write(old_str)
        .unwrap()
        .write_all(b"rename test data")
        .unwrap();

    assert!(fs.exists(old_str).unwrap());
    assert!(!fs.exists(new_str).unwrap());

    fs.rename(old_str, new_str).unwrap();

    assert!(!fs.exists(old_str).unwrap());
    assert!(fs.exists(new_str).unwrap());

    let mut content = String::new();
    fs.open_read(new_str)
        .unwrap()
        .read_to_string(&mut content)
        .unwrap();
    assert_eq!(content, "rename test data");

    let _ = fs::remove_dir_all(&temp_dir);
}

#[test]
fn test_local_filesystem_copy() {
    let fs = LocalFileSystem::new();
    let temp_dir = std::env::temp_dir().join(format!("beam_copy_test_{}", std::process::id()));
    let src_path = temp_dir.join("source.txt");
    let dst_path = temp_dir.join("dest.txt");
    let src_str = src_path.to_str().unwrap();
    let dst_str = dst_path.to_str().unwrap();

    fs::create_dir_all(&temp_dir).unwrap();
    fs.open_write(src_str)
        .unwrap()
        .write_all(b"copy test data")
        .unwrap();

    fs.copy(src_str, dst_str).unwrap();

    assert!(fs.exists(src_str).unwrap());
    assert!(fs.exists(dst_str).unwrap());

    let mut content = String::new();
    fs.open_read(dst_str)
        .unwrap()
        .read_to_string(&mut content)
        .unwrap();
    assert_eq!(content, "copy test data");

    let _ = fs::remove_dir_all(&temp_dir);
}

#[test]
fn test_local_filesystem_remove() {
    let fs = LocalFileSystem::new();
    let temp_dir = std::env::temp_dir().join(format!("beam_remove_test_{}", std::process::id()));
    let file_path = temp_dir.join("to_delete.txt");
    let file_str = file_path.to_str().unwrap();

    fs::create_dir_all(&temp_dir).unwrap();
    fs.open_write(file_str)
        .unwrap()
        .write_all(b"delete me")
        .unwrap();

    assert!(fs.exists(file_str).unwrap());
    fs.remove(file_str).unwrap();
    assert!(!fs.exists(file_str).unwrap());

    let _ = fs::remove_dir_all(&temp_dir);
}

/// A fresh local scratch directory, removed on drop.
struct LocalScratch(std::path::PathBuf);

impl LocalScratch {
    fn new(tag: &str) -> Self {
        let dir = std::env::temp_dir().join(format!(
            "beam_fs_err_{tag}_{}_{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        Self(dir)
    }
    fn path(&self, rel: &str) -> String {
        self.0.join(rel).to_string_lossy().into_owned()
    }
}

impl Drop for LocalScratch {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

#[test]
fn test_local_filesystem_missing_paths_report_not_found() {
    let s = LocalScratch::new("missing");
    let lfs = LocalFileSystem::new();
    let missing = s.path("nope.txt");
    let kind = |r: std::io::Result<()>| r.unwrap_err().kind();

    assert_eq!(
        lfs.open_read(&missing).map(|_| ()).unwrap_err().kind(),
        std::io::ErrorKind::NotFound
    );
    assert_eq!(kind(lfs.remove(&missing)), std::io::ErrorKind::NotFound);
    assert_eq!(
        kind(lfs.rename(&missing, &s.path("other.txt"))),
        std::io::ErrorKind::NotFound
    );
    assert_eq!(
        kind(lfs.copy(&missing, &s.path("other.txt"))),
        std::io::ErrorKind::NotFound
    );
    assert_eq!(
        lfs.size(&missing).unwrap_err().kind(),
        std::io::ErrorKind::NotFound
    );
    assert!(!lfs.exists(&missing).unwrap());
    assert!(!Path::new(&s.path("other.txt")).exists());
}

#[test]
fn test_local_filesystem_open_append_preserves_and_creates() {
    let s = LocalScratch::new("append");
    let lfs = LocalFileSystem::new();
    let path = s.path("nested/dir/log.txt");

    // Creates the file (and its parents) when missing.
    lfs.open_append(&path).unwrap().write_all(b"one\n").unwrap();
    // Appends rather than truncating when present.
    lfs.open_append(&path).unwrap().write_all(b"two\n").unwrap();
    assert_eq!(fs::read_to_string(&path).unwrap(), "one\ntwo\n");

    // open_write, by contrast, truncates.
    lfs.open_write(&path)
        .unwrap()
        .write_all(b"three\n")
        .unwrap();
    assert_eq!(fs::read_to_string(&path).unwrap(), "three\n");
    assert_eq!(lfs.size(&path).unwrap(), 6);
}

#[test]
fn test_local_filesystem_open_read_range() {
    let s = LocalScratch::new("range");
    let lfs = LocalFileSystem::new();
    let path = s.path("r.txt");
    fs::write(&path, "0123456789").unwrap();

    let read = |start, len| {
        let mut out = String::new();
        lfs.open_read_range(&path, start, len)
            .unwrap()
            .read_to_string(&mut out)
            .unwrap();
        out
    };
    assert_eq!(read(0, 0), "0123456789", "length 0 means to end of file");
    assert_eq!(read(3, 4), "3456");
    assert_eq!(read(8, 0), "89");
    assert_eq!(read(8, 100), "89");
    assert_eq!(read(20, 0), "");
}

#[test]
fn test_local_filesystem_remove_dir() {
    let s = LocalScratch::new("rmdir");
    let lfs = LocalFileSystem::new();
    let dir = s.path("d");
    fs::create_dir_all(&dir).unwrap();
    fs::write(s.path("d/f"), "x").unwrap();

    // A non-empty directory is kept (and reported), so in-flight files survive.
    assert!(lfs.remove_dir(&dir).is_err());
    assert_eq!(fs::read_to_string(s.path("d/f")).unwrap(), "x");

    fs::remove_file(s.path("d/f")).unwrap();
    lfs.remove_dir(&dir).unwrap();
    assert!(!Path::new(&dir).exists());
    // Removing a directory that is already gone is not an error.
    lfs.remove_dir(&dir).unwrap();
}

#[test]
fn test_local_filesystem_rename_and_copy_create_target_parents() {
    let s = LocalScratch::new("parents");
    let lfs = LocalFileSystem::new();
    let src = s.path("src.txt");
    fs::write(&src, "data").unwrap();

    lfs.copy(&src, &s.path("a/b/copy.txt")).unwrap();
    lfs.rename(&src, &s.path("c/d/moved.txt")).unwrap();
    assert_eq!(fs::read_to_string(s.path("a/b/copy.txt")).unwrap(), "data");
    assert_eq!(fs::read_to_string(s.path("c/d/moved.txt")).unwrap(), "data");
    assert!(!Path::new(&src).exists());
}

#[test]
fn test_local_filesystem_accepts_file_scheme() {
    let s = LocalScratch::new("scheme");
    let path = s.path("f.txt");
    let uri = format!("file://{path}");
    write_bytes(&uri, b"via uri").unwrap();
    assert_eq!(read_to_string(&path).unwrap(), "via uri");
    assert_eq!(read_to_bytes(&uri).unwrap(), b"via uri");
    assert!(exists(&uri).unwrap());
    assert_eq!(
        LocalFileSystem::new().match_files(&uri).unwrap(),
        vec![path]
    );
}
