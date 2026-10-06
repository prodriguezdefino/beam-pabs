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

use std::cell::Cell;
use std::io::{self, Read, Write};

use file::filesystem::{FileSystem, GlobMatcher, get_filesystem};
use futures::executor::block_on;
use futures::stream;
use gcp::gcs::{collect_object_names, glob_prefix, literal_match, to_sorted_uris};
use gcp::{GcsFileSystem, parse_gcs_uri};

#[test]
fn test_parse_gcs_uri_valid() {
    let (bucket, object) = parse_gcs_uri("gs://my-bucket/path/to/file.txt").expect("valid GCS URI");
    assert_eq!(bucket, "my-bucket");
    assert_eq!(object, "path/to/file.txt");

    let (bucket, object) = parse_gcs_uri("gs://my-bucket/data/*.txt").expect("valid glob URI");
    assert_eq!(bucket, "my-bucket");
    assert_eq!(object, "data/*.txt");

    let (bucket, object) = parse_gcs_uri("gs://my-bucket/").expect("root URI");
    assert_eq!(bucket, "my-bucket");
    assert_eq!(object, "");

    let (bucket, object) = parse_gcs_uri("gs://my-bucket").expect("bare bucket");
    assert_eq!(bucket, "my-bucket");
    assert_eq!(object, "");
}

#[test]
fn test_parse_gcs_uri_invalid() {
    assert!(parse_gcs_uri("file:///tmp/file.txt").is_err());
    assert!(parse_gcs_uri("s3://my-bucket/file.txt").is_err());
    assert!(parse_gcs_uri("gs://").is_err());
    assert!(parse_gcs_uri("gs:///file.txt").is_err());
}

#[test]
fn test_glob_prefix_stops_at_last_slash_before_first_wildcard() {
    for (pattern, prefix) in [
        // Literal objects have no prefix.
        ("path/to/file.txt", None),
        ("", None),
        ("data/*.txt", Some("data/")),
        ("logs/2026/**/x?.log", Some("logs/2026/")),
        ("logs/part-[0-9].csv", Some("logs/")),
        ("logs/ab*/c.txt", Some("logs/")),
        ("*.txt", Some("")),
    ] {
        assert_eq!(glob_prefix(pattern), prefix, "{pattern:?}");
    }
}

#[test]
fn test_literal_match_returns_uri_only_when_found() {
    let uri = || "gs://b/o.txt".to_string();
    assert_eq!(literal_match(uri(), Ok(true)).unwrap(), vec![uri()]);
    assert!(literal_match(uri(), Ok(false)).unwrap().is_empty());
}

#[test]
fn test_literal_match_propagates_existence_check_failures() {
    // A failed existence check (such as expired credentials) must return the error,
    // not report the object as missing.
    let failure = io::Error::new(io::ErrorKind::PermissionDenied, "credentials expired");
    let err = literal_match("gs://b/o.txt".to_string(), Err(failure)).unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::PermissionDenied);
    assert!(err.to_string().contains("credentials expired"));
}

#[test]
fn test_collect_object_names_drains_every_page() {
    // Collection must drain every listing page. Items arrive as the SDK item paginator
    // yields them, with the pages flattened into one stream.
    let pages: [&[&str]; 3] = [&["a/1.txt", "a/2.txt"], &["a/3.txt"], &["a/4.txt"]];
    let names = pages
        .iter()
        .flat_map(|page| page.iter())
        .map(|name| Ok::<_, String>(name.to_string()));
    let collected = block_on(collect_object_names(stream::iter(names))).unwrap();
    assert_eq!(collected, ["a/1.txt", "a/2.txt", "a/3.txt", "a/4.txt"]);
}

#[test]
fn test_collect_object_names_stops_at_first_listing_error() {
    let polled_after_error = Cell::new(false);
    let names = [Ok("a/1.txt".to_string()), Err("403 Forbidden".to_string())]
        .into_iter()
        .chain(std::iter::from_fn(|| {
            polled_after_error.set(true);
            None
        }));
    let err = block_on(collect_object_names(stream::iter(names))).unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::PermissionDenied);
    assert!(!polled_after_error.get());
}

#[test]
fn test_to_sorted_uris_filters_by_glob_and_sorts() {
    let matcher = GlobMatcher::new("logs/**/*.log").unwrap();
    let names = [
        "logs/b/2.log",
        "logs/readme.txt",
        "logs/a/1.log",
        "other/x.log",
    ]
    .map(String::from)
    .to_vec();
    assert_eq!(
        to_sorted_uris("bkt", &matcher, names),
        ["gs://bkt/logs/a/1.log", "gs://bkt/logs/b/2.log"]
    );
}

#[test]
fn test_gcs_scheme_registered_in_global_registry() {
    // Verify that gs:// paths automatically resolve through get_filesystem
    let default_fs = get_filesystem("gs://apache-beam-samples/shakespeare/kinglear.txt")
        .expect("gs scheme should be registered by default");
    assert!(format!("{default_fs:?}").contains("GcsFileSystem"));
}

fn get_test_bucket() -> Option<String> {
    std::env::var("BEAM_TEST_GCS_BUCKET").ok()
}

#[test]
fn test_gcs_filesystem_operations() {
    let Some(bucket) = get_test_bucket() else {
        eprintln!("SKIP: test_gcs_filesystem_operations: BEAM_TEST_GCS_BUCKET not set");
        return;
    };
    let fs = GcsFileSystem::new();
    let pid = std::process::id();
    let file_uri = format!("gs://{bucket}/test_{pid}_doc.txt");
    let backup_uri = format!("gs://{bucket}/backup/test_{pid}_doc.txt");
    let renamed_uri = format!("gs://{bucket}/renamed_test_{pid}.txt");

    // Initial exists check.
    assert!(!fs.exists(&file_uri).unwrap());

    // Write content.
    {
        let mut writer = fs.open_write(&file_uri).unwrap();
        writer.write_all(b"Hello Google Cloud Storage!\n").unwrap();
        writer.flush().unwrap();
    }

    // Check existence, size, and metadata.
    assert!(fs.exists(&file_uri).unwrap());
    assert_eq!(fs.size(&file_uri).unwrap(), 28);
    assert!(fs.last_modified(&file_uri).is_ok());

    // Open reader and verify content.
    {
        let mut reader = fs.open_read(&file_uri).unwrap();
        let mut content = String::new();
        reader.read_to_string(&mut content).unwrap();
        assert_eq!(content, "Hello Google Cloud Storage!\n");
    }

    // Copy to backup path.
    fs.copy(&file_uri, &backup_uri).unwrap();
    assert!(fs.exists(&backup_uri).unwrap());
    assert_eq!(fs.size(&backup_uri).unwrap(), 28);

    // Rename backup file.
    fs.rename(&backup_uri, &renamed_uri).unwrap();
    assert!(!fs.exists(&backup_uri).unwrap());
    assert!(fs.exists(&renamed_uri).unwrap());

    // Append mode preserves existing content.
    {
        let mut appender = fs.open_append(&renamed_uri).unwrap();
        appender.write_all(b"Appended line.\n").unwrap();
        appender.flush().unwrap();
    }
    {
        let mut reader = fs.open_read(&renamed_uri).unwrap();
        let mut content = String::new();
        reader.read_to_string(&mut content).unwrap();
        assert_eq!(content, "Hello Google Cloud Storage!\nAppended line.\n");
        assert_eq!(fs.size(&renamed_uri).unwrap(), 43);
    }

    // Remove file.
    fs.remove(&file_uri).unwrap();
    assert!(!fs.exists(&file_uri).unwrap());
    let _ = fs.remove(&renamed_uri);
}

#[test]
fn test_gcs_glob_matching() {
    let Some(bucket) = get_test_bucket() else {
        eprintln!("SKIP: test_gcs_glob_matching: BEAM_TEST_GCS_BUCKET not set");
        return;
    };
    let fs = GcsFileSystem::new();
    let pid = std::process::id();

    let paths = vec![
        format!("gs://{bucket}/logs_{pid}/2026/01/access.log"),
        format!("gs://{bucket}/logs_{pid}/2026/02/access.log"),
        format!("gs://{bucket}/logs_{pid}/2026/03/error.log"),
        format!("gs://{bucket}/logs_{pid}/readme.txt"),
    ];

    for p in &paths {
        let mut w = fs.open_write(p).unwrap();
        w.write_all(b"test").unwrap();
        w.flush().unwrap();
    }

    let matches = fs
        .match_files(&format!("gs://{bucket}/logs_{pid}/**/*.log"))
        .expect("glob match");
    assert_eq!(matches, paths[..3]);

    for p in &paths {
        let _ = fs.remove(p);
    }
}

/// Every operation validates its URIs before it creates a client, so this test runs offline.
#[test]
fn test_gcs_filesystem_rejects_non_gcs_uris_before_any_request() {
    let fs = GcsFileSystem::new();
    let bad = "s3://bucket/object.txt";
    let good = "gs://bucket/object.txt";
    let results: [(&str, io::Result<()>); 11] = [
        ("match_files literal", fs.match_files(bad).map(drop)),
        (
            "match_files glob",
            fs.match_files("s3://bucket/*.txt").map(drop),
        ),
        ("size", fs.size(bad).map(drop)),
        ("last_modified", fs.last_modified(bad).map(drop)),
        ("remove", fs.remove(bad)),
        ("copy source", fs.copy(bad, good)),
        ("copy destination", fs.copy(good, bad)),
        ("rename", fs.rename(bad, good)),
        ("open_read", fs.open_read(bad).map(drop)),
        ("open_write", fs.open_write(bad).map(drop)),
        ("open_append", fs.open_append(bad).map(drop)),
    ];
    for (op, result) in results {
        let err = result.expect_err(op);
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput, "{op}");
        assert!(
            err.to_string().contains("expected 'gs://' scheme"),
            "{op}: {err}"
        );
    }
}
