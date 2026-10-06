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

use file::filesystem::{
    FileSystem, exists, get_filesystem, parse_scheme, read_to_bytes, read_to_string,
    register_filesystem, write_bytes,
};
use std::fs;
use std::io::{Read, Write};
use std::sync::Arc;

use testutils::InMemoryFileSystem;

#[test]
fn test_parse_scheme() {
    assert_eq!(
        parse_scheme("gs://bucket/object.txt"),
        ("gs", "bucket/object.txt")
    );
    assert_eq!(
        parse_scheme("file:///tmp/data.txt"),
        ("file", "/tmp/data.txt")
    );
    assert_eq!(parse_scheme("/var/log/syslog"), ("", "/var/log/syslog"));
    assert_eq!(parse_scheme("relative/path.csv"), ("", "relative/path.csv"));
}

#[test]
fn test_unregistered_scheme_error() {
    let res = get_filesystem("unknown_scheme://data.txt");
    assert!(res.is_err());
    let err = res.unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::NotFound);
    assert!(
        err.to_string()
            .contains("No FileSystem registered for scheme 'unknown_scheme'")
    );
}

#[test]
fn test_filesystem_convenience_utils() {
    let temp_dir = std::env::temp_dir().join(format!("beam_utils_test_{}", std::process::id()));
    let file_path = temp_dir.join("convenience.txt");
    let file_str = file_path.to_str().unwrap();

    fs::create_dir_all(&temp_dir).unwrap();

    write_bytes(file_str, b"convenience utility test\nsecond line").unwrap();

    assert!(exists(file_str).unwrap());

    let text = read_to_string(file_str).unwrap();
    assert_eq!(text, "convenience utility test\nsecond line");

    let bytes = read_to_bytes(file_str).unwrap();
    assert_eq!(bytes, b"convenience utility test\nsecond line");

    let _ = fs::remove_dir_all(&temp_dir);
}

#[test]
fn test_custom_filesystem_registration() {
    register_filesystem("mem", Arc::new(InMemoryFileSystem::new())).unwrap();
    write_bytes("mem://data/sample.csv", b"col1,col2\n1,2\n").unwrap();
    let fs = get_filesystem("mem://data/sample.csv").unwrap();
    assert_eq!(fs.size("mem://data/sample.csv").unwrap(), 14);

    let mut reader = fs.open_read("mem://data/sample.csv").unwrap();
    let mut s = String::new();
    reader.read_to_string(&mut s).unwrap();
    assert_eq!(s, "col1,col2\n1,2\n");
}

#[test]
fn test_in_memory_filesystem_lifecycle() {
    let fs = InMemoryFileSystem::new();
    let path = "mem://data/sample.txt";
    let backup = "mem://data/backup.txt";
    let renamed = "mem://data/renamed.txt";

    assert!(!fs.exists(path).unwrap());
    assert!(fs.open_read(path).is_err());
    assert!(fs.size(path).is_err());

    {
        let mut w = fs.open_write(path).unwrap();
        w.write_all(b"hello in-memory").unwrap();
        w.flush().unwrap();
    }
    assert!(fs.exists(path).unwrap());
    assert_eq!(fs.size(path).unwrap(), 15);

    {
        let mut r = fs.open_read(path).unwrap();
        let mut content = String::new();
        r.read_to_string(&mut content).unwrap();
        assert_eq!(content, "hello in-memory");
    }

    {
        let mut app = fs.open_append(path).unwrap();
        app.write_all(b" world").unwrap();
        app.flush().unwrap();
    }
    assert_eq!(fs.size(path).unwrap(), 21);
    assert_eq!(
        String::from_utf8(fs.get_file(path).unwrap()).unwrap(),
        "hello in-memory world"
    );

    fs.copy(path, backup).unwrap();
    assert!(fs.exists(backup).unwrap());
    fs.rename(backup, renamed).unwrap();
    assert!(!fs.exists(backup).unwrap());
    assert!(fs.exists(renamed).unwrap());

    fs.insert_file("mem://direct.txt", b"direct payload");
    assert!(fs.contains_file("mem://direct.txt"));
    assert_eq!(fs.all_paths().len(), 3);

    fs.remove(path).unwrap();
    assert!(!fs.exists(path).unwrap());
    fs.clear();
    assert_eq!(fs.all_paths().len(), 0);
}

#[test]
fn test_in_memory_filesystem_glob_matching() {
    let fs = InMemoryFileSystem::new();
    fs.insert_file("mem://events/2026/01/a.log", b"1");
    fs.insert_file("mem://events/2026/02/b.log", b"2");
    fs.insert_file("mem://events/2026/03/c.txt", b"3");
    fs.insert_file("mem://events/2025/12/d.log", b"4");

    let matches = fs.match_files("mem://events/2026/**/*.log").unwrap();
    assert_eq!(
        matches,
        vec![
            "mem://events/2026/01/a.log".to_string(),
            "mem://events/2026/02/b.log".to_string(),
        ]
    );

    let all_logs = fs.match_files("mem://events/**/*.log").unwrap();
    assert_eq!(all_logs.len(), 3);
}
