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

use std::sync::Arc;

use beam::coders::DefaultCoder;
use beam::transforms::ProcessContext;
use beam::transforms::sdf::{OffsetRange, OffsetRangeTracker, SplittableDoFn};
use file::filebasedsource::{FileRecordReader, file_initial_restriction, file_split_restriction};
use file::filesystem::{FileSystem, register_filesystem};
use file::textio::{ReadFileLinesFn, ReadFileLinesWithFilenameFn, TextLineReader};
use testutils::InMemoryFileSystem;

/// Runs a SplittableDoFn by obtaining its splits and executing process_element across all splits.
fn run_splittable_dofn<F: SplittableDoFn<In = String, Tracker = OffsetRangeTracker>>(
    dofn: &F,
    element: &str,
) -> Vec<F::Out> {
    let initial = dofn.initial_restriction(&element.to_string());
    let splits = dofn.split_restriction(&element.to_string(), &initial);
    let splits = if splits.is_empty() {
        vec![initial]
    } else {
        splits
    };
    let mut all_results = Vec::new();
    for split in splits {
        let tracker = dofn.create_tracker(&split);
        let mut sink: Vec<Vec<u8>> = Vec::new();
        let mut receiver = ProcessContext::new(&mut sink);
        dofn.process_element(element.to_string(), &tracker, &mut receiver)
            .expect("SplittableDoFn process_element should succeed");
        for bytes in sink {
            all_results.push(F::Out::decode(&bytes).expect("Failed to decode emitted element"));
        }
    }
    all_results
}

#[test]
fn test_file_split_restriction_byte_ranges() {
    let fs = Arc::new(InMemoryFileSystem::new());
    let _ = register_filesystem("mem-split", Arc::clone(&fs) as Arc<dyn FileSystem>);

    // Create a 24-byte file
    let path = "mem-split:///data.txt";
    fs.insert_file(path, b"12345\n12345\n12345\n12345\n");

    let initial = file_initial_restriction(path);
    assert_eq!(initial, OffsetRange::new(0, 24));

    let splits = file_split_restriction(&initial, 10);
    assert_eq!(
        splits,
        vec![
            OffsetRange::new(0, 10),
            OffsetRange::new(10, 20),
            OffsetRange::new(20, 24),
        ]
    );

    // Verify ReadFileLinesFn delegates split calculation properly
    let dofn = ReadFileLinesFn::new(10);
    let dofn_initial = dofn.initial_restriction(&path.to_string());
    assert_eq!(dofn_initial, initial);
    let dofn_splits = dofn.split_restriction(&path.to_string(), &dofn_initial);
    assert_eq!(dofn_splits, splits);
}

#[test]
fn test_file_split_restriction_empty() {
    let fs = Arc::new(InMemoryFileSystem::new());
    let _ = register_filesystem("mem-empty", Arc::clone(&fs) as Arc<dyn FileSystem>);

    let path = "mem-empty:///empty.txt";
    fs.insert_file(path, b"");

    let initial = file_initial_restriction(path);
    assert_eq!(initial, OffsetRange::new(0, 0));

    let splits = file_split_restriction(&initial, 10);
    assert_eq!(splits, vec![OffsetRange::new(0, 0)]);

    // When executed, 0 elements are emitted for an empty file
    let read_fn = ReadFileLinesFn::new(10);
    let lines = run_splittable_dofn(&read_fn, path);
    assert!(lines.is_empty());
}

#[test]
fn test_read_split_lines_boundary_exact_match() {
    let fs = Arc::new(InMemoryFileSystem::new());
    let _ = register_filesystem("mem-boundary", Arc::clone(&fs) as Arc<dyn FileSystem>);

    let path = "mem-boundary:///lines.txt";
    let content = "line0\nline1\nline2\nline3\nline4\nline5\nline6\nline7\nline8\nline9\n";
    fs.insert_file(path, content.as_bytes());

    let expected: Vec<String> = (0..10).map(|i| format!("line{i}")).collect();

    // Test multiple split sizes from 3 bytes up to content length
    for split_size in [3, 5, 6, 7, 10, 12, 17, 25, 100] {
        let read_fn = ReadFileLinesFn::new(split_size);
        let lines = run_splittable_dofn(&read_fn, path);
        assert_eq!(
            lines, expected,
            "Failed line reconstruction with split_size {split_size}"
        );
    }
}

#[test]
fn test_read_split_lines_crlf() {
    let fs = Arc::new(InMemoryFileSystem::new());
    let _ = register_filesystem("mem-crlf", Arc::clone(&fs) as Arc<dyn FileSystem>);

    let path = "mem-crlf:///crlf.txt";
    let content = "hello\r\nworld\r\nbeam\r\nrust\r\nsdk\r\n";
    fs.insert_file(path, content.as_bytes());

    for split_size in [4, 7, 8, 9, 13, 20] {
        let read_fn = ReadFileLinesFn::new(split_size);
        let lines = run_splittable_dofn(&read_fn, path);
        assert_eq!(
            lines,
            vec!["hello", "world", "beam", "rust", "sdk"],
            "Failed CRLF reconstruction with split_size {split_size}"
        );
    }
}

#[test]
fn test_read_split_lines_with_filename() {
    let fs = Arc::new(InMemoryFileSystem::new());
    let _ = register_filesystem("mem-fname", Arc::clone(&fs) as Arc<dyn FileSystem>);

    let path = "mem-fname:///data.txt";
    fs.insert_file(path, b"alpha\nbeta\ngamma\n");

    let read_fn = ReadFileLinesWithFilenameFn::new(6);
    let paired = run_splittable_dofn(&read_fn, path);
    assert_eq!(
        paired,
        vec![
            (path.to_string(), "alpha".to_string()),
            (path.to_string(), "beta".to_string()),
            (path.to_string(), "gamma".to_string()),
        ]
    );
}

/// Reads every split of `path` with `ReadFileLinesFn::new(split_size)`, stopping at the
/// first error.
fn try_read_lines(path: &str, split_size: u64) -> beam::Result<Vec<String>> {
    let dofn = ReadFileLinesFn::new(split_size);
    let element = path.to_string();
    let initial = dofn.initial_restriction(&element);
    let mut lines = Vec::new();
    for split in dofn.split_restriction(&element, &initial) {
        let tracker = dofn.create_tracker(&split);
        let mut sink: Vec<Vec<u8>> = Vec::new();
        let mut receiver = ProcessContext::new(&mut sink);
        dofn.process_element(element.clone(), &tracker, &mut receiver)?;
        for bytes in sink {
            lines.push(String::decode(&bytes)?);
        }
    }
    Ok(lines)
}

#[test]
fn test_read_split_last_line_without_trailing_newline() {
    let fs = Arc::new(InMemoryFileSystem::new());
    let _ = register_filesystem("mem-no-eol", Arc::clone(&fs) as Arc<dyn FileSystem>);
    let path = "mem-no-eol:///f.txt";
    fs.insert_file(path, b"first\nsecond\nlast-no-eol");

    for split_size in [1, 2, 5, 6, 7, 12, 13, 14, 24, 100] {
        assert_eq!(
            try_read_lines(path, split_size).unwrap(),
            ["first", "second", "last-no-eol"],
            "split_size {split_size}"
        );
    }
}

#[test]
fn test_read_split_line_spanning_many_splits_is_read_exactly_once() {
    let fs = Arc::new(InMemoryFileSystem::new());
    let _ = register_filesystem("mem-long", Arc::clone(&fs) as Arc<dyn FileSystem>);
    let path = "mem-long:///f.txt";
    let long = "x".repeat(50);
    fs.insert_file(path, format!("a\n{long}\nb\n").as_bytes());

    // With 3-byte splits the long line starts in split 0 and covers ~17 more splits,
    // none of which may emit it again or drop the following line.
    for split_size in [1, 3, 4, 7] {
        assert_eq!(
            try_read_lines(path, split_size).unwrap(),
            ["a", long.as_str(), "b"],
            "split_size {split_size}"
        );
    }
}

/// `\n`, `\r\n`, and a lone `\r` are delimiters; blank lines are kept.
#[test]
fn test_read_split_line_delimiters() {
    let fs = Arc::new(InMemoryFileSystem::new());
    let _ = register_filesystem("mem-blank", Arc::clone(&fs) as Arc<dyn FileSystem>);
    let blank = "mem-blank:///f.txt";
    let cr = "mem-blank:///cr.txt";
    fs.insert_file(blank, b"\n\na\n\r\n\nb\n");
    fs.insert_file(cr, b"one\rtwo\r\nthree\n");

    for (path, split_sizes, expected) in [
        (blank, &[1, 2, 3, 100][..], &["", "", "a", "", "", "b"][..]),
        (cr, &[100], &["one", "two", "three"]),
    ] {
        for &split_size in split_sizes {
            assert_eq!(
                try_read_lines(path, split_size).unwrap(),
                expected,
                "{path} split_size {split_size}"
            );
        }
    }

    // The receiver hands each line to the sink as it is read instead of buffering.
    struct CountingSink {
        seen: usize,
    }
    impl beam::internals::ElementSink for CountingSink {
        fn push(&mut self, _element: Vec<u8>) -> Result<(), String> {
            self.seen += 1;
            Ok(())
        }
    }
    let stream = "mem-blank:///stream.txt";
    fs.insert_file(stream, b"a\nb\nc\nd\n");
    let mut sink = CountingSink { seen: 0 };
    let tracker = OffsetRangeTracker::new(OffsetRange::new(0, 8));
    let mut receiver = ProcessContext::new(&mut sink);
    TextLineReader
        .read_records(fs.as_ref(), stream, &tracker, &mut receiver)
        .expect("Read split must succeed");
    assert_eq!(sink.seen, 4, "Each line should be pushed as it is read");
}

/// Invalid UTF-8 fails the read with a decode error rather than emitting
/// replacement characters.
#[test]
fn test_read_split_invalid_utf8_fails_with_decode_error() {
    let fs = Arc::new(InMemoryFileSystem::new());
    let _ = register_filesystem("mem-utf8", Arc::clone(&fs) as Arc<dyn FileSystem>);
    let path = "mem-utf8:///f.txt";
    fs.insert_file(path, b"ok\nbad\xff\xfe\n");

    let err = try_read_lines(path, 100).unwrap_err();
    assert!(
        err.to_string().contains("UTF-8 decode error"),
        "unexpected error: {err}"
    );
}

#[test]
fn test_read_split_missing_file_reports_path() {
    let fs = Arc::new(InMemoryFileSystem::new());
    let _ = register_filesystem("mem-missing", Arc::clone(&fs) as Arc<dyn FileSystem>);
    let path = "mem-missing:///nope.txt";
    let tracker = OffsetRangeTracker::new(OffsetRange::new(0, 10));
    let mut sink: Vec<Vec<u8>> = Vec::new();
    let mut receiver = ProcessContext::new(&mut sink);
    let err = TextLineReader
        .read_records(fs.as_ref(), path, &tracker, &mut receiver)
        .unwrap_err();
    assert!(
        err.to_string()
            .starts_with(&format!("Failed to open range for '{path}'")),
        "unexpected error: {err}"
    );
    assert!(sink.is_empty());
}
