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

//! Integration tests for native file-matching and file-reading transforms.
//!
//! Verifies end-to-end execution of `Match`, `ReadFiles`, and `ReadFilesWithFilename`
//! on PrismRunner.

use std::fs;
use std::path::PathBuf;

use beam::pipeline::Pipeline;
use beam::transforms::Create;
use file::fileio::{FileMetadata, Match};
use file::textio::{self, ReadFiles, ReadFilesWithFilename};
use fluent::prelude::PCollectionExt;
use prism::PrismRunner;

struct TempDirFixture {
    path: PathBuf,
}

impl TempDirFixture {
    fn new(prefix: &str) -> Self {
        let path =
            std::env::temp_dir().join(format!("beam_fileio_{prefix}_{}", std::process::id()));
        fs::create_dir_all(&path).expect("failed to create temp test directory");
        Self { path }
    }

    fn create_file(&self, name: &str, content: &str) -> PathBuf {
        let file_path = self.path.join(name);
        fs::write(&file_path, content).expect("failed to write test fixture file");
        file_path
    }
}

impl Drop for TempDirFixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

#[tokio::test]
async fn test_match_files_emits_matching_paths() {
    let fixture = TempDirFixture::new("match_files");
    let file1 = fixture.create_file("alpha.txt", "data 1\n");
    let file2 = fixture.create_file("beta.txt", "data 2\n");
    let _ignored = fixture.create_file("gamma.log", "ignored\n");

    let out_file = fixture.path.join("out_matches.txt");
    let pattern = format!("{}/[ab]*.txt", fixture.path.to_str().unwrap());

    let p = Pipeline::new();
    let matches = p
        .apply(Match::new("FileIO.MatchMetadata", &pattern))
        .map("Path", |m: FileMetadata| m.path);
    matches
        .apply(textio::Write::new("TextIO.Write", out_file.to_str().unwrap()).without_sharding());

    let res = p.run_with_runner(&PrismRunner::new()).await;
    assert!(res.is_ok(), "PrismRunner execution failed: {res:?}");

    let out_content = fs::read_to_string(&out_file).expect("output file must exist");
    let mut matched_lines: Vec<&str> = out_content.lines().collect();
    matched_lines.sort();

    let mut expected = vec![file1.to_str().unwrap(), file2.to_str().unwrap()];
    expected.sort();

    assert_eq!(matched_lines, expected);
}

#[tokio::test]
async fn test_read_files_and_read_files_with_filename() {
    let fixture = TempDirFixture::new("read_files");
    let file1 = fixture.create_file("part1.txt", "line A1\nline A2\n");
    let file2 = fixture.create_file("part2.txt", "line B1\nline B2\n");

    let out_plain = fixture.path.join("out_plain.txt");
    let out_pairs = fixture.path.join("out_pairs.txt");
    let f1_str = file1.to_str().unwrap().to_string();
    let f2_str = file2.to_str().unwrap().to_string();

    let p = Pipeline::new();
    let paths = p.apply(Create::new("Create", vec![f1_str.clone(), f2_str.clone()]));

    paths
        .clone()
        .apply(ReadFiles::new("TextIO.ReadFiles"))
        .apply(
            textio::Write::new("TextIO.WritePlain", out_plain.to_str().unwrap()).without_sharding(),
        );

    paths
        .apply(ReadFilesWithFilename::new("TextIO.ReadFilesWithFilename"))
        .map("FormatPair", |(file, line): (String, String)| {
            format!("{file}==>{line}")
        })
        .apply(
            textio::Write::new("TextIO.WritePairs", out_pairs.to_str().unwrap()).without_sharding(),
        );

    let res = p.run_with_runner(&PrismRunner::new()).await;
    assert!(res.is_ok(), "PrismRunner execution failed: {res:?}");

    let plain_content = fs::read_to_string(&out_plain).expect("plain output file must exist");
    let mut plain_lines: Vec<&str> = plain_content.lines().collect();
    plain_lines.sort();
    assert_eq!(
        plain_lines,
        vec!["line A1", "line A2", "line B1", "line B2"]
    );

    let pairs_content = fs::read_to_string(&out_pairs).expect("pairs output file must exist");
    let mut pair_lines: Vec<&str> = pairs_content.lines().collect();
    pair_lines.sort();
    let mut expected_pairs = [
        format!("{f1_str}==>line A1"),
        format!("{f1_str}==>line A2"),
        format!("{f2_str}==>line B1"),
        format!("{f2_str}==>line B2"),
    ];
    expected_pairs.sort();
    let expected_refs: Vec<&str> = expected_pairs.iter().map(String::as_str).collect();
    assert_eq!(pair_lines, expected_refs);
}
