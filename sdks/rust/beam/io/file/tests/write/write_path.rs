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

//! The write path: round-robin shard assignment, and `textio::Write` end to end on
//! Prism (fixed and runner-chosen sharding, record and byte rolling, naming options).

use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use beam::coders::DefaultCoder;
use beam::prelude::*;
use beam::transforms::{DoFn, ProcessContext};
use file::textio;
use file::write_files::AssignShardFn;
use testing::{TestPipeline, passert};

#[test]
fn assign_shard_cycles_through_every_shard() {
    for num_shards in [1u32, 3, 4] {
        let mut assign = AssignShardFn::<String>::new(num_shards);
        let mut sink: Vec<Vec<u8>> = Vec::new();
        let mut out = ProcessContext::new(&mut sink);
        for i in 0..2 * num_shards {
            assign.process_element(format!("e{i}"), &mut out).unwrap();
        }
        let assigned: Vec<(i32, String)> = sink
            .iter()
            .map(|bytes| <(i32, String)>::decode(bytes).unwrap())
            .collect();
        let n = i32::try_from(num_shards).unwrap();
        let first = assigned[0].0;
        assert!((0..n).contains(&first), "{first} of {num_shards}");
        for (i, (shard, element)) in (0..).zip(&assigned) {
            assert_eq!(
                *shard,
                (first + i) % n,
                "element {i} of {num_shards} shards"
            );
            assert_eq!(*element, format!("e{i}"));
        }
    }
}

/// A fresh output directory, removed on drop.
struct Scratch(PathBuf);

impl Scratch {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!(
            "beam-write-path-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        Self(dir)
    }

    fn path(&self, rel: &str) -> String {
        self.0.join(rel).to_string_lossy().into_owned()
    }

    /// Regular files directly under the directory as `(name, lines)`, sorted by name.
    fn files(&self) -> Vec<(String, Vec<String>)> {
        let mut files: Vec<_> = fs::read_dir(&self.0)
            .unwrap()
            .map(|e| e.unwrap())
            .filter(|e| e.file_type().unwrap().is_file())
            .map(|e| {
                let body = fs::read_to_string(e.path()).unwrap();
                let lines = body.lines().map(str::to_string).collect();
                (e.file_name().to_string_lossy().into_owned(), lines)
            })
            .collect();
        files.sort();
        files
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn lines(n: usize) -> Vec<String> {
    (0..n).map(|i| format!("line{i}")).collect()
}

/// Runs `write` over `input` on Prism and asserts the file names it reports.
async fn run(input: Vec<String>, write: textio::Write, expected_names: Vec<String>) {
    let p = TestPipeline::new();
    let names = p.apply(Create::new("Create", input)).apply(write);
    passert::that("AssertNames", &names).contains_in_any_order(expected_names);
    p.run().await.expect("pipeline must succeed");
}

/// Every input line was written exactly once across `files`.
fn assert_all_lines_written(files: &[(String, Vec<String>)], input: &[String]) {
    let mut written: Vec<String> = files.iter().flat_map(|(_, l)| l.clone()).collect();
    written.sort();
    let mut expected = input.to_vec();
    expected.sort();
    assert_eq!(written, expected, "{files:?}");
}

#[tokio::test]
async fn fixed_shards_write_one_file_per_shard() {
    let s = Scratch::new();
    let input = lines(9);
    let names: Vec<String> = (0..3)
        .map(|i| format!("out-0000{i}-of-00003.txt"))
        .collect();
    run(
        input.clone(),
        textio::Write::new("Write", s.path("out.txt")).with_num_shards(3),
        names.iter().map(|n| s.path(n)).collect(),
    )
    .await;
    let files = s.files();
    assert_eq!(
        files.iter().map(|(n, _)| n.clone()).collect::<Vec<_>>(),
        names
    );
    assert_all_lines_written(&files, &input);
}

#[tokio::test]
async fn fixed_shard_rolls_after_max_records() {
    let s = Scratch::new();
    let input = lines(5);
    let names: Vec<String> = (0..3).map(|i| format!("out-{i}-of-3.txt")).collect();
    run(
        input.clone(),
        textio::Write::new("Write", s.path("out"))
            .with_num_shards(1)
            .with_max_records_per_file(2)
            .with_shard_template("-S-of-N")
            .with_suffix(".txt"),
        names.iter().map(|n| s.path(n)).collect(),
    )
    .await;
    let files = s.files();
    let mut sizes: Vec<_> = files.iter().map(|(_, l)| l.len()).collect();
    sizes.sort_unstable();
    assert_eq!(sizes, [1, 2, 2], "{files:?}");
    assert_all_lines_written(&files, &input);
}

#[tokio::test]
async fn runner_sharding_rolls_after_max_bytes() {
    let s = Scratch::new();
    // `lineN\n` is six bytes, so a file is full after two lines.
    let input = lines(6);
    let p = TestPipeline::new();
    p.apply(Create::new("Create", input.clone())).apply(
        textio::Write::new("Write", s.path("out.txt"))
            .with_max_bytes_per_file(12)
            .with_temp_directory(s.path("tmp")),
    );
    p.run().await.expect("pipeline must succeed");
    let files = s.files();
    assert!(files.len() >= 3, "{files:?}");
    for (name, lines) in &files {
        assert!((1..=2).contains(&lines.len()), "{name}: {lines:?}");
        assert!(name.starts_with("out-") && name.ends_with(".txt"), "{name}");
    }
    assert_all_lines_written(&files, &input);
}

#[tokio::test]
async fn without_sharding_writes_exactly_the_named_file() {
    let s = Scratch::new();
    let input = lines(4);
    run(
        input.clone(),
        textio::Write::new("Write", s.path("single.txt")).without_sharding(),
        vec![s.path("single.txt")],
    )
    .await;
    let files = s.files();
    assert_eq!(files.len(), 1, "{files:?}");
    assert_eq!(files[0].0, "single.txt");
    assert_all_lines_written(&files, &input);
}

#[tokio::test]
async fn filename_policy_names_the_files() {
    let s = Scratch::new();
    let input = lines(4);
    let policy = |base: &str, ctx: &file::FileNamingContext<'_>| {
        format!("{base}-custom{}{}", ctx.shard_string(), ctx.suffix)
    };
    run(
        input.clone(),
        textio::Write::new("Write", s.path("p"))
            .with_num_shards(2)
            .with_shard_template("-S")
            .with_suffix(".csv")
            .with_filename_policy(policy),
        vec![s.path("p-custom-0.csv"), s.path("p-custom-1.csv")],
    )
    .await;
    let files = s.files();
    assert_eq!(
        files.iter().map(|(n, _)| n.as_str()).collect::<Vec<_>>(),
        ["p-custom-0.csv", "p-custom-1.csv"]
    );
    assert_all_lines_written(&files, &input);
}

#[tokio::test]
async fn windowed_writes_name_files_after_the_window() {
    let s = Scratch::new();
    let input = lines(3);
    run(
        input.clone(),
        textio::Write::new("Write", s.path("w.txt"))
            .with_windowed_writes()
            .with_num_shards(1)
            .with_shard_template("-S-of-N"),
        vec![s.path("w-GlobalWindow-0-of-1.txt")],
    )
    .await;
    let files = s.files();
    assert_eq!(files.len(), 1, "{files:?}");
    assert_eq!(files[0].0, "w-GlobalWindow-0-of-1.txt");
    assert_all_lines_written(&files, &input);
}
