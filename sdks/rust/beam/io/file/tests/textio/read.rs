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

//! Runs the TextIO read facades end to end on Prism, asserting their output with
//! `passert`. Tiny split sizes force every file through many restrictions, so a line
//! lost or duplicated at a split boundary fails the assertion.

use std::fs;
use std::path::PathBuf;

use beam::prelude::*;
use file::{Compression, textio};
use testing::{TestPipeline, passert};

/// A scratch directory holding two small text files, removed on drop.
struct Inputs {
    dir: PathBuf,
}

impl Inputs {
    fn new(tag: &str) -> Self {
        let dir = std::env::temp_dir().join(format!(
            "beam_textio_read_{tag}_{}_{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join("a.txt"), "alpha\nbravo\r\ncharlie").unwrap();
        fs::write(dir.join("b.txt"), "delta\n\necho\n").unwrap();
        fs::write(dir.join("skip.csv"), "not,matched\n").unwrap();
        Self { dir }
    }

    fn path(&self, name: &str) -> String {
        self.dir.join(name).to_string_lossy().into_owned()
    }

    fn pattern(&self) -> String {
        self.path("*.txt")
    }
}

impl Drop for Inputs {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.dir);
    }
}

fn strings(v: &[&str]) -> Vec<String> {
    v.iter().map(ToString::to_string).collect()
}

const A_LINES: [&str; 3] = ["alpha", "bravo", "charlie"];
const B_LINES: [&str; 3] = ["delta", "", "echo"];

#[tokio::test]
async fn read_glob_with_small_split_size_reads_every_line_once() {
    let inputs = Inputs::new("glob");
    let p = TestPipeline::new();
    let lines = p.apply(
        "TextIO.Read",
        textio::Read::new(inputs.pattern()).with_split_size(3),
    );
    passert::that("AssertLines", &lines)
        .contains_in_any_order(strings(&[A_LINES, B_LINES].concat()));
    p.run().await.expect("pipeline must succeed");
}

#[tokio::test]
async fn read_with_filename_pairs_each_line_with_its_file() {
    let inputs = Inputs::new("with_filename");
    let (a, b) = (inputs.path("a.txt"), inputs.path("b.txt"));
    let p = TestPipeline::new();
    let pairs = p.apply(
        "TextIO.ReadWithFilename",
        textio::ReadWithFilename::new(inputs.pattern()).with_split_size(4),
    );
    let expected: Vec<(String, String)> = A_LINES
        .iter()
        .map(|l| (a.clone(), l.to_string()))
        .chain(B_LINES.iter().map(|l| (b.clone(), l.to_string())))
        .collect();
    passert::that("AssertPairs", &pairs).contains_in_any_order(expected);
    p.run().await.expect("pipeline must succeed");
}

#[tokio::test]
async fn read_files_reads_each_listed_path() {
    let inputs = Inputs::new("read_files");
    let p = TestPipeline::new();
    // Only b.txt is listed: ReadFiles must read exactly the paths it is given.
    let lines = p
        .apply("Create", Create::new(vec![inputs.path("b.txt")]))
        .apply("ReadListed", textio::ReadFiles::new().with_split_size(2));
    passert::that("AssertLines", &lines).contains_in_any_order(strings(&B_LINES));

    let components = p.to_proto().components.expect("components present");
    assert!(
        components
            .transforms
            .values()
            .any(|t| t.unique_name == "ReadListed/Read"),
        "with_name must name the read step"
    );
    p.run().await.expect("pipeline must succeed");
}

#[tokio::test]
async fn read_files_with_filename_pairs_each_line_with_its_file() {
    let inputs = Inputs::new("read_files_with_filename");
    let (a, b) = (inputs.path("a.txt"), inputs.path("b.txt"));
    let p = TestPipeline::new();
    let pairs = p
        .apply("Create", Create::new(vec![a.clone(), b.clone()]))
        .apply(
            "TextIO.ReadFilesWithFilename",
            textio::ReadFilesWithFilename::new().with_split_size(5),
        );
    let expected: Vec<(String, String)> = A_LINES
        .iter()
        .map(|l| (a.clone(), l.to_string()))
        .chain(B_LINES.iter().map(|l| (b.clone(), l.to_string())))
        .collect();
    passert::that("AssertPairs", &pairs).contains_in_any_order(expected);
    p.run().await.expect("pipeline must succeed");
}

/// A read of a pattern that matches nothing fails the pipeline instead of producing
/// no output.
#[tokio::test]
async fn read_of_pattern_matching_nothing_fails_the_pipeline() {
    let inputs = Inputs::new("no_match");
    let pattern = inputs.path("*.missing");
    let p = TestPipeline::new();
    let lines = p.apply("TextIO.Read", textio::Read::new(pattern.clone()));
    passert::that("AssertLines", &lines).empty();
    let err = p.run().await.expect_err("an empty match must fail");
    assert!(
        err.to_string()
            .contains(&format!("No files matched pattern '{pattern}'")),
        "unexpected error: {err}"
    );
}

fn write_compressed(path: &str, codec: Compression, body: &str) {
    use std::io::Write;

    let raw = fs::File::create(path).unwrap();
    let mut writer = codec.writer(raw).unwrap();
    writer.write_all(body.as_bytes()).unwrap();
    writer.finish().unwrap();
}

#[tokio::test]
async fn read_compressed_files_reads_full_decompressed_stream_across_codecs() {
    let inputs = Inputs::new("compressed");
    // 200 lines compress to a tiny fraction of the decompressed byte length, verifying
    // that the reader does not stop at the compressed file size or split mid-stream.
    let expected_per_file: Vec<String> = (0..200)
        .map(|i| format!("highly-compressible-record-line-{i:04}"))
        .collect();
    let payload = format!("{}\n", expected_per_file.join("\n"));

    let codecs = [
        (Compression::Gzip, "part_gz.txt.gz"),
        (Compression::Zstd, "part_zst.txt.zst"),
        (Compression::Deflate, "part_def.txt.deflate"),
        (Compression::Snappy, "part_sz.txt.sz"),
    ];
    for (codec, name) in codecs {
        let p = inputs.path(name);
        write_compressed(&p, codec, &payload);
        let on_disk = fs::metadata(&p).unwrap().len();
        assert!(on_disk < (payload.len() as u64) / 2, "{name}: {on_disk}");
    }

    let mut expected_all = Vec::new();
    for _ in codecs {
        expected_all.extend(expected_per_file.iter().cloned());
    }

    let p = TestPipeline::new();
    let lines = p.apply(
        "TextIO.ReadCompressed",
        textio::Read::new(inputs.path("part_*")).with_split_size(16),
    );
    passert::that("AssertCompressedLines", &lines).contains_in_any_order(expected_all);
    p.run().await.expect("pipeline must succeed");
}

#[tokio::test]
async fn read_with_explicit_compression_overrides_extension() {
    let inputs = Inputs::new("explicit_codec");
    let custom_path = inputs.path("custom.dat");
    write_compressed(&custom_path, Compression::Zstd, "one\ntwo\nthree\n");

    let p = TestPipeline::new();
    let lines = p.apply(
        "TextIO.ReadExplicit",
        textio::Read::new(&custom_path).with_compression(Compression::Zstd),
    );
    passert::that("AssertExplicit", &lines)
        .contains_in_any_order(strings(&["one", "two", "three"]));
    p.run().await.expect("pipeline must succeed");
}
