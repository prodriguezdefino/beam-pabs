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

//! Sharded and rolling writes (`WriteFiles`) and `ReadMatches` on PrismRunner.

use std::collections::BTreeSet;
use std::fs;
use std::path::Path;
use std::time::Duration;

use beam::Instant;
use beam::io::file::fileio::{self, ReadableFile};
use beam::io::file::{FormatSink, TextFormat, WriteFiles, textio};
use beam::pipeline::Pipeline;
use beam::testing::TestStream;
use beam::transforms::{Create, Map};
use beam::windowing::{AccumulationMode, FixedWindows, Trigger, WindowInto};
use beam_runner_prism::PrismRunner;
use beam_runner_tests::TempDir;

fn lines_of(path: &Path) -> Vec<String> {
    fs::read_to_string(path)
        .unwrap_or_else(|e| panic!("reading {path:?}: {e}"))
        .lines()
        .map(str::to_string)
        .collect()
}

fn words(n: usize) -> Vec<String> {
    (0..n).map(|i| format!("word-{i:02}")).collect()
}

#[tokio::test]
async fn fixed_shards_write_exactly_n_named_files() {
    let dir = TempDir::new("fixed");
    let p = Pipeline::new();
    p.apply("Create", Create::new(words(20))).apply(
        "TextIO.Write",
        textio::Write::new(dir.join("out.txt")).with_num_shards(3),
    );
    p.run_with_runner(&PrismRunner::new())
        .await
        .expect("pipeline failed");

    assert_eq!(
        dir.entries(),
        vec![
            "out-00000-of-00003.txt",
            "out-00001-of-00003.txt",
            "out-00002-of-00003.txt",
        ],
        "temporary files must be cleaned up"
    );
    let mut all: Vec<String> = dir
        .entries()
        .iter()
        .flat_map(|name| lines_of(&dir.as_path().join(name)))
        .collect();
    all.sort();
    assert_eq!(all, words(20));
}

#[tokio::test]
async fn rolling_writes_cap_records_per_file() {
    let dir = TempDir::new("rolling");
    let p = Pipeline::new();
    p.apply("Create", Create::new(words(7))).apply(
        "TextIO.Write",
        textio::Write::new(dir.join("roll.txt")).with_max_records_per_file(2),
    );
    p.run_with_runner(&PrismRunner::new())
        .await
        .expect("pipeline failed");

    let files = dir.entries();
    assert!(
        files.len() >= 4,
        "7 records at 2 per file need 4+ files: {files:?}"
    );
    let total = files.len();
    let mut all = Vec::new();
    for (i, name) in files.iter().enumerate() {
        assert_eq!(name, &format!("roll-{i:05}-of-{total:05}.txt"));
        let lines = lines_of(&dir.as_path().join(name));
        assert!(lines.len() <= 2, "{name} holds {} records", lines.len());
        all.extend(lines);
    }
    all.sort();
    assert_eq!(all, words(7));
}

#[tokio::test]
async fn empty_input_still_writes_empty_output() {
    let dir = TempDir::new("empty");
    let p = Pipeline::new();
    let empty = p.apply("Create", Create::new(Vec::<String>::new()));
    empty.apply(
        "WriteSingle",
        textio::Write::new(dir.join("single.txt")).without_sharding(),
    );
    empty.apply(
        "WriteSharded",
        textio::Write::new(dir.join("sharded.txt")).with_num_shards(2),
    );
    p.run_with_runner(&PrismRunner::new())
        .await
        .expect("pipeline failed");

    assert_eq!(
        dir.entries(),
        vec![
            "sharded-00000-of-00002.txt",
            "sharded-00001-of-00002.txt",
            "single.txt",
        ]
    );
    for name in dir.entries() {
        assert_eq!(
            fs::read_to_string(dir.as_path().join(&name)).expect("read"),
            ""
        );
    }
}

#[tokio::test]
async fn write_files_reports_final_filenames_with_suffix() {
    let dir = TempDir::new("filenames");
    let listing_dir = TempDir::new("writefiles_listing");
    let listing = listing_dir.as_path().join("listing.txt");
    let p = Pipeline::new();
    p.apply("Create", Create::new(words(5)))
        .apply(
            "WriteFiles",
            WriteFiles::new(dir.join("part"), FormatSink::new(TextFormat))
                .with_suffix(".log")
                .with_num_shards(2),
        )
        .apply(
            "TextIO.Write",
            textio::Write::new(listing.to_string_lossy().into_owned()).without_sharding(),
        );
    p.run_with_runner(&PrismRunner::new())
        .await
        .expect("pipeline failed");

    let mut reported = lines_of(&listing);
    reported.sort();
    assert_eq!(
        reported,
        vec![
            dir.join("part-00000-of-00002.log"),
            dir.join("part-00001-of-00002.log")
        ]
    );
}

#[tokio::test]
async fn read_matches_yields_readable_files() {
    let dir = TempDir::new("read_matches");
    dir.create_file("a.json", "{\"a\":1}");
    dir.create_file("b.json", "{\"b\":22}");
    dir.create_file("skip.txt", "no");
    let out_dir = TempDir::new("writefiles_read_matches");
    let out = out_dir.as_path().join("out.txt");

    let p = Pipeline::new();
    let from_metadata = p
        .apply(
            "FileIO.MatchMetadata",
            fileio::Match::new(dir.join("*.json")),
        )
        .apply("ReadMetadataMatches", fileio::ReadMatches::new());
    let from_paths = p
        .apply("Paths", Create::new(vec![dir.join("a.json")]))
        .apply("ReadPathMatches", fileio::ReadMatches::new());
    beam::values::PCollectionList::of(from_metadata)
        .and(from_paths)
        .apply("Both", beam::transforms::Flatten::new())
        .apply(
            "Describe",
            Map::new(|f: ReadableFile| {
                let name = Path::new(f.path())
                    .file_name()
                    .map(|n| n.to_string_lossy().into_owned())
                    .unwrap_or_default();
                let body = f.read_fully_as_utf8_string().unwrap_or_default();
                format!("{name} {} {body}", f.size_bytes())
            }),
        )
        .apply(
            "TextIO.Write",
            textio::Write::new(out.to_string_lossy().into_owned()).without_sharding(),
        );
    p.run_with_runner(&PrismRunner::new())
        .await
        .expect("pipeline failed");

    let mut lines = lines_of(&out);
    lines.sort();
    assert_eq!(
        lines,
        vec![
            "a.json 7 {\"a\":1}",
            "a.json 7 {\"a\":1}",
            "b.json 8 {\"b\":22}",
        ]
    );
}

#[tokio::test]
async fn match_disallows_empty_results_by_default() {
    let dir = TempDir::new("empty_match");
    let p = Pipeline::new();
    p.apply(
        "FileIO.MatchMetadata",
        fileio::Match::new(dir.join("*.none")),
    )
    .apply("FileIO.ReadMatches", fileio::ReadMatches::new());
    let err = p
        .run_with_runner(&PrismRunner::new())
        .await
        .expect_err("an empty match must fail by default");
    let message = format!("{err:?}");
    assert!(message.contains("No files matched pattern"), "{message}");

    let p = Pipeline::new();
    p.apply(
        "FileIO.MatchMetadata",
        fileio::Match::new(dir.join("*.none"))
            .with_empty_match_treatment(fileio::EmptyMatchTreatment::AllowIfWildcard),
    )
    .apply("FileIO.ReadMatches", fileio::ReadMatches::new());
    p.run_with_runner(&PrismRunner::new())
        .await
        .expect("wildcard empty match is allowed");
}

/// Writes a stream of three batches in one window with a trigger that fires for each
/// element, and returns the sorted names of the output files.
async fn write_repeated_early_panes(dir: &TempDir, num_shards: u32) -> Vec<String> {
    let at = |word: &str, millis: i64| (word.to_string(), Instant::from_millis(millis));
    let p = Pipeline::new();
    p.apply(
        "Events",
        TestStream::new()
            .add_timestamped_elements([at("a", 1_000), at("b", 1_500)])
            .advance_watermark_to(Instant::from_millis(2_000))
            .add_timestamped_elements([at("c", 3_000)])
            .advance_watermark_to(Instant::from_millis(4_000))
            .add_timestamped_elements([at("d", 5_000), at("e", 5_500)])
            .advance_watermark_to_infinity(),
    )
    .apply(
        "FixedWindows",
        WindowInto::new(FixedWindows::of(Duration::from_secs(10)))
            .with_trigger(Trigger::repeatedly(Trigger::after_count(1)))
            .with_accumulation_mode(AccumulationMode::Discarding),
    )
    .apply(
        "TextIO.Write",
        textio::Write::new(dir.join("out.txt"))
            .with_num_shards(num_shards)
            .with_windowed_writes(),
    );
    p.run_with_runner(&PrismRunner::new())
        .await
        .expect("pipeline failed");
    dir.entries()
}

/// Checks that the files hold every element exactly once.
fn assert_each_element_once(dir: &TempDir, entries: &[String]) {
    let mut all: Vec<String> = entries
        .iter()
        .flat_map(|name| lines_of(&dir.as_path().join(name)))
        .collect();
    all.sort();
    assert_eq!(all, ["a", "b", "c", "d", "e"], "files: {entries:?}");
}

#[tokio::test]
async fn windowed_runner_sharded_writes_keep_every_firing() {
    let dir = TempDir::new("windowed_runner_sharded");
    let entries = write_repeated_early_panes(&dir, 0).await;
    assert!(
        entries.iter().all(|name| name.starts_with("out-0-10000")),
        "{entries:?}"
    );
    assert!(
        entries.len() > 1,
        "each firing writes its own files: {entries:?}"
    );
    assert_each_element_once(&dir, &entries);
}

#[tokio::test]
async fn windowed_fixed_shard_writes_keep_every_firing() {
    let dir = TempDir::new("windowed_fixed_shards");
    let entries = write_repeated_early_panes(&dir, 2).await;
    assert_each_element_once(&dir, &entries);
    // Each shard firing writes one file, named by its own pane and its shard index. A shard
    // with no data in a firing writes no file, so no file is empty.
    let panes: BTreeSet<&str> = entries
        .iter()
        .map(|name| {
            name.strip_suffix("-00000-of-00002.txt")
                .or_else(|| name.strip_suffix("-00001-of-00002.txt"))
                .unwrap_or_else(|| panic!("unexpected file {name}"))
        })
        .collect();
    assert!(
        panes.len() > 1,
        "each firing writes its own files: {entries:?}"
    );
    for name in &entries {
        assert!(
            !lines_of(&dir.as_path().join(name)).is_empty(),
            "{name} is empty: {entries:?}"
        );
    }
}
