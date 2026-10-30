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

use std::fs;
use std::path::Path;

use beam::io::file::fileio::{self, ReadableFile};
use beam::io::file::{FormatSink, TextFormat, WriteFiles, textio};
use beam::pipeline::Pipeline;
use beam::transforms::{Create, Map};
use prism::PrismRunner;
use tests::TempDir;

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
    let listing = std::env::temp_dir().join(format!(
        "beam_writefiles_listing_{}.txt",
        std::process::id()
    ));
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
    let _ = fs::remove_file(&listing);
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
    let out = std::env::temp_dir().join(format!(
        "beam_writefiles_read_matches_{}.txt",
        std::process::id()
    ));

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
    let _ = fs::remove_file(&out);
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
