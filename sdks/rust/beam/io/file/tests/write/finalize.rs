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

//! Fault-tolerance paths of WriteFiles finalization: retried (idempotent) renames,
//! the fixed-shard layout and its positional fallback, and temp-directory cleanup.
//! These are the paths that prevent data loss or duplication on retry.

use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use beam::coders::{IntervalWindow, PaneInfo};
use file::write_files::{Finalizer, WriterConfig};
use file::{DefaultFilenamePolicy, FileFormat, FormatSink};

/// Text lines framed by a header and footer, so empty files are recognisable.
struct Framed;

impl FileFormat<String> for Framed {
    fn write_element(&self, e: &String, w: &mut dyn Write) -> beam::Result {
        Ok(writeln!(w, "{e}")?)
    }
    fn write_header(&self, w: &mut dyn Write) -> beam::Result {
        Ok(w.write_all(b"H\n")?)
    }
    fn write_footer(&self, w: &mut dyn Write) -> beam::Result {
        Ok(w.write_all(b"F\n")?)
    }
}

/// A fresh scratch directory with a `tmp/` subdirectory, removed on drop.
struct Scratch(PathBuf);

impl Scratch {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!(
            "beam-finalize-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(dir.join("tmp")).unwrap();
        Self(dir)
    }
    fn path(&self, rel: &str) -> String {
        self.0.join(rel).to_string_lossy().into_owned()
    }
    fn temp_dir(&self) -> String {
        self.path("tmp")
    }
    /// Writes a temp file with `body` and returns its path.
    fn temp_file(&self, name: &str, body: &str) -> String {
        let p = self.path(&format!("tmp/{name}"));
        fs::write(&p, body).unwrap();
        p
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn finalizer(s: &Scratch, num_shards: u32, rolling: bool, windowed: bool) -> Finalizer<String> {
    Finalizer {
        prefix: s.path("out/part"),
        suffix: ".txt".to_string(),
        shard_template: "-SSSSS-of-NNNNN".to_string(),
        num_shards,
        rolling,
        windowed,
        policy: Arc::new(DefaultFilenamePolicy),
        writer: Arc::new(WriterConfig {
            sink: Arc::new(FormatSink::new(Framed)),
            temp_dir: s.temp_dir(),
            max_records: None,
            max_bytes: None,
        }),
    }
}

fn read(path: &str) -> String {
    fs::read_to_string(path).unwrap()
}

/// Final paths relative to the output directory.
fn names(s: &Scratch, targets: &[String]) -> Vec<String> {
    let base = s.path("out/");
    targets
        .iter()
        .map(|t| t.strip_prefix(&base).unwrap_or(t).to_string())
        .collect()
}

#[test]
fn fixed_sharding_keeps_shard_index_and_fills_missing_shards_with_empty_files() {
    let s = Scratch::new();
    let f = finalizer(&s, 3, false, false);
    let a = s.temp_file("a", "H\nx\nF\n");
    let c = s.temp_file("c", "H\nz\nF\n");
    // Out of order: the shard index, not input order, decides the name.
    let targets = f
        .finalize(vec![(c.clone(), (2, 0)), (a.clone(), (0, 0))], None, None)
        .unwrap();
    assert_eq!(
        names(&s, &targets),
        [
            "part-00000-of-00003.txt",
            "part-00001-of-00003.txt",
            "part-00002-of-00003.txt"
        ]
    );
    assert_eq!(read(&targets[0]), "H\nx\nF\n");
    assert_eq!(
        read(&targets[1]),
        "H\nF\n",
        "missing shard must be header+footer"
    );
    assert_eq!(read(&targets[2]), "H\nz\nF\n");
    assert!(!Path::new(&a).exists() && !Path::new(&c).exists());
}

#[test]
fn retried_finalization_is_idempotent() {
    let s = Scratch::new();
    let f = finalizer(&s, 0, false, false);
    let results = vec![
        (s.temp_file("a", "one\n"), (-1, 0)),
        (s.temp_file("b", "two\n"), (-1, 1)),
    ];
    let first = f.finalize(results.clone(), None, None).unwrap();
    // The retry finds every temp file already renamed and must succeed unchanged.
    let second = f.finalize(results, None, None).unwrap();
    assert_eq!(first, second);
    assert_eq!(
        names(&s, &first),
        ["part-00000-of-00002.txt", "part-00001-of-00002.txt"]
    );
    assert_eq!(read(&first[0]), "one\n");
    assert_eq!(read(&first[1]), "two\n");
}

#[test]
fn missing_temp_file_without_target_is_an_error() {
    let s = Scratch::new();
    let f = finalizer(&s, 0, false, false);
    let gone = s.path("tmp/never-written");
    let err = f
        .finalize(vec![(gone.clone(), (-1, 0))], None, None)
        .unwrap_err();
    assert!(
        err.to_string()
            .starts_with(&format!("Failed to move '{gone}' to '")),
        "unexpected error: {err}"
    );
}

#[test]
fn out_of_range_shard_falls_back_to_positional_numbering() {
    let s = Scratch::new();
    let f = finalizer(&s, 2, false, false);
    let results = vec![
        (s.temp_file("a", "a\n"), (0, 0)),
        (s.temp_file("b", "b\n"), (5, 0)),
    ];
    let targets = f.finalize(results, None, None).unwrap();
    // Two files numbered by position, and no empty filler for shard 1.
    assert_eq!(
        names(&s, &targets),
        ["part-00000-of-00002.txt", "part-00001-of-00002.txt"]
    );
    assert_eq!(read(&targets[0]), "a\n");
    assert_eq!(read(&targets[1]), "b\n");
}

#[test]
fn duplicate_shard_falls_back_to_positional_numbering() {
    let s = Scratch::new();
    let f = finalizer(&s, 4, false, false);
    let results = vec![
        (s.temp_file("b", "b\n"), (0, 1)),
        (s.temp_file("a", "a\n"), (0, 0)),
    ];
    let targets = f.finalize(results, None, None).unwrap();
    assert_eq!(
        names(&s, &targets),
        ["part-00000-of-00002.txt", "part-00001-of-00002.txt"]
    );
    // Ordered by (shard, sequence), not input order.
    assert_eq!(read(&targets[0]), "a\n");
    assert_eq!(read(&targets[1]), "b\n");
}

#[test]
fn rolling_numbers_files_by_position_even_with_fixed_shards() {
    let s = Scratch::new();
    let f = finalizer(&s, 3, true, false);
    let targets = f
        .finalize(vec![(s.temp_file("a", "a\n"), (1, 0))], None, None)
        .unwrap();
    assert_eq!(names(&s, &targets), ["part-00000-of-00001.txt"]);
    assert_eq!(read(&targets[0]), "a\n");
}

#[test]
fn duplicate_results_are_finalized_once() {
    let s = Scratch::new();
    let f = finalizer(&s, 0, false, false);
    let a = s.temp_file("a", "a\n");
    let targets = f
        .finalize(vec![(a.clone(), (-1, 0)), (a, (-1, 0))], None, None)
        .unwrap();
    assert_eq!(names(&s, &targets), ["part-00000-of-00001.txt"]);
    assert_eq!(read(&targets[0]), "a\n");
}

#[test]
fn empty_input_writes_one_empty_file_unless_windowed() {
    let s = Scratch::new();
    let targets = finalizer(&s, 0, false, false)
        .finalize(Vec::new(), None, None)
        .unwrap();
    assert_eq!(names(&s, &targets), ["part-00000-of-00001.txt"]);
    assert_eq!(read(&targets[0]), "H\nF\n");

    let s = Scratch::new();
    let targets = finalizer(&s, 0, false, true)
        .finalize(Vec::new(), None, None)
        .unwrap();
    assert!(
        targets.is_empty(),
        "windowed empty firing wrote {targets:?}"
    );
    assert!(!Path::new(&s.path("out")).exists());
}

#[test]
fn windowed_names_include_the_window() {
    let s = Scratch::new();
    let f = finalizer(&s, 0, false, true);
    let window = IntervalWindow::new(0, 60_000);
    let targets = f
        .finalize(
            vec![(s.temp_file("a", "a\n"), (-1, 0))],
            Some(window),
            Some(PaneInfo::NO_FIRING),
        )
        .unwrap();
    assert_eq!(names(&s, &targets), ["part-0-60000-00000-of-00001.txt"]);
}

#[test]
fn cleanup_removes_leftover_temp_files_and_directory() {
    let s = Scratch::new();
    let f = finalizer(&s, 0, false, false);
    s.temp_file("orphan-1", "x");
    s.temp_file("orphan-2", "y");
    f.cleanup();
    assert!(!Path::new(&s.temp_dir()).exists());
    // Idempotent: cleaning an already removed directory is a no-op.
    f.cleanup();
    assert!(!Path::new(&s.temp_dir()).exists());
}

#[test]
fn remove_temp_dir_if_empty_keeps_in_flight_files() {
    let s = Scratch::new();
    let f = finalizer(&s, 0, false, true);
    let in_flight = s.temp_file("other-window", "x");
    f.remove_temp_dir_if_empty();
    assert_eq!(read(&in_flight), "x");

    fs::remove_file(&in_flight).unwrap();
    f.remove_temp_dir_if_empty();
    assert!(!Path::new(&s.temp_dir()).exists());
}
