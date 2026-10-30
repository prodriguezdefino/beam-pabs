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

//! Bundle finalization: a `DoFn`, a `StatefulDoFn` and a `SplittableDoFn` that declare
//! `WithFinalizer` register callbacks, and each callback writes one marker file.
//!
//! A callback runs in the worker after the runner commits the bundle, so the driver checks the
//! markers after the run. The directory of the markers travels in the `Create` elements:
//! the graph carries the elements of the driver, so the workers use the driver's directory.
//! A runner can skip a finalization, so on Dataflow the check expects at least one marker
//! of each kind; Prism finalizes every bundle, so on Prism every marker must be there.
//!
//! The `DoFn` and the `StatefulDoFn` follow `Create` in their stage to verify that fused
//! transforms finalize their bundles.

use std::collections::BTreeSet;
use std::sync::OnceLock;
use std::time::{SystemTime, UNIX_EPOCH};

use beam::io::file::get_filesystem;
use beam::prelude::*;
use beam::runners::PipelineResult;
use beam::testing::{TestPipeline, TestPipelineError};
use beam::transforms::WithFinalizer;
use beam::transforms::sdf::{
    OffsetRange, OffsetRangeTracker, ProcessContinuation, RestrictionTracker, SplittableDoFn,
    SplittableParDo,
};

use crate::env::scratch_root;

/// The words of the `DoFn` and the `StatefulDoFn`.
const WORDS: [&str; 4] = ["alpha", "beta", "gamma", "delta"];
/// The offsets `[0, OFFSETS)` that the `SplittableDoFn` reads.
const OFFSETS: i64 = 20;

/// The marker directory of this run. The driver computes it once; a worker that rebuilds
/// the pipeline computes another one, which no step uses.
fn marker_dir() -> &'static str {
    static DIR: OnceLock<String> = OnceLock::new();
    DIR.get_or_init(|| {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let dir = format!(
            "{}/bundle-finalization-{}-{nanos}",
            scratch_root().trim_end_matches('/'),
            std::process::id()
        );
        if !dir.contains("://") {
            std::fs::create_dir_all(&dir).expect("create the local marker directory");
        }
        dir
    })
}

/// Writes the empty marker `name` in `dir`.
fn write_marker(dir: &str, name: &str) -> Result {
    let path = format!("{dir}/{name}");
    get_filesystem(&path)
        .and_then(|fs| fs.open_write(&path).map(drop))
        .map_err(|e| Error::msg(format!("Cannot write the marker {path}: {e}")))
}

/// Registers a callback for each word that writes `dofn-<word>`.
#[derive(Clone)]
pub struct MarkingDoFn;

impl DoFn for MarkingDoFn {
    type In = (String, String);
    type Out = WithFinalizer<String>;

    fn process_element(
        &mut self,
        (dir, word): (String, String),
        ctx: &mut ProcessContext<'_, Self::Out>,
    ) -> Result {
        let marker = format!("dofn-{word}");
        ctx.finalizer(move || write_marker(&dir, &marker))
            .register()?;
        ctx.emit(word.into())
    }
}

/// Per-key state of [`MarkingStatefulDoFn`], so runners treat the transform as stateful.
#[derive(State)]
pub struct MarkingState {
    count: ValueState<i64>,
}

/// Registers a callback for each key that writes `stateful-<key>`.
#[derive(Clone)]
pub struct MarkingStatefulDoFn;

impl StatefulDoFn for MarkingStatefulDoFn {
    type Key = String;
    type Value = String;
    type Out = WithFinalizer<(String, String)>;
    type State = MarkingState;
    type Timer = NoTimers;

    fn process_element(
        &mut self,
        word: &String,
        dir: String,
        state: &mut MarkingState,
        ctx: &mut StatefulContext<'_, Self>,
    ) -> Result {
        state.count.write(state.count.read()?.unwrap_or(0) + 1)?;
        let marker = format!("stateful-{word}");
        let marker_dir = dir.clone();
        ctx.finalizer(move || write_marker(&marker_dir, &marker))
            .register()?;
        ctx.emit((dir, word.clone()).into())
    }
}

/// Reads the offsets `[0, OFFSETS)` of its element and registers, for each range that one
/// call claims, a callback that writes `sdf-<start>-<end>`.
#[derive(Clone)]
pub struct MarkingSdf;

impl SplittableDoFn for MarkingSdf {
    type In = String;
    type Out = WithFinalizer<i64>;
    type Restriction = OffsetRange;
    type Tracker = OffsetRangeTracker;

    fn initial_restriction(&self, _dir: &String) -> Result<OffsetRange> {
        Ok(OffsetRange::new(0, OFFSETS))
    }

    fn create_tracker(&self, restriction: &OffsetRange) -> OffsetRangeTracker {
        OffsetRangeTracker::new(*restriction)
    }

    fn process_element(
        &mut self,
        dir: String,
        tracker: &OffsetRangeTracker,
        ctx: &mut ProcessContext<'_, Self::Out>,
    ) -> Result<ProcessContinuation> {
        let start = tracker.current_restriction().start;
        let claimed: Vec<i64> = (start..)
            .take_while(|offset| tracker.try_claim(offset))
            .collect();
        if let Some(end) = claimed.last().map(|last| last + 1) {
            let marker = format!("sdf-{start}-{end}");
            ctx.finalizer(move || write_marker(&dir, &marker))
                .register()?;
        }
        claimed
            .into_iter()
            .try_for_each(|offset| ctx.emit(offset.into()))?;
        Ok(ProcessContinuation::Stop)
    }
}

/// Builds the bundle finalization test: the functions above, with their outputs
/// asserted in the pipeline. See [`check_bundle_finalization`] for the markers.
pub fn build_bundle_finalization(p: &TestPipeline) {
    let dir = marker_dir().to_string();
    let pairs: Vec<(String, String)> = WORDS
        .iter()
        .map(|word| (dir.clone(), (*word).to_string()))
        .collect();
    let words = p
        .apply("Words", Create::new(pairs))
        .apply("MarkingDoFn", ParDo::new(MarkingDoFn));
    beam::testing::passert::that("AssertWords", &words)
        .contains_in_any_order(WORDS.map(String::from));

    let keyed = p.apply(
        "Keyed",
        Create::new(
            WORDS
                .iter()
                .map(|word| (word.to_string(), dir.clone()))
                .collect::<Vec<_>>(),
        ),
    );
    let stateful = keyed
        .apply("MarkingStatefulDoFn", ParDo::new(MarkingStatefulDoFn))
        .apply("MarkingFusedDoFn", ParDo::new(MarkingDoFn));
    beam::testing::passert::that("AssertStateful", &stateful)
        .contains_in_any_order(WORDS.map(String::from));

    let offsets = p
        .apply("Dir", Create::new(vec![dir]))
        .apply("MarkingSdf", SplittableParDo::new(MarkingSdf));
    beam::testing::passert::that("AssertOffsets", &offsets).contains_in_any_order(0..OFFSETS);
}

/// Checks the markers of [`build_bundle_finalization`] after a successful run.
pub fn check_bundle_finalization(result: std::result::Result<PipelineResult, TestPipelineError>) {
    result.expect("the pipeline should succeed");
    let dir = marker_dir();
    let markers: BTreeSet<String> = get_filesystem(dir)
        .and_then(|fs| fs.match_files(&format!("{dir}/*")))
        .expect("list the markers")
        .iter()
        .filter_map(|path| path.rsplit('/').next().map(str::to_string))
        .collect();
    let of_kind = |kind: &str| -> BTreeSet<String> {
        markers
            .iter()
            .filter_map(|m| m.strip_prefix(&format!("{kind}-")).map(str::to_string))
            .collect()
    };
    let ranges: Vec<(i64, i64)> = of_kind("sdf")
        .iter()
        .filter_map(|range| range.split_once('-'))
        .filter_map(|(start, end)| Some((start.parse().ok()?, end.parse().ok()?)))
        .collect();
    let mut covered: Vec<i64> = ranges
        .iter()
        .flat_map(|(start, end)| *start..*end)
        .collect();
    covered.sort_unstable();
    let distinct = covered.len() == covered.iter().collect::<BTreeSet<_>>().len();
    assert!(distinct, "the SDF finalized overlapping ranges: {ranges:?}");
    assert!(
        covered.iter().all(|offset| (0..OFFSETS).contains(offset)),
        "the SDF finalized offsets that it did not claim: {ranges:?}"
    );

    let words: BTreeSet<String> = WORDS.iter().map(|w| w.to_string()).collect();
    if dir.contains("://") {
        // A runner of the cloud can skip a finalization: expect each kind at least once.
        assert!(!of_kind("dofn").is_empty(), "no DoFn marker in {markers:?}");
        assert!(
            !of_kind("stateful").is_empty(),
            "no StatefulDoFn marker in {markers:?}"
        );
        assert!(
            !ranges.is_empty(),
            "no SplittableDoFn marker in {markers:?}"
        );
    } else {
        assert_eq!(of_kind("dofn"), words, "DoFn markers");
        assert_eq!(of_kind("stateful"), words, "StatefulDoFn markers");
        assert_eq!(
            covered,
            (0..OFFSETS).collect::<Vec<_>>(),
            "SDF ranges {ranges:?}"
        );
    }
}
