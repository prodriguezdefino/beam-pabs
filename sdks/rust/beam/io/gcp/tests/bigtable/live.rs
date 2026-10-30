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

//! Live integration test against Cloud Bigtable.
//!
//! Marked `#[ignore]` so regular test runs do not call Google Cloud. Set
//! `BIGTABLE_PROJECT`, `BIGTABLE_INSTANCE`, `BIGTABLE_TABLE`, and optionally
//! `BIGTABLE_APP_PROFILE` before running:
//! `./gradlew :sdks:rust:test -Ppkg=apache-beam-io-gcp -PtestArgs="live_ --ignored"`

use std::time::Duration;

use beam::coders::DefaultCoder;
use beam::internals::{ElementSink, HandlerContext};
use beam::schema::{Row, RowElement};
use beam::transforms::DoFn;
use beam::transforms::sdf::{ProcessContinuation, RestrictionTracker, SplittableDoFn};
use gcp::bigtable::RowFilterExt as _;
use gcp::bigtable::proto::RowFilter;
use gcp::bigtable::read::{KeyRangeRestriction, ReadOptions, ReadRangesFn, SampleKeyRangesFn};
use gcp::bigtable::target::ReadTarget;
use gcp::bigtable::{BigtableColumn, flattened_row_schema};

#[derive(Default)]
struct LiveRowSink(Vec<Vec<u8>>);

impl ElementSink for LiveRowSink {
    fn push(&mut self, element: Vec<u8>) -> Result<(), String> {
        self.0.push(element);
        Ok(())
    }
}

fn sample_live_ranges(
    target: ReadTarget,
    app_profile_id: Option<String>,
) -> Vec<KeyRangeRestriction> {
    let mut sample_fn = SampleKeyRangesFn {
        target,
        app_profile_id,
        start_key: Vec::new(),
        end_key: Vec::new(),
        max_initial_bundles: 16,
        sub_split_tablets: true,
        attempt_timeout: Duration::from_secs(10),
        endpoint: None,
        client: None,
    };

    let mut range_sink = LiveRowSink::default();
    let mut hctx = HandlerContext::new(&mut range_sink);
    let mut sample_ctx = hctx.as_process_context::<KeyRangeRestriction>();
    sample_fn
        .process_element(Vec::new(), &mut sample_ctx)
        .expect("live Bigtable SampleRowKeys must succeed");

    range_sink
        .0
        .iter()
        .map(|b| KeyRangeRestriction::decode(b).expect("valid KeyRangeRestriction bytes"))
        .collect()
}

fn read_first_live_range(options: ReadOptions, range: &KeyRangeRestriction) -> Vec<Vec<u8>> {
    let mut read_fn: ReadRangesFn<RowElement> = ReadRangesFn::new(options, false, None);
    let tracker = read_fn.create_tracker(range);
    let mut row_sink = LiveRowSink::default();
    let mut hctx = HandlerContext::new(&mut row_sink);
    let mut read_ctx = hctx.as_process_context::<RowElement>();
    let cont = read_fn
        .process_element(range.clone(), &tracker, &mut read_ctx)
        .expect("live Bigtable ReadRows must succeed");
    assert_eq!(cont, ProcessContinuation::Stop);
    tracker.check_done().expect("restriction tracker done");
    row_sink.0
}

/// Reads the target and the optional app profile from the environment.
fn live_target() -> (ReadTarget, Option<String>) {
    let project = std::env::var("BIGTABLE_PROJECT").unwrap_or_else(|_| "test-project".to_string());
    let instance =
        std::env::var("BIGTABLE_INSTANCE").unwrap_or_else(|_| "test-instance".to_string());
    let table = std::env::var("BIGTABLE_TABLE").unwrap_or_else(|_| "test-table".to_string());
    let app_profile = std::env::var("BIGTABLE_APP_PROFILE")
        .ok()
        .filter(|s| !s.is_empty());
    (ReadTarget::table(&project, &instance, &table), app_profile)
}

fn live_options(target: ReadTarget, app_profile: Option<String>) -> ReadOptions {
    let options = ReadOptions::new(target);
    match app_profile {
        Some(profile) => options.with_app_profile_id(profile),
        None => options,
    }
}

#[test]
#[ignore = "calls Cloud Bigtable"]
fn live_sample_and_read_from_bigtable() {
    let (target, app_profile) = live_target();
    let emitted_ranges = sample_live_ranges(target.clone(), app_profile.clone());
    assert!(
        !emitted_ranges.is_empty(),
        "expected at least one range from SampleRowKeys"
    );

    let rows = read_first_live_range(live_options(target, app_profile), &emitted_ranges[0]);
    assert!(!rows.is_empty(), "expected rows from live Bigtable table");

    let first_decoded = Row::from_row_bytes(&flattened_row_schema(), &rows[0]).unwrap();
    let col = BigtableColumn::try_from(&first_decoded).unwrap();
    assert!(!col.key.is_empty());
}

/// Checks that Cloud Bigtable accepts a `value_bitmask_filter`. The rows depend on the table,
/// so the test checks only that the read succeeds.
#[test]
#[ignore = "calls Cloud Bigtable"]
fn live_read_with_value_bitmask_filter() {
    let (target, app_profile) = live_target();
    let options =
        live_options(target, app_profile).with_row_filter(&RowFilter::value_bitmask(vec![0x00]));
    read_first_live_range(
        options,
        &KeyRangeRestriction::new(Vec::new(), Vec::new(), 1),
    );
}
