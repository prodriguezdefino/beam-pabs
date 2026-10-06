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

//! DoFn lifecycle: the call order of multi-output and stateful functions, and errors in
//! the bundle methods.

use beam::prelude::*;
use beam::testing::{TestPipeline, passert};

use crate::Expectation;
use crate::dofns::*;

const ELEMENTS: [&str; 3] = ["elem1", "elem2", "elem3"];

/// Checks the `finish:<n>` lines of a bundle: one line or more, and `processed` elements
/// in all.
fn check_finish_lines<'a>(lines: impl Iterator<Item = &'a str>, processed: usize) -> Result {
    let counts: Vec<usize> = lines
        .map(|line| {
            line.strip_prefix("finish:")
                .and_then(|n| n.parse().ok())
                .ok_or_else(|| format!("bad finish line {line}"))
        })
        .collect::<std::result::Result<_, _>>()?;
    match (counts.is_empty(), counts.iter().sum::<usize>() == processed) {
        (false, true) => Ok(()),
        (true, _) | (false, false) => Err(format!(
            "finish_bundle must report each of {processed} elements once: {counts:?}"
        )
        .into()),
    }
}

/// Validates the call order of a multi-output DoFn. Each element is processed once in a
/// bundle, and each bundle reports its count on the second output.
///
/// Java: `ParDoLifecycleTest.testFnCallSequenceMulti`.
pub fn build_multi_output_dofn_lifecycle(p: &TestPipeline) {
    let LifecycleCallsPCollections {
        processed,
        finished,
    } = p
        .apply("Create", Create::new(ELEMENTS.map(String::from).to_vec()))
        .par_do("Lifecycle", MultiOutputLifecycleDoFn::default());
    passert::that("Processed", &processed)
        .contains_in_any_order(ELEMENTS.map(|e| format!("process:{e}")));
    passert::that("Finished", &finished).satisfies(|lines: &[String]| {
        check_finish_lines(lines.iter().map(String::as_str), ELEMENTS.len())
    });
}

/// Validates the call order of a stateful DoFn. Each element is processed once in a bundle,
/// and the bundle counts add up to the element count.
///
/// Java: `ParDoLifecycleTest.testFnCallSequenceStateful`.
pub fn build_stateful_dofn_lifecycle(p: &TestPipeline) {
    let out = p
        .apply(
            "Create",
            Create::new(vec![
                ("k1".to_string(), "a".to_string()),
                ("k1".to_string(), "b".to_string()),
                ("k2".to_string(), "c".to_string()),
            ]),
        )
        .apply("Lifecycle", ParDo::new(StatefulLifecycleDoFn::default()));
    passert::that("AssertCalls", &out).satisfies(|lines: &[String]| {
        let (finished, mut processed): (Vec<&str>, Vec<&str>) = lines
            .iter()
            .map(String::as_str)
            .partition(|line| line.starts_with("finish:"));
        processed.sort_unstable();
        if processed != ["process:k1=a", "process:k1=b", "process:k2=c"] {
            return Err(format!("each element must be processed once: {lines:?}").into());
        }
        check_finish_lines(finished.into_iter(), processed.len())
    });
}

/// An error in `start_bundle` fails the job with the error message.
pub const START_BUNDLE_FAILURE: Expectation = Expectation::FailsWith(&["start_bundle failure"]);

/// Builds a pipeline whose DoFn fails in `start_bundle`; see [`START_BUNDLE_FAILURE`].
///
/// Java: `ParDoTest$LifecycleTests.testParDoWithErrorInStartBatch`.
pub fn build_start_bundle_failure(p: &TestPipeline) {
    bundle_failure(p, BundleMethod::StartBundle);
}

/// An error in `finish_bundle` fails the job with the error message.
pub const FINISH_BUNDLE_FAILURE: Expectation = Expectation::FailsWith(&["finish_bundle failure"]);

/// Builds a pipeline whose DoFn fails in `finish_bundle`; see [`FINISH_BUNDLE_FAILURE`].
///
/// Java: `ParDoTest$LifecycleTests.testParDoWithErrorInFinishBatch`.
pub fn build_finish_bundle_failure(p: &TestPipeline) {
    bundle_failure(p, BundleMethod::FinishBundle);
}

fn bundle_failure(p: &TestPipeline, method: BundleMethod) {
    let out = p
        .apply("Create", Create::new(vec![1i64, 2, 3]))
        .par_do("Fail", FailingBundleDoFn { method });
    passert::that("Unreachable", &out).has_count(3);
}
