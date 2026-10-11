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

//! Finding assertions in a pipeline and checking, from metrics, that they ran and passed.

use std::collections::{BTreeMap, BTreeSet};

use beam::metrics::{MetricFilter, MetricKey, MetricResults};
use beam::pipeline::Pipeline;
use beam::runners::PipelineResult;

use super::{FAILURE_COUNTER, PASSERT_ANNOTATION, PASSERT_NAMESPACE, SUCCESS_COUNTER};

/// Returns how many assertions `pipeline` contains.
pub fn count_assertions(pipeline: &Pipeline) -> usize {
    pipeline
        .lock()
        .components
        .transforms
        .values()
        .filter(|t| t.annotations.contains_key(PASSERT_ANNOTATION))
        .count()
}

/// Returns the names of the assertions `pipeline` contains, sorted.
///
/// These are the names [`verify_assertions`] expects per-assertion counters for.
pub fn assertion_names(pipeline: &Pipeline) -> Vec<String> {
    let mut names: Vec<String> = pipeline
        .lock()
        .components
        .transforms
        .values()
        .filter_map(|t| {
            let recorded = t.annotations.get(PASSERT_ANNOTATION)?;
            Some(if recorded.is_empty() {
                t.unique_name.clone()
            } else {
                String::from_utf8_lossy(recorded).into_owned()
            })
        })
        .collect();
    names.sort();
    names
}

/// Name of the counter, in [`PASSERT_NAMESPACE`], that the assertion named `assertion`
/// increments when it passes.
pub fn success_counter_name(assertion: &str) -> String {
    format!("{SUCCESS_COUNTER}/{assertion}")
}

/// Name of the counter, in [`PASSERT_NAMESPACE`], that the assertion named `assertion`
/// increments when it fails.
pub fn failure_counter_name(assertion: &str) -> String {
    format!("{FAILURE_COUNTER}/{assertion}")
}

/// Checks that exactly `expected` assertions ran and passed.
///
/// An assertion whose input never produces a group, typically because its window never
/// closed, does not fail: it never runs. Counting successes against the number of
/// assertions in the pipeline is what reveals it.
///
/// This compares one total, so an assertion counted twice (for instance through a
/// retried bundle) can mask one that never ran. Prefer [`verify_assertions`], which
/// checks every assertion individually; [`TestPipeline`](crate::TestPipeline) uses it.
///
/// Returns an error when the runner reported no metrics, or reported a different count.
pub fn verify_success_count(result: &PipelineResult, expected: i64) -> Result<(), String> {
    let metrics = result
        .metrics()
        .ok_or_else(|| "the runner reported no metrics for this pipeline".to_string())?;
    let (passed, failed) = assertion_counts(metrics);
    if failed > 0 {
        return Err(format!("{failed} PAssert assertion(s) failed"));
    }
    if passed != expected {
        return Err(format!(
            "expected {expected} PAssert assertion(s) to pass, but {passed} did"
        ));
    }
    Ok(())
}

/// Checks that every assertion named in `assertions` passed at least once, and that no
/// assertion failed.
///
/// `assertions` are the names [`assertion_names`] returns for the pipeline that ran.
/// See the [module documentation](crate::passert#checking-that-assertions-ran) for why each
/// assertion must pass *at least* once rather than exactly once.
///
/// Returns an error naming the assertions that failed or never ran, or saying that the
/// runner reported no metrics.
pub fn verify_assertions<S: AsRef<str>>(
    result: &PipelineResult,
    assertions: &[S],
) -> Result<(), String> {
    let metrics = result
        .metrics()
        .ok_or_else(|| "the runner reported no metrics for this pipeline".to_string())?;
    match assertion_status(metrics, assertions) {
        AssertionStatus::AllPassed(_) => Ok(()),
        AssertionStatus::Failed { failed, .. } if !failed.is_empty() => {
            Err(format!("PAssert assertion(s) failed: {failed:?}"))
        }
        AssertionStatus::Failed { total, .. } => {
            Err(format!("{total} PAssert assertion(s) failed"))
        }
        AssertionStatus::Pending { missing, .. } => Err(format!(
            "{} of {} PAssert assertion(s) never ran: {missing:?}. An assertion runs once \
             its input's window closes; on an unbounded input, advance the watermark to \
             infinity",
            missing.len(),
            assertions.len()
        )),
    }
}

/// Where a set of assertions stands, according to the metrics a runner reported.
///
/// Returned by [`assertion_status`]. Runners that watch a job while it runs, such as a
/// streaming test runner, poll it until it is no longer [`Pending`](Self::Pending).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AssertionStatus {
    /// No assertion failed, but some have not passed yet.
    Pending {
        /// How many of the expected assertions have passed.
        passed: usize,
        /// The expected assertions that have not passed yet, sorted.
        missing: Vec<String>,
    },
    /// Every expected assertion passed at least once, and none failed. Holds how many
    /// assertions were expected.
    AllPassed(usize),
    /// At least one assertion failed.
    Failed {
        /// The assertions whose own failure counter is non-zero, sorted. May be empty
        /// when only the aggregate [`FAILURE_COUNTER`] was reported.
        failed: Vec<String>,
        /// The aggregate [`FAILURE_COUNTER`].
        total: i64,
    },
}

/// Returns where the assertions named in `assertions` stand, according to `metrics`.
///
/// Exactly the rule [`verify_assertions`] applies: any failure counter above zero is a
/// failure; otherwise each assertion's own success counter must be at least one.
/// Counters are matched exactly against [`PASSERT_NAMESPACE`], [`SUCCESS_COUNTER`],
/// [`FAILURE_COUNTER`] and the per-assertion names. The aggregate success counter is
/// not enough on its own: one assertion counted twice must not hide another that never
/// ran.
pub fn assertion_status<S: AsRef<str>>(
    metrics: &MetricResults,
    assertions: &[S],
) -> AssertionStatus {
    let counters = metrics.query_counters(&MetricFilter::all().with_namespace(PASSERT_NAMESPACE));
    let tally = counters
        .iter()
        .filter_map(|c| Some((classify(&c.key)?, c.result().unwrap_or(0))))
        .fold(Tally::default(), Tally::add);
    let expected: BTreeSet<&str> = assertions.iter().map(AsRef::as_ref).collect();
    status(&tally, &expected)
}

/// A `PAssert` counter, and the assertion it belongs to (`None` for the aggregate).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PAssertCounter<'a> {
    Success(Option<&'a str>),
    Failure(Option<&'a str>),
}

/// Recognises a `PAssert` counter by its key. The only place that knows how the
/// counters are named.
fn classify(key: &MetricKey) -> Option<PAssertCounter<'_>> {
    if key.namespace != PASSERT_NAMESPACE {
        return None;
    }
    counter_owner(&key.name, SUCCESS_COUNTER)
        .map(PAssertCounter::Success)
        .or_else(|| counter_owner(&key.name, FAILURE_COUNTER).map(PAssertCounter::Failure))
}

/// `Some(None)` when `name` is the aggregate `counter`, `Some(Some(assertion))` when it
/// is `counter/assertion`, and `None` otherwise.
fn counter_owner<'a>(name: &'a str, counter: &str) -> Option<Option<&'a str>> {
    if name == counter {
        return Some(None);
    }
    name.strip_prefix(counter)?.strip_prefix('/').map(Some)
}

/// `PAssert` counters summed per assertion.
#[derive(Debug, Default)]
struct Tally<'a> {
    passed: BTreeMap<&'a str, i64>,
    failed: BTreeMap<&'a str, i64>,
    total_failed: i64,
}

impl<'a> Tally<'a> {
    fn add(mut self, (counter, value): (PAssertCounter<'a>, i64)) -> Self {
        match counter {
            PAssertCounter::Success(Some(name)) => *self.passed.entry(name).or_default() += value,
            // The aggregate success count cannot tell which assertion passed.
            PAssertCounter::Success(None) => {}
            PAssertCounter::Failure(Some(name)) => *self.failed.entry(name).or_default() += value,
            PAssertCounter::Failure(None) => self.total_failed += value,
        }
        self
    }
}

/// Decides where the `expected` assertions stand, given the counters in `tally`.
fn status(tally: &Tally<'_>, expected: &BTreeSet<&str>) -> AssertionStatus {
    let failed: Vec<String> = tally
        .failed
        .iter()
        .filter(|&(_, &n)| n > 0)
        .map(|(name, _)| (*name).to_string())
        .collect();
    if !failed.is_empty() || tally.total_failed > 0 {
        return AssertionStatus::Failed {
            failed,
            total: tally.total_failed,
        };
    }
    let missing: Vec<String> = expected
        .iter()
        .filter(|name| tally.passed.get(*name).copied().unwrap_or(0) < 1)
        .map(|name| (*name).to_string())
        .collect();
    if missing.is_empty() {
        AssertionStatus::AllPassed(expected.len())
    } else {
        AssertionStatus::Pending {
            passed: expected.len() - missing.len(),
            missing,
        }
    }
}

/// Returns how many assertions passed and failed, according to `metrics`.
pub fn assertion_counts(metrics: &MetricResults) -> (i64, i64) {
    let passed = metrics
        .counter(PASSERT_NAMESPACE, SUCCESS_COUNTER)
        .unwrap_or(0);
    let failed = metrics
        .counter(PASSERT_NAMESPACE, FAILURE_COUNTER)
        .unwrap_or(0);
    (passed, failed)
}
