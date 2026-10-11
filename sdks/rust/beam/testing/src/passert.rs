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

//! In-graph assertions over the contents of a [`PCollection`].
//!
//! An assertion is part of the pipeline: it runs on the workers, alongside the
//! transforms it checks, and a failed assertion fails the bundle, and with it the
//! job. So [`run`](beam::runners::run) returns an error naming the
//! assertion and describing the mismatch; no element ever has to be brought back to
//! the driver.
//!
//! ```no_run
//! use beam::prelude::*;
//! use testing::{TestPipeline, passert};
//!
//! # async fn example() -> Result<(), testing::TestPipelineError> {
//! let p = TestPipeline::new();
//! let doubled = p
//!     .apply(Create::new("Create", vec![1i64, 2, 3]))
//!     .apply(Map::new("Double", |x: i64| x * 2));
//!
//! passert::that("AssertDoubled", &doubled).contains_in_any_order([2, 4, 6]);
//! passert::that("AssertDoubled", &doubled).has_count(3);
//! passert::that("AssertDoubled", &doubled).all("positive", |x| *x > 0);
//!
//! p.run().await?;
//! # Ok(())
//! # }
//! ```
//!
//! Build assertions on a [`TestPipeline`](crate::TestPipeline) rather than a plain
//! [`Pipeline`]: only a `TestPipeline` checks, after the run, that every assertion
//! executed (see [Checking that assertions ran](#checking-that-assertions-ran)).
//!
//! [`that`] only starts an assertion; a check method such as
//! [`contains_in_any_order`](PAssert::contains_in_any_order) must follow. A builder
//! dropped without any check panics, rather than silently asserting nothing.
//!
//! # How it works
//!
//! Every element is moved into the global window, keyed onto one key and grouped, so one
//! check sees the whole collection. A sentinel is flattened in first; without it an empty
//! collection gives no group, and
//! [`contains_in_any_order`](PAssert::contains_in_any_order) on it would pass silently.
//!
//! On an unbounded collection, the global window closes only when the watermark
//! reaches the end of time. Finish a [`TestStream`](super::TestStream) with
//! [`advance_watermark_to_infinity`](super::TestStream::advance_watermark_to_infinity).
//!
//! # Windows and panes
//!
//! [`in_window`](PAssert::in_window) and the pane selectors restrict an assertion to
//! the elements that a given window, or a given firing of it, produced. They read the
//! window and pane of each element, so use them on the output of a windowed
//! aggregation, where a trigger firing emitted every element.
//!
//! Selection fails closed: if the windows of an element (including the global window)
//! cannot be read as [`IntervalWindow`]s, the assertion fails. Otherwise
//! `in_window(w).empty()` would pass on a collection that was never windowed.
//!
//! [`that_windowed`] asserts on `(element, window)` pairs. Use it to check which windows
//! the elements landed in, without naming each window first.
//!
//! # Checking that assertions ran
//!
//! An assertion whose input never produces a group, usually because its window never
//! closed, does not fail: it never runs. So every assertion reports metrics:
//!
//! - The aggregate [`SUCCESS_COUNTER`] and [`FAILURE_COUNTER`] counters.
//! - A counter of its own, named after the assertion by [`success_counter_name`] and
//!   [`failure_counter_name`].
//!
//! [`TestPipeline`](crate::TestPipeline) calls [`verify_assertions`] after every run.
//! It requires the success counter of each assertion to be **at least one**, and every
//! failure counter to be zero. "At least once", because a runner can retry a bundle and,
//! if it reports attempted metrics, count a success twice. Per-assertion checks mean a
//! double count cannot hide an assertion that did not run, as a single total
//! ([`verify_success_count`]) could.

#[cfg(doc)]
use beam::{coders::IntervalWindow, pipeline::Pipeline, values::PCollection};

mod builder;
mod grouped;
mod mismatch;
mod singleton;
mod verify;
mod windowed;

pub use builder::{PAssert, that};
pub use grouped::{Group, GroupedAssert, that_grouped};
pub use singleton::{SingletonAssert, that_singleton};
pub use verify::{
    AssertionStatus, assertion_counts, assertion_names, assertion_status, count_assertions,
    failure_counter_name, success_counter_name, verify_assertions, verify_success_count,
};
pub use windowed::{WindowedAssert, interval_windows, that_windowed};

/// Metric namespace of the counters reported by assertions.
pub const PASSERT_NAMESPACE: &str = "PAssert";

/// Counter incremented once by each assertion that passes.
pub const SUCCESS_COUNTER: &str = "PAssertSuccess";

/// Counter incremented once by each assertion that fails.
pub const FAILURE_COUNTER: &str = "PAssertFailure";

/// Annotation marking the composite transform of every assertion.
///
/// Lets tooling such as [`TestPipeline`](crate::TestPipeline) count the assertions in a
/// pipeline from its graph alone. Runners ignore annotations they do not recognise.
pub const PASSERT_ANNOTATION: &str = "beam:rust:testing:passert:v1";
