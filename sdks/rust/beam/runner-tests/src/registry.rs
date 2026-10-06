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

//! The ValidatesRunner suite as data.
//!
//! [`validates_runner_suite!`](crate::validates_runner_suite) lists every test once, by
//! suite. [`VALIDATES_RUNNER_TESTS`] makes it a table of [`ValidatesRunnerTest`]s, each
//! runner's test binary makes it `#[tokio::test]` functions that call [`run_test`], and the
//! Dataflow worker looks tests up in the table.
//!
//! A test is a builder that only constructs its pipeline, plus the expected outcome. The
//! pipeline's closures exist only in code, so a Dataflow worker rebuilds the pipeline: the
//! driver passes the test id as the [`ValidatesRunnerOptions::vr_test`] option.

use beam::options::{OptionGroupRegistration, PipelineOptionGroup};
use beam::runners::{PipelineResult, PipelineRunner};
use beam::testing::{TestPipeline, TestPipelineError};
use serde::{Deserialize, Serialize};

use crate::pipelines::*;

/// Lists the ValidatesRunner suite, passing it to the macro `$generate`.
///
/// `$generate` receives one `suite { id => build_fn, ... }` group per suite, where
/// `build_fn(&pipeline)` constructs the pipeline of the test `id`. An entry may end in
/// `[expectation]`, the [`Expectation`](crate::Expectation) of running it, which
/// defaults to `Succeeds`. Ids are unique across suites.
///
/// To add a test, add one line here.
#[macro_export]
macro_rules! validates_runner_suite {
    ($generate:ident) => {
        $generate! {
            /// Core Beam model transforms: element-wise ParDo, GroupByKey, Combine,
            /// Flatten, Partition, SDF.
            core_transforms {
                // Element-wise & DoFn lifecycle.
                map_and_filter => build_map_and_filter,
                flat_map => build_flat_map,
                dofn_lifecycle => build_dofn_lifecycle,
                diamond_dag => build_diamond_dag,
                multi_output_pardo => build_multi_output_pardo,
                // GroupByKey & combiners.
                group_by_key => build_group_by_key,
                combine_per_key => build_combine_per_key,
                combine_max_min => build_combine_max_min,
                combine_flushes_accumulators_at_capacity =>
                    build_combine_flushes_accumulators_at_capacity,
                count_per_element => build_count_per_element,
                combine_globally => build_combine_globally,
                count_globally => build_count_globally,
                folds => build_folds,
                // Collection operations (Flatten, Partition, Reshuffle, KV).
                flatten_many => build_flatten_many,
                flatten_singleton_list => build_flatten_singleton_list,
                flatten_then_pardo => build_flatten_then_pardo,
                flatten_multiple_copies => build_flatten_multiple_copies,
                create_empty => build_create_empty,
                kv_swap => build_kv_swap,
                reshuffle => build_reshuffle,
                partition => build_partition,
                partition_many => build_partition_many,
                // Sequence generation (splittable DoFn).
                generate_sequence => build_generate_sequence,
                periodic_impulse => build_periodic_impulse,
            }
            /// Relational joins and CoGroupByKey.
            joins {
                cogroup_by_key => build_cogroup_by_key,
                joins => build_joins,
                join_cross_product => build_join_cross_product,
            }
            /// BeamRow schema encoding/decoding.
            schemas {
                row_schema_coder => build_row_schema_coder,
            }
            /// Side inputs (Fn API state access).
            side_inputs {
                side_input_views => build_side_input_views,
                empty_iterable_side_input => build_empty_iterable_side_input,
                windowed_side_input => build_windowed_side_input,
            }
            /// Stateful ParDo (ValueState, BagState, MapState, SetState).
            state_and_timers {
                stateful_pardo => build_stateful_pardo,
                windowed_stateful_pardo => build_windowed_stateful_pardo,
                map_and_set_state => build_map_and_set_state,
                map_and_set_state_clear => build_map_and_set_state_clear,
                bundle_lifecycle_batching => build_bundle_lifecycle_batching,
            }
            /// Windowing and windowed aggregations.
            windowing {
                windowed_group_by_key => build_windowed_group_by_key,
                sliding_windows_pardo => build_sliding_windows_pardo,
                window_sums_gbk => build_window_sums_gbk,
                window_sums_lifted => build_window_sums_lifted,
                rewindow_preserves_multiplicity => build_rewindow_preserves_multiplicity,
            }
            /// In-pipeline assertions (`passert`).
            assertions {
                passert_success => build_passert_success,
            }
        }
    };
}

/// Constructs the pipeline of one ValidatesRunner test, without running it.
pub type BuildFn = fn(&TestPipeline);

/// What running a test's pipeline must produce.
#[derive(Clone, Copy, Debug)]
pub enum Expectation {
    /// The job succeeds and every `passert` assertion ran and passed.
    Succeeds,
    /// The job itself fails, with an error whose debug rendering contains every fragment.
    FailsWith(&'static [&'static str]),
    /// The run's outcome is checked by this function, which panics to fail the test.
    Custom(fn(Result<PipelineResult, TestPipelineError>)),
}

impl Expectation {
    /// Checks the outcome of running the test `id` and panics if it is not the expected one.
    pub fn check(&self, id: &str, result: Result<PipelineResult, TestPipelineError>) {
        match (self, result) {
            (Self::Succeeds, Ok(_)) => {}
            (Self::Succeeds, Err(err)) => panic!("{id}: the pipeline should succeed: {err:?}"),
            (Self::FailsWith(_), Ok(result)) => {
                panic!(
                    "{id}: the pipeline should fail, but ended in {}",
                    result.state
                )
            }
            (Self::FailsWith(fragments), Err(err)) => {
                let message = format!("{err:?}");
                assert!(
                    matches!(err, TestPipelineError::Runner(_)),
                    "{id}: the job should fail, not only the verification of its assertions: \
                     {message}"
                );
                let missing: Vec<&str> = fragments
                    .iter()
                    .copied()
                    .filter(|fragment| !message.contains(fragment))
                    .collect();
                assert!(
                    missing.is_empty(),
                    "{id}: the error should mention {missing:?}: {message}"
                );
            }
            (Self::Custom(check), result) => check(result),
        }
    }
}

/// One entry of the ValidatesRunner suite.
#[derive(Clone, Copy, Debug)]
pub struct ValidatesRunnerTest {
    /// The suite (test module) the test belongs to.
    pub suite: &'static str,
    /// The test's id, unique across suites.
    pub id: &'static str,
    pub build: BuildFn,
    pub expect: Expectation,
}

/// Runs `test` on `runner`: builds its pipeline into a fresh [`test_pipeline`], runs it,
/// and checks the outcome against the test's [`Expectation`].
///
/// [`test_pipeline`]: crate::test_pipeline
pub async fn run_test(test: &ValidatesRunnerTest, runner: &dyn PipelineRunner) {
    run_pipeline(test.id, test.build, test.expect, runner).await;
}

/// Runs a pipeline built by `build` on `runner` and checks the outcome against `expect`,
/// as [`run_test`] does for a registered test. `id` names it in failure messages.
pub async fn run_pipeline(
    id: &str,
    build: BuildFn,
    expect: Expectation,
    runner: &dyn PipelineRunner,
) {
    let p = test_pipeline();
    build(&p);
    expect.check(id, p.run_with(runner).await);
}

#[allow(
    unused_macro_rules,
    reason = "every registered test currently succeeds; the arm is how an entry would \
              declare another expectation"
)]
macro_rules! table {
    (@expect) => { Expectation::Succeeds };
    (@expect $expect:expr) => { $expect };
    ($($(#[$doc:meta])* $suite:ident {
        $($id:ident => $build:ident $([$expect:expr])?),* $(,)?
    })*) => {
        /// Every ValidatesRunner test, in suite order.
        pub static VALIDATES_RUNNER_TESTS: &[ValidatesRunnerTest] = &[
            $($(ValidatesRunnerTest {
                suite: stringify!($suite),
                id: stringify!($id),
                build: $build,
                expect: table!(@expect $($expect)?),
            },)*)*
        ];
    };
}

validates_runner_suite!(table);

pub fn find_validates_runner_test(id: &str) -> Option<&'static ValidatesRunnerTest> {
    VALIDATES_RUNNER_TESTS.iter().find(|t| t.id == id)
}

/// Pipeline options of a ValidatesRunner job. Registered, so the worker sees the driver's
/// value.
#[derive(clap::Args, Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ValidatesRunnerOptions {
    /// Id of the ValidatesRunner test whose pipeline the job runs. Tells the worker
    /// which pipeline to rebuild; see [`find_validates_runner_test`].
    #[arg(long)]
    #[serde(default)]
    pub vr_test: Option<String>,
}

inventory::submit! { OptionGroupRegistration::of::<ValidatesRunnerOptions>() }

impl PipelineOptionGroup for ValidatesRunnerOptions {
    fn group_name() -> &'static str {
        "ValidatesRunnerOptions"
    }
}
