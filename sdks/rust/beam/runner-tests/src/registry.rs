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
//! [`VALIDATES_RUNNER_TESTS`] lists every test once, by suite, and [`RUNNER_SKIPS`] lists
//! the entries that a runner skips in batch or streaming mode. Both test binaries call
//! [`run_validates_runner_suite`](crate::run_validates_runner_suite), and the Dataflow
//! worker looks tests up in [`VALIDATES_RUNNER_TESTS`].
//!
//! A test is a builder that only constructs its pipeline, plus the expected outcome. The
//! pipeline's closures exist only in code, so a Dataflow worker rebuilds the pipeline: the
//! driver passes the test id as the [`ValidatesRunnerOptions::vr_test`] option.

use beam::options::{OptionGroupRegistration, PipelineOptionGroup};
use beam::runners::dataflow::TestDataflowRunner;
use beam::runners::{PipelineResult, PipelineRunner};
use beam::testing::{TestPipeline, TestPipelineError};
use beam_runner_prism::PrismRunner;
use serde::{Deserialize, Serialize};

use crate::pipelines::*;

macro_rules! validates_runner_tests {
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
                expect: validates_runner_tests!(@expect $($expect)?),
            },)*)*
        ];
    };
}

validates_runner_tests! {
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
                group_by_key_large_iterable => build_group_by_key_large_iterable,
                combine_composed_accumulators => build_combine_composed_accumulators,
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
                // Keyed batching on a sharded key (`beam:coder:sharded_key:v1`).
                group_into_batches_sharded_key => build_group_into_batches_sharded_key,
                // Sequence generation and splittable DoFns.
                generate_sequence => build_generate_sequence,
                generate_sequence_with_rate_in_fixed_windows =>
                    build_generate_sequence_with_rate_in_fixed_windows,
                periodic_impulse => build_periodic_impulse,
                sdf_self_checkpoint => build_sdf_self_checkpoint,
                sdf_watermark_estimator => build_sdf_watermark_estimator,
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
                dynamic_row_state => build_dynamic_row_state,
                multimap_row_key_side_input => build_multimap_row_key_side_input,
            }
            /// Registered function types, which the worker builds from the graph.
            registered_functions {
                registered_dofn => build_registered_dofn,
                registered_splittable_dofn => build_registered_splittable_dofn,
                registered_combine_per_key => build_registered_combine_per_key,
                registered_combine_globally => build_registered_combine_globally,
            }
            /// Side inputs (Fn API state access).
            side_inputs {
                side_input_views => build_side_input_views,
                empty_iterable_side_input => build_empty_iterable_side_input,
                windowed_side_input => build_windowed_side_input,
                side_input_multiple_and_coders => build_side_input_multiple_and_coders,
            }
            /// Stateful ParDo (ValueState, BagState, MapState, SetState, MultimapState,
            /// OrderedListState), timers, window expiration, time-sorted input and bundle
            /// lifecycle.
            state_and_timers {
                stateful_pardo => build_stateful_pardo,
                combining_state => build_combining_state,
                windowed_combining_state => build_windowed_combining_state,
                windowed_stateful_pardo => build_windowed_stateful_pardo,
                map_and_set_state => build_map_and_set_state,
                map_and_set_state_clear => build_map_and_set_state_clear,
                map_and_set_state_remove => build_map_and_set_state_remove,
                ordered_list_state => build_ordered_list_state,
                ordered_list_state_clear => build_ordered_list_state_clear,
                multimap_state => build_multimap_state,
                multimap_state_clear_remove => build_multimap_state_clear_remove,
                on_window_expiration => build_on_window_expiration,
                on_window_expiration_with_lateness => build_on_window_expiration_with_lateness,
                on_window_expiration_same_key_two_windows =>
                    build_on_window_expiration_same_key_two_windows,
                bundle_lifecycle_batching => build_bundle_lifecycle_batching,
                timer_strict_ordering => build_timer_strict_ordering,
                timer_looping => build_timer_looping,
                timer_output_timestamps => build_timer_output_timestamps,
                timer_dynamic_tags => build_timer_dynamic_tags,
                processing_time_timer => build_processing_time_timer,
                timer_with_side_input => build_timer_with_side_input,
                requires_time_sorted_input => build_requires_time_sorted_input,
            }
            /// Windowing and windowed aggregations.
            windowing {
                windowed_group_by_key => build_windowed_group_by_key,
                sliding_windows_pardo => build_sliding_windows_pardo,
                window_sums_gbk => build_window_sums_gbk,
                window_sums_lifted => build_window_sums_lifted,
                rewindow_preserves_multiplicity => build_rewindow_preserves_multiplicity,
                timestamps_and_window_reify => build_timestamps_and_window_reify,
                distinct_per_window => build_distinct_per_window,
            }
            /// In-pipeline assertions (`passert`) and user metrics.
            assertions {
                passert_success => build_passert_success,
                user_metrics => build_user_metrics [Expectation::Custom(check_user_metrics)],
            }
            /// Bundle finalization of a DoFn, a StatefulDoFn and a SplittableDoFn.
            bundle_finalization {
                bundle_finalization => build_bundle_finalization
                    [Expectation::Custom(check_bundle_finalization)],
            }
            /// Side inputs: windowed and empty views, views of combines and flattens, nested
            /// iterables, multi-output and stateful DoFns, timers and bundle finalization.
            side_inputs_more {
                global_side_inputs_in_fixed_windows => build_global_side_inputs_in_fixed_windows,
                side_inputs_in_dofn_and_closure => build_side_inputs_in_dofn_and_closure,
                broadcast_joins => build_broadcast_joins,
                windowed_multimap_side_input => build_windowed_multimap_side_input,
                empty_multimap_side_input => build_empty_multimap_side_input,
                combine_globally_as_singleton => build_combine_globally_as_singleton,
                empty_flatten_side_input => build_empty_flatten_side_input,
                nested_iterable_side_input => build_nested_iterable_side_input,
                side_input_read_by_every_element => build_side_input_read_by_every_element,
                multi_output_side_inputs => build_multi_output_side_inputs,
                bag_state_side_input => build_bag_state_side_input,
                state_side_input => build_state_side_input,
                side_input_not_ready_timer => build_side_input_not_ready_timer,
                triggered_side_input => build_triggered_side_input,
                bundle_finalization_with_side_input => build_bundle_finalization_with_side_input
                    [Expectation::Custom(check_bundle_finalization_with_side_input)],
            }
            /// State cells: one state id in two DoFns, tagged outputs, a multimap cell after
            /// every key is removed, ordered list ranges, and a triggered global combine.
            state_more {
                value_state_same_id_in_two_dofns => build_value_state_same_id_in_two_dofns,
                stateful_dofn_tagged_outputs => build_stateful_dofn_tagged_outputs,
                multimap_state_empty_after_remove => build_multimap_state_empty_after_remove,
                ordered_list_state_read_range => build_ordered_list_state_read_range,
                global_combine_with_defaults_and_trigger =>
                    build_global_combine_with_defaults_and_trigger,
            }
            /// Core transforms: empty and large-key grouping, reshuffle metadata, empty
            /// flattens, batching limits, an empty global combine and schema'd DoFns.
            core_more {
                group_by_key_empty => build_group_by_key_empty,
                group_by_key_large_keys_10kb => build_group_by_key_large_keys_10kb,
                group_by_key_large_keys_100kb => build_group_by_key_large_keys_100kb,
                group_by_key_large_keys_1mb => build_group_by_key_large_keys_1mb,
                group_by_key_large_keys_10mb => build_group_by_key_large_keys_10mb,
                group_by_key_large_keys_100mb => build_group_by_key_large_keys_100mb,
                reshuffle_keeps_element_metadata => build_reshuffle_keeps_element_metadata,
                flatten_empty_inputs_then_pardo => build_flatten_empty_inputs_then_pardo,
                group_into_batches_count_and_byte_limits =>
                    build_group_into_batches_count_and_byte_limits,
                combine_globally_empty_without_defaults =>
                    build_combine_globally_empty_without_defaults,
                simple_combine_empty => build_simple_combine_empty,
                schema_dofn_reads_and_writes_rows => build_schema_dofn_reads_and_writes_rows,
                schema_dofn_writes_rows_to_tagged_outputs =>
                    build_schema_dofn_writes_rows_to_tagged_outputs,
                schema_kept_through_filter => build_schema_kept_through_filter,
            }
            /// Windowing: output times, re-windowing, windowed CoGroupByKey, reshuffle of
            /// windowed data, session merging, `finish_bundle` output, windowed combines and
            /// flatten.
            windowing_more {
                rewindow_with_output_time => build_rewindow_with_output_time,
                cogroup_by_key_windowed => build_cogroup_by_key_windowed,
                reshuffle_keeps_windows => build_reshuffle_keeps_windows,
                reshuffle_after_group_by_key_keeps_windows =>
                    build_reshuffle_after_group_by_key_keeps_windows,
                session_windows_merge_in_group_by_key =>
                    build_session_windows_merge_in_group_by_key,
                group_by_key_output_time => build_group_by_key_output_time,
                finish_bundle_windowed_output => build_finish_bundle_windowed_output,
                windowed_combine_per_key => build_windowed_combine_per_key,
                windowed_combine_globally => build_windowed_combine_globally,
                flatten_preserves_windowing => build_flatten_preserves_windowing,
            }
            /// Timers: after a GroupByKey, reset, timers that set each other, watermark
            /// holds, processing-time families, global window expiration, pipeline options
            /// and tagged output timestamps.
            timers_more {
                gbk_then_timers => build_gbk_then_timers,
                event_time_timer_reset => build_event_time_timer_reset,
                timers_set_each_other => build_timers_set_each_other,
                timer_output_hold => build_timer_output_hold,
                processing_time_timer_output_hold => build_processing_time_timer_output_hold,
                processing_time_timer_family => build_processing_time_timer_family,
                on_window_expiration_global => build_on_window_expiration_global,
                pipeline_options_in_dofn => build_pipeline_options_in_dofn,
                tagged_output_timestamps => build_tagged_output_timestamps,
            }
            /// Splittable DoFns: side inputs, windowed side inputs across checkpoints,
            /// tagged outputs and lifecycle order.
            sdf_more {
                sdf_side_input => build_sdf_side_input,
                sdf_windowed_side_input => build_sdf_windowed_side_input,
                sdf_windowed_side_input_checkpoints => build_sdf_windowed_side_input_checkpoints,
                sdf_tagged_outputs => build_sdf_tagged_outputs,
                sdf_lifecycle => build_sdf_lifecycle,
            }
            /// DoFn lifecycle: call order of multi-output and stateful functions.
            lifecycle_more {
                multi_output_dofn_lifecycle => build_multi_output_dofn_lifecycle,
                stateful_dofn_lifecycle => build_stateful_dofn_lifecycle,
            }
            /// Assertions on windows, empty tagged outputs, `Impulse` and an identity
            /// composite.
            assertions_more {
                windowed_singleton_is_equal_to => build_windowed_singleton_is_equal_to,
                windowed_satisfies => build_windowed_satisfies,
                multi_output_pardo_empty_outputs => build_multi_output_pardo_empty_outputs,
                impulse => build_impulse,
                identity_composite => build_identity_composite,
            }
            /// The bounds of event time: the `Impulse` timestamp and the firing times at the
            /// end of the global window.
            time_bounds {
                impulse_timestamp_is_min => build_impulse_timestamp_is_min,
                end_of_global_window_firing_times => build_end_of_global_window_firing_times,
            }
            /// `Option` elements and KV values through `beam:coder:nullable:v1`, also around a
            /// Row that the runner length-prefixes.
            nullable_elements {
                create_with_nulls_and_values => build_create_with_nulls_and_values,
                nullable_rows_through_reshuffle => build_nullable_rows_through_reshuffle,
                nullable_values_grouped_by_key => build_nullable_values_grouped_by_key,
            }
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
    /// The run's outcome on a runner is checked by this function, which panics to fail the
    /// test.
    Custom(fn(RunnerKind, Result<PipelineResult, TestPipelineError>)),
}

/// The runner that runs a test. A [`Expectation::Custom`] check reads it where runners
/// report different results, for example metrics.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RunnerKind {
    /// The local Prism runner.
    Prism,
    /// Google Cloud Dataflow.
    Dataflow,
}

/// A runner of the ValidatesRunner tests.
pub trait SuiteRunner: PipelineRunner {
    /// The kind of this runner.
    const KIND: RunnerKind;
}

impl SuiteRunner for PrismRunner {
    const KIND: RunnerKind = RunnerKind::Prism;
}

impl SuiteRunner for TestDataflowRunner {
    const KIND: RunnerKind = RunnerKind::Dataflow;
}

impl Expectation {
    /// Checks the outcome of running the test `id` on `runner` and panics if it is not the
    /// expected one.
    pub fn check(
        &self,
        id: &str,
        runner: RunnerKind,
        result: Result<PipelineResult, TestPipelineError>,
    ) {
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
            (Self::Custom(check), result) => check(runner, result),
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
    /// Function that builds the pipeline for this test.
    pub build: BuildFn,
    /// Expected outcome of running this test.
    pub expect: Expectation,
}

/// Runs `test` on `runner`: builds its pipeline into a fresh [`test_pipeline`], runs it,
/// and checks the outcome against the test's [`Expectation`].
///
/// [`test_pipeline`]: crate::test_pipeline
pub async fn run_test(test: &ValidatesRunnerTest, runner: &impl SuiteRunner) {
    run_pipeline(test.id, test.build, test.expect, runner).await;
}

/// Runs a pipeline built by `build` on `runner` and checks the outcome against `expect`,
/// as [`run_test`] does for a registered test. `id` names it in failure messages.
pub async fn run_pipeline<R: SuiteRunner>(
    id: &str,
    build: BuildFn,
    expect: Expectation,
    runner: &R,
) {
    let p = test_pipeline();
    build(&p);
    expect.check(id, R::KIND, p.run_with(runner).await);
}

/// Finds a registered ValidatesRunner test by its id.
pub fn find_validates_runner_test(id: &str) -> Option<&'static ValidatesRunnerTest> {
    VALIDATES_RUNNER_TESTS.iter().find(|t| t.id == id)
}

/// Execution mode of a `ValidatesRunner` suite.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SuiteMode {
    /// Bounded input.
    Batch,
    /// The runner runs the pipeline as a streaming job.
    Streaming,
}

/// A test that `runner` skips in `modes`, and the reason.
#[derive(Clone, Copy, Debug)]
pub struct RunnerSkip {
    /// The id of the skipped test.
    pub id: &'static str,
    /// The runner that skips it.
    pub runner: RunnerKind,
    /// The modes in which the runner skips it.
    pub modes: &'static [SuiteMode],
    /// The runner limit or bug.
    pub reason: &'static str,
}

const BOTH: &[SuiteMode] = &[SuiteMode::Batch, SuiteMode::Streaming];

/// Tests that a runner skips in batch or streaming mode because of a runner limit or bug.
pub const RUNNER_SKIPS: &[RunnerSkip] = &[
    RunnerSkip {
        id: "on_window_expiration_same_key_two_windows",
        runner: RunnerKind::Prism,
        modes: BOTH,
        reason: "Prism registers a key for window expiration only in its first window of a batch",
    },
    RunnerSkip {
        id: "multimap_state_empty_after_remove",
        runner: RunnerKind::Prism,
        modes: BOTH,
        reason: "Prism MultimapKeysUserState still returns keys removed through MultimapUserState",
    },
    RunnerSkip {
        id: "requires_time_sorted_input",
        runner: RunnerKind::Dataflow,
        modes: BOTH,
        reason: "Runner V2 rejects beam:requirement:pardo:time_sorted_input:v1",
    },
    RunnerSkip {
        id: "processing_time_timer",
        runner: RunnerKind::Dataflow,
        modes: &[SuiteMode::Batch],
        reason: "Runner V2 batch does not fire processing-time timers",
    },
    RunnerSkip {
        id: "processing_time_timer_output_hold",
        runner: RunnerKind::Dataflow,
        modes: &[SuiteMode::Batch],
        reason: "Runner V2 batch does not fire processing-time timers",
    },
    RunnerSkip {
        id: "processing_time_timer_family",
        runner: RunnerKind::Dataflow,
        modes: &[SuiteMode::Batch],
        reason: "Runner V2 batch does not fire processing-time timers",
    },
    RunnerSkip {
        id: "group_by_key_large_keys_10kb",
        runner: RunnerKind::Dataflow,
        modes: &[SuiteMode::Streaming],
        reason: "Dataflow streaming does not support keys of 10 KiB or more",
    },
    RunnerSkip {
        id: "group_by_key_large_keys_100kb",
        runner: RunnerKind::Dataflow,
        modes: &[SuiteMode::Streaming],
        reason: "Dataflow streaming does not support keys of 10 KiB or more",
    },
    RunnerSkip {
        id: "group_by_key_large_keys_1mb",
        runner: RunnerKind::Dataflow,
        modes: &[SuiteMode::Streaming],
        reason: "Dataflow streaming does not support keys of 10 KiB or more",
    },
    RunnerSkip {
        id: "group_by_key_large_keys_10mb",
        runner: RunnerKind::Dataflow,
        modes: BOTH,
        reason: "Dataflow does not support keys of 10 MiB or more (10 KiB in streaming)",
    },
    RunnerSkip {
        id: "group_by_key_large_keys_100mb",
        runner: RunnerKind::Dataflow,
        modes: BOTH,
        reason: "Dataflow does not support keys of 10 MiB or more (10 KiB in streaming)",
    },
    RunnerSkip {
        id: "multimap_state",
        runner: RunnerKind::Dataflow,
        modes: &[SuiteMode::Streaming],
        reason: "Streaming Engine returns no values for a multimap key that only got appends",
    },
    RunnerSkip {
        id: "multimap_state_clear_remove",
        runner: RunnerKind::Dataflow,
        modes: &[SuiteMode::Streaming],
        reason: "Streaming Engine returns no values for a multimap key that only got appends",
    },
    RunnerSkip {
        id: "multimap_state_empty_after_remove",
        runner: RunnerKind::Dataflow,
        modes: &[SuiteMode::Streaming],
        reason: "Streaming Engine returns no values for a multimap key that only got appends",
    },
    RunnerSkip {
        id: "rewindow_with_output_time",
        runner: RunnerKind::Dataflow,
        modes: &[SuiteMode::Batch],
        reason: "Runner V2 batch ignores OutputTime::LatestInPane on GlobalWindows (apache/beam#20931)",
    },
    RunnerSkip {
        id: "bundle_finalization_with_side_input",
        runner: RunnerKind::Dataflow,
        modes: &[SuiteMode::Streaming],
        reason: "Streaming Engine sends FinalizeBundle only to stages that read a shuffled value \
                 stream, and this stage reads a GroupByKey",
    },
];

/// Returns the skip reason when `(runner, mode)` skips `id`, or `None` when it runs `id`.
pub fn runner_skip_reason(id: &str, runner: RunnerKind, mode: SuiteMode) -> Option<&'static str> {
    RUNNER_SKIPS
        .iter()
        .find(|skip| skip.id == id && skip.runner == runner && skip.modes.contains(&mode))
        .map(|skip| skip.reason)
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
