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
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use beam::pipeline::URN_COMBINE_PER_KEY;
use beam::prelude::*;
use beam::transforms::Sum;
use beam_model::pipeline as proto;
use beam_runner_prism::PrismRunner;
use prost::Message;

/// Counts how many times `merge_accumulators` sees each accumulator, so a test
/// can tell partial combining apart from plain grouping.
struct CountingSum {
    accumulators_merged: Arc<AtomicUsize>,
}

impl CombineFn for CountingSum {
    type Input = i64;
    type Accum = i64;
    type Output = i64;

    fn create_accumulator(&self) -> Result<i64> {
        Ok(0)
    }

    fn add_input(&self, accumulator: i64, input: i64) -> Result<i64> {
        Ok(accumulator + input)
    }

    fn merge_accumulators(&self, accumulators: Vec<i64>) -> Result<i64> {
        self.accumulators_merged
            .fetch_add(accumulators.len(), Ordering::SeqCst);
        Ok(accumulators.into_iter().sum())
    }

    fn extract_output(&self, accumulator: i64) -> Result<i64> {
        Ok(accumulator)
    }
}
/// Combiners fold values into an accumulator before the shuffle. The merge
/// step receives one accumulator per key per bundle rather than one per input
/// element.
#[tokio::test]
async fn test_combine_per_key_aggregates_before_shuffle() {
    let accumulators_merged = Arc::new(AtomicUsize::new(0));

    let p = Pipeline::new();
    let elements: Vec<(String, i64)> = (0..100).map(|i| ("hot".to_string(), i)).collect();

    p.apply("Create", Create::new(elements)).combine_per_key(
        "SumByKey",
        CountingSum {
            accumulators_merged: Arc::clone(&accumulators_merged),
        },
    );

    p.run_with_runner(&PrismRunner::new())
        .await
        .expect("combine pipeline must succeed");

    let merged = accumulators_merged.load(Ordering::SeqCst);
    assert_eq!(
        merged, 1,
        "100 values for one key should reach the merge step as a single \
         per-bundle accumulator, but {merged} accumulators were merged"
    );
}

#[test]
fn test_combine_per_key_composite_representation() {
    let p = Pipeline::new();

    let out = p
        .apply(
            "Input",
            Create::new(vec![("k".to_string(), 1i64), ("k".to_string(), 2i64)]),
        )
        .combine_per_key("SumByKey", Sum::new());

    p.validate().expect("combine DAG must be valid");

    let proto = p.to_proto();
    let components = proto.components.expect("components must exist");

    let (composite_id, composite) = components
        .transforms
        .iter()
        .find(|(_, t)| t.unique_name == "SumByKey")
        .expect("SumByKey composite transform must exist");

    let spec = composite.spec.as_ref().expect("spec must exist");
    assert_eq!(spec.urn, URN_COMBINE_PER_KEY);

    let payload = proto::CombinePayload::decode(&spec.payload[..])
        .expect("payload must decode as CombinePayload");
    assert!(!payload.accumulator_coder_id.is_empty());
    assert!(
        components
            .coders
            .contains_key(&payload.accumulator_coder_id)
    );

    assert_eq!(composite.subtransforms.len(), 2);
    for sub_id in &composite.subtransforms {
        assert!(
            components.transforms.contains_key(sub_id),
            "subtransform {sub_id} must exist in transforms map"
        );
    }

    let sub_names: Vec<&str> = composite
        .subtransforms
        .iter()
        .map(|id| components.transforms[id].unique_name.as_str())
        .collect();

    for stage in ["SumByKey/GroupByKey", "SumByKey/CombineGroupedValues"] {
        assert!(
            sub_names.contains(&stage),
            "expected stage '{stage}' among {sub_names:?}"
        );
    }

    assert_eq!(composite.outputs.get("out"), Some(&out.id().to_string()));
    assert!(!composite_id.is_empty());
}

#[tokio::test]
async fn test_combine_globally_with_fanout_empty_default_and_without_defaults() {
    use beam::testing::{TestPipeline, passert};

    let p = TestPipeline::new();
    let empty_with_default = p
        .apply("EmptyDefault", Create::new(Vec::<i64>::new()))
        .apply(
            "SumDefault",
            CombineGlobally::new(Sum::new()).with_fanout(4),
        );
    let empty_no_default = p
        .apply("EmptyNoDefault", Create::new(Vec::<i64>::new()))
        .apply(
            "SumNoDefault",
            CombineGlobally::new(Sum::new())
                .without_defaults()
                .with_fanout(4),
        );
    passert::that("DefaultZero", &empty_with_default).contains_in_any_order([0i64]);
    passert::that("EmptyOut", &empty_no_default).empty();
    p.run_with(&PrismRunner::new())
        .await
        .expect("fanout empty input cases must pass");
}

/// Sums `i64` values. Fails when an accumulator does not come from this `CombineFn`, or when
/// `merge_accumulators` gets no accumulators.
///
/// Python: `CombineFnLifecycleTest` (`test_combine`, `test_non_liftable_combine`,
/// `test_combining_value_state`).
#[derive(Clone, Default)]
struct CheckedAccumulatorSum;

impl CheckedAccumulatorSum {
    fn check_phase(caller: &str, phase: i64) -> Result<()> {
        match phase {
            1..=3 => Ok(()),
            other => Err(format!("{caller}: invalid accumulator phase {other}").into()),
        }
    }
}

impl CombineFn for CheckedAccumulatorSum {
    type Input = i64;
    type Accum = (i64, i64);
    type Output = i64;

    fn create_accumulator(&self) -> Result<(i64, i64)> {
        Ok((1, 0))
    }

    fn add_input(&self, (phase, sum): (i64, i64), input: i64) -> Result<(i64, i64)> {
        Self::check_phase("add_input", phase)?;
        Ok((2, sum + input))
    }

    fn merge_accumulators(&self, accumulators: Vec<(i64, i64)>) -> Result<(i64, i64)> {
        if accumulators.is_empty() {
            return Err("merge_accumulators received no accumulators".into());
        }
        let sum = accumulators
            .into_iter()
            .try_fold(0i64, |acc, (phase, value)| {
                Self::check_phase("merge_accumulators", phase)?;
                Ok::<_, Error>(acc + value)
            })?;
        Ok((3, sum))
    }

    fn extract_output(&self, (phase, sum): (i64, i64)) -> Result<i64> {
        Self::check_phase("extract_output", phase)?;
        Ok(sum)
    }
}

#[derive(State)]
struct CheckedCombiningState {
    sum: CombiningState<CheckedAccumulatorSum>,
}

#[derive(Clone)]
struct CheckedCombiningStateDoFn;

impl StatefulDoFn for CheckedCombiningStateDoFn {
    type Key = String;
    type Value = i64;
    type Out = (String, i64);
    type State = CheckedCombiningState;
    type Timer = NoTimers;
    const ON_WINDOW_EXPIRATION: bool = true;

    fn process_element(
        &mut self,
        _key: &Self::Key,
        value: Self::Value,
        state: &mut Self::State,
        _ctx: &mut StatefulContext<'_, Self>,
    ) -> Result {
        state.sum.add(value)
    }

    fn on_window_expiration(
        &mut self,
        key: &Self::Key,
        state: &mut Self::State,
        ctx: &mut StatefulContext<'_, Self>,
    ) -> Result {
        let total = state.sum.read()?;
        state.sum.clear()?;
        let cleared = state.sum.read()?;
        if cleared != 0 {
            return Err(format!("expected 0 after clear, got {cleared}").into());
        }
        ctx.emit((key.clone(), total))
    }
}

#[tokio::test]
async fn test_combine_fn_accumulators_lifted_non_lifted_and_state() {
    use beam::testing::{TestPipeline, passert};

    let p = TestPipeline::new();
    let lifted = p
        .apply("LiftedInput", Create::new((1i64..=8).collect::<Vec<_>>()))
        .apply(
            "LiftedCombine",
            CombineGlobally::new(CheckedAccumulatorSum).with_fanout(2),
        );
    let lifted_empty = p
        .apply("LiftedEmpty", Create::new(Vec::<i64>::new()))
        .apply(
            "LiftedEmptyCombine",
            CombineGlobally::new(CheckedAccumulatorSum),
        );

    let non_lifted_panes = p
        .apply("NonLiftedInput", Create::new(vec![1i64, 2, 3, 4]))
        .apply(
            "TriggeredGlobal",
            WindowInto::new(GlobalWindows)
                .with_trigger(Trigger::repeatedly(Trigger::after_count(2)))
                .with_accumulation_mode(AccumulationMode::Discarding),
        )
        .apply(
            "NonLiftedCombine",
            CombineGlobally::new(CheckedAccumulatorSum).without_defaults(),
        )
        .apply("RewindowGlobal", WindowInto::new(GlobalWindows))
        .sum_globally("SumNonLiftedPanes");

    let stateful = p
        .apply(
            "StateInput",
            Create::new(vec![
                ("a".to_string(), 10i64),
                ("a".to_string(), 20),
                ("b".to_string(), 7),
            ]),
        )
        .apply("CombiningState", ParDo::new(CheckedCombiningStateDoFn));

    passert::that_singleton("AssertLifted", &lifted).is_equal_to(36);
    passert::that_singleton("AssertLiftedEmpty", &lifted_empty).is_equal_to(0);
    passert::that_singleton("AssertNonLiftedSum", &non_lifted_panes).is_equal_to(10);
    passert::that("AssertStateful", &stateful)
        .contains_in_any_order([("a".to_string(), 30i64), ("b".to_string(), 7)]);

    p.run_with(&PrismRunner::new())
        .await
        .expect("CombineFn accumulator checks must pass");
}
