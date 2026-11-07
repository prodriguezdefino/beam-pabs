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

//! Hot-key fanout of `CombinePerKey` and `CombineGlobally`: graph shape, phase windowing,
//! and runs on Prism.

use std::time::Duration;

use beam::coders::{BeamIterable, DefaultCoder};
use beam::internals::{PipelineInternals, registered_combine_handler};
use beam::pipeline::{
    CombineStage, URN_COMBINE_GLOBALLY, URN_COMBINE_PER_KEY, URN_RUST_COMBINE_FN,
    URN_RUST_COMBINE_FN_REGISTERED, stage_key,
};
use beam::prelude::*;
use beam_model::pipeline as proto;
use beam_testing::{TestPipeline, passert};

use super::common::{
    OpaqueKey, RowKey, ScaledSum, combine_fn_spec, do_fn_spec, kvs, run_bundle, transform,
};

#[test]
fn fanout_zero_or_one_turns_fanout_off() {
    for fanout in [0, 1] {
        let p = Pipeline::new();
        let _ = p.apply("Create", Create::new(kvs(&[("a", 1)]))).apply(
            "Sum",
            CombinePerKey::new(ScaledSum { factor: 1 }).with_hot_key_fanout(fanout),
        );
        assert_eq!(
            transform(&p, "Sum").spec.expect("spec").urn,
            URN_COMBINE_PER_KEY
        );

        let pg = Pipeline::new();
        let _ = pg.apply("Create", Create::new(vec![1i64])).apply(
            "Total",
            CombineGlobally::new(ScaledSum { factor: 1 }).with_fanout(fanout),
        );
        assert_eq!(
            transform(&pg, "Total").spec.expect("spec").urn,
            URN_COMBINE_GLOBALLY
        );
    }
}

#[tokio::test]
async fn registered_combine_with_hot_key_fanout_runs_without_registered_handlers() {
    let p = TestPipeline::new();
    let out = p
        .apply(
            "Create",
            Create::new(kvs(&[("a", 1), ("b", 2), ("a", 3), ("a", 4), ("b", 5)])),
        )
        .apply(
            "Sum",
            CombinePerKey::new(ScaledSum { factor: 10 }).with_hot_key_fanout(4),
        );
    passert::that("Totals", &out).contains_in_any_order(kvs(&[("a", 80), ("b", 70)]));

    assert!(transform(p.pipeline(), "Sum").spec.is_none());
    assert_eq!(
        combine_fn_spec(p.pipeline(), "Sum/PreCombine").urn,
        URN_RUST_COMBINE_FN_REGISTERED
    );
    assert_eq!(
        combine_fn_spec(p.pipeline(), "Sum/PostCombine").urn,
        URN_RUST_COMBINE_FN_REGISTERED
    );
    assert!(
        p.pipeline()
            .transform_handlers()
            .keys()
            .all(|key| !key.starts_with("Sum")),
        "{:?}",
        p.pipeline().transform_handlers().keys()
    );
    p.run().await.expect("pipeline and assertions pass");
}

#[tokio::test]
async fn row_key_and_distinct_accum_type_work_with_fanout() {
    let k1 = RowKey {
        group: "a".to_string(),
        bucket: 1,
    };
    let k2 = RowKey {
        group: "b".to_string(),
        bucket: 2,
    };
    let p = TestPipeline::new();
    let out = p
        .apply(
            "Create",
            Create::new(vec![
                (k1.clone(), 10i64),
                (k2.clone(), 4),
                (k1.clone(), 20),
                (k1.clone(), 30),
            ]),
        )
        .apply(
            "Mean",
            CombinePerKey::new(Mean::new()).with_hot_key_fanout(3),
        );
    passert::that("ExpectedMeans", &out).contains_in_any_order(vec![(k1, 20.0), (k2, 4.0)]);
    assert!(
        p.pipeline()
            .transform_handlers()
            .keys()
            .all(|key| !key.starts_with("Mean")),
        "{:?}",
        p.pipeline().transform_handlers().keys()
    );
    p.run().await.expect("pipeline and assertions pass");
}

#[tokio::test]
async fn combine_globally_with_fanout_leaves_outer_spec_unset_and_runs() {
    let p = TestPipeline::new();
    let out = p.apply("Create", Create::new(vec![1i64, 2, 3, 4])).apply(
        "Total",
        CombineGlobally::new(ScaledSum { factor: 2 }).with_fanout(4),
    );
    passert::that("AssertTotal", &out).contains_in_any_order([20i64]);

    assert!(transform(p.pipeline(), "Total").spec.is_none());
    assert!(transform(p.pipeline(), "Total/Combine").spec.is_none());
    assert_eq!(
        combine_fn_spec(p.pipeline(), "Total/Combine/PreCombine").urn,
        URN_RUST_COMBINE_FN_REGISTERED
    );
    assert_eq!(
        combine_fn_spec(p.pipeline(), "Total/Combine/PostCombine").urn,
        URN_RUST_COMBINE_FN_REGISTERED
    );
    p.run().await.expect("pipeline and assertions pass");
}

#[test]
fn opaque_key_with_fanout_falls_back_to_rebuild_handlers() {
    let p = Pipeline::new();
    let _ = p
        .apply(
            "Create",
            Create::new(vec![
                (OpaqueKey(1), 2i64),
                (OpaqueKey(1), 5),
                (OpaqueKey(2), 3),
            ]),
        )
        .apply(
            "Sum",
            CombinePerKey::new(ScaledSum { factor: 10 }).with_hot_key_fanout(3),
        );

    assert_eq!(
        combine_fn_spec(&p, "Sum/PreCombine").urn,
        URN_RUST_COMBINE_FN
    );
    assert_eq!(
        combine_fn_spec(&p, "Sum/PostCombine").urn,
        URN_RUST_COMBINE_FN
    );
    assert_eq!(p.validate(), Ok(()));

    let handlers = p.transform_handlers();
    let add_shard =
        beam::internals::registered_handler(&do_fn_spec(&p, "Sum/AddShard").payload).unwrap();
    let strip_shard =
        beam::internals::registered_handler(&do_fn_spec(&p, "Sum/StripShard").payload).unwrap();
    let raw = [
        (OpaqueKey(1), 2i64).encode().unwrap(),
        (OpaqueKey(1), 5i64).encode().unwrap(),
        (OpaqueKey(2), 3i64).encode().unwrap(),
    ];
    let sharded = run_bundle(add_shard.as_ref(), &raw);
    let pre = run_bundle(
        handlers[&stage_key("Sum/PreCombine", CombineStage::Precombine)].as_ref(),
        &sharded,
    );
    let unsharded = run_bundle(strip_shard.as_ref(), &pre);
    let merged = run_bundle(
        handlers[&stage_key("Sum/PostCombine", CombineStage::Precombine)].as_ref(),
        &unsharded,
    );
    let extracted = run_bundle(
        handlers[&stage_key("Sum/PostCombine", CombineStage::Extract)].as_ref(),
        &merged,
    );
    let mut totals = extracted
        .iter()
        .map(|bytes| <(OpaqueKey, i64)>::decode(bytes).unwrap())
        .collect::<Vec<_>>();
    totals.sort_by_key(|(OpaqueKey(k), _)| *k);
    assert_eq!(totals, vec![(OpaqueKey(1), 70), (OpaqueKey(2), 30)]);
}

/// A runner that does not lift the second phase runs its `ConvertToAccumulators` and
/// `CombineGroupedValues` stages. Prism lifts it, so this test runs the two stages itself.
#[test]
fn the_second_phase_runs_without_lifting() {
    let p = Pipeline::new();
    let _ = p
        .apply("Create", Create::new(vec![(OpaqueKey(1), 2i64)]))
        .apply(
            "Sum",
            CombinePerKey::new(ScaledSum { factor: 10 }).with_hot_key_fanout(3),
        );
    let handlers = p.transform_handlers();
    let stage = |stage| handlers[&stage_key("Sum/PostCombine", stage)].as_ref();

    // The inputs are first-phase accumulators: they pass through unchanged.
    let partials = [(OpaqueKey(1), 20i64), (OpaqueKey(1), 50)];
    let encoded: Vec<Vec<u8>> = partials.iter().map(|kv| kv.encode().unwrap()).collect();
    let converted = run_bundle(stage(CombineStage::ConvertToAccumulators), &encoded);
    assert_eq!(converted, encoded);

    let grouped = [
        (OpaqueKey(1), BeamIterable::from_vec(vec![20i64, 50]))
            .encode()
            .unwrap(),
        (OpaqueKey(2), BeamIterable::<i64>::from_vec(vec![]))
            .encode()
            .unwrap(),
    ];
    let combined: Vec<(OpaqueKey, i64)> =
        run_bundle(stage(CombineStage::CombineGroupedValues), &grouped)
            .iter()
            .map(|bytes| <(OpaqueKey, i64)>::decode(bytes).unwrap())
            .collect();
    // An empty group gives the output of a new accumulator.
    assert_eq!(combined, vec![(OpaqueKey(1), 70), (OpaqueKey(2), 0)]);
}

#[test]
fn the_second_phase_merges_windows_and_fires_once_per_first_phase_pane() {
    let p = Pipeline::new();
    let _ = p
        .apply("Create", Create::new(kvs(&[("a", 1)])))
        .apply(
            "Window",
            WindowInto::new(Sessions::with_gap_duration(Duration::from_secs(10)))
                .with_trigger(
                    Trigger::after_end_of_window().with_early_firings(Trigger::after_count(3)),
                )
                .with_accumulation_mode(AccumulationMode::Accumulating),
        )
        .apply(
            "Sum",
            CombinePerKey::new(ScaledSum { factor: 1 }).with_hot_key_fanout(4),
        );

    let components = p.to_proto().components.expect("components");
    let strategy = |name: &str| {
        let out = &transform(&p, name).outputs["out"];
        components.windowing_strategies[&components.pcollections[out].windowing_strategy_id].clone()
    };
    let trigger = |ws: &proto::WindowingStrategy| {
        Trigger::try_from(ws.trigger.as_ref().expect("trigger")).expect("trigger decodes")
    };
    let early_after = |count| -> Trigger {
        Trigger::after_end_of_window()
            .with_early_firings(Trigger::after_count(count))
            .into()
    };

    let pre = strategy("Sum/AddShard");
    assert_eq!(
        pre.accumulation_mode(),
        proto::accumulation_mode::Enum::Discarding
    );
    assert_eq!(trigger(&pre), early_after(3));

    let post = strategy("Sum/StripShard");
    assert_eq!(
        post.accumulation_mode(),
        proto::accumulation_mode::Enum::Accumulating
    );
    assert_eq!(post.merge_status(), proto::merge_status::Enum::NeedsMerge);
    assert_eq!(trigger(&post), early_after(1));
}

#[test]
fn an_unknown_phase_does_not_build() {
    let p = Pipeline::new();
    let _ = p.apply("Create", Create::new(kvs(&[("a", 1)]))).apply(
        "Sum",
        CombinePerKey::new(ScaledSum { factor: 1 }).with_hot_key_fanout(2),
    );
    let mut payload = combine_fn_spec(&p, "Sum/PreCombine").payload;
    // Field 6 (`phase`) as a varint. Protobuf keeps the last value of a repeated field.
    payload.extend([0x30, 0x07]);

    let error = registered_combine_handler(&payload, CombineStage::Precombine.as_str())
        .err()
        .expect("7 is not a phase");
    assert!(error.contains("unknown phase 7"), "{error}");
}
