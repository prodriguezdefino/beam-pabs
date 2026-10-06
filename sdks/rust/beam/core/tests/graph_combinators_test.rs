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

//! Tests for graph combinators: Flatten, Partition, ParDoMulti, and PCollectionList.
//!
//! Graph-shape tests check the exact wiring of the exported proto. Behavior tests run the
//! pipelines on Prism with `passert`.

mod support;

use beam::pipeline::{URN_FLATTEN, URN_PAR_DO};
use beam::prelude::*;
use beam::transforms::ProcessContext;
use beam::transforms::display_data::DisplayDataItem;
use beam::transforms::{Flatten, ParDoMulti, Partition};
use testing::{TestPipeline, passert};

#[test]
fn pcollection_list_preserves_insertion_order() {
    let p = Pipeline::new();
    let col1 = p.apply(Create::new("C1", vec![1, 2]));
    let col2 = p.apply(Create::new("C2", vec![3, 4]));
    let col3 = p.apply(Create::new("C3", vec![5, 6]));
    let expected = [
        col1.id().to_string(),
        col2.id().to_string(),
        col3.id().to_string(),
    ];

    let list = PCollectionList::of(col1).and(col2).and(col3);
    assert_eq!(list.len(), 3);
    assert!(!list.is_empty());
    let ids: Vec<_> = list
        .collections()
        .iter()
        .map(|c| c.id().to_string())
        .collect();
    assert_eq!(ids, expected);
    assert_eq!(list[1].id(), expected[1]);
    assert_eq!(
        list.get(2).map(|c| c.id().to_string()),
        Some(expected[2].clone())
    );
    assert!(list.get(3).is_none());

    let ids: Vec<_> = list.into_vec().iter().map(|c| c.id().to_string()).collect();
    assert_eq!(ids, expected);
}

#[test]
fn flatten_consumes_exactly_its_inputs() {
    let p = Pipeline::new();
    let a = p.apply(Create::new("A", vec![10, 20]));
    let b = p.apply(Create::new("B", vec![30, 40]));
    let c = p.apply(Create::new("C", vec![50, 60]));

    let merged = Flatten::pcollections("MergeABC", &[&a, &b, &c]);

    let proto = p.to_proto();
    let flatten = support::transform(&proto, "MergeABC");
    assert_eq!(support::urn(flatten), URN_FLATTEN);
    assert_eq!(
        support::input_producer_names(&proto, "MergeABC"),
        ["A/Process", "B/Process", "C/Process"]
    );
    assert_eq!(support::single_output(&proto, "MergeABC"), merged.id());
    // Flatten carries an environment ID to permit runner fusion.
    assert_eq!(flatten.environment_id, "env_default");
    assert_eq!(support::pcoll_coder(&proto, merged.id()), "varint");

    let dd_items: Vec<_> = flatten
        .display_data
        .iter()
        .filter_map(|d| DisplayDataItem::from_proto(d).ok())
        .collect();
    assert!(
        dd_items
            .iter()
            .any(|d| d.key == "transform" && d.value == "Flatten")
    );
}

#[test]
fn partition_is_one_pardo_with_indexed_outputs() {
    let p = Pipeline::new();
    let input = p.apply(Create::new("Input", vec![1, 2, 3, 4, 5, 6]));

    let parts = input.apply(Partition::new("SplitMod3", 3, |x: &i32| (*x % 3) as usize));
    assert_eq!(parts.len(), 3);

    let proto = p.to_proto();
    let t = support::transform(&proto, "SplitMod3");
    assert_eq!(support::urn(t), URN_PAR_DO);
    assert_eq!(
        support::input_producer_names(&proto, "SplitMod3"),
        ["Input/Process"]
    );
    // Output tag `i` maps to the `i`-th PCollection in the returned list.
    let mut outputs: Vec<_> = t.outputs.iter().collect();
    outputs.sort();
    let expected: Vec<(String, String)> = (0..3)
        .map(|i| (i.to_string(), parts[i].id().to_string()))
        .collect();
    assert_eq!(
        outputs
            .into_iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect::<Vec<_>>(),
        expected
    );
}

#[tokio::test]
async fn partition_routes_each_element_to_its_index() {
    let p = TestPipeline::new();
    let input = p.apply(Create::new("Input", vec![1i64, 2, 3, 4, 5, 6, 7]));
    let parts = input.apply(Partition::new("SplitMod3", 3, |x: &i64| (*x % 3) as usize));

    passert::that("Part0", &parts[0]).contains_in_any_order([3i64, 6]);
    passert::that("Part1", &parts[1]).contains_in_any_order([1i64, 4, 7]);
    passert::that("Part2", &parts[2]).contains_in_any_order([2i64, 5]);

    p.run().await.expect("pipeline and assertions");
}

#[test]
fn partition_then_flatten_rejoins_both_partitions() {
    let p = Pipeline::new();
    let input = p.apply(Create::new("Input", vec![1, 2, 3, 4, 5, 6]));
    let parts = input.apply(Partition::new("SplitMod2", 2, |x: &i32| (*x % 2) as usize));
    let rejoined = Flatten::pcollections("Rejoin", &[&parts[0], &parts[1]]);

    let proto = p.to_proto();
    let flatten = support::transform(&proto, "Rejoin");
    assert_eq!(support::urn(flatten), URN_FLATTEN);
    let mut inputs: Vec<_> = flatten.inputs.values().cloned().collect();
    inputs.sort();
    let mut expected = vec![parts[0].id().to_string(), parts[1].id().to_string()];
    expected.sort();
    assert_eq!(inputs, expected);
    assert_eq!(support::single_output(&proto, "Rejoin"), rejoined.id());
}

#[tokio::test]
async fn partition_then_flatten_keeps_every_element() {
    let p = TestPipeline::new();
    let input = p.apply(Create::new("Input", vec![1i64, 2, 3, 4, 5, 6]));
    let parts = input.apply(Partition::new("SplitMod2", 2, |x: &i64| (*x % 2) as usize));
    let rejoined = parts.apply(Flatten::new("Rejoin"));

    passert::that("AssertRejoined", &rejoined).contains_in_any_order([1i64, 2, 3, 4, 5, 6]);
    p.run().await.expect("pipeline and assertions");
}

#[derive(Clone)]
struct RouteBySign;

impl DoFn for RouteBySign {
    type In = i64;
    type Out = i64;

    fn process_element(
        &mut self,
        element: Self::In,
        out: &mut ProcessContext<'_, Self::Out>,
    ) -> Result {
        match element {
            e if e < 0 => out.output(e).to(0).emit(),
            0 => out.output(0).to(1).emit(),
            e => {
                // A positive element goes to two outputs.
                out.output(e).to(2).emit()?;
                out.output(e * 100).to(1).emit()
            }
        }
    }
}

#[test]
fn pardo_multi_declares_one_output_per_tag() {
    let p = Pipeline::new();
    let input = p.apply(Create::new("Input", vec![1i64, 2, 3]));
    let multi_out = input.apply(ParDoMulti::new(
        "MultiParDo",
        ["tag_a", "tag_b", "tag_c"],
        RouteBySign,
    ));

    assert_eq!(multi_out.len(), 3);
    p.validate().expect("Pipeline must be valid");

    let proto = p.to_proto();
    let t = support::transform(&proto, "MultiParDo");
    assert_eq!(support::urn(t), URN_PAR_DO);
    assert_eq!(
        support::input_producer_names(&proto, "MultiParDo"),
        ["Input/Process"]
    );
    for (i, tag) in ["tag_a", "tag_b", "tag_c"].into_iter().enumerate() {
        assert_eq!(t.outputs[tag], multi_out[i].id(), "output {tag}");
        assert_eq!(support::pcoll_coder(&proto, multi_out[i].id()), "varint");
    }
}

/// Runs `RouteBySign` with `tags` and checks that `output(v).to(i)` reaches the `i`-th
/// returned PCollection, which is the `i`-th tag in declaration order.
async fn assert_pardo_multi_routes_by_declaration_order(tags: [&str; 3]) {
    let p = TestPipeline::new();
    let input = p.apply(Create::new("Input", vec![-2i64, -1, 0, 1, 2]));
    let outs = input.apply(ParDoMulti::new("MultiParDo", tags, RouteBySign));

    passert::that("Output0", &outs[0]).contains_in_any_order([-2i64, -1]);
    passert::that("Output1", &outs[1]).contains_in_any_order([0i64, 100, 200]);
    passert::that("Output2", &outs[2]).contains_in_any_order([1i64, 2]);

    p.run().await.expect("pipeline and assertions");
}

#[tokio::test]
async fn pardo_multi_routes_emit_at_to_the_matching_output() {
    // Here, declaration order is the same as lexicographic order.
    assert_pardo_multi_routes_by_declaration_order(["a_negative", "b_zero", "c_positive"]).await;
}

#[tokio::test]
async fn pardo_multi_routes_emit_at_by_declaration_order_for_unsorted_tags() {
    assert_pardo_multi_routes_by_declaration_order(["negative", "zero_or_scaled", "positive"])
        .await;
}

#[tokio::test]
async fn partition_with_more_than_ten_outputs_routes_numerically() {
    // Tags "10" and "11" sort before "2". Routing must follow the index.
    let p = TestPipeline::new();
    let input = p.apply(Create::new("Input", (0i64..24).collect::<Vec<_>>()));
    let parts = input.apply(Partition::new("Mod12", 12, |x: &i64| (*x % 12) as usize));
    for (i, part) in parts.collections().iter().enumerate() {
        let i = i as i64;
        passert::that(format!("Part{i}"), part).contains_in_any_order([i, i + 12]);
    }
    p.run().await.expect("pipeline and assertions");
}
