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
use model::pipeline as proto;
use prism::PrismRunner;
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

    fn create_accumulator(&self) -> i64 {
        0
    }

    fn add_input(&self, accumulator: i64, input: i64) -> i64 {
        accumulator + input
    }

    fn merge_accumulators(&self, accumulators: Vec<i64>) -> i64 {
        self.accumulators_merged
            .fetch_add(accumulators.len(), Ordering::SeqCst);
        accumulators.into_iter().sum()
    }

    fn extract_output(&self, accumulator: i64) -> i64 {
        accumulator
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

    p.apply(Create::new("Create", elements)).combine_per_key(
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
        .apply(Create::new(
            "Input",
            vec![("k".to_string(), 1i64), ("k".to_string(), 2i64)],
        ))
        .combine_per_key("SumByKey", Sum);

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

    assert_eq!(composite.subtransforms.len(), 3);
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

    for stage in [
        "SumByKey/PartialCombine",
        "SumByKey/GroupAccumulators",
        "SumByKey/MergeAccumulators",
    ] {
        assert!(
            sub_names.contains(&stage),
            "expected stage '{stage}' among {sub_names:?}"
        );
    }

    assert_eq!(composite.outputs.get("out"), Some(&out.id().to_string()));
    assert!(!composite_id.is_empty());
}
