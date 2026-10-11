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

//! The SDK combine functions are registered for the standard element types, and
//! `register!` registers other instantiations, so they travel in the graph.

use beam::pipeline::{URN_RUST_COMBINE_FN, URN_RUST_COMBINE_FN_REGISTERED};
use beam::prelude::*;
use beam::transforms::{
    AnySample, ApproximateQuantiles, ApproximateUnique, Max, Mean, Min, Natural, Reversed,
};
use model::pipeline as proto;
use prost::Message;
use rust_decimal::Decimal;

beam::register!(Top<(i64, String), Natural>);
beam::register!(Top<(i64, String), Reversed>, id = "scores::lowest");

/// The payload of a `beam:combine_fn:rust:registered:v1` spec.
#[derive(Clone, PartialEq, Message)]
struct RegisteredSpec {
    #[prost(string, tag = "1")]
    id: String,
    #[prost(bytes = "vec", tag = "2")]
    fields: Vec<u8>,
}

/// The `combine_fn` spec of the transform named `name`.
fn combine_fn_spec(p: &Pipeline, name: &str) -> proto::FunctionSpec {
    let spec = p
        .to_proto()
        .components
        .expect("components")
        .transforms
        .into_values()
        .find(|t| t.unique_name == name)
        .expect("transform exists")
        .spec
        .expect("spec");
    proto::CombinePayload::decode(spec.payload.as_slice())
        .expect("CombinePayload")
        .combine_fn
        .expect("combine_fn")
}

/// The registration id of the combine named `name`, or `None` for the rebuild path.
fn registered_id(p: &Pipeline, name: &str) -> Option<String> {
    let spec = combine_fn_spec(p, name);
    (spec.urn == URN_RUST_COMBINE_FN_REGISTERED).then(|| {
        RegisteredSpec::decode(spec.payload.as_slice())
            .expect("RegisteredSpec")
            .id
    })
}

fn keyed<V: Clone>(values: &[V]) -> Vec<(String, V)> {
    values
        .iter()
        .map(|v| ("k".to_string(), v.clone()))
        .collect()
}

#[test]
fn the_sdk_combine_fns_are_registered_for_the_standard_types() {
    let p = Pipeline::new();
    let ints = p.apply("Ints", Create::new(keyed(&[1i64, 2, 3])));
    let floats = p.apply("Floats", Create::new(keyed(&[1.5f64])));
    let decimals = p.apply("Decimals", Create::new(keyed(&[Decimal::ONE])));
    let words = p.apply("Words", Create::new(keyed(&["a".to_string()])));
    ints.apply("Sum", CombinePerKey::new(Sum::new()));
    ints.apply("Max", CombinePerKey::new(Max::new()));
    floats.apply("Min", CombinePerKey::new(Min::new()));
    decimals.apply("Mean", CombinePerKey::new(Mean::new()));
    words.apply("Largest", CombinePerKey::new(Top::largest(2)));
    words.apply("Smallest", CombinePerKey::new(Top::smallest(2)));
    words.apply("Sample", CombinePerKey::new(Sample::new(2)));
    floats.apply("AnySample", CombinePerKey::new(AnySample::new(2)));
    words.apply(
        "Unique",
        CombinePerKey::new(ApproximateUnique::with_sample_size(16).expect("sample size")),
    );
    ints.apply(
        "Quantiles",
        CombinePerKey::new(ApproximateQuantiles::new(3).expect("quantiles")),
    );

    let expected = [
        ("Sum", "beam::transforms::Sum<i64>"),
        ("Max", "beam::transforms::Max<i64>"),
        ("Min", "beam::transforms::Min<f64>"),
        ("Mean", "beam::transforms::Mean<Decimal>"),
        ("Largest", "beam::transforms::Top<String, Natural>"),
        ("Smallest", "beam::transforms::Top<String, Reversed>"),
        ("Sample", "beam::transforms::Sample<String>"),
        ("AnySample", "beam::transforms::AnySample<f64>"),
        ("Unique", "beam::transforms::ApproximateUnique<String>"),
        (
            "Quantiles",
            "beam::transforms::ApproximateQuantiles<i64, Natural>",
        ),
    ];
    for (name, id) in expected {
        assert_eq!(registered_id(&p, name).as_deref(), Some(id), "{name}");
    }
    p.validate().expect("valid pipeline");
}

#[test]
fn register_registers_another_instantiation() {
    let p = Pipeline::new();
    let scores = p.apply(
        "Scores",
        Create::new(keyed(&[(3i64, "ann".to_string()), (5, "bob".to_string())])),
    );
    scores.apply("Best", CombinePerKey::new(Top::largest(1)));
    scores.apply("Worst", CombinePerKey::new(Top::smallest(1)));

    let best = registered_id(&p, "Best").expect("registered");
    assert!(
        best.starts_with(module_path!()) && best.contains("Top"),
        "the default id is the module path and the type: {best}"
    );
    assert_eq!(
        registered_id(&p, "Worst").as_deref(),
        Some("scores::lowest")
    );
    p.validate().expect("valid pipeline");
}

#[test]
fn a_closure_ordering_keeps_the_rebuild_path() {
    let p = Pipeline::new();
    p.apply("Words", Create::new(keyed(&["a".to_string()])))
        .apply(
            "Longest",
            CombinePerKey::new(Top::of(1, |a: &String, b: &String| a.len().cmp(&b.len()))),
        );

    assert_eq!(combine_fn_spec(&p, "Longest").urn, URN_RUST_COMBINE_FN);
    assert_eq!(
        p.rebuild_path_transforms(),
        vec!["Longest/CombineGroupedValues".to_string()]
    );
}

#[test]
fn the_rebuild_path_lists_closures_but_not_registered_types() {
    let p = Pipeline::new();
    p.apply("Numbers", Create::new(keyed(&[1i64, 2])))
        .apply("Total", CombinePerKey::new(Sum::new()))
        .apply(
            "Format",
            Map::new(|(key, total): (String, i64)| format!("{key}: {total}")),
        );

    let rebuilt = p.rebuild_path_transforms();
    assert!(
        rebuilt.iter().any(|name| name.starts_with("Format")),
        "{rebuilt:?}"
    );
    assert!(
        !rebuilt.iter().any(|name| name.starts_with("Total")),
        "{rebuilt:?}"
    );
}
