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

//! Hierarchical transform names: the name at `apply`, the scope of a composite, primitive
//! and composite nodes, and the rule that sibling transforms need distinct names.

use std::collections::HashMap;

use beam::internals::PipelineInternals;
use beam::pipeline::{PipelineError, URN_COMBINE_PER_KEY};
use beam::prelude::*;
use beam::transforms::InnerJoin;
use beam::values::PBegin;
use model::pipeline as proto;

/// A user composite: one `Map` named `Leaf`.
struct AddOne;

impl PTransform<PCollection<i64>> for AddOne {
    type Output = PCollection<i64>;

    fn expand(&self, input: &PCollection<i64>) -> PCollection<i64> {
        input.apply("Leaf", Map::new(|x: i64| x + 1))
    }
}

/// A user composite that applies [`AddOne`] under the name `Inner`.
struct Nested;

impl PTransform<PCollection<i64>> for Nested {
    type Output = PCollection<i64>;

    fn expand(&self, input: &PCollection<i64>) -> PCollection<i64> {
        input.apply("Inner", AddOne)
    }
}

/// A user composite that adds two children with the same name.
struct DuplicateChildren;

impl PTransform<PCollection<i64>> for DuplicateChildren {
    type Output = PCollection<i64>;

    fn expand(&self, input: &PCollection<i64>) -> PCollection<i64> {
        input
            .apply("Leaf", Map::new(|x: i64| x + 1))
            .apply("Leaf", Map::new(|x: i64| x * 2))
    }
}

fn numbers(p: &Pipeline) -> PCollection<i64> {
    p.apply("Numbers", Create::new(vec![1_i64, 2, 3]))
}

fn transforms(p: &Pipeline) -> HashMap<String, proto::PTransform> {
    p.to_proto()
        .components
        .expect("a pipeline proto has components")
        .transforms
}

fn transform<'a>(
    transforms: &'a HashMap<String, proto::PTransform>,
    name: &str,
) -> &'a proto::PTransform {
    let found = transforms.values().find(|t| t.unique_name == name);
    let mut names: Vec<&str> = transforms
        .values()
        .map(|t| t.unique_name.as_str())
        .collect();
    names.sort_unstable();
    found.unwrap_or_else(|| panic!("no transform named '{name}' in {names:?}"))
}

#[test]
fn a_primitive_takes_the_name_at_apply() {
    let p = Pipeline::new();
    let output = numbers(&p).apply("Double", Map::new(|x: i64| x * 2));

    p.validate().expect("the names are distinct");
    let transforms = transforms(&p);
    let double = transform(&transforms, "Double");
    assert!(
        double.subtransforms.is_empty(),
        "a primitive has no children"
    );
    assert_eq!(
        double.outputs.values().next(),
        Some(&output.id().to_string())
    );
}

#[test]
fn nested_composites_compose_their_names() {
    let p = Pipeline::new();
    numbers(&p).apply("Outer", Nested);

    p.validate().expect("the names are distinct");
    let transforms = transforms(&p);
    assert_eq!(
        transform(&transforms, "Outer").subtransforms,
        ["Outer/Inner"]
    );
    assert_eq!(
        transform(&transforms, "Outer/Inner").subtransforms,
        ["Outer/Inner/Leaf"]
    );
    assert!(
        transform(&transforms, "Outer/Inner/Leaf")
            .subtransforms
            .is_empty()
    );
}

#[test]
fn a_composite_node_reads_the_input_and_writes_the_output() {
    let p = Pipeline::new();
    let input = numbers(&p);
    let output = input.apply("AddOne", AddOne);

    p.validate().expect("the names are distinct");
    let transforms = transforms(&p);
    let composite = transform(&transforms, "AddOne");
    assert!(composite.spec.is_none(), "a user composite has no spec");
    assert_eq!(composite.subtransforms, ["AddOne/Leaf"]);
    assert_eq!(
        composite.inputs,
        HashMap::from([("in".to_string(), input.id().to_string())])
    );
    assert_eq!(
        composite.outputs,
        HashMap::from([("out".to_string(), output.id().to_string())])
    );
}

#[test]
fn a_root_composite_has_no_inputs() {
    let p = Pipeline::new();
    let output = numbers(&p);

    let transforms = transforms(&p);
    let create = transform(&transforms, "Numbers");
    assert!(create.inputs.is_empty());
    assert_eq!(
        create.outputs,
        HashMap::from([("out".to_string(), output.id().to_string())])
    );
    assert_eq!(
        create.subtransforms,
        ["Numbers/Impulse", "Numbers/Process", "Numbers/Reshuffle"]
    );
}

#[test]
fn one_composite_applied_twice_under_two_names_has_two_nodes() {
    let p = Pipeline::new();
    let input = numbers(&p);
    input.apply("First", AddOne);
    input.apply("Second", AddOne);

    p.validate()
        .expect("the same child name in two scopes is not a duplicate");
    let transforms = transforms(&p);
    assert_eq!(
        transform(&transforms, "First").subtransforms,
        ["First/Leaf"]
    );
    assert_eq!(
        transform(&transforms, "Second").subtransforms,
        ["Second/Leaf"]
    );
}

#[test]
fn sdk_composites_nest_in_the_scope_of_their_parent() {
    let p = Pipeline::new();
    numbers(&p).apply("Count", CountPerElement::new());

    p.validate().expect("the names are distinct");
    let transforms = transforms(&p);
    assert_eq!(
        transform(&transforms, "Count").subtransforms,
        ["Count/PairWithOne", "Count/Sum"]
    );
    let sum = transform(&transforms, "Count/Sum");
    assert_eq!(
        sum.spec.as_ref().map(|s| s.urn.as_str()),
        Some(URN_COMBINE_PER_KEY),
        "a composite can set its spec from expand"
    );
    assert_eq!(
        sum.subtransforms,
        ["Count/Sum/GroupByKey", "Count/Sum/CombineGroupedValues"]
    );
}

#[test]
fn duplicate_sibling_names_fail_validation() {
    let p = Pipeline::new();
    let input = numbers(&p);
    input.apply("Double", Map::new(|x: i64| x * 2));
    input.apply("Double", Map::new(|x: i64| x * 3));

    let error = p.validate().unwrap_err();
    assert_eq!(
        error,
        PipelineError::DuplicateTransformName {
            name: "Double".to_string()
        }
    );
    let message = error.to_string();
    assert!(message.contains("'Double'"), "{message}");
    assert!(message.contains("distinct names"), "{message}");
}

#[test]
fn duplicate_children_of_a_composite_fail_validation_with_the_full_name() {
    let p = Pipeline::new();
    numbers(&p).apply("Outer", DuplicateChildren);

    let error = p.validate().unwrap_err();
    assert_eq!(
        error,
        PipelineError::DuplicateTransformName {
            name: "Outer/Leaf".to_string()
        }
    );
    assert!(error.to_string().contains("'Outer/Leaf'"), "{error}");
}

#[test]
fn a_duplicate_keeps_the_graph_consistent() {
    let p = Pipeline::new();
    let input = numbers(&p);
    let first = input.apply("Double", Map::new(|x: i64| x * 2));
    let second = input.apply("Double", Map::new(|x: i64| x * 3));

    // Construction continues: the second transform gets a suffix, and both outputs exist.
    assert_ne!(first.id(), second.id());
    let transforms = transforms(&p);
    transform(&transforms, "Double");
    transform(&transforms, "Double_2");
}

#[test]
fn a_scope_closes_after_its_transform() {
    let p = Pipeline::new();
    let input = numbers(&p);
    input.apply("Outer", AddOne);
    input.apply("Leaf", Map::new(|x: i64| x));

    p.validate()
        .expect("a root `Leaf` is not the child `Outer/Leaf`");
    let transforms = transforms(&p);
    transform(&transforms, "Leaf");
    transform(&transforms, "Outer/Leaf");
}

#[test]
fn a_composite_node_reads_every_collection_from_outside_its_scope() {
    let p = Pipeline::new();
    let users = p.apply("Users", Create::new(vec![(1_i64, "a".to_string())]));
    let orders = p.apply("Orders", Create::new(vec![(1_i64, 10_i64)]));
    users.apply("Join", InnerJoin::new(&orders));

    p.validate().expect("the names are distinct");
    let transforms = transforms(&p);
    let inputs: Vec<&String> = transform(&transforms, "Join").inputs.values().collect();
    for side in [users.id(), orders.id()] {
        assert!(
            inputs.iter().any(|input| *input == side),
            "'Join' must read '{side}', reads {inputs:?}"
        );
    }
}

/// A user transform that adds two primitives with the plumbing API. The first one takes the
/// name of the scope.
struct TwoImpulses;

impl PTransform<PBegin> for TwoImpulses {
    type Output = PCollection<Vec<u8>>;

    fn expand(&self, input: &PBegin) -> PCollection<Vec<u8>> {
        input.pipeline().add_impulse();
        input.pipeline().add_impulse().1
    }
}

#[test]
fn a_scope_that_a_primitive_took_cannot_contain_other_transforms() {
    let p = Pipeline::new();
    p.apply("Twice", TwoImpulses);

    let error = p
        .validate()
        .expect_err("a second primitive has no parent node");
    assert!(
        matches!(&error, PipelineError::InvalidConstruction { transform_id, .. } if transform_id == "Twice"),
        "{error:?}"
    );
}
