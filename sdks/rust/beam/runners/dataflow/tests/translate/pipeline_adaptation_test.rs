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

//! Integration tests for the pipeline proto that Dataflow receives.
//!
//! Covers composites, combine specs, resource hints and container image overrides.

use beam::pipeline::{Pipeline, ResourceHints, URN_COMBINE_PER_KEY, URN_ENV_DOCKER};
use beam::pipeline::{URN_RESOURCE_CPU_COUNT, URN_RESOURCE_MIN_RAM_BYTES};
use beam::transforms::{Create, Map, WithResourceHintsExt};
use dataflow::{apply_environment_overrides, resolve_sdk_container_image};
use fluent::prelude::*;
use model::pipeline as proto;
use prost::Message;

/// Composites must reach the runner intact.
///
/// Dataflow Runner v2 reads the graph from the staged pipeline proto -- the submitted job
/// carries no step list at all -- and expands composites itself. Top-level composites
/// must stay roots. Flattening them in the SDK would also remove the parent names that the
/// harness uses to map a runner-lifted combine stage to the handler that implements it.
#[test]
fn test_composites_survive_translation() {
    let p = Pipeline::new();
    let words = p.apply(
        "Create",
        Create::new(vec![
            "hello".to_string(),
            "world".to_string(),
            "hello".to_string(),
        ]),
    );
    let counted = words.count_per_element("CountElements");
    let _ = counted.map("format", |(w, c): (String, i64)| format!("{w}: {c}"));

    let proto = p.to_proto();
    let components = proto.components.as_ref().unwrap();

    let named = |name: &str| {
        components
            .transforms
            .values()
            .find(|t| t.unique_name == name)
            .unwrap_or_else(|| panic!("{name} must exist"))
    };
    // CountPerElement is a composite of PairWithOne and the Sum combine, itself a composite
    // of a GroupByKey and the step that combines the grouped values.
    let count_composite = named("CountElements");
    assert_eq!(count_composite.subtransforms.len(), 2);
    assert_eq!(named("CountElements/Sum").subtransforms.len(), 2);

    let create_composite = components
        .transforms
        .values()
        .find(|t| t.unique_name.contains("Create") && !t.subtransforms.is_empty())
        .expect("Create must remain a composite");
    // Impulse, Process and the Reshuffle that spreads the elements.
    assert_eq!(create_composite.subtransforms.len(), 3);

    // Composites are top-level, so they are roots; their children are not.
    assert!(
        proto
            .root_transform_ids
            .contains(&count_composite.unique_name)
    );
    let children_in_roots = count_composite
        .subtransforms
        .iter()
        .filter(|sub| proto.root_transform_ids.contains(sub))
        .count();
    assert_eq!(
        children_in_roots, 0,
        "subtransforms of a composite must not also be roots"
    );
}

/// Dataflow lifts a `combine_per_key` composite whose subtransforms are a `GroupByKey` and a
/// `combine_grouped_values` step, so the composite keeps its spec and its payload.
#[test]
fn test_combine_per_key_has_the_shape_that_dataflow_lifts() {
    use beam::pipeline::{URN_COMBINE_GROUPED_VALUES, URN_GROUP_BY_KEY};

    let p = Pipeline::new();
    let words = p.apply(
        "Create",
        Create::new(vec![
            "apple".to_string(),
            "banana".to_string(),
            "apple".to_string(),
        ]),
    );
    let _ = words.count_per_element("CountWords");

    let proto = p.to_proto();
    let components = proto.components.as_ref().expect("components must exist");
    let combine = components
        .transforms
        .values()
        .find(|t| {
            t.spec
                .as_ref()
                .is_some_and(|s| s.urn == URN_COMBINE_PER_KEY)
        })
        .expect("the pipeline must contain a combine_per_key composite");
    let payload = &combine.spec.as_ref().expect("spec").payload;
    let urns: Vec<&str> = combine
        .subtransforms
        .iter()
        .map(|id| {
            components.transforms[id]
                .spec
                .as_ref()
                .expect("spec")
                .urn
                .as_str()
        })
        .collect();
    assert_eq!(urns, [URN_GROUP_BY_KEY, URN_COMBINE_GROUPED_VALUES]);
    // The step after the GroupByKey carries the same CombinePayload as the composite.
    let grouped_values = &components.transforms[&combine.subtransforms[1]];
    assert_eq!(
        &grouped_values.spec.as_ref().expect("spec").payload,
        payload
    );
}

#[test]
fn resolve_sdk_container_image_table() {
    let overrides = vec![
        ".*java.*,apache/beam_java21_sdk:custom".to_string(),
        "python=apache/beam_python:v2".to_string(),
    ];

    let cases: &[(&str, &[String], &str)] = &[
        (
            "apache/beam_java21_sdk:1.2.3.dev",
            &overrides,
            "apache/beam_java21_sdk:custom",
        ),
        (
            "apache/beam_python3.11_sdk:latest",
            &overrides,
            "apache/beam_python:v2",
        ),
        (
            "apache/beam_rust_sdk:latest",
            &overrides,
            "apache/beam_rust_sdk:latest",
        ),
        (
            "apache/beam_java21_sdk:1.2.3.dev",
            &[],
            "apache/beam_java21_sdk:1.2.3.dev",
        ),
        (
            "apache/beam_java17_sdk:1.2.3-SNAPSHOT",
            &[],
            "apache/beam_java17_sdk:1.2.3-SNAPSHOT",
        ),
        (
            "apache/beam_java21_sdk:2.64.0",
            &[],
            "apache/beam_java21_sdk:2.64.0",
        ),
    ];

    for (image, env_overrides, expected) in cases {
        assert_eq!(
            resolve_sdk_container_image(image, env_overrides),
            *expected,
            "failed resolving {image}"
        );
    }
}

#[test]
fn test_apply_environment_overrides() {
    let mut pipeline = proto::Pipeline {
        components: Some(proto::Components {
            environments: [(
                "java_env".to_string(),
                proto::Environment {
                    urn: URN_ENV_DOCKER.to_string(),
                    payload: proto::DockerPayload {
                        container_image: "apache/beam_java21_sdk:1.2.3.dev".to_string(),
                    }
                    .encode_to_vec(),
                    ..Default::default()
                },
            )]
            .into_iter()
            .collect(),
            ..Default::default()
        }),
        ..Default::default()
    };

    let overrides = vec![".*java.*,apache/beam_java21_sdk:latest".to_string()];
    apply_environment_overrides(&mut pipeline, &overrides);

    let env = pipeline
        .components
        .as_ref()
        .unwrap()
        .environments
        .get("java_env")
        .unwrap();
    let payload = proto::DockerPayload::decode(env.payload.as_slice()).unwrap();
    assert_eq!(payload.container_image, "apache/beam_java21_sdk:latest");
}

#[test]
fn test_environment_resource_hints_reach_dataflow() {
    let p = Pipeline::new()
        .with_resource_hints(ResourceHints::new().with_min_ram_bytes(16_000_000_000));
    let col = p.apply("Create", Create::new(vec!["hello".to_string()]));
    let _ = col.apply(
        "heavy",
        Map::new(|s: String| s).with_resource_hints(
            ResourceHints::new()
                .with_min_ram_bytes(32_000_000_000)
                .with_cpu_count(8),
        ),
    );

    let components = p.to_proto().components.unwrap();

    // Default environment hints are preserved for runner service resolution
    let default_env = &components.environments[&p.default_environment_id()];
    assert_eq!(
        default_env
            .resource_hints
            .get(URN_RESOURCE_MIN_RAM_BYTES)
            .map(|b| std::str::from_utf8(b).unwrap()),
        Some("16000000000")
    );

    // Heavy transform environment hints are also preserved
    let heavy = components
        .transforms
        .values()
        .find(|t| t.unique_name.starts_with("heavy"))
        .unwrap();
    assert_ne!(heavy.environment_id, p.default_environment_id());
    let heavy_env = &components.environments[&heavy.environment_id];
    assert_eq!(
        heavy_env
            .resource_hints
            .get(URN_RESOURCE_CPU_COUNT)
            .map(|b| std::str::from_utf8(b).unwrap()),
        Some("8")
    );
    assert_eq!(
        heavy_env
            .resource_hints
            .get(URN_RESOURCE_MIN_RAM_BYTES)
            .map(|b| std::str::from_utf8(b).unwrap()),
        Some("32000000000")
    );
}
