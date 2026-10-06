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

//! Integration tests for adapting a pipeline proto for Dataflow before it is staged.
//!
//! Covers composites, combine specs, resource hints and container image overrides.

use beam::pipeline::constants::{URN_RESOURCE_CPU_COUNT, URN_RESOURCE_MIN_RAM_BYTES};
use beam::pipeline::{Pipeline, ResourceHints, URN_COMBINE_PER_KEY, URN_ENV_DOCKER};
use beam::transforms::{Create, Map, WithResourceHintsExt};
use dataflow::translate::adapt_pipeline_for_dataflow;
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
    let words = p.apply(Create::new(
        "Create",
        vec![
            "hello".to_string(),
            "world".to_string(),
            "hello".to_string(),
        ],
    ));
    let counted = words.count_per_element("CountElements");
    let _ = counted.map("format", |(w, c): (String, i64)| format!("{w}: {c}"));

    // What Dataflow receives is the adapted graph, so check that one.
    let original_roots = p.to_proto().root_transform_ids;
    let proto = adapt_pipeline_for_dataflow(p.to_proto());
    assert_eq!(proto.root_transform_ids, original_roots);
    let components = proto.components.as_ref().unwrap();

    let count_composite = components
        .transforms
        .values()
        .find(|t| t.unique_name.contains("CountElements") && !t.subtransforms.is_empty())
        .expect("CountElements must remain a composite");
    assert_eq!(count_composite.subtransforms.len(), 3);

    let create_composite = components
        .transforms
        .values()
        .find(|t| t.unique_name.contains("Create") && !t.subtransforms.is_empty())
        .expect("Create must remain a composite");
    assert_eq!(create_composite.subtransforms.len(), 2);

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

#[test]
fn test_adapt_pipeline_for_dataflow_clears_combine_spec() {
    let p = Pipeline::new();
    let words = p.apply(Create::new(
        "Create",
        vec![
            "apple".to_string(),
            "banana".to_string(),
            "apple".to_string(),
        ],
    ));
    let _ = words.count_per_element("CountWords");

    let proto = p.to_proto();
    let raw_components = proto.components.as_ref().expect("components must exist");

    // Verify that the original proto contains a CombinePerKey composite with URN_COMBINE_PER_KEY
    let raw_combine = raw_components
        .transforms
        .values()
        .find(|t| {
            t.spec
                .as_ref()
                .is_some_and(|s| s.urn == URN_COMBINE_PER_KEY)
        })
        .expect("Original pipeline must contain a URN_COMBINE_PER_KEY composite");
    let combine_unique_name = raw_combine.unique_name.clone();
    let combine_subtransforms = raw_combine.subtransforms.clone();
    assert_eq!(
        combine_subtransforms.len(),
        3,
        "CombinePerKey must have 3 subtransforms in fallback expansion"
    );

    // Adapt the pipeline for Dataflow submission
    let adapted = adapt_pipeline_for_dataflow(proto);
    let adapted_components = adapted.components.as_ref().expect("components must exist");

    // The combine composite must still exist with the exact same unique name and subtransforms,
    // but its spec must be None so Dataflow treats it as a transparent composite.
    let adapted_combine = adapted_components
        .transforms
        .get(&combine_unique_name)
        .expect("Combine composite must remain in adapted pipeline");
    assert!(
        adapted_combine.spec.is_none(),
        "Combine composite spec must be cleared for Dataflow"
    );
    assert_eq!(
        adapted_combine.subtransforms, combine_subtransforms,
        "Subtransforms of combine composite must be preserved"
    );

    // Verify that all 3 subtransforms are intact in the adapted components map
    for sub_id in &adapted_combine.subtransforms {
        assert!(
            adapted_components.transforms.contains_key(sub_id),
            "Subtransform {sub_id} must exist in adapted components"
        );
    }

    // Verify non-combine transforms (like Create) still have their specs intact
    let create_transform = adapted_components
        .transforms
        .values()
        .find(|t| t.unique_name.contains("Create") && t.spec.is_some())
        .expect("Create transform must retain its spec");
    assert!(create_transform.spec.is_some());
}

#[test]
fn test_adapt_pipeline_for_dataflow_idempotent() {
    let p = Pipeline::new();
    let words = p.apply(Create::new("Create", vec!["test".to_string()]));
    let _ = words.count_per_element("Count");

    let adapted_once = adapt_pipeline_for_dataflow(p.to_proto());
    let adapted_twice = adapt_pipeline_for_dataflow(adapted_once.clone());

    assert_eq!(adapted_once, adapted_twice);
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
fn test_adapt_pipeline_for_dataflow_preserves_environment_resource_hints() {
    let p = Pipeline::new()
        .with_resource_hints(ResourceHints::new().with_min_ram_bytes(16_000_000_000));
    let col = p.apply(Create::new("Create", vec!["hello".to_string()]));
    let _ = col.apply(
        Map::new("heavy", |s: String| s).with_resource_hints(
            ResourceHints::new()
                .with_min_ram_bytes(32_000_000_000)
                .with_cpu_count(8),
        ),
    );

    let proto = p.to_proto();
    let adapted = adapt_pipeline_for_dataflow(proto);
    let components = adapted.components.unwrap();

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
