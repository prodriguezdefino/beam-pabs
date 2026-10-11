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

//! Checks that building a pipeline twice produces the same portable graph and handler keys.

use std::collections::{BTreeSet, HashMap};

use beam::internals::PipelineInternals;
use beam::pipeline::{Pipeline, URN_PAR_DO};
use beam_model::pipeline as proto;
use prost::Message;

/// Calls `build` twice and asserts that both pipelines produce identical graphs and handler
/// keys.
///
/// Compares root transforms, requirements, display data, registered handler keys, and every
/// component (`transforms`, `pcollections`, `windowing_strategies`, `coders`, `environments`).
/// For `URN_PAR_DO` transforms, decodes `ParDoPayload` structurally so map field iteration
/// order does not cause false failures.
///
/// # Panics
///
/// Panics with a message naming the differing part if the two builds do not match.
pub fn assert_deterministic_pipeline(build: impl Fn() -> Pipeline) {
    let first = build();
    let second = build();

    let proto::Pipeline {
        components: first_components,
        root_transform_ids: first_roots,
        display_data: first_display,
        requirements: first_requirements,
    } = first.to_proto();
    let proto::Pipeline {
        components: second_components,
        root_transform_ids: second_roots,
        display_data: second_display,
        requirements: second_requirements,
    } = second.to_proto();

    assert_eq!(
        first_roots, second_roots,
        "pipeline root transforms differ between builds"
    );
    assert_eq!(
        first_requirements, second_requirements,
        "pipeline requirements differ between builds"
    );
    assert_eq!(
        first_display, second_display,
        "pipeline display data differs between builds"
    );

    let first_handlers: BTreeSet<String> = first.transform_handlers().into_keys().collect();
    let second_handlers: BTreeSet<String> = second.transform_handlers().into_keys().collect();
    assert_eq!(
        first_handlers, second_handlers,
        "registered transform handler keys differ between builds"
    );

    match (first_components, second_components) {
        (Some(left), Some(right)) => assert_components_eq(&left, &right),
        (None, None) => {}
        (left, right) => panic!(
            "pipeline components presence differs between builds: first={}, second={}",
            left.is_some(),
            right.is_some()
        ),
    }
}

fn assert_components_eq(left: &proto::Components, right: &proto::Components) {
    let proto::Components {
        transforms: left_transforms,
        pcollections: left_pcolls,
        windowing_strategies: left_ws,
        coders: left_coders,
        environments: left_envs,
    } = left;
    let proto::Components {
        transforms: right_transforms,
        pcollections: right_pcolls,
        windowing_strategies: right_ws,
        coders: right_coders,
        environments: right_envs,
    } = right;

    assert_map_eq("pcollections", left_pcolls, right_pcolls);
    assert_map_eq("windowing_strategies", left_ws, right_ws);
    assert_map_eq("coders", left_coders, right_coders);
    assert_map_eq("environments", left_envs, right_envs);
    assert_transforms_eq(left_transforms, right_transforms);
}

fn assert_map_eq<V: PartialEq + std::fmt::Debug>(
    kind: &str,
    left: &HashMap<String, V>,
    right: &HashMap<String, V>,
) {
    let left_keys: BTreeSet<&str> = left.keys().map(String::as_str).collect();
    let right_keys: BTreeSet<&str> = right.keys().map(String::as_str).collect();
    assert_eq!(
        left_keys, right_keys,
        "component {kind} keys differ between builds"
    );
    for key in left_keys {
        assert_eq!(
            left.get(key),
            right.get(key),
            "component {kind} entry '{key}' differs between builds"
        );
    }
}

fn assert_transforms_eq(
    left: &HashMap<String, proto::PTransform>,
    right: &HashMap<String, proto::PTransform>,
) {
    let left_keys: BTreeSet<&str> = left.keys().map(String::as_str).collect();
    let right_keys: BTreeSet<&str> = right.keys().map(String::as_str).collect();
    assert_eq!(
        left_keys, right_keys,
        "component transforms keys differ between builds"
    );
    for key in left_keys {
        let (Some(left_t), Some(right_t)) = (left.get(key), right.get(key)) else {
            continue;
        };
        assert_transform_eq(key, left_t, right_t);
    }
}

fn assert_transform_eq(id: &str, left: &proto::PTransform, right: &proto::PTransform) {
    let proto::PTransform {
        unique_name: left_name,
        spec: left_spec,
        subtransforms: left_subs,
        inputs: left_inputs,
        outputs: left_outputs,
        display_data: left_dd,
        environment_id: left_env,
        annotations: left_ann,
    } = left;
    let proto::PTransform {
        unique_name: right_name,
        spec: right_spec,
        subtransforms: right_subs,
        inputs: right_inputs,
        outputs: right_outputs,
        display_data: right_dd,
        environment_id: right_env,
        annotations: right_ann,
    } = right;

    assert_eq!(
        left_name, right_name,
        "transform '{id}' unique_name differs"
    );
    assert_eq!(
        left_subs, right_subs,
        "transform '{id}' subtransforms differ"
    );
    assert_eq!(left_inputs, right_inputs, "transform '{id}' inputs differ");
    assert_eq!(
        left_outputs, right_outputs,
        "transform '{id}' outputs differ"
    );
    assert_eq!(left_dd, right_dd, "transform '{id}' display_data differs");
    assert_eq!(
        left_env, right_env,
        "transform '{id}' environment_id differs"
    );
    assert_eq!(left_ann, right_ann, "transform '{id}' annotations differ");
    assert_spec_eq(id, left_spec.as_ref(), right_spec.as_ref());
}

fn assert_spec_eq(
    transform_id: &str,
    left: Option<&proto::FunctionSpec>,
    right: Option<&proto::FunctionSpec>,
) {
    match (left, right) {
        (Some(left), Some(right)) => {
            assert_eq!(
                left.urn, right.urn,
                "transform '{transform_id}' spec URN differs"
            );
            if left.urn == URN_PAR_DO
                && let (Ok(left_pardo), Ok(right_pardo)) = (
                    proto::ParDoPayload::decode(left.payload.as_slice()),
                    proto::ParDoPayload::decode(right.payload.as_slice()),
                )
            {
                assert_eq!(
                    left_pardo, right_pardo,
                    "transform '{transform_id}' ParDoPayload differs"
                );
                return;
            }
            assert_eq!(
                left.payload, right.payload,
                "transform '{transform_id}' spec payload differs"
            );
        }
        (None, None) => {}
        (left, right) => panic!(
            "transform '{transform_id}' spec presence differs: first={left:?}, second={right:?}"
        ),
    }
}
