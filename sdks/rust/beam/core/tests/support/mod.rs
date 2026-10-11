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

//! Helpers for the graph-structure tests in `core/tests`.
//!
//! The helpers find transforms by unique name and describe coders as short strings such as
//! `kv(string_utf8,varint)`. So a test can check the exact wiring of a pipeline proto,
//! not only that some transform with a URN exists.

#![allow(dead_code, reason = "shared test helper")]
#![expect(clippy::unwrap_used, reason = "test helper")]

use std::collections::BTreeMap;

use model::pipeline as proto;

/// The components of `pipeline`, which every exported proto has.
pub fn components(pipeline: &proto::Pipeline) -> &proto::Components {
    pipeline
        .components
        .as_ref()
        .expect("pipeline proto has components")
}

/// The transform whose `unique_name` is exactly `name`.
pub fn transform<'a>(pipeline: &'a proto::Pipeline, name: &str) -> &'a proto::PTransform {
    let found: Vec<_> = components(pipeline)
        .transforms
        .values()
        .filter(|t| t.unique_name == name)
        .collect();
    match found.as_slice() {
        [t] => t,
        [] => panic!(
            "no transform named {name:?}; transforms: {:?}",
            transform_names(pipeline)
        ),
        _ => panic!("{} transforms named {name:?}", found.len()),
    }
}

/// The sorted unique names of every transform.
pub fn transform_names(pipeline: &proto::Pipeline) -> Vec<String> {
    let mut names: Vec<_> = components(pipeline)
        .transforms
        .values()
        .map(|t| t.unique_name.clone())
        .collect();
    names.sort();
    names
}

/// The URN of `t`, or `""` for a composite without a spec.
pub fn urn(t: &proto::PTransform) -> &str {
    t.spec.as_ref().map_or("", |s| s.urn.as_str())
}

/// The unique name of the transform producing `pcoll_id`, ignoring composites (whose
/// outputs repeat those of their last primitive).
pub fn producer(pipeline: &proto::Pipeline, pcoll_id: &str) -> String {
    let producers: Vec<_> = components(pipeline)
        .transforms
        .values()
        .filter(|t| t.subtransforms.is_empty() && t.outputs.values().any(|o| o == pcoll_id))
        .map(|t| t.unique_name.clone())
        .collect();
    assert_eq!(
        producers.len(),
        1,
        "expected exactly one primitive producer of {pcoll_id}, got {producers:?}"
    );
    producers.into_iter().next().unwrap()
}

/// For each input tag of the transform named `name`, the name of the primitive
/// producing that input.
pub fn input_producers(pipeline: &proto::Pipeline, name: &str) -> BTreeMap<String, String> {
    transform(pipeline, name)
        .inputs
        .iter()
        .map(|(tag, pcoll)| (tag.clone(), producer(pipeline, pcoll)))
        .collect()
}

/// The sorted names of the primitives producing the inputs of the transform `name`.
pub fn input_producer_names(pipeline: &proto::Pipeline, name: &str) -> Vec<String> {
    let mut names: Vec<_> = input_producers(pipeline, name).into_values().collect();
    names.sort();
    names
}

/// The single output PCollection id of the transform `name`.
pub fn single_output<'a>(pipeline: &'a proto::Pipeline, name: &str) -> &'a str {
    let t = transform(pipeline, name);
    assert_eq!(t.outputs.len(), 1, "{name} has outputs {:?}", t.outputs);
    t.outputs.values().next().unwrap()
}

/// Describes the coder `coder_id` as `short_urn(components...)`, e.g.
/// `kv(string_utf8,iterable(varint))`.
pub fn coder_shape(pipeline: &proto::Pipeline, coder_id: &str) -> String {
    let coder = components(pipeline)
        .coders
        .get(coder_id)
        .unwrap_or_else(|| panic!("coder {coder_id} is not registered"));
    let urn = coder.spec.as_ref().map_or("", |s| s.urn.as_str());
    let short = urn
        .strip_prefix("beam:coder:")
        .and_then(|u| u.rsplit_once(':'))
        .map_or(urn, |(name, _version)| name);
    if coder.component_coder_ids.is_empty() {
        short.to_string()
    } else {
        let parts: Vec<_> = coder
            .component_coder_ids
            .iter()
            .map(|c| coder_shape(pipeline, c))
            .collect();
        format!("{short}({})", parts.join(","))
    }
}

/// The coder shape of the PCollection `pcoll_id`.
pub fn pcoll_coder(pipeline: &proto::Pipeline, pcoll_id: &str) -> String {
    let pc = &components(pipeline).pcollections[pcoll_id];
    coder_shape(pipeline, &pc.coder_id)
}

/// The coder shape of the single output of the transform `name`.
pub fn output_coder(pipeline: &proto::Pipeline, name: &str) -> String {
    let out = single_output(pipeline, name).to_string();
    pcoll_coder(pipeline, &out)
}

/// The window-fn URN of the PCollection `pcoll_id`.
pub fn window_fn_urn(pipeline: &proto::Pipeline, pcoll_id: &str) -> String {
    let c = components(pipeline);
    let ws_id = &c.pcollections[pcoll_id].windowing_strategy_id;
    c.windowing_strategies[ws_id]
        .window_fn
        .as_ref()
        .map(|f| f.urn.clone())
        .unwrap_or_default()
}
