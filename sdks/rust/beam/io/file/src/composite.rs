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

use std::collections::{HashMap, HashSet};

use beam::internals::PipelineInternals;
use beam::pipeline::Pipeline;
use beam::transforms::display_data::{DisplayDataBuilder, HasDisplayData};
use beam::values::{PBegin, PCollection};

/// The ids of every transform currently in `pipeline`.
pub(crate) fn transform_ids(pipeline: &Pipeline) -> HashSet<String> {
    pipeline
        .lock()
        .components
        .transforms
        .keys()
        .cloned()
        .collect()
}

/// Groups every transform added since `existing` under a composite `name`. Only the
/// outermost new transforms become direct children.
pub(crate) fn add_composite(
    pipeline: &Pipeline,
    name: &str,
    urn: Option<&str>,
    payload: Vec<u8>,
    existing: &HashSet<String>,
    inputs: HashMap<String, String>,
    outputs: HashMap<String, String>,
) -> String {
    let children = {
        let lock = pipeline.lock();
        let transforms = &lock.components.transforms;
        let added: Vec<&String> = transforms
            .keys()
            .filter(|id| !existing.contains(*id))
            .collect();
        let nested: HashSet<&String> = added
            .iter()
            .filter_map(|id| transforms.get(*id))
            .flat_map(|t| &t.subtransforms)
            .collect();
        let mut children: Vec<String> = added
            .into_iter()
            .filter(|id| !nested.contains(id))
            .cloned()
            .collect();
        children.sort();
        children
    };
    pipeline.add_composite_transform(name, urn, payload, inputs, outputs, children)
}

fn expand_internal<T: HasDisplayData, Out: 'static>(
    pipeline: &Pipeline,
    raw_name: &str,
    urn: Option<&str>,
    payload: Vec<u8>,
    inputs: HashMap<String, String>,
    transform: &T,
    build: impl FnOnce(&Pipeline, &str) -> PCollection<Out>,
) -> PCollection<Out> {
    let name = pipeline.unique_transform_name(raw_name);
    let existing = transform_ids(pipeline);
    let out = build(pipeline, &name);
    let outputs = HashMap::from([("out".to_string(), out.id().to_string())]);
    let transform_id = add_composite(pipeline, &name, urn, payload, &existing, inputs, outputs);
    let mut builder = DisplayDataBuilder::with_namespace(&name);
    transform.populate_display_data(&mut builder);
    pipeline.set_transform_display_data(&transform_id, builder.into_proto());
    out
}

/// Builds and registers a composite transform that starts from the pipeline root.
pub(crate) fn expand_root<T: HasDisplayData, Out: 'static>(
    input: &PBegin,
    raw_name: &str,
    transform: &T,
    build: impl FnOnce(&Pipeline, &str) -> PCollection<Out>,
) -> PCollection<Out> {
    expand_internal(
        input.pipeline(),
        raw_name,
        None,
        Vec::new(),
        HashMap::new(),
        transform,
        build,
    )
}

/// Builds and registers a composite transform that consumes an input collection.
pub(crate) fn expand<T: HasDisplayData, In: 'static, Out: 'static>(
    input: &PCollection<In>,
    raw_name: &str,
    transform: &T,
    build: impl FnOnce(&Pipeline, &str) -> PCollection<Out>,
) -> PCollection<Out> {
    let inputs = HashMap::from([("in".to_string(), input.id().to_string())]);
    expand_internal(
        input.pipeline(),
        raw_name,
        None,
        Vec::new(),
        inputs,
        transform,
        build,
    )
}

/// Builds and registers a composite transform with a `FunctionSpec` (`urn` and `payload`).
pub(crate) fn expand_with_spec<T: HasDisplayData, In: 'static, Out: 'static>(
    input: &PCollection<In>,
    raw_name: &str,
    urn: &str,
    payload: Vec<u8>,
    transform: &T,
    build: impl FnOnce(&Pipeline, &str) -> PCollection<Out>,
) -> PCollection<Out> {
    let inputs = HashMap::from([("in".to_string(), input.id().to_string())]);
    expand_internal(
        input.pipeline(),
        raw_name,
        Some(urn),
        payload,
        inputs,
        transform,
        build,
    )
}
