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

//! Graph splicing utilities for integrating expanded cross-language subgraphs.

use std::collections::HashMap;

use beam::pipeline::PipelineInner;
use model::expansion::ExpansionResponse;
use model::pipeline as proto;

use super::error::ExpansionError;

/// Extracts the component closure needed by an expansion service to interpret input PCollections.
pub fn extract_input_components(
    inner: &PipelineInner,
    input_pcoll_ids: &[&str],
) -> proto::Components {
    let pcollections: HashMap<String, proto::PCollection> = input_pcoll_ids
        .iter()
        .filter_map(|&id| {
            inner
                .components
                .pcollections
                .get(id)
                .map(|pcoll| (id.to_string(), pcoll.clone()))
        })
        .collect();

    let windowing_strategies: HashMap<String, proto::WindowingStrategy> = pcollections
        .values()
        .map(|p| p.windowing_strategy_id.as_str())
        .filter_map(|ws_id| {
            inner
                .components
                .windowing_strategies
                .get(ws_id)
                .map(|ws| (ws_id.to_string(), ws.clone()))
        })
        .collect();

    let environments: HashMap<String, proto::Environment> = windowing_strategies
        .values()
        .filter(|ws| !ws.environment_id.is_empty())
        .filter_map(|ws| {
            inner
                .components
                .environments
                .get(&ws.environment_id)
                .map(|env| (ws.environment_id.clone(), env.clone()))
        })
        .collect();

    let seed_coder_ids = pcollections.values().map(|p| p.coder_id.as_str()).chain(
        windowing_strategies
            .values()
            .filter(|ws| !ws.window_coder_id.is_empty())
            .map(|ws| ws.window_coder_id.as_str()),
    );

    let coders = collect_coder_closure(&inner.components.coders, seed_coder_ids);

    proto::Components {
        transforms: HashMap::new(),
        pcollections,
        windowing_strategies,
        coders,
        environments,
    }
}

/// Collects the closure of coder dependencies starting from seed coder IDs.
fn collect_coder_closure<'a>(
    available: &HashMap<String, proto::Coder>,
    seeds: impl IntoIterator<Item = &'a str>,
) -> HashMap<String, proto::Coder> {
    let mut collected = HashMap::new();
    let mut queue: Vec<&str> = seeds.into_iter().collect();

    while let Some(coder_id) = queue.pop() {
        if let Some(coder) = available
            .get(coder_id)
            .filter(|_| !collected.contains_key(coder_id))
        {
            queue.extend(
                coder
                    .component_coder_ids
                    .iter()
                    .filter(|comp_id| !collected.contains_key(comp_id.as_str()))
                    .map(String::as_str),
            );
            collected.insert(coder_id.to_string(), coder.clone());
        }
    }

    collected
}

/// Splices an expanded transform and its components into the pipeline graph.
///
/// Returns the top-level transform ID of the spliced expansion.
///
/// # Errors
/// Returns [`ExpansionError`] if the expansion response contains an error or lacks a transform.
pub fn splice_expansion_response(
    inner: &mut PipelineInner,
    response: ExpansionResponse,
) -> Result<String, ExpansionError> {
    if !response.error.is_empty() {
        return Err(ExpansionError::ExpansionFailed(response.error));
    }

    let expanded_transform = response.transform.ok_or_else(|| {
        ExpansionError::InvalidResponse("Missing transform in ExpansionResponse".into())
    })?;

    let root_transform_id = if !expanded_transform.unique_name.is_empty() {
        expanded_transform.unique_name.clone()
    } else {
        beam::pipeline::next_id("expanded_transform")
    };

    if let Some(components) = response.components {
        merge_components(&mut inner.components.coders, components.coders);
        merge_components(
            &mut inner.components.windowing_strategies,
            components.windowing_strategies,
        );
        merge_components(&mut inner.components.environments, components.environments);
        merge_components(&mut inner.components.pcollections, components.pcollections);
        merge_components(&mut inner.components.transforms, components.transforms);
    }

    inner
        .components
        .transforms
        .insert(root_transform_id.clone(), expanded_transform);

    if !inner.transform_order.contains(&root_transform_id) {
        inner.transform_order.push(root_transform_id.clone());
    }

    Ok(root_transform_id)
}

/// Merges entries from `source` into `target`, preserving existing entries.
fn merge_components<K: std::hash::Hash + Eq, V>(target: &mut HashMap<K, V>, source: HashMap<K, V>) {
    source.into_iter().for_each(|(k, v)| {
        target.entry(k).or_insert(v);
    });
}
