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

//! Computes shallow topological root transforms for an Apache Beam pipeline.

use model::pipeline as proto;
use std::collections::{HashMap, HashSet, VecDeque};

/// Computes the root (top-level) transform IDs in shallow topological order, as
/// `beam_runner_api.proto` requires: a recursive traversal in this order is topological.
pub fn compute_root_transform_ids(
    components: &proto::Components,
    transform_order: &[String],
) -> Vec<String> {
    let subtransforms: HashSet<&str> = components
        .transforms
        .values()
        .flat_map(|t| &t.subtransforms)
        .map(String::as_str)
        .collect();

    // `transforms` is a `HashMap`. Sort the ids that `transform_order` does not name, so the
    // root order and the serialized pipeline are the same in each run.
    let mut unordered: Vec<&str> = components.transforms.keys().map(String::as_str).collect();
    unordered.sort_unstable();

    // Top-level candidates: insertion order first, then the sorted remainder.
    let mut seen = HashSet::new();
    let candidates: Vec<&str> = transform_order
        .iter()
        .map(String::as_str)
        .chain(unordered)
        .filter(|&id| !subtransforms.contains(id) && components.transforms.contains_key(id))
        .filter(|&id| seen.insert(id))
        .collect();

    if candidates.is_empty() {
        Vec::new()
    } else {
        let pcoll_to_producer = map_pcollections_to_producers(&candidates, components);
        sort_candidates_topologically(&candidates, components, &pcoll_to_producer)
    }
}

/// Maps each PCollection id to the candidate that produces it, directly or in a subtransform.
fn map_pcollections_to_producers<'a>(
    candidates: &[&'a str],
    components: &'a proto::Components,
) -> HashMap<&'a str, &'a str> {
    candidates
        .iter()
        .filter_map(|&top_id| components.transforms.get(top_id).map(|t| (top_id, t)))
        .flat_map(|(top_id, top_t)| {
            let direct_outputs = top_t.outputs.values().map(String::as_str);
            let sub_outputs = top_t
                .subtransforms
                .iter()
                .filter_map(|sub_id| components.transforms.get(sub_id))
                .flat_map(|sub_t| sub_t.outputs.values().map(String::as_str));

            direct_outputs
                .chain(sub_outputs)
                .map(move |pcoll| (pcoll, top_id))
        })
        .collect()
}

/// Returns all input PCollections that a transform or its subtransforms consume.
fn transform_input_pcollections<'a>(
    transform_id: &str,
    components: &'a proto::Components,
) -> HashSet<&'a str> {
    components
        .transforms
        .get(transform_id)
        .map(|t| {
            let direct = t.inputs.values().map(String::as_str);
            let sub = t
                .subtransforms
                .iter()
                .filter_map(|sub_id| components.transforms.get(sub_id))
                .flat_map(|sub_t| sub_t.inputs.values().map(String::as_str));
            direct.chain(sub).collect()
        })
        .unwrap_or_default()
}

/// Sorts candidates in shallow topological order (Kahn's algorithm). Independent candidates
/// keep their candidate order.
fn sort_candidates_topologically<'a>(
    candidates: &[&'a str],
    components: &'a proto::Components,
    pcoll_to_producer: &HashMap<&str, &'a str>,
) -> Vec<String> {
    let dependencies: HashMap<&str, HashSet<&str>> = candidates
        .iter()
        .map(|&consumer| {
            let upstreams = transform_input_pcollections(consumer, components)
                .into_iter()
                .filter_map(|pcoll| pcoll_to_producer.get(pcoll).copied())
                .filter(|&producer| producer != consumer)
                .collect();
            (consumer, upstreams)
        })
        .collect();

    let mut in_degree: HashMap<&str, usize> = dependencies
        .iter()
        .map(|(&consumer, deps)| (consumer, deps.len()))
        .collect();

    // Build adjacency in candidate order, not hash order, so `root_transform_ids` is stable.
    let adj = candidates
        .iter()
        .fold(HashMap::<&str, Vec<&str>>::new(), |mut adj, &consumer| {
            dependencies
                .get(consumer)
                .into_iter()
                .flatten()
                .for_each(|&producer| adj.entry(producer).or_default().push(consumer));
            adj
        });

    let mut queue: VecDeque<&str> = candidates
        .iter()
        .copied()
        .filter(|&id| in_degree.get(id).copied().unwrap_or(0) == 0)
        .collect();

    let mut sorted = Vec::with_capacity(candidates.len());
    let mut visited = HashSet::with_capacity(candidates.len());

    while let Some(curr) = queue.pop_front() {
        visited.insert(curr);
        sorted.push(curr.to_string());

        adj.get(curr).into_iter().flatten().for_each(|&consumer| {
            in_degree.entry(consumer).and_modify(|deg| {
                *deg = deg.saturating_sub(1);
                if *deg == 0 {
                    queue.push_back(consumer);
                }
            });
        });
    }

    // Append the unvisited candidates, which form cycles, in candidate order.
    let unvisited = candidates
        .iter()
        .filter(|&&id| !visited.contains(id))
        .map(|&id| id.to_string());
    sorted.extend(unvisited);

    sorted
}
