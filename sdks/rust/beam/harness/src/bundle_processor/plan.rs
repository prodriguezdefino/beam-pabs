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

//! Plans a bundle: topological order, data sinks and timer families.

use prost::Message;
use std::collections::{HashMap, HashSet, VecDeque};

use super::{BundleError, URN_DATA_SINK, URN_DATA_SOURCE};
use model::fn_execution::{ProcessBundleDescriptor, RemoteGrpcPort};
use model::pipeline::PTransform;

/// Returns true when the spec of a transform has the given URN.
pub(super) fn has_urn(transform: &PTransform, urn: &str) -> bool {
    transform.spec.as_ref().is_some_and(|s| s.urn == urn)
}

/// Decodes the `RemoteGrpcPort` payload of a data source or sink transform.
pub(super) fn decode_port(transform: &PTransform) -> Result<RemoteGrpcPort, BundleError> {
    let payload = transform
        .spec
        .as_ref()
        .map(|s| s.payload.as_slice())
        .unwrap_or_default();
    Ok(RemoteGrpcPort::decode(payload)?)
}

/// Collects every DATA_SINK transform in the descriptor with its decoded port.
pub(super) fn collect_sinks(
    descriptor: &ProcessBundleDescriptor,
) -> Result<Vec<(String, RemoteGrpcPort)>, BundleError> {
    descriptor
        .transforms
        .iter()
        .filter(|(_, t)| has_urn(t, URN_DATA_SINK))
        .map(|(id, t)| decode_port(t).map(|port| (id.clone(), port)))
        .collect()
}

/// Maps each ParDo transform id to its declared `(timer_family_id, timer_coder_id)` pairs.
pub(super) fn collect_timer_families(
    descriptor: &ProcessBundleDescriptor,
) -> HashMap<String, Vec<(String, String)>> {
    use beam::pipeline::constants::URN_PAR_DO;
    descriptor
        .transforms
        .iter()
        .filter_map(|(t_id, t)| {
            let spec = t.spec.as_ref()?;
            (spec.urn == URN_PAR_DO).then_some((t_id, &spec.payload))
        })
        .filter_map(|(t_id, payload)| {
            model::pipeline::ParDoPayload::decode(payload.as_slice())
                .ok()
                .map(|pardo| (t_id, pardo))
        })
        .filter(|(_, pardo)| !pardo.timer_family_specs.is_empty())
        .map(|(t_id, pardo)| {
            let families = pardo
                .timer_family_specs
                .into_iter()
                .map(|(fam_id, spec)| (fam_id, spec.timer_family_coder_id))
                .collect();
            (t_id.clone(), families)
        })
        .collect()
}

/// Orders the transforms other than data sources and sinks with Kahn's algorithm, each after
/// the producers of its inputs. Transforms in a cycle are appended in arbitrary order.
pub(super) fn topological_order(
    descriptor: &ProcessBundleDescriptor,
    source_id: &str,
    source_t: &PTransform,
) -> Vec<String> {
    let intermediate_transforms: HashMap<&str, &PTransform> = descriptor
        .transforms
        .iter()
        .filter(|(_, t)| !has_urn(t, URN_DATA_SOURCE) && !has_urn(t, URN_DATA_SINK))
        .map(|(t_id, t)| (t_id.as_str(), t))
        .collect();

    // Map each PCollection to its producer. An intermediate transform wins over the source.
    let source_outputs = source_t
        .outputs
        .values()
        .map(|out_pcol| (out_pcol.as_str(), source_id));
    let intermediate_outputs = intermediate_transforms.iter().flat_map(|(&t_id, t)| {
        t.outputs
            .values()
            .map(move |out_pcol| (out_pcol.as_str(), t_id))
    });
    let pcol_producers: HashMap<&str, &str> = source_outputs.chain(intermediate_outputs).collect();

    let initial_adj: HashMap<&str, Vec<&str>> = intermediate_transforms
        .keys()
        .map(|&t_id| (t_id, Vec::new()))
        .collect();
    let initial_in_degree: HashMap<&str, usize> =
        HashMap::with_capacity(intermediate_transforms.len());

    let (adj, mut in_degree) = intermediate_transforms.iter().fold(
        (initial_adj, initial_in_degree),
        |(mut adj, mut in_degree), (&t_id, t)| {
            // The source is not part of the order. A producer can feed several inputs of the
            // same transform, so remove duplicates before counting edges.
            let producers_for_this_t: HashSet<&str> = t
                .inputs
                .values()
                .filter_map(|in_pcol| pcol_producers.get(in_pcol.as_str()).copied())
                .filter(|&producer_id| {
                    producer_id != source_id && intermediate_transforms.contains_key(producer_id)
                })
                .collect();

            in_degree.insert(t_id, producers_for_this_t.len());
            for producer_id in producers_for_this_t {
                if let Some(neighbors) = adj.get_mut(producer_id) {
                    neighbors.push(t_id);
                }
            }
            (adj, in_degree)
        },
    );

    let mut queue: VecDeque<&str> = in_degree
        .iter()
        .filter(|&(_, &deg)| deg == 0)
        .map(|(&id, _)| id)
        .collect();

    let mut ordered_transform_ids: Vec<String> = Vec::new();
    while let Some(curr) = queue.pop_front() {
        ordered_transform_ids.push(curr.to_string());
        for &nxt in adj.get(curr).into_iter().flatten() {
            let Some(deg) = in_degree.get_mut(nxt) else {
                continue;
            };
            *deg -= 1;
            if *deg == 0 {
                queue.push_back(nxt);
            }
        }
    }

    if ordered_transform_ids.len() < intermediate_transforms.len() {
        let unordered: Vec<String> = intermediate_transforms
            .keys()
            .filter(|&&t_id| !ordered_transform_ids.iter().any(|id| id == t_id))
            .map(|&t_id| t_id.to_string())
            .collect();
        ordered_transform_ids.extend(unordered);
    }

    ordered_transform_ids
}
