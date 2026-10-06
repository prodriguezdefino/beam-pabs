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

//! Validation, runner requirements and proto export of the pipeline graph.

use std::collections::HashMap;

use model::pipeline as proto;

use super::PipelineInner;
use crate::pipeline::error::PipelineError;
use crate::pipeline::validation;

impl PipelineInner {
    /// Checks that every transform was built and that the graph is internally consistent.
    pub fn validate(&self) -> Result<(), PipelineError> {
        self.construction_errors
            .first()
            .cloned()
            .map_or(Ok(()), Err)
            .and_then(|()| validation::validate(&self.components, &self.transform_order))
    }

    /// Standard URNs of the features that a runner must support to run the graph.
    pub fn requirements(&self) -> Vec<String> {
        compute_pipeline_requirements(&self.components)
    }

    pub fn to_proto(&self) -> proto::Pipeline {
        let roots = self.compute_root_transform_ids();
        let requirements = self.requirements();
        proto::Pipeline {
            components: Some(self.components.clone()),
            root_transform_ids: roots,
            display_data: Vec::new(),
            requirements,
        }
    }
}

/// Returns the standard requirement URNs that the runner must support for `components`.
fn compute_pipeline_requirements(components: &proto::Components) -> Vec<String> {
    use crate::pipeline::constants::{
        URN_PAR_DO, URN_REQUIREMENT_BUNDLE_FINALIZATION, URN_REQUIREMENT_ON_WINDOW_EXPIRATION,
        URN_REQUIREMENT_SPLITTABLE_DOFN, URN_REQUIREMENT_STABLE_INPUT, URN_REQUIREMENT_STATEFUL,
        URN_REQUIREMENT_TIME_SORTED_INPUT,
    };
    use prost::Message;
    use std::collections::BTreeSet;

    components
        .transforms
        .values()
        .filter_map(|t| t.spec.as_ref())
        .filter(|s| s.urn == URN_PAR_DO && !s.payload.is_empty())
        .filter_map(|s| proto::ParDoPayload::decode(s.payload.as_slice()).ok())
        .flat_map(|pardo| {
            [
                (!pardo.state_specs.is_empty() || !pardo.timer_family_specs.is_empty())
                    .then_some(URN_REQUIREMENT_STATEFUL),
                (!pardo.restriction_coder_id.is_empty()).then_some(URN_REQUIREMENT_SPLITTABLE_DOFN),
                pardo
                    .requests_finalization
                    .then_some(URN_REQUIREMENT_BUNDLE_FINALIZATION),
                pardo
                    .requires_stable_input
                    .then_some(URN_REQUIREMENT_STABLE_INPUT),
                pardo
                    .requires_time_sorted_input
                    .then_some(URN_REQUIREMENT_TIME_SORTED_INPUT),
                (!pardo.on_window_expiration_timer_family_spec.is_empty())
                    .then_some(URN_REQUIREMENT_ON_WINDOW_EXPIRATION),
            ]
            .into_iter()
            .flatten()
            .map(str::to_string)
        })
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

/// Collects the closure of coder dependencies from seed coder IDs.
pub fn collect_coder_closure<'a>(
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
