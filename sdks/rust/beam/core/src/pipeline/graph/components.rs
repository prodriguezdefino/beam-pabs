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

//! Registration of coders, windowing strategies and environments.

use std::collections::HashMap;

use beam_model::pipeline as proto;

use super::defaults::default_environment;
use super::{PipelineInner, next_id};
use crate::pipeline::resources::ResourceHints;

impl PipelineInner {
    pub fn register_coder(&mut self, urn: &str, component_coder_ids: Vec<String>) -> String {
        self.register_coder_with_payload(urn, component_coder_ids, Vec::new())
    }

    pub fn register_coder_with_payload(
        &mut self,
        urn: &str,
        component_coder_ids: Vec<String>,
        payload: Vec<u8>,
    ) -> String {
        let existing = self
            .components
            .coders
            .iter()
            .find(|(_, coder)| {
                coder
                    .spec
                    .as_ref()
                    .is_some_and(|spec| spec.urn == urn && spec.payload == payload)
                    && coder.component_coder_ids == component_coder_ids
            })
            .map(|(id, _)| id.clone());

        if let Some(id) = existing {
            return id;
        }

        let coder_id = next_id("coder");
        let proto_coder = proto::Coder {
            spec: Some(proto::FunctionSpec {
                urn: urn.to_string(),
                payload,
            }),
            component_coder_ids,
        };
        self.components.coders.insert(coder_id.clone(), proto_coder);
        coder_id
    }

    pub fn register_windowing_strategy(&mut self, strategy: proto::WindowingStrategy) -> String {
        let existing = self
            .components
            .windowing_strategies
            .iter()
            .find(|(_, ws)| **ws == strategy)
            .map(|(id, _)| id.clone());

        if let Some(id) = existing {
            return id;
        }

        let ws_id = next_id("ws");
        self.components
            .windowing_strategies
            .insert(ws_id.clone(), strategy);
        ws_id
    }

    pub fn push_scoped_resource_hints(&mut self, hints: ResourceHints) {
        self.scoped_resource_hints.push(hints);
    }

    pub fn pop_scoped_resource_hints(&mut self) -> Option<ResourceHints> {
        self.scoped_resource_hints.pop()
    }

    /// Merges the active hint stack, inner scopes over outer ones.
    pub fn effective_scoped_resource_hints(&self) -> ResourceHints {
        self.scoped_resource_hints
            .iter()
            .fold(ResourceHints::new(), |effective, hints| {
                hints.merge_with_outer(&effective)
            })
    }

    /// Sets the pipeline-level hints on the default environment.
    pub fn set_default_resource_hints(&mut self, hints: ResourceHints) {
        if let Some(env) = self
            .components
            .environments
            .get_mut(&self.default_environment_id)
        {
            env.resource_hints = hints.into();
        }
    }

    pub fn default_resource_hints(&self) -> ResourceHints {
        self.components
            .environments
            .get(&self.default_environment_id)
            .map(|env| ResourceHints::from(&env.resource_hints))
            .unwrap_or_default()
    }

    /// Returns the id of an environment with `hints`, reusing an identical one if it exists.
    pub fn get_or_create_environment_for_hints(&mut self, hints: &ResourceHints) -> String {
        if hints.is_empty() {
            return self.default_environment_id.clone();
        }

        let default_env = self
            .components
            .environments
            .get(&self.default_environment_id)
            .cloned()
            .unwrap_or_else(default_environment);

        let outer_hints = ResourceHints::from(&default_env.resource_hints);
        let merged_hints = hints.merge_with_outer(&outer_hints);
        let proto_hints = HashMap::from(&merged_hints);

        if proto_hints == default_env.resource_hints {
            return self.default_environment_id.clone();
        }

        if let Some((id, _)) = self.components.environments.iter().find(|(_, env)| {
            env.urn == default_env.urn
                && env.payload == default_env.payload
                && env.capabilities == default_env.capabilities
                && env.dependencies == default_env.dependencies
                && env.resource_hints == proto_hints
        }) {
            return id.clone();
        }

        let env_id = next_id("env_hints");
        let mut new_env = default_env;
        new_env.resource_hints = proto_hints;
        self.components.environments.insert(env_id.clone(), new_env);
        env_id
    }
}
