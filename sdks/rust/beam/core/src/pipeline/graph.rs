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

//! Internal DAG representation and component registration for pipelines.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use model::pipeline as proto;

use crate::pipeline::ExpansionMode;
use crate::pipeline::constants::{
    URN_ENV_DEFAULT, URN_WINDOW_FN_GLOBAL_WINDOWS, is_runner_primitive_urn, standard_capabilities,
};
use crate::pipeline::error::PipelineError;
use crate::pipeline::roots;
use crate::pipeline::validation;
use crate::values::IsBounded;

/// Process-wide counter that makes node ids in the pipeline DAG unique.
static NODE_COUNTER: AtomicUsize = AtomicUsize::new(1);

pub fn next_id(prefix: &str) -> String {
    let id = NODE_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{prefix}_{id}")
}

/// Internal shared pipeline state.
#[derive(Clone)]
pub struct PipelineInner {
    pub components: proto::Components,
    pub transform_order: Vec<String>,
    pub default_environment_id: String,
    pub default_windowing_strategy_id: String,
    pub expansion_mode: ExpansionMode,
    /// Local files that expansion services used (typically the JAR), which a runner must ship
    /// to its workers. In expansion order, without duplicates.
    pub xlang_artifacts: Vec<std::path::PathBuf>,
    /// Expansion service clients by target. Transforms with the same target share one client,
    /// so they share one auto-started JVM. A client stops its service on drop.
    pub expansion_clients: HashMap<String, Arc<dyn std::any::Any + Send + Sync>>,
    /// Active resource hint scopes. Composite or transform expansion pushes them.
    pub scoped_resource_hints: Vec<crate::pipeline::resources::ResourceHints>,
    pub options: crate::options::PipelineOptions,
}

impl std::fmt::Debug for PipelineInner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PipelineInner")
            .field("components", &self.components)
            .field("transform_order", &self.transform_order)
            .field("default_environment_id", &self.default_environment_id)
            .field(
                "default_windowing_strategy_id",
                &self.default_windowing_strategy_id,
            )
            .field("expansion_mode", &self.expansion_mode)
            .field("xlang_artifacts", &self.xlang_artifacts)
            .field(
                "expansion_client_targets",
                &self.expansion_clients.keys().collect::<Vec<_>>(),
            )
            .field("scoped_resource_hints", &self.scoped_resource_hints)
            .finish()
    }
}

impl Default for PipelineInner {
    fn default() -> Self {
        Self::new()
    }
}

/// The environment that a transform gets when no other environment applies.
fn default_environment() -> proto::Environment {
    proto::Environment {
        urn: URN_ENV_DEFAULT.to_string(),
        payload: Vec::new(),
        display_data: Vec::new(),
        capabilities: standard_capabilities(),
        resource_hints: HashMap::new(),
        dependencies: Vec::new(),
    }
}

/// The coder for the single window that spans all time.
fn global_window_coder() -> proto::Coder {
    proto::Coder {
        spec: Some(proto::FunctionSpec {
            urn: crate::coders::URN_GLOBAL_WINDOW.to_string(),
            payload: Vec::new(),
        }),
        component_coder_ids: Vec::new(),
    }
}

/// The default windowing strategy: one global, non-merging window.
fn global_windows_strategy(
    window_coder_id: String,
    environment_id: String,
) -> proto::WindowingStrategy {
    proto::WindowingStrategy {
        window_fn: Some(proto::FunctionSpec {
            urn: URN_WINDOW_FN_GLOBAL_WINDOWS.to_string(),
            payload: Vec::new(),
        }),
        merge_status: proto::merge_status::Enum::NonMerging as i32,
        window_coder_id,
        trigger: Some(proto::Trigger {
            trigger: Some(proto::trigger::Trigger::Default(proto::trigger::Default {})),
        }),
        accumulation_mode: proto::accumulation_mode::Enum::Discarding as i32,
        output_time: proto::output_time::Enum::EndOfWindow as i32,
        closing_behavior: proto::closing_behavior::Enum::EmitIfNonempty as i32,
        allowed_lateness: 0,
        on_time_behavior: proto::on_time_behavior::Enum::FireIfNonempty as i32,
        assigns_to_one_window: true,
        environment_id,
    }
}

impl PipelineInner {
    pub fn new() -> Self {
        let default_environment_id = "env_default".to_string();
        let global_window_coder_id = "coder_global_window".to_string();
        let default_windowing_strategy_id = "ws_global_default".to_string();

        let components = proto::Components {
            environments: HashMap::from([(default_environment_id.clone(), default_environment())]),
            coders: HashMap::from([(global_window_coder_id.clone(), global_window_coder())]),
            windowing_strategies: HashMap::from([(
                default_windowing_strategy_id.clone(),
                global_windows_strategy(global_window_coder_id, default_environment_id.clone()),
            )]),
            ..Default::default()
        };

        Self {
            components,
            transform_order: Vec::new(),
            default_environment_id,
            default_windowing_strategy_id,
            expansion_mode: ExpansionMode::default(),
            xlang_artifacts: Vec::new(),
            expansion_clients: HashMap::new(),
            scoped_resource_hints: Vec::new(),
            options: crate::options::PipelineOptions::default(),
        }
    }

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

    /// Returns `name` if no transform uses it, else `name` with the first free suffix (`name_2`).
    pub fn unique_transform_name(&mut self, name: &str) -> String {
        unique_name_in(&self.components.transforms, name, |t| &t.unique_name)
    }

    /// Allocates a unique PCollection name.
    pub fn unique_pcollection_name(&mut self, name: &str) -> String {
        unique_name_in(&self.components.pcollections, name, |p| &p.unique_name)
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

    pub fn add_pcollection_with_windowing(
        &mut self,
        name: &str,
        coder_id: &str,
        is_bounded: IsBounded,
        windowing_strategy_id: &str,
    ) -> String {
        let pcoll_id = self.unique_pcollection_name(name);
        let proto_pcollection = proto::PCollection {
            unique_name: pcoll_id.clone(),
            coder_id: coder_id.to_string(),
            is_bounded: proto::is_bounded::Enum::from(is_bounded) as i32,
            windowing_strategy_id: windowing_strategy_id.to_string(),
            display_data: Vec::new(),
        };

        self.components
            .pcollections
            .insert(pcoll_id.clone(), proto_pcollection);
        pcoll_id
    }

    pub fn add_pcollection(&mut self, name: &str, coder_id: &str, is_bounded: IsBounded) -> String {
        let default_ws = self.default_windowing_strategy_id.clone();
        self.add_pcollection_with_windowing(name, coder_id, is_bounded, &default_ws)
    }

    pub fn push_scoped_resource_hints(&mut self, hints: crate::pipeline::resources::ResourceHints) {
        self.scoped_resource_hints.push(hints);
    }

    pub fn pop_scoped_resource_hints(
        &mut self,
    ) -> Option<crate::pipeline::resources::ResourceHints> {
        self.scoped_resource_hints.pop()
    }

    /// Merges the active hint stack, inner scopes over outer ones.
    pub fn effective_scoped_resource_hints(&self) -> crate::pipeline::resources::ResourceHints {
        self.scoped_resource_hints.iter().fold(
            crate::pipeline::resources::ResourceHints::new(),
            |effective, hints| hints.merge_with_outer(&effective),
        )
    }

    /// Sets the pipeline-level hints on the default environment.
    pub fn set_default_resource_hints(&mut self, hints: crate::pipeline::resources::ResourceHints) {
        if let Some(env) = self
            .components
            .environments
            .get_mut(&self.default_environment_id)
        {
            env.resource_hints = hints.to_proto_map();
        }
    }

    pub fn default_resource_hints(&self) -> crate::pipeline::resources::ResourceHints {
        self.components
            .environments
            .get(&self.default_environment_id)
            .map(|env| {
                crate::pipeline::resources::ResourceHints::from_proto_map(&env.resource_hints)
            })
            .unwrap_or_default()
    }

    /// Returns the id of an environment with `hints`, reusing an identical one if it exists.
    pub fn get_or_create_environment_for_hints(
        &mut self,
        hints: &crate::pipeline::resources::ResourceHints,
    ) -> String {
        if hints.is_empty() {
            return self.default_environment_id.clone();
        }

        let default_env = self
            .components
            .environments
            .get(&self.default_environment_id)
            .cloned()
            .unwrap_or_else(default_environment);

        let outer_hints =
            crate::pipeline::resources::ResourceHints::from_proto_map(&default_env.resource_hints);
        let merged_hints = hints.merge_with_outer(&outer_hints);
        let proto_hints = merged_hints.to_proto_map();

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

    #[expect(
        clippy::too_many_arguments,
        reason = "graph wiring takes transform components, resource hints, and display data explicitly"
    )]
    pub fn add_transform_with_hints_and_display_data(
        &mut self,
        name: &str,
        urn: &str,
        payload: Vec<u8>,
        inputs: HashMap<String, String>,
        outputs: HashMap<String, String>,
        hints: Option<&crate::pipeline::resources::ResourceHints>,
        display_data: Vec<proto::DisplayData>,
    ) -> String {
        let unique_name = self.unique_transform_name(name);
        let environment_id = if is_runner_primitive_urn(urn) {
            String::new()
        } else {
            let scoped = self.effective_scoped_resource_hints();
            let effective = match hints {
                Some(explicit) => explicit.merge_with_outer(&scoped),
                None => scoped,
            };
            self.get_or_create_environment_for_hints(&effective)
        };

        let proto_transform = proto::PTransform {
            unique_name: unique_name.clone(),
            spec: Some(proto::FunctionSpec {
                urn: urn.to_string(),
                payload,
            }),
            subtransforms: Vec::new(),
            inputs,
            outputs,
            display_data,
            environment_id,
            annotations: HashMap::new(),
        };

        let transform_id = unique_name;
        self.components
            .transforms
            .insert(transform_id.clone(), proto_transform);
        self.transform_order.push(transform_id.clone());

        transform_id
    }

    pub fn add_transform_with_display_data(
        &mut self,
        name: &str,
        urn: &str,
        payload: Vec<u8>,
        inputs: HashMap<String, String>,
        outputs: HashMap<String, String>,
        display_data: Vec<proto::DisplayData>,
    ) -> String {
        self.add_transform_with_hints_and_display_data(
            name,
            urn,
            payload,
            inputs,
            outputs,
            None,
            display_data,
        )
    }

    pub fn add_transform(
        &mut self,
        name: &str,
        urn: &str,
        payload: Vec<u8>,
        inputs: HashMap<String, String>,
        outputs: HashMap<String, String>,
    ) -> String {
        self.add_transform_with_display_data(name, urn, payload, inputs, outputs, Vec::new())
    }

    pub fn set_transform_display_data(
        &mut self,
        transform_id: &str,
        display_data: Vec<proto::DisplayData>,
    ) {
        if let Some(t) = self.components.transforms.get_mut(transform_id) {
            t.display_data = display_data;
        }
    }

    pub fn add_composite_transform(
        &mut self,
        name: &str,
        urn: Option<&str>,
        payload: Vec<u8>,
        inputs: HashMap<String, String>,
        outputs: HashMap<String, String>,
        subtransforms: Vec<String>,
    ) -> String {
        let unique_name = self.unique_transform_name(name);
        let spec = urn.map(|u| proto::FunctionSpec {
            urn: u.to_string(),
            payload,
        });
        let proto_transform = proto::PTransform {
            unique_name: unique_name.clone(),
            spec,
            subtransforms,
            inputs,
            outputs,
            display_data: Vec::new(),
            environment_id: String::new(),
            annotations: HashMap::new(),
        };

        let transform_id = unique_name;
        self.components
            .transforms
            .insert(transform_id.clone(), proto_transform);
        self.transform_order.push(transform_id.clone());

        transform_id
    }

    /// Computes the root (top-level) transform IDs in shallow topological order.
    pub fn compute_root_transform_ids(&self) -> Vec<String> {
        roots::compute_root_transform_ids(&self.components, &self.transform_order)
    }

    pub fn producer_transform_id(&self, pcollection_id: &str) -> Option<String> {
        let subtransforms: std::collections::HashSet<&str> = self
            .components
            .transforms
            .values()
            .flat_map(|t| &t.subtransforms)
            .map(String::as_str)
            .collect();

        let produces_pcoll = |id: &&String| {
            self.components
                .transforms
                .get(*id)
                .is_some_and(|t| t.outputs.values().any(|out| out == pcollection_id))
        };

        self.transform_order
            .iter()
            .rev()
            .find(|id| !subtransforms.contains(id.as_str()) && produces_pcoll(id))
            .or_else(|| self.transform_order.iter().rev().find(produces_pcoll))
            .cloned()
    }

    /// Checks that the graph is internally consistent.
    pub fn validate(&self) -> Result<(), PipelineError> {
        validation::validate(&self.components, &self.transform_order)
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

fn unique_name_in<V>(
    map: &HashMap<String, V>,
    name: &str,
    unique_name: impl Fn(&V) -> &str,
) -> String {
    let is_free = |candidate: &str| {
        !map.contains_key(candidate) && !map.values().any(|v| unique_name(v) == candidate)
    };
    if is_free(name) {
        name.to_string()
    } else {
        (2..)
            .map(|count| format!("{name}_{count}"))
            .find(|candidate| is_free(candidate))
            .expect("infinite sequence must find a free suffix")
    }
}

/// Returns the standard requirement URNs that the runner must support for `components`.
fn compute_pipeline_requirements(components: &proto::Components) -> Vec<String> {
    use crate::pipeline::constants::{
        URN_PAR_DO, URN_REQUIREMENT_BUNDLE_FINALIZATION, URN_REQUIREMENT_SPLITTABLE_DOFN,
        URN_REQUIREMENT_STATEFUL,
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
            ]
            .into_iter()
            .flatten()
            .map(str::to_string)
        })
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}
