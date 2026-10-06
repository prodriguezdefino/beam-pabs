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

//! Registration of transforms and PCollections, and queries on the transform order.

use std::collections::{HashMap, HashSet};

use model::pipeline as proto;

use super::PipelineInner;
use crate::pipeline::constants::is_runner_primitive_urn;
use crate::pipeline::resources::ResourceHints;
use crate::pipeline::roots;
use crate::values::IsBounded;

impl PipelineInner {
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

    /// Adds a primitive transform. `resolved_name` comes from `unique_transform_name`.
    #[expect(
        clippy::too_many_arguments,
        reason = "graph wiring takes transform components, resource hints, and display data explicitly"
    )]
    pub fn add_transform_with_hints_and_display_data(
        &mut self,
        resolved_name: &str,
        urn: &str,
        payload: Vec<u8>,
        inputs: HashMap<String, String>,
        outputs: HashMap<String, String>,
        hints: Option<&ResourceHints>,
        display_data: Vec<proto::DisplayData>,
    ) -> String {
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
            unique_name: resolved_name.to_string(),
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
        self.insert_transform(resolved_name, proto_transform)
    }

    /// Inserts a built transform under the ID `resolved_name` and appends it to the add order.
    fn insert_transform(&mut self, resolved_name: &str, transform: proto::PTransform) -> String {
        self.used_transform_names.insert(resolved_name.to_string());
        self.components
            .transforms
            .insert(resolved_name.to_string(), transform);
        self.transform_order.push(resolved_name.to_string());
        resolved_name.to_string()
    }

    pub fn add_transform_with_display_data(
        &mut self,
        resolved_name: &str,
        urn: &str,
        payload: Vec<u8>,
        inputs: HashMap<String, String>,
        outputs: HashMap<String, String>,
        display_data: Vec<proto::DisplayData>,
    ) -> String {
        self.add_transform_with_hints_and_display_data(
            resolved_name,
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
        resolved_name: &str,
        urn: &str,
        payload: Vec<u8>,
        inputs: HashMap<String, String>,
        outputs: HashMap<String, String>,
    ) -> String {
        self.add_transform_with_display_data(
            resolved_name,
            urn,
            payload,
            inputs,
            outputs,
            Vec::new(),
        )
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

    /// Adds a composite transform. `resolved_name` comes from `unique_transform_name`.
    pub fn add_composite_transform(
        &mut self,
        resolved_name: &str,
        urn: Option<&str>,
        payload: Vec<u8>,
        inputs: HashMap<String, String>,
        outputs: HashMap<String, String>,
        subtransforms: Vec<String>,
    ) -> String {
        // A runner that replaces or lifts a composite with a spec, such as a combine, runs
        // the new stages in the environment of the composite.
        let environment_id = match urn {
            Some(urn) if !is_runner_primitive_urn(urn) => {
                let scoped = self.effective_scoped_resource_hints();
                self.get_or_create_environment_for_hints(&scoped)
            }
            Some(_) | None => String::new(),
        };
        let spec = urn.map(|u| proto::FunctionSpec {
            urn: u.to_string(),
            payload,
        });
        let proto_transform = proto::PTransform {
            unique_name: resolved_name.to_string(),
            spec,
            subtransforms,
            inputs,
            outputs,
            display_data: Vec::new(),
            environment_id,
            annotations: HashMap::new(),
        };
        self.insert_transform(resolved_name, proto_transform)
    }

    /// Returns the transforms at `first..` of the add order that no other of them contains,
    /// in add order: the direct children of a scope that opened at `first`.
    pub fn outermost_transforms_from(&self, first: usize) -> Vec<String> {
        let added = self.transform_order.get(first..).unwrap_or_default();
        let nested: HashSet<&String> = added
            .iter()
            .filter_map(|id| self.components.transforms.get(id))
            .flat_map(|t| &t.subtransforms)
            .collect();
        added
            .iter()
            .filter(|id| !nested.contains(id))
            .cloned()
            .collect()
    }

    /// Computes the root (top-level) transform IDs in shallow topological order.
    pub fn compute_root_transform_ids(&self) -> Vec<String> {
        roots::compute_root_transform_ids(&self.components, &self.transform_order)
    }

    pub fn producer_transform_id(&self, pcollection_id: &str) -> Option<String> {
        let subtransforms: HashSet<&str> = self
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
}
