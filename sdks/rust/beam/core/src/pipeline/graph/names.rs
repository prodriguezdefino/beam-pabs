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

//! Unique names and name scopes of transforms and PCollections.

use std::collections::{HashMap, HashSet};

use model::pipeline as proto;

use super::{NameScope, PipelineInner};
use crate::pipeline::error::PipelineError;

impl PipelineInner {
    /// Returns `name` in the innermost name scope (`Outer/Inner`) and reserves it. A name
    /// in use records a [`PipelineError::DuplicateTransformName`] and gets a suffix (`_2`).
    pub fn unique_transform_name(&mut self, name: &str) -> String {
        let full_name = match self.name_scopes.last() {
            Some(scope) => format!("{}/{name}", scope.name),
            None => name.to_string(),
        };
        let resolved = if self.used_transform_names.contains(&full_name) {
            let suffixed = (2..)
                .map(|count| format!("{full_name}_{count}"))
                .find(|candidate| !self.used_transform_names.contains(candidate))
                .expect("infinite sequence must find a free suffix");
            self.construction_errors
                .push(PipelineError::DuplicateTransformName { name: full_name });
            suffixed
        } else {
            full_name
        };
        self.used_transform_names.insert(resolved.clone());
        resolved
    }

    /// Returns the name of a primitive transform that is about to be added. The first
    /// primitive of a scope that has no transforms yet takes the name of the scope, so the
    /// applied transform is that primitive. Another primitive gets the child name `kind`.
    pub fn primitive_name(&mut self, kind: &str) -> String {
        let order_len = self.transform_order.len();
        match self.name_scopes.last_mut() {
            Some(scope) if !scope.claimed && scope.first_transform == order_len => {
                scope.claimed = true;
                scope.name.clone()
            }
            Some(_) | None => self.unique_transform_name(kind),
        }
    }

    /// The full name of the innermost open scope, or `None` at the root.
    pub fn scope_name(&self) -> Option<&str> {
        self.name_scopes.last().map(|scope| scope.name.as_str())
    }

    /// The innermost open scope, or `None` at the root.
    pub fn current_scope_mut(&mut self) -> Option<&mut NameScope> {
        self.name_scopes.last_mut()
    }

    /// Returns a unique PCollection name: `name`, with a suffix (`_2`) if it is in use.
    pub fn unique_pcollection_name(&self, name: &str) -> String {
        unique_name_in(&self.components.pcollections, name, |p| &p.unique_name)
    }

    /// Marks the ID and the unique name of a transform as in use. Code that inserts
    /// transforms into the components directly, such as expansion, calls it for each one.
    pub fn record_transform_name(&mut self, transform_id: &str, transform: &proto::PTransform) {
        self.used_transform_names.insert(transform_id.to_string());
        self.used_transform_names
            .insert(transform.unique_name.clone());
    }

    /// Opens the scope of the transform `resolved_name`.
    pub fn push_name_scope(&mut self, resolved_name: String) {
        let first_transform = self.transform_order.len();
        self.name_scopes.push(NameScope {
            name: resolved_name,
            first_transform,
            claimed: false,
            spec: None,
            display_data: Vec::new(),
            annotations: HashMap::new(),
        });
    }

    /// Closes the innermost scope.
    pub fn pop_name_scope(&mut self) -> Option<NameScope> {
        self.name_scopes.pop()
    }

    /// Records the node of a closed scope. A claimed scope already has its primitive node,
    /// which gets the display data and annotations. Any other scope becomes a composite of
    /// the outermost transforms that it added. The inputs of the composite are `inputs` and
    /// every PCollection that a transform of the scope reads and no transform of the scope
    /// produces. An input of the scope is not an output of the composite. A claimed scope
    /// with more transforms records a [`PipelineError::InvalidConstruction`]. Returns the
    /// name of the node.
    pub fn close_name_scope(
        &mut self,
        scope: NameScope,
        inputs: HashMap<String, String>,
        outputs: HashMap<String, String>,
    ) -> String {
        let NameScope {
            name,
            first_transform,
            claimed,
            spec,
            display_data,
            annotations,
        } = scope;
        let subtransforms = self.outermost_transforms_from(first_transform);
        if claimed && self.transform_order.len() > first_transform + 1 {
            self.construction_errors
                .push(PipelineError::InvalidConstruction {
                    transform_id: name.clone(),
                    reason: "a primitive took the name of the scope, so the scope cannot \
                             contain other transforms"
                        .to_string(),
                });
        }
        // A scope that added nothing and has no spec gets no node: a node without children
        // or spec reads as a primitive.
        if !claimed && (!subtransforms.is_empty() || spec.is_some()) {
            let inputs = self.with_external_inputs(first_transform, inputs);
            let outputs = outputs
                .into_iter()
                .filter(|(_, id)| !inputs.values().any(|input| input == id))
                .collect();
            let (urn, payload) = spec.map_or((None, Vec::new()), |s| (Some(s.urn), s.payload));
            self.add_composite_transform(
                &name,
                urn.as_deref(),
                payload,
                inputs,
                outputs,
                subtransforms,
            );
        }
        if let Some(transform) = self.components.transforms.get_mut(&name) {
            if !display_data.is_empty() {
                transform.display_data = display_data;
            }
            transform.annotations.extend(annotations);
        }
        name
    }

    /// Adds to `inputs` every PCollection that a transform at `first..` of the add order
    /// reads and no transform at `first..` produces. An added input has the tag of its first
    /// consumer, in add order and then tag order, with a suffix (`_2`) if the tag is in use.
    fn with_external_inputs(
        &self,
        first: usize,
        mut inputs: HashMap<String, String>,
    ) -> HashMap<String, String> {
        let added: Vec<&proto::PTransform> = self
            .transform_order
            .get(first..)
            .unwrap_or_default()
            .iter()
            .filter_map(|id| self.components.transforms.get(id))
            .collect();
        let produced: HashSet<&String> = added.iter().flat_map(|t| t.outputs.values()).collect();
        let mut listed: HashSet<String> = inputs.values().cloned().collect();
        for transform in &added {
            let mut reads: Vec<(&String, &String)> = transform.inputs.iter().collect();
            reads.sort();
            for (tag, id) in reads {
                if produced.contains(id) || !listed.insert(id.clone()) {
                    continue;
                }
                let free_tag = if inputs.contains_key(tag) {
                    (2..)
                        .map(|count| format!("{tag}_{count}"))
                        .find(|candidate| !inputs.contains_key(candidate))
                        .expect("infinite sequence must find a free suffix")
                } else {
                    tag.clone()
                };
                inputs.insert(free_tag, id.clone());
            }
        }
        inputs
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
