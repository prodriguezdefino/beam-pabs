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

mod components;
mod defaults;
mod export;
mod names;
mod transforms;

pub use export::collect_coder_closure;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use beam_model::pipeline as proto;

use crate::pipeline::ExpansionMode;
use crate::pipeline::error::PipelineError;
use crate::pipeline::resources::ResourceHints;
use crate::transforms::TransformFn;
use defaults::{default_environment, global_window_coder, global_windows_strategy};

/// Process-wide counter that makes node IDs in the pipeline graph unique.
///
/// Component IDs advance across pipelines built within the same process. Driver
/// and worker IDs still match when both execute the same `main` binary.
static NODE_COUNTER: AtomicUsize = AtomicUsize::new(1);

pub fn next_id(prefix: &str) -> String {
    let id = NODE_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{prefix}_{id}")
}

/// The scope of a transform while it expands. Its name is the prefix of the names of the
/// transforms that it applies.
#[derive(Clone, Debug)]
pub struct NameScope {
    /// The full name of the applied transform.
    pub name: String,
    /// The length of the transform order when the scope opened.
    pub first_transform: usize,
    /// True when a primitive took the name of the scope.
    pub claimed: bool,
    /// The spec of the composite node, for a composite that a runner can replace.
    pub spec: Option<proto::FunctionSpec>,
    /// The display data of the node.
    pub display_data: Vec<proto::DisplayData>,
    /// The annotations of the node.
    pub annotations: HashMap<String, Vec<u8>>,
}

/// Internal shared pipeline state.
#[derive(Clone)]
pub struct PipelineInner {
    pub components: proto::Components,
    pub transform_order: Vec<String>,
    pub default_environment_id: String,
    pub default_windowing_strategy_id: String,
    pub transform_handlers: HashMap<String, TransformFn>,
    pub expansion_mode: ExpansionMode,
    /// Local files that expansion services used (typically the JAR), which a runner must ship
    /// to its workers. In expansion order, without duplicates.
    pub xlang_artifacts: Vec<std::path::PathBuf>,
    /// Expansion service clients by target. Transforms with the same target share one client,
    /// so they share one auto-started JVM. A client stops its service on drop.
    /// `Any` erases the type, so the crate does not depend on `apache-beam-external`.
    pub expansion_clients: HashMap<String, Arc<dyn std::any::Any + Send + Sync>>,
    /// Active resource hint scopes. Composite or transform expansion pushes them.
    pub scoped_resource_hints: Vec<ResourceHints>,
    /// The open scopes of applied transforms, outermost first.
    pub name_scopes: Vec<NameScope>,
    /// Every transform name in use: the names that
    /// [`unique_transform_name`](Self::unique_transform_name) gave out, and the IDs and unique
    /// names of the added and imported transforms.
    pub used_transform_names: HashSet<String>,
    /// Errors of `expand` calls, which cannot return them. [`validate`](Self::validate)
    /// reports the first one.
    pub construction_errors: Vec<PipelineError>,
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
            .field("transform_handlers_count", &self.transform_handlers.len())
            .field("expansion_mode", &self.expansion_mode)
            .field("xlang_artifacts", &self.xlang_artifacts)
            .field(
                "expansion_client_targets",
                &self.expansion_clients.keys().collect::<Vec<_>>(),
            )
            .field("scoped_resource_hints", &self.scoped_resource_hints)
            .field("name_scopes", &self.name_scopes)
            .field("construction_errors", &self.construction_errors)
            .finish()
    }
}

impl Default for PipelineInner {
    fn default() -> Self {
        Self::new()
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
            transform_handlers: HashMap::new(),
            expansion_mode: ExpansionMode::default(),
            xlang_artifacts: Vec::new(),
            expansion_clients: HashMap::new(),
            scoped_resource_hints: Vec::new(),
            name_scopes: Vec::new(),
            used_transform_names: HashSet::new(),
            construction_errors: Vec::new(),
            options: crate::options::PipelineOptions::default(),
        }
    }
}
