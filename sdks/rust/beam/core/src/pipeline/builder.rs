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

//! Construction and mutation of a [`Pipeline`], a cheap `Clone` handle to a shared
//! [`PipelineInner`]. The struct is declared in [`super`] so that the other `pipeline`
//! submodules can reach its private state.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use model::pipeline as proto;

use super::constants::*;
use super::error::PipelineError;
use super::graph::PipelineInner;
use super::{ExpansionMode, Pipeline};
use crate::coders::CoderRegistry;
use crate::runners::{PipelineResult, PipelineRunner};
use crate::transforms::PTransform;
use crate::values::{IsBounded, PBegin, PCollection};

impl Default for Pipeline {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for Pipeline {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Pipeline").finish_non_exhaustive()
    }
}

impl Pipeline {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(PipelineInner::new())),
        }
    }

    /// Creates a pipeline configured for `options`: the entry point for pipeline binaries. The
    /// same `main` runs as driver and as worker, and `options` selects the [`ExpansionMode`]
    /// (remote at submission, placeholder in the worker) and the default resource hints.
    pub fn create(options: &crate::options::PipelineOptions) -> Self {
        let p = Self::new()
            .with_expansion_mode(ExpansionMode::from_options(options))
            .with_resource_hints(options.resource_hints().unwrap_or_default());
        p.lock().options = options.clone();
        p
    }

    pub fn options(&self) -> crate::options::PipelineOptions {
        self.lock().options.clone()
    }

    /// Sets the pipeline-level default resource hints.
    pub fn with_resource_hints(self, hints: crate::pipeline::resources::ResourceHints) -> Self {
        self.set_resource_hints(hints);
        self
    }

    pub fn set_resource_hints(&self, hints: crate::pipeline::resources::ResourceHints) {
        self.lock().set_default_resource_hints(hints);
    }

    pub fn resource_hints(&self) -> crate::pipeline::resources::ResourceHints {
        self.lock().default_resource_hints()
    }

    /// Enters a scoped resource hints block, returning an RAII guard that pops the scope on drop.
    pub fn enter_resource_hints_scope(
        &self,
        hints: crate::pipeline::resources::ResourceHints,
    ) -> ResourceHintsScopeGuard {
        self.lock().push_scoped_resource_hints(hints);
        ResourceHintsScopeGuard {
            pipeline: self.clone(),
        }
    }

    /// Overrides how cross-language transforms are expanded.
    pub fn with_expansion_mode(self, mode: ExpansionMode) -> Self {
        self.lock().expansion_mode = mode;
        self
    }

    /// How cross-language transforms are expanded in this pipeline.
    pub fn expansion_mode(&self) -> ExpansionMode {
        self.lock().expansion_mode
    }

    pub fn lock(&self) -> std::sync::MutexGuard<'_, PipelineInner> {
        self.inner.lock().expect("Pipeline lock poisoned")
    }

    pub fn root_transform_ids(&self) -> Vec<String> {
        self.lock().compute_root_transform_ids()
    }

    pub fn unique_transform_name(&self, name: &str) -> String {
        self.lock().unique_transform_name(name)
    }

    /// Registers a coder and returns its id, reusing an identical registered coder.
    pub fn register_coder(&self, urn: &str, component_coder_ids: Vec<String>) -> String {
        self.lock().register_coder(urn, component_coder_ids)
    }

    /// Returns the component coder ids of `coder_id`, or an empty list if it is not registered.
    pub fn coder_components(&self, coder_id: &str) -> Vec<String> {
        self.lock()
            .components
            .coders
            .get(coder_id)
            .map(|c| c.component_coder_ids.clone())
            .unwrap_or_default()
    }

    /// Registers a `WindowingStrategy` and returns its id, reusing an identical registered one.
    pub fn register_windowing_strategy(&self, strategy: proto::WindowingStrategy) -> String {
        self.lock().register_windowing_strategy(strategy)
    }

    pub fn add_pcollection<T: 'static>(
        &self,
        name: &str,
        coder_id: &str,
        is_bounded: IsBounded,
    ) -> PCollection<T> {
        let pcoll_id = self.lock().add_pcollection(name, coder_id, is_bounded);
        PCollection::new(pcoll_id, coder_id.to_string(), self.clone())
    }

    pub fn add_pcollection_with_windowing<T: 'static>(
        &self,
        name: &str,
        coder_id: &str,
        is_bounded: IsBounded,
        windowing_strategy_id: &str,
    ) -> PCollection<T> {
        let pcoll_id = self.lock().add_pcollection_with_windowing(
            name,
            coder_id,
            is_bounded,
            windowing_strategy_id,
        );
        PCollection::new(pcoll_id, coder_id.to_string(), self.clone())
    }

    pub fn add_transform(
        &self,
        name: &str,
        urn: &str,
        payload: Vec<u8>,
        inputs: HashMap<String, String>,
        outputs: HashMap<String, String>,
    ) -> String {
        self.lock()
            .add_transform(name, urn, payload, inputs, outputs)
    }

    pub fn add_transform_with_display_data(
        &self,
        name: &str,
        urn: &str,
        payload: Vec<u8>,
        inputs: HashMap<String, String>,
        outputs: HashMap<String, String>,
        display_data: Vec<proto::DisplayData>,
    ) -> String {
        self.lock().add_transform_with_display_data(
            name,
            urn,
            payload,
            inputs,
            outputs,
            display_data,
        )
    }

    pub fn set_transform_display_data(
        &self,
        transform_id: &str,
        display_data: Vec<proto::DisplayData>,
    ) {
        self.lock()
            .set_transform_display_data(transform_id, display_data);
    }

    pub fn add_composite_transform(
        &self,
        name: &str,
        urn: Option<&str>,
        payload: Vec<u8>,
        inputs: HashMap<String, String>,
        outputs: HashMap<String, String>,
        subtransforms: Vec<String>,
    ) -> String {
        self.lock()
            .add_composite_transform(name, urn, payload, inputs, outputs, subtransforms)
    }

    /// Finds the ID of the primitive transform that produces `pcollection_id`.
    pub fn producer_transform_id(&self, pcollection_id: &str) -> Option<String> {
        self.lock().producer_transform_id(pcollection_id)
    }

    /// Creates a named primitive impulse transform and its output PCollection.
    pub fn add_impulse(&self, name: &str) -> (String, PCollection<Vec<u8>>) {
        let bytes_coder_id = self.register_coder(crate::coders::URN_BYTES, Vec::new());
        let out_pcoll = self.add_pcollection::<Vec<u8>>(
            &format!("{name}_out"),
            &bytes_coder_id,
            IsBounded::Bounded,
        );

        let outputs = HashMap::from([("out".to_string(), out_pcoll.id().to_string())]);

        let transform_id =
            self.add_transform(name, URN_IMPULSE, Vec::new(), HashMap::new(), outputs);

        (transform_id, out_pcoll)
    }

    /// Root entrypoint for creating the initial impulse PCollection.
    pub fn impulse(&self) -> PCollection<Vec<u8>> {
        self.add_impulse("Impulse").1
    }

    /// The beginning of this pipeline, the input to any root transform.
    pub fn begin(&self) -> PBegin {
        PBegin::new(self.clone())
    }

    /// Validates graph consistency before execution or submission to a runner.
    pub fn validate(&self) -> Result<(), PipelineError> {
        self.lock().validate()
    }

    /// Exports the pipeline as a Runner API `Pipeline` proto.
    pub fn to_proto(&self) -> proto::Pipeline {
        self.lock().to_proto()
    }

    /// Applies a root transform, such as a source: `self.begin().apply(transform)`.
    pub fn apply<Tform>(&self, transform: Tform) -> Tform::Output
    where
        Tform: PTransform<PBegin>,
    {
        self.begin().apply(transform)
    }

    /// Replaces the default and Rust environments with a Docker environment that runs
    /// `container_image`. Environments from cross-language expansion do not change.
    pub fn set_docker_environment(&self, container_image: impl Into<String>) {
        use prost::Message;
        let payload = proto::DockerPayload {
            container_image: container_image.into(),
        }
        .encode_to_vec();

        let mut inner = self.lock();
        let default_id = inner.default_environment_id.clone();
        for (id, env) in inner.components.environments.iter_mut() {
            if id == &default_id || is_rust_environment(env) {
                env.urn = URN_ENV_DOCKER.to_string();
                env.payload = payload.clone();
                if env.capabilities.is_empty() {
                    env.capabilities = standard_capabilities();
                }
            }
        }
    }

    /// Declares a staged pipeline binary at `url` as a dependency of the default environment.
    /// The runner sends it in `ProvisionInfo`; the boot program fetches it from the artifact
    /// retrieval service and runs it.
    pub fn set_worker_binary_artifact(&self, url: impl Into<String>, sha256: impl Into<String>) {
        use prost::Message;
        self.set_worker_binary_dependency(
            URN_ARTIFACT_TYPE_URL,
            proto::ArtifactUrlPayload {
                url: url.into(),
                sha256: sha256.into(),
            }
            .encode_to_vec(),
        );
    }

    /// Declares a pipeline binary at `path` on the submitting machine as a dependency, for
    /// runners that fetch artifact bytes through `ReverseArtifactRetrievalService`.
    pub fn set_worker_binary_file(&self, path: impl Into<String>, sha256: impl Into<String>) {
        use prost::Message;
        self.set_worker_binary_dependency(
            URN_ARTIFACT_TYPE_FILE,
            proto::ArtifactFilePayload {
                path: path.into(),
                sha256: sha256.into(),
            }
            .encode_to_vec(),
        );
    }

    /// Attaches the worker-binary dependency to the Rust environments only. Cross-language
    /// environments keep the dependencies that their own SDK declared.
    fn set_worker_binary_dependency(&self, type_urn: &str, type_payload: Vec<u8>) {
        let dependency = proto::ArtifactInformation {
            type_urn: type_urn.to_string(),
            type_payload,
            role_urn: URN_ARTIFACT_ROLE_WORKER_BINARY.to_string(),
            role_payload: Vec::new(),
        };

        let mut inner = self.lock();
        let default_id = inner.default_environment_id.clone();
        for (id, env) in inner.components.environments.iter_mut() {
            if id == &default_id || is_rust_environment(env) {
                // Replace, do not append, so a second run does not leave two binaries.
                env.dependencies
                    .retain(|dep| dep.role_urn != URN_ARTIFACT_ROLE_WORKER_BINARY);
                env.dependencies.push(dependency.clone());
            }
        }
    }

    /// Runners use this id to tell the Rust environment apart from cross-language
    /// environments, which must stay bound to their original transforms.
    pub fn default_environment_id(&self) -> String {
        self.lock().default_environment_id.clone()
    }

    pub fn default_environment(&self) -> proto::Environment {
        let inner = self.lock();
        let env_id = &inner.default_environment_id;
        inner
            .components
            .environments
            .get(env_id)
            .cloned()
            .unwrap_or_else(|| proto::Environment {
                urn: URN_ENV_DEFAULT.to_string(),
                payload: Vec::new(),
                display_data: Vec::new(),
                capabilities: standard_capabilities(),
                resource_hints: HashMap::new(),
                dependencies: Vec::new(),
            })
    }

    /// Validates and runs the pipeline with the runner that its options select (default Prism).
    pub async fn run(&self) -> Result<PipelineResult, crate::runners::RunnerError> {
        let options = self.options();
        crate::runners::run(self, &options).await
    }

    /// Executes the pipeline using an explicitly constructed runner.
    pub async fn run_with_runner<R: PipelineRunner + ?Sized>(
        &self,
        runner: &R,
    ) -> Result<PipelineResult, crate::runners::RunnerError> {
        self.validate()?;
        runner.run(self).await
    }

    /// Registers a standard Beam Row coder (`beam:coder:row:v1`) carrying `schema`.
    pub fn register_row_coder(&self, schema: &crate::schema::Schema) -> String {
        self.lock().register_coder_with_payload(
            crate::coders::URN_ROW,
            Vec::new(),
            schema.to_proto_bytes(),
        )
    }
}

impl CoderRegistry for Pipeline {
    fn register_coder(&self, urn: &str, component_coder_ids: Vec<String>) -> String {
        self.lock().register_coder(urn, component_coder_ids)
    }

    fn register_coder_with_payload(
        &self,
        urn: &str,
        component_coder_ids: Vec<String>,
        payload: Vec<u8>,
    ) -> String {
        self.lock()
            .register_coder_with_payload(urn, component_coder_ids, payload)
    }
}

/// RAII guard that pops a scoped set of resource hints from the pipeline on drop.
#[derive(Debug)]
pub struct ResourceHintsScopeGuard {
    pipeline: Pipeline,
}

impl Drop for ResourceHintsScopeGuard {
    fn drop(&mut self) {
        self.pipeline.lock().pop_scoped_resource_hints();
    }
}

/// Returns true if `env` is a Rust SDK environment, not a cross-language environment.
fn is_rust_environment(env: &proto::Environment) -> bool {
    env.urn == URN_ENV_DEFAULT
        || env
            .capabilities
            .iter()
            .any(|c| c.starts_with(SDK_BASE_VERSION_CAPABILITY_PREFIX))
}
