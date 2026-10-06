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
use crate::transforms::{PTransform, TransformFn, TryPTransform};
use crate::values::{IsBounded, PBegin, PCollection, POutput};

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
    /// Creates a new empty pipeline.
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

    /// Returns the pipeline options configured for this pipeline.
    pub fn options(&self) -> crate::options::PipelineOptions {
        self.lock().options.clone()
    }

    /// Sets the pipeline-level default resource hints.
    pub fn with_resource_hints(self, hints: crate::pipeline::resources::ResourceHints) -> Self {
        self.set_resource_hints(hints);
        self
    }

    /// Sets default resource hints on the pipeline.
    pub fn set_resource_hints(&self, hints: crate::pipeline::resources::ResourceHints) {
        self.lock().set_default_resource_hints(hints);
    }

    /// Returns the default resource hints configured on the pipeline.
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

    /// Locks and returns the internal pipeline state.
    pub(crate) fn lock(&self) -> std::sync::MutexGuard<'_, PipelineInner> {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Returns the IDs of the root transforms in the pipeline.
    pub fn root_transform_ids(&self) -> Vec<String> {
        self.lock().compute_root_transform_ids()
    }

    /// Returns `name` in the innermost name scope (`Outer/Inner`) and reserves it. A name in
    /// use records a [`PipelineError::DuplicateTransformName`] and gets a suffix (`_2`).
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

    /// Adds a PCollection to the pipeline graph.
    pub fn add_pcollection<T: 'static>(
        &self,
        name: &str,
        coder_id: &str,
        is_bounded: IsBounded,
    ) -> PCollection<T> {
        let pcoll_id = self.lock().add_pcollection(name, coder_id, is_bounded);
        PCollection::new(pcoll_id, coder_id.to_string(), self.clone())
    }

    /// Adds a PCollection with a specified windowing strategy to the pipeline graph.
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

    /// Adds a primitive transform. `resolved_name` comes from
    /// [`unique_transform_name`](Self::unique_transform_name).
    pub fn add_transform(
        &self,
        resolved_name: &str,
        urn: &str,
        payload: Vec<u8>,
        inputs: HashMap<String, String>,
        outputs: HashMap<String, String>,
    ) -> String {
        self.lock()
            .add_transform(resolved_name, urn, payload, inputs, outputs)
    }

    /// Adds a primitive transform with display data. `resolved_name` comes from
    /// [`unique_transform_name`](Self::unique_transform_name).
    pub fn add_transform_with_display_data(
        &self,
        resolved_name: &str,
        urn: &str,
        payload: Vec<u8>,
        inputs: HashMap<String, String>,
        outputs: HashMap<String, String>,
        display_data: Vec<proto::DisplayData>,
    ) -> String {
        self.lock().add_transform_with_display_data(
            resolved_name,
            urn,
            payload,
            inputs,
            outputs,
            display_data,
        )
    }

    /// Sets display data on an existing transform.
    pub fn set_transform_display_data(
        &self,
        transform_id: &str,
        display_data: Vec<proto::DisplayData>,
    ) {
        self.lock()
            .set_transform_display_data(transform_id, display_data);
    }

    /// Adds a composite transform with `subtransforms` as its children. `resolved_name` comes
    /// from [`unique_transform_name`](Self::unique_transform_name).
    pub fn add_composite_transform(
        &self,
        resolved_name: &str,
        urn: Option<&str>,
        payload: Vec<u8>,
        inputs: HashMap<String, String>,
        outputs: HashMap<String, String>,
        subtransforms: Vec<String>,
    ) -> String {
        self.lock().add_composite_transform(
            resolved_name,
            urn,
            payload,
            inputs,
            outputs,
            subtransforms,
        )
    }

    /// Applies `build` as the transform `name`, resolved in the innermost scope. `build` runs
    /// in the scope of the transform, without the graph lock. A primitive that `build` adds
    /// first takes the name; otherwise the scope becomes a composite that reads `inputs`.
    /// This is the only way a transform gets a node.
    pub(crate) fn apply_scoped<O: POutput>(
        &self,
        name: &str,
        inputs: HashMap<String, String>,
        build: impl FnOnce() -> O,
    ) -> O {
        match self.try_apply_scoped(name, inputs, || Ok::<O, std::convert::Infallible>(build())) {
            Ok(output) => output,
        }
    }

    /// [`apply_scoped`](Self::apply_scoped) for a `build` that can fail. After an error, the
    /// scope gets no node.
    pub(crate) fn try_apply_scoped<O: POutput, E>(
        &self,
        name: &str,
        inputs: HashMap<String, String>,
        build: impl FnOnce() -> Result<O, E>,
    ) -> Result<O, E> {
        {
            let mut inner = self.lock();
            let resolved = inner.unique_transform_name(name);
            inner.push_name_scope(resolved);
        }
        let mut guard = NameScopeGuard {
            pipeline: self,
            open: true,
        };
        let output = build();
        guard.open = false;
        let mut inner = self.lock();
        let scope = inner
            .pop_name_scope()
            .expect("the scope that this call opened is still open");
        if let Ok(output) = &output {
            inner.close_name_scope(scope, inputs, output.pcollection_ids());
        }
        output
    }

    /// Applies one primitive as the transform `name`. `build` gets the resolved name, adds
    /// exactly one primitive with it, and returns the output.
    pub(crate) fn add_scoped_primitive<O: POutput>(
        &self,
        name: &str,
        build: impl FnOnce(String) -> O,
    ) -> O {
        self.apply_scoped(name, HashMap::new(), || build(self.primitive_name(name)))
    }

    /// The full name of the transform that is expanding, or `None` at the root.
    pub(crate) fn scope_name(&self) -> Option<String> {
        self.lock().scope_name().map(str::to_string)
    }

    /// Sets the spec of the composite node of the transform that is expanding, for a runner
    /// that replaces the composite. A primitive node ignores it.
    pub(crate) fn set_scope_spec(&self, spec: proto::FunctionSpec) {
        if let Some(scope) = self.lock().current_scope_mut() {
            scope.spec = Some(spec);
        }
    }

    /// Sets the display data of the node of the transform that is expanding.
    pub(crate) fn set_scope_display_data(&self, display_data: Vec<proto::DisplayData>) {
        if let Some(scope) = self.lock().current_scope_mut() {
            scope.display_data = display_data;
        }
    }

    /// Adds an annotation to the node of the transform that is expanding.
    pub(crate) fn set_scope_annotation(&self, key: &str, value: Vec<u8>) {
        if let Some(scope) = self.lock().current_scope_mut() {
            scope.annotations.insert(key.to_string(), value);
        }
    }

    /// Finds the ID of the primitive transform that produces `pcollection_id`.
    pub fn producer_transform_id(&self, pcollection_id: &str) -> Option<String> {
        self.lock().producer_transform_id(pcollection_id)
    }

    /// Returns the name of a primitive that is about to be added. See
    /// [`PipelineInner::primitive_name`].
    pub(crate) fn primitive_name(&self, kind: &str) -> String {
        self.lock().primitive_name(kind)
    }

    /// Adds an impulse that takes the name of the current transform. Returns its id and output.
    pub(crate) fn add_impulse(&self) -> (String, PCollection<Vec<u8>>) {
        let name = self.primitive_name("Impulse");
        let bytes_coder_id = self.register_coder(crate::coders::URN_BYTES, Vec::new());
        let out_pcoll = self.add_pcollection::<Vec<u8>>(
            &format!("{name}_out"),
            &bytes_coder_id,
            IsBounded::Bounded,
        );

        let outputs = HashMap::from([("out".to_string(), out_pcoll.id().to_string())]);

        let transform_id =
            self.add_transform(&name, URN_IMPULSE, Vec::new(), HashMap::new(), outputs);

        (transform_id, out_pcoll)
    }

    /// The beginning of this pipeline, the input to any root transform.
    pub fn begin(&self) -> PBegin {
        PBegin::new(self.clone())
    }

    /// Validates graph consistency before execution or submission to a runner.
    pub fn validate(&self) -> Result<(), PipelineError> {
        self.lock().validate()
    }

    /// Records an error of a transform's `expand`, which cannot return it. Then
    /// [`validate`](Self::validate) fails, so the pipeline is not submitted.
    pub fn record_construction_error(&self, error: PipelineError) {
        self.lock().construction_errors.push(error);
    }

    /// Exports the pipeline as a Runner API `Pipeline` proto.
    pub fn to_proto(&self) -> proto::Pipeline {
        self.lock().to_proto()
    }

    /// The unique names of the transforms that use the rebuild path, in name order: closures
    /// and unregistered function types. The worker takes their code from the pipeline that it
    /// builds again, so a value that differs between the launcher and the worker changes
    /// them. A registered type travels in the graph with the values of the launcher; see
    /// `#[derive(Register)]` and `beam::register!`.
    pub fn rebuild_path_transforms(&self) -> Vec<String> {
        use prost::Message;
        let components = self.to_proto().components.unwrap_or_default();
        let mut names: Vec<String> = components
            .transforms
            .values()
            .filter(|transform| transform.subtransforms.is_empty())
            .filter(|transform| {
                let Some(spec) = &transform.spec else {
                    return false;
                };
                match spec.urn.as_str() {
                    URN_PAR_DO => proto::ParDoPayload::decode(spec.payload.as_slice())
                        .ok()
                        .and_then(|payload| payload.do_fn)
                        .is_some_and(|do_fn| do_fn.urn == URN_RUST_DOFN),
                    URN_COMBINE_GROUPED_VALUES => {
                        proto::CombinePayload::decode(spec.payload.as_slice())
                            .ok()
                            .and_then(|payload| payload.combine_fn)
                            .is_some_and(|combine_fn| combine_fn.urn == URN_RUST_COMBINE_FN)
                    }
                    _ => false,
                }
            })
            .map(|transform| transform.unique_name.clone())
            .collect();
        names.sort();
        names
    }

    /// Logs the transforms that use the rebuild path, if any.
    pub(crate) fn log_rebuild_path(&self) {
        let names = self.rebuild_path_transforms();
        if !names.is_empty() {
            tracing::info!(
                transforms = %names.join(", "),
                "{} transforms use the rebuild path: the worker builds the pipeline again to \
                 find their code. Closures always do; for a named function type, derive \
                 Register or use beam::register!",
                names.len()
            );
        }
    }

    /// Applies the root transform `transform`, such as a source, under the name `name`.
    pub fn apply<Tform>(&self, name: impl Into<String>, transform: Tform) -> Tform::Output
    where
        Tform: PTransform<PBegin>,
    {
        self.begin().apply(name, transform)
    }

    /// Applies a root transform whose expansion can fail, under the name `name`.
    pub fn try_apply<Tform>(
        &self,
        name: impl Into<String>,
        transform: Tform,
    ) -> Result<Tform::Output, Tform::Error>
    where
        Tform: TryPTransform<PBegin>,
    {
        self.begin().try_apply(name, transform)
    }

    /// Registers the executable handler under `key`, replacing any existing one.
    ///
    /// The harness derives `key` from the transform payload. For a `ParDo`, `key` is the
    /// `do_fn` payload: the unique name that
    /// [`ParDoRegistration`](crate::internals::ParDoRegistration) sets. Lookup is exact.
    /// The worker rebuilds the same graph, so a miss is a graph-construction bug.
    pub(crate) fn register_transform_handler(&self, key: impl Into<String>, handler: TransformFn) {
        self.lock().transform_handlers.insert(key.into(), handler);
    }

    /// Returns a map of registered transform handlers.
    pub(crate) fn transform_handlers(&self) -> HashMap<String, TransformFn> {
        self.lock().transform_handlers.clone()
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
            role_urn: URN_RUST_ARTIFACT_ROLE_WORKER_BINARY.to_string(),
            role_payload: Vec::new(),
        };

        let mut inner = self.lock();
        let default_id = inner.default_environment_id.clone();
        for (id, env) in inner.components.environments.iter_mut() {
            if id == &default_id || is_rust_environment(env) {
                // Replace, do not append, so a second run does not leave two binaries.
                env.dependencies
                    .retain(|dep| dep.role_urn != URN_RUST_ARTIFACT_ROLE_WORKER_BINARY);
                env.dependencies.push(dependency.clone());
            }
        }
    }

    /// Runners use this id to tell the Rust environment apart from cross-language
    /// environments, which must stay bound to their original transforms.
    pub fn default_environment_id(&self) -> String {
        self.lock().default_environment_id.clone()
    }

    /// Returns the default environment proto for this pipeline.
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

    /// Executes the pipeline with an explicitly constructed runner.
    pub async fn run_with_runner<R: PipelineRunner + ?Sized>(
        &self,
        runner: &R,
    ) -> Result<PipelineResult, crate::runners::RunnerError> {
        self.validate()?;
        self.log_rebuild_path();
        runner.run(self).await
    }

    /// Registers a standard Beam Row coder (`beam:coder:row:v1`) that carries `schema`.
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

/// Closes the innermost scope if `build` panics, so the scope does not stay open.
struct NameScopeGuard<'p> {
    pipeline: &'p Pipeline,
    open: bool,
}

impl Drop for NameScopeGuard<'_> {
    fn drop(&mut self) {
        if self.open {
            self.pipeline.lock().pop_name_scope();
        }
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
