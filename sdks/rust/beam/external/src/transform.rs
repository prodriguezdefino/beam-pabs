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

//! External cross-language transform definitions and `PTransform` implementations.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt::Write as _;
use std::sync::Arc;

use beam::internals::{PipelineInner, PipelineInternals};
use beam::pipeline::{ExpansionMode, Pipeline, UNEXPANDED_PLACEHOLDER_ID};
use beam::schema::Row;
use beam::transforms::{PTransform, TryPTransform};
use beam::values::{IsBounded, PBegin, PCollection, PDone, POutput};
use model::expansion::ExpansionRequest;
use model::pipeline as proto;

use beam::pipeline::URN_EXPANSION_SCHEMA_TRANSFORM;

use super::client::ExpansionClient;
use super::error::ExpansionError;
use super::payload::encode_schema_transform_payload;
use super::splicing::{extract_input_components, splice_expansion_response};

/// Configuration and builder for a transform executed by an expansion service.
#[derive(Clone, Debug)]
pub struct ExternalTransform {
    urn: String,
    payload: Vec<u8>,
    endpoint: String,
    namespace: Option<String>,
    inputs: HashMap<String, String>,
    output_coder_requests: HashMap<String, String>,
    output_tags: BTreeSet<String>,
}

impl ExternalTransform {
    /// Creates a new external transform.
    pub fn new(urn: impl Into<String>, endpoint: impl Into<String>, payload: Vec<u8>) -> Self {
        Self {
            urn: urn.into(),
            payload,
            endpoint: endpoint.into(),
            namespace: None,
            inputs: HashMap::new(),
            output_coder_requests: HashMap::new(),
            output_tags: BTreeSet::new(),
        }
    }

    /// Targets a SchemaTransform provider on the expansion service.
    pub fn schema_transform(
        identifier: impl Into<String>,
        endpoint: impl Into<String>,
        config_row: &Row,
    ) -> Result<Self, ExpansionError> {
        let payload = encode_schema_transform_payload(identifier, config_row)?;
        Ok(Self::new(URN_EXPANSION_SCHEMA_TRANSFORM, endpoint, payload))
    }

    /// Sets the component namespace prefix.
    pub fn with_namespace(mut self, namespace: impl Into<String>) -> Self {
        self.namespace = Some(namespace.into());
        self
    }

    /// Adds an input collection with the given tag.
    pub fn with_input(mut self, tag: impl Into<String>, pcoll_id: impl Into<String>) -> Self {
        self.inputs.insert(tag.into(), pcoll_id.into());
        self
    }

    /// Requests a specific coder for an output tag.
    pub fn with_output_coder_request(
        mut self,
        tag: impl Into<String>,
        coder_id: impl Into<String>,
    ) -> Self {
        self.output_coder_requests
            .insert(tag.into(), coder_id.into());
        self
    }

    /// Returns the transform URN.
    pub fn urn(&self) -> &str {
        &self.urn
    }

    /// Returns the raw payload bytes.
    pub fn payload(&self) -> &[u8] {
        &self.payload
    }

    /// Returns the expansion service endpoint.
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// Prefix of the component IDs that the expansion creates. By default it comes from `name`,
    /// so the driver and the workers derive the same one.
    pub fn namespace(&self) -> Option<&str> {
        self.namespace.as_deref()
    }

    /// Returns the map of input tags to collection identifiers.
    pub fn inputs(&self) -> &HashMap<String, String> {
        &self.inputs
    }

    /// Returns the map of output tags to requested coder identifiers.
    pub fn output_coder_requests(&self) -> &HashMap<String, String> {
        &self.output_coder_requests
    }

    /// Declared output tags. Placeholder expansion (in a worker) cannot ask a service which
    /// outputs exist, so declare every consumed tag. Remote expansion checks them.
    pub fn output_tags(&self) -> &BTreeSet<String> {
        &self.output_tags
    }

    /// Adds declared output tags. Placeholder expansion creates a PCollection for each one;
    /// remote expansion fails if the service does not return one.
    pub fn with_output_tags<I, S>(mut self, tags: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.output_tags.extend(tags.into_iter().map(Into::into));
        self
    }

    /// Unexpanded transform proto sent to the expansion service.
    fn to_proto(&self, unique_name: &str, outputs: HashMap<String, String>) -> proto::PTransform {
        proto::PTransform {
            unique_name: unique_name.to_string(),
            spec: Some(proto::FunctionSpec {
                urn: self.urn.clone(),
                payload: self.payload.clone(),
            }),
            subtransforms: Vec::new(),
            inputs: self.inputs.clone(),
            outputs,
            display_data: Vec::new(),
            environment_id: String::new(),
            annotations: HashMap::new(),
        }
    }

    /// Expands into `pipeline` per [`ExpansionMode`]. Returns the root transform ID and proto.
    pub fn expand_with_pipeline(
        &self,
        pipeline: &Pipeline,
    ) -> Result<(String, proto::PTransform), ExpansionError> {
        // The driver and the workers apply transforms in the same order, so both get the same
        // unique name, and from it the same namespace. The expanded root takes the name of the
        // transform that is expanding.
        let unique_name = pipeline.primitive_name("External");
        let namespace = self
            .namespace
            .clone()
            .unwrap_or_else(|| namespace_for(&unique_name));

        let mode = pipeline.lock().expansion_mode;
        match mode {
            ExpansionMode::Placeholder => {
                let mut lock = pipeline.lock();
                Ok(self.expand_as_placeholder(&mut lock, unique_name, namespace))
            }
            ExpansionMode::Remote => self.expand_remotely(pipeline, &unique_name, namespace),
        }
    }

    /// Records the transform and synthetic outputs without a service, for Fn API workers: the
    /// runner supplies the graph and each `ProcessBundleDescriptor` the real coders, so outputs
    /// use a placeholder coder ID.
    ///
    /// The transform has the ID that remote expansion gives it, its unique name. The output tags
    /// are `"output"`, the main output tag of the services, and the declared tags, which remote
    /// expansion checks against the service outputs. `"output"` and coder-request tags share
    /// `{namespace}_out`; each other tag gets `{namespace}_out_{tag}`.
    fn expand_as_placeholder(
        &self,
        lock: &mut PipelineInner,
        root_id: String,
        namespace: String,
    ) -> (String, proto::PTransform) {
        let main_id = format!("{namespace}_out");
        let shared: BTreeSet<String> = std::iter::once("output".to_string())
            .chain(self.output_coder_requests.keys().cloned())
            .collect();
        let outputs: HashMap<String, String> = shared
            .iter()
            .map(|tag| (tag.clone(), main_id.clone()))
            .chain(
                self.output_tags
                    .iter()
                    .filter(|tag| !shared.contains(*tag))
                    .map(|tag| (tag.clone(), format!("{main_id}_{tag}"))),
            )
            .collect();

        let transform = self.to_proto(&root_id, outputs.clone());
        lock.record_transform_name(&root_id, &transform);
        lock.components
            .transforms
            .insert(root_id.clone(), transform.clone());

        let main_name = format!("{namespace}.out");
        // Placeholder mode runs on workers, where the runner sends the real graph. The outputs
        // take the boundedness of the inputs.
        let is_bounded: IsBounded = self
            .inputs
            .values()
            .filter_map(|id| lock.components.pcollections.get(id))
            .map(|p| IsBounded::from(p.is_bounded()))
            .collect();
        outputs
            .into_values()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .for_each(|pcoll_id| {
                let placeholder = proto::PCollection {
                    unique_name: pcoll_id.replacen(&main_id, &main_name, 1),
                    coder_id: UNEXPANDED_PLACEHOLDER_ID.to_string(),
                    is_bounded: proto::is_bounded::Enum::from(is_bounded) as i32,
                    windowing_strategy_id: UNEXPANDED_PLACEHOLDER_ID.to_string(),
                    display_data: Vec::new(),
                };
                lock.components.pcollections.insert(pcoll_id, placeholder);
            });

        (root_id, transform)
    }

    /// Fails if the expansion service did not produce every declared output tag.
    fn verify_declared_outputs(&self, expanded: &proto::PTransform) -> Result<(), ExpansionError> {
        let missing: Vec<&str> = self
            .output_tags
            .iter()
            .filter(|tag| !expanded.outputs.contains_key(*tag))
            .map(String::as_str)
            .collect();
        if missing.is_empty() {
            return Ok(());
        }
        let available: BTreeSet<&str> = expanded.outputs.keys().map(String::as_str).collect();
        Err(ExpansionError::InvalidResponse(format!(
            "expansion of {} did not produce declared output tag(s) {missing:?}; \
             it produced {available:?}",
            self.urn
        )))
    }

    /// Calls the expansion service and splices the returned subgraph into the pipeline.
    fn expand_remotely(
        &self,
        pipeline: &Pipeline,
        unique_name: &str,
        namespace: String,
    ) -> Result<(String, proto::PTransform), ExpansionError> {
        // First lock scope: extract input components and get or create the expansion client.
        let (request, client) = {
            let mut lock = pipeline.lock();
            let input_ids: Vec<&str> = self.inputs.values().map(String::as_str).collect();
            let request = ExpansionRequest {
                components: Some(extract_input_components(&lock, &input_ids)),
                transform: Some(self.to_proto(unique_name, HashMap::new())),
                namespace,
                output_coder_requests: self.output_coder_requests.clone(),
                requirements: Vec::new(),
                pipeline_options: None,
            };

            let any_client = lock
                .expansion_clients
                .entry(self.endpoint.clone())
                .or_insert_with(|| Arc::new(ExpansionClient::new(&self.endpoint)))
                .clone();
            let client = any_client
                .downcast::<ExpansionClient>()
                .expect("expansion client of type ExpansionClient");
            (request, client)
        };

        // Network RPC: executed outside the pipeline lock.
        let response = client.expand_blocking(request)?;
        let local_jar = client.local_jar_path();

        if !response.error.is_empty() {
            return Err(ExpansionError::ExpansionFailed(response.error));
        }
        let expanded_transform = response.transform.clone().ok_or_else(|| {
            ExpansionError::InvalidResponse("Missing transform in response".into())
        })?;
        // Verify outputs before splicing, so a mistyped declared tag leaves the pipeline unchanged.
        self.verify_declared_outputs(&expanded_transform)?;

        // Second lock scope: record artifacts and splice the response into the pipeline.
        let mut lock = pipeline.lock();
        if let Some(jar) = local_jar
            && !lock.xlang_artifacts.contains(&jar)
        {
            lock.xlang_artifacts.push(jar);
        }
        let root_id = splice_expansion_response(&mut lock, response)?;

        Ok((root_id, expanded_transform))
    }
}

/// Derives the expansion namespace from the unique transform name, so that the driver and each
/// worker build the same graph. Each byte that is not ASCII alphanumeric becomes `_` and two
/// hex digits, so distinct names give distinct namespaces.
fn namespace_for(unique_name: &str) -> String {
    unique_name
        .bytes()
        .fold(String::from("xlang_"), |mut namespace, byte| {
            if byte.is_ascii_alphanumeric() {
                namespace.push(char::from(byte));
            } else {
                write!(namespace, "_{byte:02x}").expect("writing to a String does not fail");
            }
            namespace
        })
}

/// Outputs of an expanded cross-language transform, keyed by output tag.
#[derive(Clone, Debug)]
pub struct ExternalOutputs {
    transform_name: String,
    pipeline: Pipeline,
    outputs: BTreeMap<String, PCollection<Row>>,
}

impl ExternalOutputs {
    /// Collects outputs from `expanded` and resolves each PCollection coder in `pipeline`.
    fn from_expanded(
        pipeline: &Pipeline,
        transform_name: &str,
        expanded: &proto::PTransform,
    ) -> Result<Self, ExpansionError> {
        let outputs = {
            let lock = pipeline.lock();
            expanded
                .outputs
                .iter()
                .map(|(tag, pcoll_id)| {
                    let coder_id = lock
                        .components
                        .pcollections
                        .get(pcoll_id)
                        .map(|p| p.coder_id.clone())
                        .ok_or_else(|| {
                            ExpansionError::ComponentNotFound(format!(
                                "Output PCollection '{pcoll_id}' (tag '{tag}') absent from pipeline components"
                            ))
                        })?;
                    Ok((
                        tag.clone(),
                        PCollection::new(pcoll_id.clone(), coder_id, pipeline.clone()),
                    ))
                })
                .collect::<Result<BTreeMap<_, _>, ExpansionError>>()?
        };
        Ok(Self {
            transform_name: transform_name.to_string(),
            pipeline: pipeline.clone(),
            outputs,
        })
    }

    pub fn get(&self, tag: &str) -> Option<PCollection<Row>> {
        self.outputs.get(tag).cloned()
    }

    /// Like [`get`](Self::get), but the error names the tags that exist.
    pub fn try_get(&self, tag: &str) -> Result<PCollection<Row>, ExpansionError> {
        self.get(tag).ok_or_else(|| {
            ExpansionError::InvalidResponse(format!(
                "'{}' has no output tagged '{tag}'; available: {:?}",
                self.transform_name,
                self.tags().collect::<Vec<_>>()
            ))
        })
    }

    /// Output tags, sorted.
    pub fn tags(&self) -> impl Iterator<Item = &str> {
        self.outputs.keys().map(String::as_str)
    }

    pub fn len(&self) -> usize {
        self.outputs.len()
    }

    pub fn is_empty(&self) -> bool {
        self.outputs.is_empty()
    }

    /// Consumes `self`, returning the outputs keyed by tag.
    pub fn into_map(self) -> BTreeMap<String, PCollection<Row>> {
        self.outputs
    }
}

impl POutput for ExternalOutputs {
    fn pipeline(&self) -> &Pipeline {
        &self.pipeline
    }

    /// The output tags of the expansion.
    fn pcollection_ids(&self) -> HashMap<String, String> {
        self.outputs
            .iter()
            .map(|(tag, pcoll)| (tag.clone(), pcoll.id().to_string()))
            .collect()
    }
}

/// Cross-language root transform (source) that outputs a [`PCollection<Row>`].
#[derive(Clone, Debug)]
pub struct ExternalSource {
    transform: ExternalTransform,
    main_output_tag: String,
}

impl ExternalSource {
    pub fn new(transform: ExternalTransform) -> Self {
        Self {
            transform,
            main_output_tag: "output".to_string(),
        }
    }

    pub fn with_output_tag(mut self, tag: impl Into<String>) -> Self {
        self.main_output_tag = tag.into();
        self
    }

    pub fn transform(&self) -> &ExternalTransform {
        &self.transform
    }

    pub fn main_output_tag(&self) -> &str {
        &self.main_output_tag
    }
}

impl ExternalSource {
    /// Expands and returns every output.
    fn expand_all(&self, input: &PBegin) -> Result<ExternalOutputs, ExpansionError> {
        let pipeline = input.pipeline();
        let (_root_id, expanded) = self.transform.expand_with_pipeline(pipeline)?;
        ExternalOutputs::from_expanded(pipeline, &expanded.unique_name, &expanded)
    }

    /// This source as a transform that yields all outputs. Declare consumed tags with
    /// [`ExternalTransform::with_output_tags`] so they exist when a worker rebuilds the
    /// pipeline without an expansion service.
    pub fn all_outputs(self) -> MultiOutputSource {
        MultiOutputSource(self)
    }
}

/// Also fails if the expansion yields no output.
impl TryPTransform<PBegin> for ExternalSource {
    type Error = ExpansionError;

    fn try_expand(&self, input: &PBegin) -> Result<PCollection<Row>, ExpansionError> {
        let outputs = self.expand_all(input)?;
        outputs
            .get(&self.main_output_tag)
            .or_else(|| outputs.outputs.values().next().cloned())
            .ok_or_else(|| {
                ExpansionError::InvalidResponse(format!(
                    "expansion of {} produced no output PCollections; expected one tagged '{}'",
                    self.transform.urn, self.main_output_tag
                ))
            })
    }
}

impl PTransform<PBegin> for ExternalSource {
    type Output = PCollection<Row>;

    /// # Panics
    ///
    /// Panics if the expansion fails. Use `try_apply` to get the error.
    fn expand(&self, input: &PBegin) -> Self::Output {
        self.try_expand(input).unwrap_or_else(|e| {
            panic!(
                "cross-language source {} failed to expand against {}: {e}",
                self.transform.urn, self.transform.endpoint
            )
        })
    }
}

/// External source that returns all outputs as [`ExternalOutputs`].
#[derive(Clone, Debug)]
pub struct MultiOutputSource(ExternalSource);

impl MultiOutputSource {
    pub fn source(&self) -> &ExternalSource {
        &self.0
    }
}

impl TryPTransform<PBegin> for MultiOutputSource {
    type Error = ExpansionError;

    fn try_expand(&self, input: &PBegin) -> Result<ExternalOutputs, ExpansionError> {
        self.0.expand_all(input)
    }
}

impl PTransform<PBegin> for MultiOutputSource {
    type Output = ExternalOutputs;

    /// # Panics
    ///
    /// Panics if the expansion fails. Use `try_apply` to get the error.
    fn expand(&self, input: &PBegin) -> Self::Output {
        let t = &self.0.transform;
        self.try_expand(input).unwrap_or_else(|e| {
            panic!(
                "cross-language source {} failed to expand against {}: {e}",
                t.urn, t.endpoint
            )
        })
    }
}

/// Cross-language sink that consumes a [`PCollection<Row>`] and returns [`PDone`].
#[derive(Clone, Debug)]
pub struct ExternalSink {
    transform: ExternalTransform,
    input_tag: String,
}

impl ExternalSink {
    pub fn new(transform: ExternalTransform) -> Self {
        Self {
            transform,
            input_tag: "input".to_string(),
        }
    }

    pub fn with_input_tag(mut self, tag: impl Into<String>) -> Self {
        self.input_tag = tag.into();
        self
    }

    pub fn transform(&self) -> &ExternalTransform {
        &self.transform
    }

    pub fn input_tag(&self) -> &str {
        &self.input_tag
    }

    /// Expands and returns every output.
    fn expand_all(&self, input: &PCollection<Row>) -> Result<ExternalOutputs, ExpansionError> {
        let pipeline = input.pipeline();
        let t = self
            .transform
            .clone()
            .with_input(&self.input_tag, input.id());

        let (_root_id, expanded) = t.expand_with_pipeline(pipeline)?;
        ExternalOutputs::from_expanded(pipeline, &expanded.unique_name, &expanded)
    }

    /// This sink as a transform that yields its outputs, not [`PDone`], such as Iceberg
    /// `snapshots` or an error output. Declare consumed tags with
    /// [`ExternalTransform::with_output_tags`]; see [`ExternalSource::all_outputs`].
    pub fn all_outputs(self) -> MultiOutputSink {
        MultiOutputSink(self)
    }
}

impl TryPTransform<PCollection<Row>> for ExternalSink {
    type Error = ExpansionError;

    fn try_expand(&self, input: &PCollection<Row>) -> Result<PDone, ExpansionError> {
        self.expand_all(input)
            .map(|outputs| PDone::new(outputs.pipeline))
    }
}

impl PTransform<PCollection<Row>> for ExternalSink {
    type Output = PDone;

    /// # Panics
    ///
    /// Panics if the expansion fails. Use `try_apply` to get the error.
    fn expand(&self, input: &PCollection<Row>) -> Self::Output {
        self.try_expand(input).unwrap_or_else(|e| {
            panic!(
                "cross-language sink {} failed to expand against {}: {e}",
                self.transform.urn, self.transform.endpoint
            )
        })
    }
}

/// External sink that returns its outputs as [`ExternalOutputs`].
#[derive(Clone, Debug)]
pub struct MultiOutputSink(ExternalSink);

impl MultiOutputSink {
    pub fn sink(&self) -> &ExternalSink {
        &self.0
    }
}

impl TryPTransform<PCollection<Row>> for MultiOutputSink {
    type Error = ExpansionError;

    fn try_expand(&self, input: &PCollection<Row>) -> Result<ExternalOutputs, ExpansionError> {
        self.0.expand_all(input)
    }
}

impl PTransform<PCollection<Row>> for MultiOutputSink {
    type Output = ExternalOutputs;

    /// # Panics
    ///
    /// Panics if the expansion fails. Use `try_apply` to get the error.
    fn expand(&self, input: &PCollection<Row>) -> Self::Output {
        let t = &self.0.transform;
        self.try_expand(input).unwrap_or_else(|e| {
            panic!(
                "cross-language sink {} failed to expand against {}: {e}",
                t.urn, t.endpoint
            )
        })
    }
}
