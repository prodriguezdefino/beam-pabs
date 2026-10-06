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
use std::sync::Arc;

use beam::pipeline::{Pipeline, PipelineInner, next_id};
use beam::schema::Row;
use beam::transforms::PTransform;
use beam::values::{PBegin, PCollection, PDone, POutput};
use model::expansion::ExpansionRequest;
use model::pipeline as proto;

use super::ExpansionMode;
use super::client::ExpansionClient;
use super::error::ExpansionError;
use super::payload::{URN_EXPANSION_SCHEMA_TRANSFORM, encode_schema_transform_payload};
use super::splicing::{extract_input_components, splice_expansion_response};

/// Configuration and builder for a transform executed by an expansion service.
#[derive(Clone, Debug)]
pub struct ExternalTransform {
    pub name: String,
    pub urn: String,
    pub payload: Vec<u8>,
    pub endpoint: String,
    pub namespace: Option<String>,
    pub inputs: HashMap<String, String>,
    pub output_coder_requests: HashMap<String, String>,
    /// Declared output tags. Placeholder expansion (in a worker) cannot ask a service which
    /// outputs exist, so declare every consumed tag. Remote expansion checks them.
    pub output_tags: BTreeSet<String>,
}

impl ExternalTransform {
    pub fn new(
        name: impl Into<String>,
        urn: impl Into<String>,
        endpoint: impl Into<String>,
        payload: Vec<u8>,
    ) -> Self {
        Self {
            name: name.into(),
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
        name: impl Into<String>,
        identifier: impl Into<String>,
        endpoint: impl Into<String>,
        config_row: &Row,
    ) -> Result<Self, ExpansionError> {
        let payload = encode_schema_transform_payload(identifier, config_row)?;
        Ok(Self::new(
            name,
            URN_EXPANSION_SCHEMA_TRANSFORM,
            endpoint,
            payload,
        ))
    }

    pub fn with_namespace(mut self, namespace: impl Into<String>) -> Self {
        self.namespace = Some(namespace.into());
        self
    }

    pub fn with_input(mut self, tag: impl Into<String>, pcoll_id: impl Into<String>) -> Self {
        self.inputs.insert(tag.into(), pcoll_id.into());
        self
    }

    pub fn with_output_coder_request(
        mut self,
        tag: impl Into<String>,
        coder_id: impl Into<String>,
    ) -> Self {
        self.output_coder_requests
            .insert(tag.into(), coder_id.into());
        self
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
    fn to_proto(&self, outputs: HashMap<String, String>) -> proto::PTransform {
        proto::PTransform {
            unique_name: self.name.clone(),
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
        let mut lock = pipeline.lock();
        let namespace = self
            .namespace
            .clone()
            .unwrap_or_else(|| next_id("xlang_ns"));

        match lock.expansion_mode {
            ExpansionMode::Placeholder => Ok(self.expand_as_placeholder(&mut lock, namespace)),
            ExpansionMode::Remote => self.expand_remotely(&mut lock, namespace),
        }
    }

    /// Records the transform and synthetic outputs without a service, for Fn API workers: the
    /// runner supplies the graph and each `ProcessBundleDescriptor` the real coders, so outputs
    /// use a placeholder coder ID. `"output"` and coder-request tags share `{root}_out`; each
    /// other tag gets `{root}_out_{tag}`.
    fn expand_as_placeholder(
        &self,
        lock: &mut PipelineInner,
        root_id: String,
    ) -> (String, proto::PTransform) {
        let main_id = format!("{root_id}_out");
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

        let transform = self.to_proto(outputs.clone());
        lock.components
            .transforms
            .insert(root_id.clone(), transform.clone());

        let main_name = format!("{root_id}.out");
        outputs
            .into_values()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .for_each(|pcoll_id| {
                let placeholder = proto::PCollection {
                    unique_name: pcoll_id.replacen(&main_id, &main_name, 1),
                    coder_id: super::UNEXPANDED_PLACEHOLDER_ID.to_string(),
                    is_bounded: proto::is_bounded::Enum::Bounded as i32,
                    windowing_strategy_id: super::UNEXPANDED_PLACEHOLDER_ID.to_string(),
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
            "expansion of '{}' ({}) did not produce declared output tag(s) {missing:?}; \
             it produced {available:?}",
            self.name, self.urn
        )))
    }

    /// Calls the expansion service and splices the returned subgraph into the pipeline.
    fn expand_remotely(
        &self,
        lock: &mut PipelineInner,
        namespace: String,
    ) -> Result<(String, proto::PTransform), ExpansionError> {
        let input_ids: Vec<&str> = self.inputs.values().map(String::as_str).collect();
        let request = ExpansionRequest {
            components: Some(extract_input_components(lock, &input_ids)),
            transform: Some(self.to_proto(HashMap::new())),
            namespace,
            output_coder_requests: self.output_coder_requests.clone(),
            requirements: Vec::new(),
            pipeline_options: None,
        };

        // Share one auto-started service per target, so each transform skips a JVM cold start.
        let (response, local_jar) = {
            let any_client = lock
                .expansion_clients
                .entry(self.endpoint.clone())
                .or_insert_with(|| Arc::new(ExpansionClient::new(&self.endpoint)))
                .clone();
            let client = any_client
                .downcast::<ExpansionClient>()
                .expect("expansion client of type ExpansionClient");
            let response = client.expand_blocking(request)?;
            (response, client.local_jar_path())
        };

        // The runner must ship the auto-started service JAR to its workers.
        if let Some(jar) = local_jar
            && !lock.xlang_artifacts.contains(&jar)
        {
            lock.xlang_artifacts.push(jar);
        }

        if !response.error.is_empty() {
            return Err(ExpansionError::ExpansionFailed(response.error));
        }
        let expanded_transform = response.transform.clone().ok_or_else(|| {
            ExpansionError::InvalidResponse("Missing transform in response".into())
        })?;
        // Verify outputs before splicing, so a mistyped declared tag leaves the pipeline unchanged.
        self.verify_declared_outputs(&expanded_transform)?;
        let root_id = splice_expansion_response(lock, response)?;

        Ok((root_id, expanded_transform))
    }
}

/// Outputs of an expanded cross-language transform, keyed by output tag.
#[derive(Clone, Debug)]
pub struct ExternalOutputs {
    transform_name: String,
    pipeline: Pipeline,
    outputs: BTreeMap<String, PCollection<Row>>,
}

impl ExternalOutputs {
    /// Collects outputs from `expanded`, resolving each PCollection coder in `pipeline`.
    fn from_expanded(
        pipeline: &Pipeline,
        transform_name: &str,
        expanded: &proto::PTransform,
    ) -> Self {
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
                        .unwrap_or_default();
                    (
                        tag.clone(),
                        PCollection::new(pcoll_id.clone(), coder_id, pipeline.clone()),
                    )
                })
                .collect()
        };
        Self {
            transform_name: transform_name.to_string(),
            pipeline: pipeline.clone(),
            outputs,
        }
    }

    pub fn get(&self, tag: &str) -> Option<PCollection<Row>> {
        self.outputs.get(tag).cloned()
    }

    /// Like [`get`](Self::get), but the error names the tags that exist.
    pub fn expect(&self, tag: &str) -> Result<PCollection<Row>, ExpansionError> {
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
    /// Fallible [`PTransform::expand`], which panics on failure because its signature is shared
    /// by all transforms. Also fails if expansion yields no output.
    pub fn try_expand(&self, input: &PBegin) -> Result<PCollection<Row>, ExpansionError> {
        let outputs = self.try_expand_all(input)?;
        outputs
            .get(&self.main_output_tag)
            .or_else(|| outputs.outputs.values().next().cloned())
            .ok_or_else(|| {
                ExpansionError::InvalidResponse(format!(
                    "expansion of '{}' ({}) produced no output PCollections; \
                     expected one tagged '{}'",
                    self.transform.name, self.transform.urn, self.main_output_tag
                ))
            })
    }

    /// Returns every output. Declare consumed tags with [`ExternalTransform::with_output_tags`]
    /// so they exist when a worker rebuilds the pipeline without an expansion service.
    pub fn try_expand_all(&self, input: &PBegin) -> Result<ExternalOutputs, ExpansionError> {
        let pipeline = input.pipeline();
        let (_root_id, expanded) = self.transform.expand_with_pipeline(pipeline)?;
        Ok(ExternalOutputs::from_expanded(
            pipeline,
            &self.transform.name,
            &expanded,
        ))
    }

    /// This source as a transform that yields all outputs.
    pub fn all_outputs(self) -> MultiOutputSource {
        MultiOutputSource(self)
    }
}

impl PTransform<PBegin> for ExternalSource {
    type Output = PCollection<Row>;

    fn expand(&self, input: &PBegin) -> Self::Output {
        self.try_expand(input).unwrap_or_else(|e| {
            panic!(
                "cross-language source '{}' ({}) failed to expand against {}: {e}",
                self.transform.name, self.transform.urn, self.transform.endpoint
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

impl PTransform<PBegin> for MultiOutputSource {
    type Output = ExternalOutputs;

    fn expand(&self, input: &PBegin) -> Self::Output {
        let t = &self.0.transform;
        self.0.try_expand_all(input).unwrap_or_else(|e| {
            panic!(
                "cross-language source '{}' ({}) failed to expand against {}: {e}",
                t.name, t.urn, t.endpoint
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

    /// Fallible [`PTransform::expand`]; see [`ExternalSource::try_expand`].
    pub fn try_expand(&self, input: &PCollection<Row>) -> Result<PDone, ExpansionError> {
        self.try_expand_all(input)
            .map(|outputs| PDone::new(outputs.pipeline))
    }

    /// Returns every output, such as Iceberg `snapshots` or an error output. Declare consumed
    /// tags with [`ExternalTransform::with_output_tags`]; see [`ExternalSource::try_expand_all`].
    pub fn try_expand_all(
        &self,
        input: &PCollection<Row>,
    ) -> Result<ExternalOutputs, ExpansionError> {
        let pipeline = input.pipeline();
        let t = self
            .transform
            .clone()
            .with_input(&self.input_tag, input.id());

        let (_root_id, expanded) = t.expand_with_pipeline(pipeline)?;
        Ok(ExternalOutputs::from_expanded(pipeline, &t.name, &expanded))
    }

    /// This sink as a transform that yields its outputs, not [`PDone`].
    pub fn all_outputs(self) -> MultiOutputSink {
        MultiOutputSink(self)
    }
}

impl PTransform<PCollection<Row>> for ExternalSink {
    type Output = PDone;

    fn expand(&self, input: &PCollection<Row>) -> Self::Output {
        self.try_expand(input).unwrap_or_else(|e| {
            panic!(
                "cross-language sink '{}' ({}) failed to expand against {}: {e}",
                self.transform.name, self.transform.urn, self.transform.endpoint
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

impl PTransform<PCollection<Row>> for MultiOutputSink {
    type Output = ExternalOutputs;

    fn expand(&self, input: &PCollection<Row>) -> Self::Output {
        let t = &self.0.transform;
        self.0.try_expand_all(input).unwrap_or_else(|e| {
            panic!(
                "cross-language sink '{}' ({}) failed to expand against {}: {e}",
                t.name, t.urn, t.endpoint
            )
        })
    }
}
