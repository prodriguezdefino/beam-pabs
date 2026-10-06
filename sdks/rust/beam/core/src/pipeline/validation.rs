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

//! Structural and semantic validation for Apache Beam pipelines.

use model::pipeline as proto;
use prost::Message;
use std::collections::HashMap;

use super::{PipelineError, UNEXPANDED_PLACEHOLDER_ID, URN_COMBINE_PER_KEY, URN_PAR_DO, roots};
use crate::values::SideInputWindowing;

/// Checks that the pipeline DAG and components are consistent. Returns the first error found.
pub fn validate(
    components: &proto::Components,
    transform_order: &[String],
) -> Result<(), PipelineError> {
    validate_roots(components, transform_order)
        .and_then(|()| validate_transforms(components))
        .and_then(|()| validate_pcollections(components))
}

/// Checks that the root transforms exist and that at least one is a source (has no inputs).
fn validate_roots(
    components: &proto::Components,
    transform_order: &[String],
) -> Result<(), PipelineError> {
    let roots = roots::compute_root_transform_ids(components, transform_order);

    if roots.is_empty() && !components.transforms.is_empty() {
        return Err(PipelineError::InvalidRootTransform {
            transform_id: "none".to_string(),
            reason: "Pipeline has transforms but no root transforms could be determined"
                .to_string(),
        });
    }

    if let Some(missing_id) = roots
        .iter()
        .find(|&root_id| !components.transforms.contains_key(root_id))
    {
        return Err(PipelineError::InvalidRootTransform {
            transform_id: missing_id.clone(),
            reason: "Root transform not found in transforms map".to_string(),
        });
    }

    let has_source = roots.iter().any(|root_id| {
        components
            .transforms
            .get(root_id)
            .is_some_and(|t| t.inputs.is_empty())
    });

    if !roots.is_empty() && !has_source {
        Err(PipelineError::InvalidRootTransform {
            transform_id: roots.first().cloned().unwrap_or_default(),
            reason:
                "Pipeline must have at least one source transform with no inputs (e.g. Impulse)"
                    .to_string(),
        })
    } else {
        Ok(())
    }
}

/// Checks that each transform refers to existing PCollections and subtransforms, and has a
/// valid spec and environment.
fn validate_transforms(components: &proto::Components) -> Result<(), PipelineError> {
    components
        .transforms
        .iter()
        .try_for_each(|(t_id, transform)| validate_transform(components, t_id, transform))
}

fn validate_transform(
    components: &proto::Components,
    id: &str,
    transform: &proto::PTransform,
) -> Result<(), PipelineError> {
    validate_subtransforms(components, id, &transform.subtransforms)
        .and_then(|()| validate_spec(components, id, transform.spec.as_ref()))
        .and_then(|()| validate_side_inputs(components, id, transform))
        .and_then(|()| validate_source_primitive(id, transform))
        .and_then(|()| {
            validate_pcollections_map(components, id, &transform.inputs, |t, tag, p| {
                PipelineError::MissingInputPCollection {
                    transform_id: t,
                    input_tag: tag,
                    pcollection_id: p,
                }
            })
        })
        .and_then(|()| {
            validate_pcollections_map(components, id, &transform.outputs, |t, tag, p| {
                PipelineError::MissingOutputPCollection {
                    transform_id: t,
                    output_tag: tag,
                    pcollection_id: p,
                }
            })
        })
        .and_then(|()| validate_environment(components, id, &transform.environment_id))
}

/// Checks that every ParDo side input has a window mapping. A Rust view leaves it out when
/// the side PCollection's windowing is unsupported, so the reason is derived again here.
fn validate_side_inputs(
    components: &proto::Components,
    id: &str,
    transform: &proto::PTransform,
) -> Result<(), PipelineError> {
    // A malformed ParDo payload is left to the runner, as before this check.
    let Some(payload) = transform
        .spec
        .as_ref()
        .filter(|spec| spec.urn == URN_PAR_DO)
        .and_then(|spec| proto::ParDoPayload::decode(spec.payload.as_slice()).ok())
    else {
        return Ok(());
    };
    payload
        .side_inputs
        .iter()
        .filter(|(_, side)| side.window_mapping_fn.is_none())
        .try_for_each(|(tag, _)| {
            let reason = transform
                .inputs
                .get(tag)
                .and_then(|pc| components.pcollections.get(pc))
                .and_then(|pc| {
                    components
                        .windowing_strategies
                        .get(&pc.windowing_strategy_id)
                })
                .and_then(|ws| ws.window_fn.as_ref())
                .and_then(|window_fn| SideInputWindowing::of(window_fn).err())
                .unwrap_or_else(|| "it has no window mapping function".to_string());
            Err(PipelineError::UnsupportedSideInput {
                transform_id: id.to_string(),
                input_tag: tag.clone(),
                reason,
            })
        })
}

/// Checks that a primitive leaf transform with no inputs is Impulse or TestStream.
fn validate_source_primitive(id: &str, transform: &proto::PTransform) -> Result<(), PipelineError> {
    if transform.subtransforms.is_empty() && transform.inputs.is_empty() {
        let urn = transform
            .spec
            .as_ref()
            .map(|s| s.urn.as_str())
            .unwrap_or("");
        if urn != crate::pipeline::URN_IMPULSE && urn != crate::pipeline::URN_TEST_STREAM {
            return Err(PipelineError::InvalidSourceTransform {
                transform_id: id.to_string(),
                urn: urn.to_string(),
            });
        }
    }
    Ok(())
}

fn validate_subtransforms(
    components: &proto::Components,
    transform_id: &str,
    subtransforms: &[String],
) -> Result<(), PipelineError> {
    if let Some(missing_sub_id) = subtransforms
        .iter()
        .find(|&sub_id| !components.transforms.contains_key(sub_id))
    {
        return Err(PipelineError::MissingSubtransform {
            transform_id: transform_id.to_string(),
            subtransform_id: missing_sub_id.clone(),
        });
    }
    Ok(())
}

fn validate_spec(
    components: &proto::Components,
    transform_id: &str,
    spec: Option<&proto::FunctionSpec>,
) -> Result<(), PipelineError> {
    match spec {
        Some(proto::FunctionSpec { urn, payload }) if urn == URN_COMBINE_PER_KEY => {
            validate_combine_payload(components, transform_id, payload)
        }
        _ => Ok(()),
    }
}

fn validate_combine_payload(
    components: &proto::Components,
    transform_id: &str,
    payload: &[u8],
) -> Result<(), PipelineError> {
    match proto::CombinePayload::decode(payload) {
        Ok(combine)
            if !components
                .coders
                .contains_key(&combine.accumulator_coder_id) =>
        {
            Err(PipelineError::MissingAccumulatorCoder {
                transform_id: transform_id.to_string(),
                coder_id: combine.accumulator_coder_id,
            })
        }
        Ok(_) => Ok(()),
        Err(e) => Err(PipelineError::InvalidCombinePayload {
            transform_id: transform_id.to_string(),
            reason: e.to_string(),
        }),
    }
}

fn validate_pcollections_map<F>(
    components: &proto::Components,
    transform_id: &str,
    pcollections: &HashMap<String, String>,
    error_ctor: F,
) -> Result<(), PipelineError>
where
    F: Fn(String, String, String) -> PipelineError,
{
    if let Some((tag, pcoll_id)) = pcollections
        .iter()
        .find(|(_, pcoll_id)| !components.pcollections.contains_key(pcoll_id.as_str()))
    {
        return Err(error_ctor(
            transform_id.to_string(),
            tag.clone(),
            pcoll_id.clone(),
        ));
    }
    Ok(())
}

fn validate_environment(
    components: &proto::Components,
    transform_id: &str,
    environment_id: &str,
) -> Result<(), PipelineError> {
    match environment_id {
        "" => Ok(()),
        env_id if components.environments.contains_key(env_id) => Ok(()),
        env_id => Err(PipelineError::MissingEnvironment {
            transform_id: transform_id.to_string(),
            environment_id: env_id.to_string(),
        }),
    }
}

/// Checks that each PCollection refers to an existing coder and windowing strategy, and has
/// at least one producer transform.
fn validate_pcollections(components: &proto::Components) -> Result<(), PipelineError> {
    components
        .pcollections
        .iter()
        .try_for_each(|(p_id, pcoll)| {
            // A placeholder marks a cross-language output that is not expanded. Report the
            // missing expansion, not a missing coder, because that is the cause.
            if pcoll.coder_id == UNEXPANDED_PLACEHOLDER_ID
                || pcoll.windowing_strategy_id == UNEXPANDED_PLACEHOLDER_ID
            {
                return Err(PipelineError::UnexpandedCrossLanguageOutput {
                    pcollection_id: p_id.clone(),
                });
            }
            match (
                components.coders.get(&pcoll.coder_id),
                components
                    .windowing_strategies
                    .get(&pcoll.windowing_strategy_id),
            ) {
                (None, _) => Err(PipelineError::MissingCoder {
                    pcollection_id: p_id.clone(),
                    coder_id: pcoll.coder_id.clone(),
                }),
                (_, None) => Err(PipelineError::MissingWindowingStrategy {
                    pcollection_id: p_id.clone(),
                    windowing_strategy_id: pcoll.windowing_strategy_id.clone(),
                }),
                (Some(_), Some(_)) => Ok(()),
            }
        })
        .and_then(|()| {
            if let Some(orphan_id) = components.pcollections.keys().find(|&p_id| {
                !components
                    .transforms
                    .values()
                    .any(|t| t.outputs.values().any(|out| out == p_id))
            }) {
                Err(PipelineError::NoProducerPCollection {
                    pcollection_id: orphan_id.clone(),
                })
            } else {
                Ok(())
            }
        })
}
