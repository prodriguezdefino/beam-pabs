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

//! Reads the provider, the config row and the input components of a request.

use crate::PCollectionId;
use crate::URN_SCHEMA_TRANSFORM_PAYLOAD;
use crate::discovery::find_provider;
use crate::{SchemaTransformProvider, config::decode_config};
use beam::schema::Row;
use model::pipeline as proto;
use std::collections::HashMap;

/// Finds the provider and its config row for a request.
///
/// Callers send a `SchemaTransformPayload` under the generic SchemaTransform URN, or an
/// `ExternalConfigurationPayload` under the provider identifier. Errors are response messages.
pub(super) fn resolve_request(
    spec: &proto::FunctionSpec,
) -> Result<(Box<dyn SchemaTransformProvider>, Row), String> {
    use prost::Message;
    let (identifier, caller_schema, row_bytes) = if spec.urn == URN_SCHEMA_TRANSFORM_PAYLOAD {
        let payload = proto::SchemaTransformPayload::decode(spec.payload.as_slice())
            .map_err(|e| format!("Failed to decode SchemaTransformPayload: {e}"))?;
        (
            payload.identifier,
            payload.configuration_schema,
            payload.configuration_row,
        )
    } else {
        let payload = proto::ExternalConfigurationPayload::decode(spec.payload.as_slice())
            .map_err(|e| {
                format!(
                    "Failed to decode ExternalConfigurationPayload for '{}': {e}",
                    spec.urn
                )
            })?;
        (spec.urn.clone(), payload.schema, payload.payload)
    };
    let provider = find_provider(&identifier)
        .ok_or_else(|| format!("No SchemaTransformProvider found for URN '{identifier}'"))?;
    let config = decode_config(provider.config_schema(), caller_schema, &row_bytes)
        .map_err(|e| format!("Failed to decode config row for '{identifier}': {e}"))?;
    Ok((provider, config))
}

/// Gives the input map that the provider receives for the request inputs.
///
/// Java and Go callers send a generated tag for a single unnamed input. When the provider
/// declares exactly one input tag and the request has exactly one input, the input goes under
/// the declared tag. Other inputs keep the request tags.
pub(super) fn provider_inputs(
    declared: &[String],
    inputs: &HashMap<String, PCollectionId>,
) -> HashMap<String, PCollectionId> {
    let mut entries = inputs.values();
    match (declared, entries.next(), entries.next()) {
        ([tag], Some(id), None) => HashMap::from([(tag.clone(), id.clone())]),
        _ => inputs.clone(),
    }
}

/// Copies the input PCollections of a request, with their coders and windowing strategies.
///
/// The provider and the worker replay start from these components only, so both register the
/// same handler keys. Ids that `caller` does not hold are skipped.
pub(super) fn input_components(
    caller: Option<&proto::Components>,
    inputs: &HashMap<String, PCollectionId>,
) -> proto::Components {
    let mut seed = proto::Components::default();
    let Some(caller) = caller else {
        return seed;
    };
    let mut coder_ids = Vec::new();
    for id in inputs.values() {
        let Some(pcoll) = caller.pcollections.get(id) else {
            continue;
        };
        coder_ids.push(pcoll.coder_id.clone());
        if let Some(ws) = caller
            .windowing_strategies
            .get(&pcoll.windowing_strategy_id)
        {
            coder_ids.push(ws.window_coder_id.clone());
            seed.windowing_strategies
                .insert(pcoll.windowing_strategy_id.clone(), ws.clone());
        }
        seed.pcollections.insert(id.clone(), pcoll.clone());
    }
    while let Some(id) = coder_ids.pop() {
        if let Some(coder) = caller.coders.get(&id)
            && !seed.coders.contains_key(&id)
        {
            coder_ids.extend(coder.component_coder_ids.iter().cloned());
            seed.coders.insert(id, coder.clone());
        }
    }
    seed
}
