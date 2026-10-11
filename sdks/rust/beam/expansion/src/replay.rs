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

//! Replays an expansion in a worker and rewrites the expanded payloads for the caller.

use crate::config::decode_config;
use crate::discovery::find_provider;
use crate::{PCollectionId, SchemaTransformProvider};
use beam::internals::TransformFn;
use beam::pipeline::Pipeline;
use beam::pipeline::{
    URN_PAR_DO, URN_RUST_DOFN, URN_SDF_PAIR_WITH_RESTRICTION,
    URN_SDF_PROCESS_SIZED_ELEMENT_AND_RESTRICTIONS, URN_SDF_SPLIT_AND_SIZE_RESTRICTIONS,
    URN_TEST_STREAM,
};
use beam::schema::Row;
use harness::replay::{ExpandedSpec, ReplayEntry, ReplayRegistration, URN_RUST_DOFN_EXPANDED};
use model::pipeline as proto;
use std::collections::HashMap;

/// Builds `provider` in a new pipeline that holds only the input components of `entry`.
///
/// The server and the worker replay both call it, so they register the same handler keys.
pub(super) fn build_expansion(
    provider: &dyn SchemaTransformProvider,
    config: Row,
    entry: &ReplayEntry,
) -> Result<(Pipeline, HashMap<String, PCollectionId>), String> {
    let mut pipeline = Pipeline::new();
    if let Some(seed) = entry.components.clone() {
        let mut inner = pipeline.lock();
        inner.components.pcollections.extend(seed.pcollections);
        inner.components.coders.extend(seed.coders);
        inner
            .components
            .windowing_strategies
            .extend(seed.windowing_strategies);
    }
    let outputs = provider.build_transform(config, entry.inputs.clone(), &mut pipeline)?;
    Ok((pipeline, outputs))
}

/// Builds the handlers of an expansion again in a worker.
fn replay(entry: &ReplayEntry) -> Result<HashMap<String, TransformFn>, String> {
    let provider = find_provider(&entry.provider).ok_or_else(|| {
        format!(
            "No SchemaTransformProvider found for URN '{}'",
            entry.provider
        )
    })?;
    let config = decode_config(
        provider.config_schema(),
        entry.config_schema.clone(),
        &entry.config_row,
    )?;
    let (pipeline, _) = build_expansion(provider.as_ref(), config, entry)?;
    Ok(pipeline.transform_handlers())
}

inventory::submit! {
    ReplayRegistration { replay }
}

/// Rewrites the payload of `spec` for the caller pipeline.
///
/// Each bare handler key in a Rust `do_fn` becomes an [`ExpandedSpec`]. Each coder id in the
/// payload gets the id of the coder in the returned components, so runners find the
/// restriction, state and timer coders.
pub(super) fn rewrite_payload(
    spec: &mut proto::FunctionSpec,
    replay: &ReplayEntry,
    coder_id: &dyn Fn(&str) -> String,
) {
    use prost::Message;
    let expand = |key_spec: &mut proto::FunctionSpec, urn: &str| {
        let expanded = ExpandedSpec {
            handler_key: String::from_utf8_lossy(&key_spec.payload).into_owned(),
            replay: Some(replay.clone()),
        };
        key_spec.urn = urn.to_string();
        key_spec.payload = expanded.encode_to_vec();
    };
    let rename = |id: &mut String| {
        if !id.is_empty() {
            *id = coder_id(id);
        }
    };
    match spec.urn.as_str() {
        URN_PAR_DO
        | URN_SDF_PAIR_WITH_RESTRICTION
        | URN_SDF_SPLIT_AND_SIZE_RESTRICTIONS
        | URN_SDF_PROCESS_SIZED_ELEMENT_AND_RESTRICTIONS => {
            let Ok(mut payload) = proto::ParDoPayload::decode(spec.payload.as_slice()) else {
                return;
            };
            if let Some(do_fn) = payload.do_fn.as_mut().filter(|f| f.urn == URN_RUST_DOFN) {
                expand(do_fn, URN_RUST_DOFN_EXPANDED);
            }
            rename(&mut payload.restriction_coder_id);
            for state in payload.state_specs.values_mut() {
                use proto::state_spec::Spec;
                match state.spec.as_mut() {
                    Some(Spec::ReadModifyWriteSpec(s)) => rename(&mut s.coder_id),
                    Some(Spec::BagSpec(s)) => rename(&mut s.element_coder_id),
                    Some(Spec::CombiningSpec(s)) => rename(&mut s.accumulator_coder_id),
                    Some(Spec::MapSpec(s)) => {
                        rename(&mut s.key_coder_id);
                        rename(&mut s.value_coder_id);
                    }
                    Some(Spec::SetSpec(s)) => rename(&mut s.element_coder_id),
                    Some(Spec::OrderedListSpec(s)) => rename(&mut s.element_coder_id),
                    Some(Spec::MultimapSpec(s)) => {
                        rename(&mut s.key_coder_id);
                        rename(&mut s.value_coder_id);
                    }
                    None => {}
                }
            }
            for timer in payload.timer_family_specs.values_mut() {
                rename(&mut timer.timer_family_coder_id);
            }
            spec.payload = payload.encode_to_vec();
        }
        URN_TEST_STREAM => {
            let Ok(mut payload) = proto::TestStreamPayload::decode(spec.payload.as_slice()) else {
                return;
            };
            rename(&mut payload.coder_id);
            spec.payload = payload.encode_to_vec();
        }
        _ => {}
    }
}
