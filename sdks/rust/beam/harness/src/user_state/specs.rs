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

//! The user state cells that the transforms of a descriptor declare.

use std::collections::HashMap;
use std::sync::Arc;

use prost::Message;

use beam::pipeline::URN_PAR_DO;
use model::fn_execution::ProcessBundleDescriptor;
use model::pipeline::{self as proto, Coder};

use crate::user_state::cells::MapStateCoders;

/// The user state cells that the transforms of a descriptor declare, with their coders. A
/// processor builds it once and its bundles share it.
pub struct UserStateSpecs {
    pub(super) state_coders: HashMap<String, HashMap<String, String>>,
    pub(super) map_state_coders: HashMap<String, HashMap<String, MapStateCoders>>,
    pub(super) ordered_list_state_coders: HashMap<String, HashMap<String, String>>,
    pub(super) coders: Arc<HashMap<String, Coder>>,
}

impl UserStateSpecs {
    /// The cells of `descriptor`, or `None` if no transform declares user state.
    pub fn from_descriptor(
        descriptor: &ProcessBundleDescriptor,
        coders: Arc<HashMap<String, Coder>>,
    ) -> Option<Self> {
        let pardo_specs = descriptor.transforms.iter().filter_map(|(t_id, t)| {
            let spec = t
                .spec
                .as_ref()
                .filter(|s| s.urn == URN_PAR_DO && !s.payload.is_empty())?;
            let pardo = proto::ParDoPayload::decode(spec.payload.as_slice()).ok()?;
            Some((t_id, pardo.state_specs))
        });

        let (state_coders, map_state_coders, ordered_list_state_coders) = pardo_specs.fold(
            (
                HashMap::<String, HashMap<String, String>>::new(),
                HashMap::<String, HashMap<String, MapStateCoders>>::new(),
                HashMap::<String, HashMap<String, String>>::new(),
            ),
            |(mut state_coders, mut map_state_coders, mut ordered_list_state_coders),
             (t_id, state_specs)| {
                for (state_id, spec) in state_specs {
                    match spec.spec {
                        Some(proto::state_spec::Spec::BagSpec(b)) => {
                            state_coders
                                .entry(t_id.clone())
                                .or_default()
                                .insert(state_id, b.element_coder_id);
                        }
                        Some(proto::state_spec::Spec::ReadModifyWriteSpec(r)) => {
                            state_coders
                                .entry(t_id.clone())
                                .or_default()
                                .insert(state_id, r.coder_id);
                        }
                        Some(proto::state_spec::Spec::MapSpec(m)) => {
                            map_state_coders.entry(t_id.clone()).or_default().insert(
                                state_id,
                                MapStateCoders {
                                    key_coder_id: m.key_coder_id,
                                    value_coder_id: m.value_coder_id,
                                },
                            );
                        }
                        Some(proto::state_spec::Spec::SetSpec(s)) => {
                            map_state_coders.entry(t_id.clone()).or_default().insert(
                                state_id,
                                MapStateCoders {
                                    key_coder_id: s.element_coder_id.clone(),
                                    value_coder_id: s.element_coder_id,
                                },
                            );
                        }
                        Some(proto::state_spec::Spec::MultimapSpec(m)) => {
                            map_state_coders.entry(t_id.clone()).or_default().insert(
                                state_id,
                                MapStateCoders {
                                    key_coder_id: m.key_coder_id,
                                    value_coder_id: m.value_coder_id,
                                },
                            );
                        }
                        Some(proto::state_spec::Spec::OrderedListSpec(o)) => {
                            ordered_list_state_coders
                                .entry(t_id.clone())
                                .or_default()
                                .insert(state_id, o.element_coder_id);
                        }
                        // A combining cell is a bag of accumulators.
                        Some(proto::state_spec::Spec::CombiningSpec(c)) => {
                            state_coders
                                .entry(t_id.clone())
                                .or_default()
                                .insert(state_id, c.accumulator_coder_id);
                        }
                        None => {}
                    }
                }
                (state_coders, map_state_coders, ordered_list_state_coders)
            },
        );

        if state_coders.is_empty()
            && map_state_coders.is_empty()
            && ordered_list_state_coders.is_empty()
        {
            return None;
        }
        Some(Self {
            state_coders,
            map_state_coders,
            ordered_list_state_coders,
            coders,
        })
    }
}
