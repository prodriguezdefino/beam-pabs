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

//! The graph check of the rebuild path. The launcher records each transform that resolves
//! through the handler map, and a worker compares its rebuilt graph with the record.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fmt;

use beam_model::pipeline as proto;
use prost::Message;
use serde::{Deserialize, Serialize};

use super::Pipeline;
use super::constants::{
    UNEXPANDED_PLACEHOLDER_ID, URN_COMBINE_GROUPED_VALUES, URN_PAR_DO, URN_RUST_COMBINE_FN,
    URN_RUST_DOFN,
};
use crate::options::{OptionsError, PipelineOptionGroup, PipelineOptions};
use crate::runners::RunnerError;

/// The handler key of a primitive that resolves through the handler map, or `None`.
pub(super) fn handler_key(transform: &proto::PTransform) -> Option<String> {
    if !transform.subtransforms.is_empty() {
        return None;
    }
    let spec = transform.spec.as_ref()?;
    let payload = spec.payload.as_slice();
    let (function, rust_urn) = match spec.urn.as_str() {
        URN_PAR_DO => (
            proto::ParDoPayload::decode(payload).ok()?.do_fn?,
            URN_RUST_DOFN,
        ),
        URN_COMBINE_GROUPED_VALUES => (
            proto::CombinePayload::decode(payload).ok()?.combine_fn?,
            URN_RUST_COMBINE_FN,
        ),
        _ => return None,
    };
    (function.urn == rust_urn).then(|| String::from_utf8_lossy(&function.payload).into_owned())
}

/// A transform that resolves through the handler map. Coder and windowing ids are not part
/// of it: they come from a process-wide counter.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct RebuildNode {
    urn: String,
    handler_key: String,
    inputs: BTreeMap<String, String>,
    outputs: BTreeMap<String, String>,
}

/// The transforms of the launcher graph that resolve through the handler map, keyed by
/// transform id. The job options carry them to the workers.
#[derive(clap::Args, Clone, Debug, Default, Serialize, Deserialize)]
struct RebuildGraph {
    #[arg(skip)]
    rebuild_graph: BTreeMap<String, RebuildNode>,
}

impl PipelineOptionGroup for RebuildGraph {}

/// How the worker graph differs from the launcher graph at one transform.
enum Difference<'a> {
    Missing,
    Added,
    Changed {
        part: &'static str,
        launcher: &'a dyn fmt::Debug,
        worker: &'a dyn fmt::Debug,
    },
}

impl fmt::Display for Difference<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Missing => f.write_str("is missing on the worker"),
            Self::Added => f.write_str("is only on the worker"),
            Self::Changed {
                part,
                launcher,
                worker,
            } => write!(
                f,
                "has the {part} {launcher:?} on the launcher and {worker:?} on the worker"
            ),
        }
    }
}

impl RebuildNode {
    /// The first part of `worker` that differs from this launcher node. An input that is a
    /// placeholder on the worker matches any launcher input with the same tag.
    fn difference<'a>(
        &'a self,
        worker: &'a Self,
        placeholders: &HashSet<&str>,
    ) -> Option<Difference<'a>> {
        let inputs_match = || {
            self.inputs.len() == worker.inputs.len()
                && self.inputs.iter().all(|(tag, id)| {
                    worker
                        .inputs
                        .get(tag)
                        .is_some_and(|own| own == id || placeholders.contains(own.as_str()))
                })
        };
        let changed = |part, launcher, worker| {
            Some(Difference::Changed {
                part,
                launcher,
                worker,
            })
        };
        if self.urn != worker.urn {
            changed("spec URN", &self.urn, &worker.urn)
        } else if self.handler_key != worker.handler_key {
            changed("handler key", &self.handler_key, &worker.handler_key)
        } else if self.outputs != worker.outputs {
            changed("outputs", &self.outputs, &worker.outputs)
        } else if inputs_match() {
            None
        } else {
            changed("inputs", &self.inputs, &worker.inputs)
        }
    }
}

fn sorted(map: &HashMap<String, String>) -> BTreeMap<String, String> {
    map.iter()
        .map(|(tag, id)| (tag.clone(), id.clone()))
        .collect()
}

/// The first transform id, in id order, at which `worker` differs from `launcher`.
fn first_difference<'a>(
    launcher: &'a BTreeMap<String, RebuildNode>,
    worker: &'a BTreeMap<String, RebuildNode>,
    placeholders: &HashSet<&str>,
) -> Option<(&'a str, Difference<'a>)> {
    let ids: BTreeSet<&String> = launcher.keys().chain(worker.keys()).collect();
    ids.into_iter().find_map(|id| {
        let difference = match (launcher.get(id), worker.get(id)) {
            (Some(recorded), Some(own)) => recorded.difference(own, placeholders),
            (Some(_), None) => Some(Difference::Missing),
            (None, Some(_)) => Some(Difference::Added),
            (None, None) => None,
        };
        difference.map(|difference| (id.as_str(), difference))
    })
}

impl Pipeline {
    /// The transforms of this graph that resolve through the handler map.
    fn rebuild_nodes(&self) -> BTreeMap<String, RebuildNode> {
        let lock = self.lock();
        lock.components
            .transforms
            .iter()
            .filter_map(|(id, transform)| {
                let handler_key = handler_key(transform)?;
                let node = RebuildNode {
                    urn: transform.spec.as_ref()?.urn.clone(),
                    handler_key,
                    inputs: sorted(&transform.inputs),
                    outputs: sorted(&transform.outputs),
                };
                Some((id.clone(), node))
            })
            .collect()
    }

    /// Records the transforms that resolve through the handler map in `options`. Call it
    /// before the runner takes the options snapshot of the job.
    pub(crate) fn record_rebuild_graph(
        &self,
        options: &PipelineOptions,
    ) -> Result<(), OptionsError> {
        options.set(RebuildGraph {
            rebuild_graph: self.rebuild_nodes(),
        })
    }

    /// Compares this rebuilt graph with the record of the launcher in `options`. Without a
    /// record, the check passes.
    pub(crate) fn check_rebuild_graph(&self, options: &PipelineOptions) -> Result<(), RunnerError> {
        let recorded = options.view_as::<RebuildGraph>()?.rebuild_graph;
        if recorded.is_empty() {
            return Ok(());
        }
        let own = self.rebuild_nodes();
        let lock = self.lock();
        let placeholders: HashSet<&str> = lock
            .components
            .pcollections
            .iter()
            .filter(|(_, pcoll)| pcoll.windowing_strategy_id == UNEXPANDED_PLACEHOLDER_ID)
            .map(|(id, _)| id.as_str())
            .collect();
        match first_difference(&recorded, &own, &placeholders) {
            Some((transform_id, difference)) => Err(RunnerError::GraphMismatch {
                transform_id: transform_id.to_string(),
                difference: difference.to_string(),
            }),
            None => Ok(()),
        }
    }
}
