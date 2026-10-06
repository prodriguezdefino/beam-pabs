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

//! Primary and residual roots in the names and the wire encoding of the runner.
//!
//! The runner reads a root with the input and output names of its own
//! `ProcessBundleDescriptor`. These names can differ from the names of the pipeline that the
//! SDK built: Dataflow renames outputs, for example. The runner ignores a residual watermark
//! on an output name that it does not know, and then holds no watermark for the stage.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use beam::internals::{DynamicSplitResult, ResidualApplication, duration_to_proto};
use beam::pipeline::URN_SDF_PROCESS_SIZED_ELEMENT_AND_RESTRICTIONS;
use model::fn_execution::{
    BundleApplication, DelayedBundleApplication, ProcessBundleDescriptor,
    ProcessBundleSplitResponse,
};
use model::pipeline::PTransform;
use tracing::warn;

use super::BundleError;
use super::decoding::PortFraming;

/// The runner's names of one splittable transform.
struct Names {
    /// The main input, or `None` when the descriptor does not name exactly one.
    main_input: Option<String>,
    outputs: Vec<String>,
}

/// Writes the roots of the transforms of one descriptor for the runner.
pub struct RootEncoder {
    framing: Arc<PortFraming>,
    /// The names of each transform that has outputs, by id.
    names: HashMap<String, Names>,
}

impl RootEncoder {
    /// Reads the names of the transforms of `descriptor`. `framing` is the encoding of the
    /// source port, where the runner sends the roots back.
    ///
    /// # Errors
    ///
    /// Returns [`BundleError`] when a transform payload fails to decode.
    pub fn new(
        descriptor: &ProcessBundleDescriptor,
        framing: Arc<PortFraming>,
    ) -> Result<Self, BundleError> {
        let names = descriptor
            .transforms
            .iter()
            .filter(|(_, t)| !t.outputs.is_empty())
            .map(|(id, t)| names(id, t).map(|n| (id.clone(), n)))
            .collect::<Result<HashMap<_, _>, _>>()?;
        Ok(Self { framing, names })
    }

    /// The residual in the encoding of the source port, with the main input of the runner.
    /// Its watermark goes on every output of the transform.
    ///
    /// # Errors
    ///
    /// Fails if the element does not reframe to the encoding of the source port.
    pub fn residual(
        &self,
        r: ResidualApplication,
    ) -> Result<DelayedBundleApplication, BundleError> {
        let names = self.names.get(&r.transform_id);
        let output_watermarks = r
            .output_watermark
            .map(Into::into)
            .into_iter()
            .flat_map(|watermark| {
                names
                    .into_iter()
                    .flat_map(|n| &n.outputs)
                    .map(move |output| (output.clone(), watermark))
            })
            .collect();
        let input_id = names
            .and_then(|n| n.main_input.clone())
            .unwrap_or(r.input_id);
        Ok(DelayedBundleApplication {
            application: Some(BundleApplication {
                element: self.framing.reframe(r.element)?,
                transform_id: r.transform_id,
                input_id,
                output_watermarks,
                ..BundleApplication::default()
            }),
            requested_time_delay: r.delay.map(duration_to_proto),
        })
    }

    /// The primary and the residual roots of a dynamic split, for the runner.
    ///
    /// # Errors
    ///
    /// Fails if one application does not reframe. The SDF already gave up the residual
    /// then, so the bundle must fail and the runner retries it.
    pub fn split(
        &self,
        split: DynamicSplitResult,
    ) -> Result<ProcessBundleSplitResponse, BundleError> {
        let DynamicSplitResult { primary, residuals } = split;
        let input_id = self
            .names
            .get(&primary.transform_id)
            .and_then(|n| n.main_input.clone())
            .unwrap_or(primary.input_id);
        let primary = BundleApplication {
            element: self.framing.reframe(primary.element)?,
            input_id,
            ..primary
        };
        Ok(ProcessBundleSplitResponse {
            primary_roots: vec![primary],
            residual_roots: residuals
                .into_iter()
                .map(|r| self.residual(r))
                .collect::<Result<_, _>>()?,
            channel_splits: Vec::new(),
        })
    }
}

fn names(id: &str, transform: &PTransform) -> Result<Names, BundleError> {
    // Only a transform with more than one input can have side inputs.
    let side_inputs: HashSet<String> = if transform.inputs.len() > 1 {
        match super::pardo_payload(transform)? {
            Some(p) => p.side_inputs.into_keys().collect(),
            None => HashSet::new(),
        }
    } else {
        HashSet::new()
    };
    let main: Vec<&String> = transform
        .inputs
        .keys()
        .filter(|k| !side_inputs.contains(*k))
        .collect();
    let splittable = transform
        .spec
        .as_ref()
        .is_some_and(|s| s.urn == URN_SDF_PROCESS_SIZED_ELEMENT_AND_RESTRICTIONS);
    let main_input = match main.as_slice() {
        [only] => Some((*only).clone()),
        _ if !splittable => None,
        _ => {
            warn!(
                "Splittable transform '{id}' has {} main inputs, not one. Its roots keep the input name of the SDK.",
                main.len()
            );
            None
        }
    };
    Ok(Names {
        main_input,
        outputs: transform.outputs.keys().cloned().collect(),
    })
}
