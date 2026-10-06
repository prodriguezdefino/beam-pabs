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

//! Builds operator graphs and runs the bundles of a `ProcessBundleDescriptor`.
//!
//! The bundle processor:
//! - Finds `DATA_SOURCE` (`beam:runner:source:v1`) and `DATA_SINK` (`beam:runner:sink:v1`).
//! - Decodes elements from inbound `BeamFnData` streams.
//! - Runs the pipeline operators.
//! - Encodes output elements and writes them to outbound `BeamFnData` streams.

mod cache;
mod chain;
mod decoding;
mod execution;
mod handlers;
mod plan;
pub(crate) mod processor;
mod roots;
mod sampler;
pub(crate) mod sampling;
pub(crate) mod split;
mod tracker;

pub use beam::internals::TransformFn;
pub use execution::BundleError;
pub use handlers::{lookup_handler, resolve_handler};
pub use processor::{BundleProcessor, URN_DATA_SINK, URN_DATA_SOURCE};
pub use sampling::{DataSampler, SamplingMode};
pub use split::{SplitPoint, SplitState, compute_split};
pub use tracker::{ActiveBundleReport, BundlePhase};

/// URNs whose spec payload is a `ParDoPayload`.
pub(crate) const PARDO_PAYLOAD_URNS: [&str; 5] = [
    beam::pipeline::URN_PAR_DO,
    beam::pipeline::URN_SDF_PAIR_WITH_RESTRICTION,
    beam::pipeline::URN_SDF_SPLIT_AND_SIZE_RESTRICTIONS,
    beam::pipeline::URN_SDF_PROCESS_SIZED_ELEMENT_AND_RESTRICTIONS,
    beam::pipeline::URN_SDF_TRUNCATE_SIZED_RESTRICTIONS,
];

/// The `ParDoPayload` of a ParDo transform, or `None` for other transforms.
pub(crate) fn pardo_payload(
    transform: &model::pipeline::PTransform,
) -> Result<Option<model::pipeline::ParDoPayload>, BundleError> {
    use prost::Message;
    match transform.spec.as_ref() {
        Some(spec) if PARDO_PAYLOAD_URNS.contains(&spec.urn.as_str()) => {
            let pardo = model::pipeline::ParDoPayload::decode(spec.payload.as_slice())?;
            Ok(Some(pardo))
        }
        _ => Ok(None),
    }
}

// Internals re-exported only for `tests/`. They are not public API.
#[doc(hidden)]
pub use chain::instances::{Downstream, Instances};
#[doc(hidden)]
pub use decoding::PortFraming;
#[doc(hidden)]
pub use processor::{PROGRESS_SYNC_INTERVAL, spawn_finalizer_sweeper};
#[doc(hidden)]
pub use roots::RootEncoder;
#[doc(hidden)]
pub use sampler::{ExecutionSampler, set_element_processing_timeout};
#[doc(hidden)]
pub use sampling::{PCollectionSampler, SamplingConfig};
#[doc(hidden)]
pub use split::inflight_split_fraction;
