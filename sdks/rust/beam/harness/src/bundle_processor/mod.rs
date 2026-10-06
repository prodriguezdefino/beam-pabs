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
pub mod processor;
mod sampler;
pub mod split;

pub use beam::internals::TransformFn;
pub use execution::BundleError;
pub use handlers::{lookup_handler, resolve_handler};
pub use processor::{BundleProcessor, URN_DATA_SINK, URN_DATA_SOURCE};
pub use split::{SplitPoint, SplitState, compute_split};

// Internals re-exported only for `tests/`. They are not public API.
#[doc(hidden)]
pub use chain::instances::{Downstream, Instances};
#[doc(hidden)]
pub use sampler::{ExecutionSampler, set_element_processing_timeout};
