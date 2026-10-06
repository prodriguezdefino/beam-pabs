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

//! ValidatesRunner integration test suite for the Apache Beam Rust SDK.
//!
//! Each `build_*` function only constructs a pipeline, with its in-graph [`passert`]
//! assertions, into a [`TestPipeline`]. Running it is left to the caller, which pairs
//! the builder with an [`Expectation`] of the run's outcome: the
//! [ValidatesRunner registry](crate::registry) and [`run_test`] for the conformance
//! suite, or a test binary for the pipelines outside it. Builders that are expected to
//! fail come with an `Expectation` constant next to them.
//!
//! Keeping construction apart from running is what lets a Dataflow worker rebuild a
//! test's pipeline without running anything. The builders are runner-agnostic: every
//! backend (Prism, Dataflow, ...) executes the same matrix to prove model conformance.
//!
//! The functions are grouped by the model area they exercise, and all of them
//! are re-exported flat from this module (and from the crate root) so callers
//! do not need to know which group a given check lives in.
//!
//! [`passert`]: beam::testing::passert
//! [`Expectation`]: crate::Expectation
//! [`run_test`]: crate::run_test

pub mod assertions;
pub mod core_transforms;
pub mod io;
pub mod joins;
pub mod schemas;
pub mod side_inputs;
pub mod state_and_timers;
pub mod triggers;
pub mod windowing;
pub mod xlang;

pub use assertions::*;
pub use core_transforms::*;
pub use io::*;
pub use joins::*;
pub use schemas::*;
pub use side_inputs::*;
pub use state_and_timers::*;
pub use triggers::*;
pub use windowing::*;
pub use xlang::*;

use beam::options::PipelineOptions;
use beam::testing::TestPipeline;

/// A [`TestPipeline`] for a builder, with default options: the runner is the one the
/// test hands to `run_with`, so the environment's runner selection is ignored, and
/// every `passert` assertion is verified to have run and passed.
pub fn test_pipeline() -> TestPipeline {
    TestPipeline::with_options(PipelineOptions::default())
}
