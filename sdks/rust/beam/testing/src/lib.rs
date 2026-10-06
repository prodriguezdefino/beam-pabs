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

//! Tools for testing Apache Beam pipelines.
//!
//! - [`passert`] — assertions that run inside the pipeline and fail the job if a
//!   collection does not match expectations.
//! - [`TestStream`] — unbounded source replaying a scripted sequence of elements,
//!   watermark advances, and processing-time advances.
//! - [`TestPipeline`] — pipeline configured from the environment. After it runs, it
//!   verifies that all assertions ran. It panics if it is never run.
//! - [`require_env!`] / [`env_or_skip`] — visible, CI-enforceable skipping for tests
//!   that need external resources named by environment variables.
//!
//! Pipeline authors reach this crate as `beam::testing` through the `testing` feature
//! of the `apache-beam` crate, usually only for tests:
//!
//! ```toml
//! [dev-dependencies]
//! beam = { package = "apache-beam", version = "0.1", features = ["prism", "testing"] }
//! ```
//!
//! All items in this crate use only the public API of `apache-beam-core`.
//!
//! # Running on Prism without the facade
//!
//! A crate that depends on `testing` directly, not through the facade, can enable its
//! `prism` feature. This links the Prism runner, so that [`TestPipeline::new`] (whose
//! default runner is `prism`) finds it:
//!
//! ```toml
//! [dev-dependencies]
//! testing = { workspace = true, features = ["prism"] }
//! ```
//!
//! The runner finds the Prism binary through `BEAM_PRISM_PATH`, or downloads it.

mod env;
pub mod passert;
mod test_pipeline;
mod test_stream;

pub use env::{EnvRequirement, STRICT_ENV_VAR, check_env, env_or_skip};
pub use test_pipeline::{
    TEST_PIPELINE_OPTIONS_ENV, TestPipeline, TestPipelineError, parse_test_pipeline_options,
    test_pipeline_options,
};
pub use test_stream::{TestStream, WATERMARK_INFINITY_MILLIS};

/// The Prism runner, for tests that name it: `testing::prism::PrismRunner`.
#[cfg(feature = "prism")]
pub use prism;

/// Force-links the runner so that its `inventory` registration stays.
#[cfg(feature = "prism")]
mod link {
    pub use harness as _;
    pub use prism as _;
}
