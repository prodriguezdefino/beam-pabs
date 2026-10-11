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
//! - [`require_env!`] / [`env_or_skip`] — visible, CI-enforceable skipping for tests
//!   that need external resources named by environment variables.
//!
//! Pipeline authors reach this crate as `beam::testing` through the `testing` feature
//! of the `apache-beam` crate, usually only for tests:
//!
//! ```toml
//! [dev-dependencies]
//! beam = { package = "apache-beam", version = "2.78.0-SNAPSHOT", features = ["prism", "testing"] }
//! ```
//!
//! # Running on Prism without the facade
//!
//! A crate that depends on `testing` directly, not through the facade, can enable its
//! `prism` feature. This links the Prism runner.
//!
//! ```toml
//! [dev-dependencies]
//! testing = { workspace = true, features = ["prism"] }
//! ```
//!
//! The runner finds the Prism binary through `BEAM_PRISM_PATH`, or downloads it.

mod env;

pub use env::{EnvRequirement, STRICT_ENV_VAR, check_env, env_or_skip};

/// Force-links the runner so that its `inventory` registration stays.
#[cfg(feature = "prism")]
mod link {
    pub use harness as _;
    pub use prism as _;
}
