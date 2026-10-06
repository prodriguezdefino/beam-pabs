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

//! Standard pipeline options for Beam Rust programs.
//!
//! Options are typed [`PipelineOptionGroup`]s that runners, I/O connectors and pipelines
//! declare and read with [`view_as`](PipelineOptions::view_as), so `beam-core` depends on
//! none of them. Pipeline arguments are also a group:
//!
//! ```
//! use clap::Args;
//! use serde::{Deserialize, Serialize};
//! use beam::options::{OptionsError, PipelineOptionGroup, Secret};
//!
//! #[derive(Args, Serialize, Deserialize, Debug, Clone)]
//! pub struct MyArgs {
//!     #[arg(long, default_value_t = 3)]
//!     pub max_retries: u32,
//!
//!     /// A credential is a reference, e.g. `env:API_KEY`, never the value itself.
//!     #[arg(long)]
//!     pub api_key: Option<Secret>,
//! }
//!
//! impl PipelineOptionGroup for MyArgs {
//!     fn validate(&self) -> Result<(), OptionsError> {
//!         (self.max_retries <= 10).then_some(()).ok_or(OptionsError::Validation {
//!             group: "MyArgs",
//!             message: "max_retries cannot exceed 10".to_string(),
//!         })
//!     }
//! }
//!
//! let (options, args) = beam::options::try_parse_from::<MyArgs, _, _>([
//!     "app", "--runner=prism", "--max_retries=5", "--api_key=env:API_KEY",
//! ])
//! .unwrap();
//! assert_eq!(options.runner, "prism");
//! assert_eq!(args.max_retries, 5);
//! assert_eq!(args.api_key.unwrap().to_string(), "env:API_KEY");
//! ```
//!
//! The groups that a driver reads form the typed [`OptionsSnapshot`] of the job. Workers
//! deserialize it instead of parsing a command line.

pub mod convert;
pub mod flags;
pub mod groups;
pub mod pipeline_options;
pub mod secret;
pub mod snapshot;

pub use convert::{json_to_prost_struct, json_to_prost_value};
pub use flags::{parse_args, parse_args_from, try_parse_args_from};
pub use groups::{
    DebugOptions, HarnessOptions, OptionGroupRegistration, OptionsError, PipelineOptionGroup,
    PortableOptions, ResourceHintsOptions, WorkerOptions, resolve_container_image,
};
pub use pipeline_options::{ParseError, PipelineOptions, parse, parse_from, try_parse_from};
pub use secret::{
    Secret, SecretError, SecretFuture, SecretProvider, SecretResolverRegistration, SecretValue,
};
pub use snapshot::{GroupSnapshot, OptionsSnapshot, SDK_OPTIONS_OPTION};
