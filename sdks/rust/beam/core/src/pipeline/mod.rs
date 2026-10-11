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

//! Pipeline representation, graph construction, and Runner API serialization.
//!
//! A [`Pipeline`] owns the graph under construction. The values that flow through it are in
//! [`crate::values`]. The transforms that extend it are in [`crate::transforms`].

use std::sync::{Arc, Mutex};

pub mod builder;
pub mod constants;
pub mod environment;
pub mod error;
pub mod graph;
pub mod resources;
pub mod roots;
pub mod validation;

pub use constants::*;
pub use environment::DockerEnvironment;
pub use error::*;
pub use graph::{PipelineInner, next_id};
pub use resources::*;

/// How a [`Pipeline`] expands cross-language transforms.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum ExpansionMode {
    /// Contact the expansion service and splice the returned subgraph into the pipeline.
    #[default]
    Remote,
    /// Record a placeholder transform and output PCollection without contacting a service.
    /// Downstream transforms attach to the placeholder output, so native handlers register
    /// under the names that the runner asks for.
    Placeholder,
}

impl ExpansionMode {
    /// Returns the mode for `options`: [`Placeholder`](Self::Placeholder) in an Fn API
    /// worker, [`Remote`](Self::Remote) in a driver.
    pub fn from_options(options: &crate::options::PipelineOptions) -> Self {
        if options.harness.is_worker() {
            Self::Placeholder
        } else {
            Self::Remote
        }
    }
}

/// An Apache Beam Pipeline handle with shared thread-safe state.
#[derive(Clone)]
pub struct Pipeline {
    inner: Arc<Mutex<PipelineInner>>,
}
