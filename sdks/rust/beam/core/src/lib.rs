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

//! Core types, traits, and pipeline model for the Apache Beam Rust SDK.
//!
//! - [`values`] — what flows through a pipeline: `PBegin`, `PCollection`, `PDone`.
//! - [`transforms`] — the [`PTransform`](transforms::PTransform) trait that builds the graph.
//! - [`coders`] — how elements are serialized on the wire.
//! - [`pipeline`] — the graph itself, and its translation to the Runner API.
//! - [`options`] and [`runners`] — configuring and dispatching execution.
//! - [`internals`] — runner and worker-harness plumbing. Pipeline authors do not need it.
//!
//! Start with [`prelude`]. This crate uses `apply` style (`pcoll.apply(Map::new(..))`); the
//! `apache-beam-fluent` crate adds `pcoll.map(..)`, and the `apache-beam` prelude has both.

pub mod coders;
mod error;
pub mod internals;
pub mod metrics;
pub mod options;
pub mod pipeline;
pub mod prelude;
pub mod runners;
pub mod schema;
pub mod transforms;
pub mod values;
pub mod windowing;

pub use error::{Error, Result};
