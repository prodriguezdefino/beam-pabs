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

//! Push-based execution of the operator chain.
//!
//! A chain is a fused sequence of transforms (an `OperatorGraph`) that runs in one bundle.
//!
//! * **Construction**: The chain comes from a `ProcessBundleDescriptor`. Construction
//!   resolves the data routing, finds the data sinks and maps each transform to its handler.
//! * **Execution**: Each element read goes through the `ChainSink`, which routes it
//!   synchronously through the chain. Each operator processes the element and gives its
//!   outputs directly to its downstream consumers. The operator holds those consumers
//!   exclusively for the duration of the call (see [`instances`]).
//!
//! Each element goes through the full chain to the sinks before the next element is read,
//! so intermediate outputs are not kept in memory. Peak memory depends on the depth of the
//! chain, not on the size of the bundle.

mod build;
pub mod instances;
mod sink;
mod stats;

pub(super) use build::{OperatorGraph, Source};
pub(super) use instances::Instances;
pub(super) use sink::{ChainCtx, finish_chain, invoke_operator, push_source};
pub(super) use stats::PCollectionStats;
