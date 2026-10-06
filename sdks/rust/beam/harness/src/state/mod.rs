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

//! Beam Fn State API client and side input materialization for portable runners.
//!
//! [`channel`] owns the Fn API state request/response transport, [`side_input_cache`] owns
//! side input reads, and [`cache`] keeps runner state across bundles per cache token.

pub(crate) mod cache;
pub(crate) mod channel;
pub(crate) mod side_input_cache;
pub(crate) mod tables;

pub use cache::{CacheTokens, DEFAULT_STATE_CACHE_BYTES, StateCache};
pub use channel::{StateChannel, StateClients};
pub(crate) use side_input_cache::split_concatenated_elements;
pub use side_input_cache::{FnApiSideInputReader, SideInputSpecs};
