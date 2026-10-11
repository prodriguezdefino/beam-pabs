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

//! Runner and harness plumbing: byte-level handlers, readers, collectors, and metrics.
//!
//! The only public path for these items. Pipeline code does not need them.

use std::collections::HashMap;

/// A foldhash `HashMap` for tables hashed per element. Crate-private, so public APIs
/// keep the standard `HashMap` and foldhash stays out of the SDK's semver surface.
pub(crate) type FastHashMap<K, V> = HashMap<K, V, foldhash::fast::RandomState>;

pub use crate::coders::display::ElementFormatter;
pub use crate::metrics::context::MetricsContainer;
pub use crate::metrics::scope::{
    MetricsScope, TransformMetrics, TransformSelection, process_wide_container,
};
pub use crate::metrics::short_ids::ShortIdCache;
pub use crate::metrics::system::{
    data_channel_read_index, element_count, finish_bundle_msecs, process_bundle_msecs,
    sampled_byte_size, start_bundle_msecs, total_msecs, work_completed, work_remaining,
};
