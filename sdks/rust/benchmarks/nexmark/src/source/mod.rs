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

//! Bounded and unbounded synthetic event sources for Nexmark pipelines.

mod bounded;
mod restriction;
mod unbounded;

pub use self::bounded::BoundedEventSource;
pub use self::restriction::{
    GeneratorRestriction, GeneratorRestrictionCoder, GeneratorRestrictionTracker,
};
pub use self::unbounded::UnboundedEventSource;
use crate::generator::GeneratorConfig;

/// Yields `(global_start, global_end, first_event_id, sub_max_events)` per generator split.
pub(crate) fn generator_segments(
    root: &GeneratorConfig,
    num_generators: usize,
) -> impl Iterator<Item = (i64, i64, i64, i64)> + use<> {
    root.split_bounds(num_generators)
        .scan(0_i64, |seg_start, (first_id, sub_max)| {
            let start = *seg_start;
            let end = start + sub_max;
            *seg_start = end;
            Some((start, end, first_id, sub_max))
        })
}
