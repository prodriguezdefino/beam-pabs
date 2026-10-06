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

//! Splittable DoFns: restrictions, trackers and [`SplittableParDo`].

pub(crate) mod dynamic_split;
mod offset_range;
mod process_continuation;
pub(crate) mod splittable_dofn;
mod tracker;

pub use offset_range::{OffsetRange, OffsetRangeCoder, OffsetRangeTracker};
pub use process_continuation::ProcessContinuation;
pub use splittable_dofn::{SplittableDoFn, SplittableParDo};
pub use tracker::{RestrictionError, RestrictionProgress, RestrictionTracker, WatermarkedTracker};
