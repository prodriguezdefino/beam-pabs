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

//! The imports an ordinary pipeline author writes.
//!
//! ```
//! use beam::prelude::*;
//! ```
//!
//! Pipeline, options, PValues, built-in transforms, the `PTransform`/`DoFn` traits, state
//! and timer specs, common windows and triggers, user metrics and rows.
//!
//! This prelude is `apply`-style; the facade's `beam::prelude` adds the fluent methods.
//! Specialised items stay in their modules: [`crate::pipeline`] (resource hints),
//! [`crate::metrics`] (query API),
//! [`crate::coders`], [`ParDoMulti`](crate::transforms::ParDoMulti) and [`crate::internals`].

pub use crate::coders::BeamIterable;
pub use crate::error::{Error, Result};
pub use crate::metrics::{Counter, Distribution, Gauge, Metrics};
pub use crate::options::{PipelineOptionGroup, PipelineOptions};
pub use crate::pipeline::Pipeline;
#[cfg(feature = "derive")]
pub use crate::schema::BeamEnum;
pub use crate::schema::{BeamRow, Row, Schema};
pub use crate::transforms::{
    BagState, BagStateSpec, Create, DoFn, Failure, Filter, FlatMap, Flatten, GroupByKey, Inspect,
    Map, MapState, MapStateSpec, PTransform, ParDo, Partition, ProcessContext, SetState,
    SetStateSpec, TimeDomain, Timer, TimerFamilySpec, TryMap, TryParDo, ValueState, ValueStateSpec,
    WithFailures,
};
pub use crate::values::{PBegin, PCollection, PCollectionList, PCollectionView, PDone};
pub use crate::windowing::{
    AccumulationMode, BoundedWindow, FixedWindows, GlobalWindows, IntervalWindow, Sessions,
    SlidingWindows, Trigger, WindowFn, WindowInto,
};
