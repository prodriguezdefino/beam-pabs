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

//! The per-element context handed to a `DoFn`.
//!
//! Each sibling module adds one `impl` block of [`ProcessContext`]: `process` (construction,
//! element facts), `output`, `side_inputs`, `metrics`, `residuals` (SDF residual roots)
//! and `finalizer`. `handler` holds the byte-level [`HandlerContext`]. The
//! struct is declared here so every sibling module can reach its private fields.

use std::borrow::Cow;
use std::marker::PhantomData;
use std::sync::Arc;

use crate::coders::WindowedHeader;
use crate::transforms::ElementSink;
use crate::transforms::dofn::side_input::SideInputReader;
use crate::transforms::dofn::state::UserStateReader;
use crate::transforms::dofn::timer::TimerCollector;

mod finalizer;
mod finish;
mod handler;
mod output;
mod process;
mod residuals;
mod side_inputs;

pub use finalizer::{BundleFinalizerCollector, FinalizationCallback};
pub use finish::{FinishBundleContext, FinishOutput};
pub use handler::HandlerContext;
pub use output::OutputBuilder;
pub use residuals::{ResidualApplication, ResidualCollector};

/// Context passed to a `DoFn` per element and timer: output, side
/// inputs, user state, timers, bundle finalization, and the window, timestamp, pane, metadata
/// and key of the current element.
pub struct ProcessContext<'a, T = ()> {
    sink: &'a mut dyn ElementSink,
    reader: Option<&'a dyn SideInputReader>,
    #[expect(dead_code, reason = "the DoFn handlers of the next commit use it")]
    state_reader: Option<&'a Arc<dyn UserStateReader>>,
    #[expect(dead_code, reason = "the DoFn handlers of the next commit use it")]
    timer_collector: Option<&'a Arc<TimerCollector>>,
    bundle_finalizer: Option<&'a Arc<BundleFinalizerCollector>>,
    header: &'a WindowedHeader,
    #[expect(dead_code, reason = "the DoFn handlers of the next commit use it")]
    key_bytes: Option<Cow<'a, [u8]>>,
    _marker: PhantomData<T>,
}
