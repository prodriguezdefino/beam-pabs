/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

//! DoFn/ParDo execution, context, state and timers.
//!
//! This module is crate-private. [`crate::transforms`] exports the user items and
//! [`crate::internals`] exports the runner plumbing.

mod batch;
mod closure;
pub(crate) mod context;
pub(crate) mod input;
pub(crate) mod outputs;
mod pardo;
pub(crate) mod registered;
pub(crate) mod side_input;
pub(crate) mod state;
mod stateful;
pub(crate) mod timer;

pub use batch::{BatchConverter, BatchedDoFn, BatchedDoFnAdapter, VecBatchConverter};
pub(super) use closure::ClosureFn;
pub use context::{
    DEFAULT_FINALIZER_EXPIRY, Finalizer, FinishBundleContext, FinishOutput, HandlerContext,
    OutputBuilder, ProcessContext, StatefulContext, Timer,
};
pub use input::{DoFnInput, PerWindow, Windowed};
pub use outputs::{Outputs, WithFinalizer};
pub use pardo::{DoFn, DoFnHandler, IntoParDoFn, ParDo, Stateful, Stateless};
pub use registered::{
    FnRegistration, Registered, RegisteredFn, build_registered, encode_registered,
    registered_handler,
};
pub use state::{BagState, MapState, OrderedListState, SetState, ValueState};
pub use stateful::{NoTimers, StateBinding, StateCell, StateCells, StatefulDoFn, TimerFamilies};
