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

//! Keyed processing with typed user state and timers.

use std::marker::PhantomData;
use std::sync::Arc;

use super::context::{FinishBundleContext, HandlerContext, ProcessContext, StatefulContext};
use super::pardo::{IntoParDoFn, Stateful, process_each_window, process_value_each_window, sealed};
use super::state::{
    AnyStateSpec, BagState, BagStateSpec, MapState, MapStateSpec, OrderedListState,
    OrderedListStateSpec, SetState, SetStateSpec, UserStateBackend, UserStateError, ValueState,
    ValueStateSpec,
};
use super::timer::{TimeDomain, TimerFamilySpec};
use crate::coders::{DefaultCoder, TimerAction, TimerRecord, WindowedHeader};
use crate::transforms::{
    BundleHandler, DisplayDataBuilder, HandlerInstance, TransformFn, TypedElement,
};
use crate::values::AnySideInput;

/// Processes keyed elements with user state and timers that the types declare.
///
/// [`State`](Self::State) is a struct of state cells and [`Timer`](Self::Timer) is an enum of
/// timer families. `#[derive(State)]` and `#[derive(Timers)]` implement them, and
/// [`ParDo`](crate::transforms::ParDo) writes their declarations into the transform. The
/// input is `(Key, Value)`; state and timers are scoped to the key and the window. Elements
/// are always processed one window at a time, so both callbacks read it with
/// [`StatefulContext::window`] (`on_timer` has no element to wrap in `Windowed`).
///
/// The lifecycle is the lifecycle of [`DoFn`](crate::transforms::DoFn), with
/// [`on_timer`](Self::on_timer) called for each fired timer.
///
/// ```
/// use beam::prelude::*;
///
/// #[derive(State)]
/// struct Counts {
///     count: ValueState<i64>,
/// }
///
/// #[derive(Timers)]
/// enum Flush {
///     #[timer(event_time)]
///     Window,
/// }
///
/// #[derive(Clone)]
/// struct CountPerKey;
///
/// impl StatefulDoFn for CountPerKey {
///     type Key = String;
///     type Value = String;
///     type Out = (String, i64);
///     type State = Counts;
///     type Timer = Flush;
///
///     fn process_element(
///         &mut self,
///         _key: &String,
///         _value: String,
///         state: &mut Counts,
///         ctx: &mut StatefulContext<'_, Self>,
///     ) -> Result {
///         state.count.write(state.count.read()?.unwrap_or(0) + 1)?;
///         ctx.timer(Flush::Window).at(ctx.window()?.max_timestamp())
///     }
///
///     fn on_timer(
///         &mut self,
///         timer: Flush,
///         key: &String,
///         state: &mut Counts,
///         ctx: &mut StatefulContext<'_, Self>,
///     ) -> Result {
///         match timer {
///             Flush::Window => ctx.emit((key.clone(), state.count.read()?.unwrap_or(0))),
///         }
///     }
/// }
///
/// # let _ = ParDo::new("Count", CountPerKey);
/// ```
pub trait StatefulDoFn: Clone + Send + Sync + 'static {
    /// The key of the input elements. State and timers are scoped to it.
    type Key: DefaultCoder;
    /// The value of the input elements.
    type Value: DefaultCoder;
    /// What this function emits; see [`Outputs`](crate::transforms::Outputs).
    type Out: crate::transforms::Outputs;
    /// The state cells. Use `()` for none.
    type State: StateCells;
    /// The timer families. Use [`NoTimers`] for none.
    type Timer: TimerFamilies;

    /// Set to `true` if the function must see the same input when the runner retries a
    /// bundle, for example a sink that commits to an external system.
    const REQUIRES_STABLE_INPUT: bool = false;

    /// Set to `true` if the function must see the elements of each key in timestamp order.
    /// The runner buffers and sorts them, so `process_element` can rely on the order.
    const REQUIRES_TIME_SORTED_INPUT: bool = false;

    /// Called once on each copy, when the bundle processor that owns it is created.
    fn setup(&mut self) -> crate::Result {
        Ok(())
    }

    /// Called once before the first element of a bundle.
    fn start_bundle(&mut self) -> crate::Result {
        Ok(())
    }

    /// Processes one input element with the state of its key.
    fn process_element(
        &mut self,
        key: &Self::Key,
        value: Self::Value,
        state: &mut Self::State,
        ctx: &mut StatefulContext<'_, Self>,
    ) -> crate::Result;

    /// Called when `timer` fires for `key`. `ctx.timestamp()` is the fire time.
    fn on_timer(
        &mut self,
        timer: Self::Timer,
        key: &Self::Key,
        state: &mut Self::State,
        ctx: &mut StatefulContext<'_, Self>,
    ) -> crate::Result {
        let _ = (timer, key, state, ctx);
        Ok(())
    }

    /// Called once after the last element of a bundle, the last chance to emit. Outputs name
    /// their window; see [`FinishBundleContext`].
    fn finish_bundle(&mut self, ctx: &mut FinishBundleContext<'_, Self::Out>) -> crate::Result {
        let _ = ctx;
        Ok(())
    }

    /// Called once on each copy, when the bundle processor that owns it is discarded.
    fn teardown(&mut self) -> crate::Result {
        Ok(())
    }

    /// Adds display data for runner UIs.
    fn populate_display_data(&self, _builder: &mut DisplayDataBuilder) {}

    /// The side input views that [`process_element`](Self::process_element) or
    /// [`on_timer`](Self::on_timer) reads.
    ///
    /// [`ParDo`](crate::transforms::ParDo) wires each view into the transform graph.
    fn side_inputs(&self) -> Vec<&dyn AnySideInput> {
        Vec::new()
    }
}

/// Where state cells read and write: the state backend, the window and the encoded key.
pub struct StateBinding<'a> {
    backend: Option<Arc<dyn UserStateBackend>>,
    window: Arc<[u8]>,
    key: Arc<[u8]>,
    _marker: PhantomData<&'a ()>,
}

impl<'a> StateBinding<'a> {
    /// Binds to `window` and `key`. Without a backend, a cell fails to bind; `()` state does
    /// not need one.
    pub fn new(
        backend: Option<Arc<dyn UserStateBackend>>,
        window: &'a [u8],
        key: &'a [u8],
    ) -> Self {
        Self {
            backend,
            window: Arc::from(window),
            key: Arc::from(key),
            _marker: PhantomData,
        }
    }

    fn parts(&self, id: &str) -> crate::Result<CellParts> {
        let backend = self
            .backend
            .as_ref()
            .ok_or_else(|| UserStateError::MissingBackend(id.to_string()))?;
        Ok(CellParts {
            backend: Arc::clone(backend),
            id: Arc::from(id),
            window: Arc::clone(&self.window),
            key: Arc::clone(&self.key),
        })
    }
}

/// What one state cell holds: where it reads and writes, its state id, its window and its key.
struct CellParts {
    backend: Arc<dyn UserStateBackend>,
    id: Arc<str>,
    window: Arc<[u8]>,
    key: Arc<[u8]>,
}

/// A struct of state cells. `#[derive(State)]` implements it; `()` has no cells.
pub trait StateCells: Sized {
    /// The spec of each cell.
    fn specs() -> Vec<Arc<dyn AnyStateSpec>>;

    /// Binds each cell to the key and window of `binding`.
    fn bind(binding: &StateBinding<'_>) -> crate::Result<Self>;
}

impl StateCells for () {
    fn specs() -> Vec<Arc<dyn AnyStateSpec>> {
        Vec::new()
    }

    fn bind(_binding: &StateBinding<'_>) -> crate::Result<Self> {
        Ok(())
    }
}

/// One state cell type. The type decides the spec, the protocol and the coder.
pub trait StateCell: Sized {
    /// The spec of the cell with state id `id`.
    fn spec(id: &str) -> Arc<dyn AnyStateSpec>;

    /// The cell with state id `id`, bound to the key and window of `binding`.
    fn bind(id: &str, binding: &StateBinding<'_>) -> crate::Result<Self>;
}

impl<V: DefaultCoder> StateCell for BagState<V> {
    fn spec(id: &str) -> Arc<dyn AnyStateSpec> {
        Arc::new(BagStateSpec::<V>::new(id))
    }

    fn bind(id: &str, binding: &StateBinding<'_>) -> crate::Result<Self> {
        let CellParts {
            backend,
            id,
            window,
            key,
        } = binding.parts(id)?;
        Ok(BagState::new(backend, id, window, key))
    }
}

impl<V: DefaultCoder> StateCell for ValueState<V> {
    fn spec(id: &str) -> Arc<dyn AnyStateSpec> {
        Arc::new(ValueStateSpec::<V>::new(id))
    }

    fn bind(id: &str, binding: &StateBinding<'_>) -> crate::Result<Self> {
        BagState::bind(id, binding).map(ValueState::new)
    }
}

impl<K: DefaultCoder, V: DefaultCoder> StateCell for MapState<K, V> {
    fn spec(id: &str) -> Arc<dyn AnyStateSpec> {
        Arc::new(MapStateSpec::<K, V>::new(id))
    }

    fn bind(id: &str, binding: &StateBinding<'_>) -> crate::Result<Self> {
        let CellParts {
            backend,
            id,
            window,
            key,
        } = binding.parts(id)?;
        Ok(MapState::new(backend, id, window, key))
    }
}

impl<T: DefaultCoder> StateCell for SetState<T> {
    fn spec(id: &str) -> Arc<dyn AnyStateSpec> {
        Arc::new(SetStateSpec::<T>::new(id))
    }

    fn bind(id: &str, binding: &StateBinding<'_>) -> crate::Result<Self> {
        MapState::bind(id, binding).map(SetState::new)
    }
}

impl<T: DefaultCoder> StateCell for OrderedListState<T> {
    fn spec(id: &str) -> Arc<dyn AnyStateSpec> {
        Arc::new(OrderedListStateSpec::<T>::new(id))
    }

    fn bind(id: &str, binding: &StateBinding<'_>) -> crate::Result<Self> {
        let CellParts {
            backend,
            id,
            window,
            key,
        } = binding.parts(id)?;
        Ok(OrderedListState::new(backend, id, window, key))
    }
}

/// An enum of timer families. `#[derive(Timers)]` implements it; [`NoTimers`] has none.
pub trait TimerFamilies: Sized + Send + 'static {
    /// The spec of each family.
    fn specs() -> Vec<TimerFamilySpec>;

    /// The family id of this timer.
    fn family(&self) -> &'static str;

    /// The dynamic tag of this timer. Empty for a family without tags.
    fn tag(&self) -> &str;

    /// The time domain of this timer.
    fn time_domain(&self) -> TimeDomain;

    /// The timer that fired in `family` with `tag`, or `None` for an unknown family.
    fn from_fired(family: &str, tag: &str) -> Option<Self>;
}

/// The [`StatefulDoFn::Timer`] of a function that sets no timers.
#[derive(Clone, Copy, Debug)]
pub enum NoTimers {}

impl TimerFamilies for NoTimers {
    fn specs() -> Vec<TimerFamilySpec> {
        Vec::new()
    }

    fn family(&self) -> &'static str {
        match *self {}
    }

    fn tag(&self) -> &str {
        match *self {}
    }

    fn time_domain(&self) -> TimeDomain {
        match *self {}
    }

    fn from_fired(_family: &str, _tag: &str) -> Option<Self> {
        None
    }
}

impl<T: StatefulDoFn> sealed::Sealed<Stateful> for T {}

impl<T: StatefulDoFn> IntoParDoFn<Stateful> for T {
    type In = (T::Key, T::Value);
    type Out = T::Out;
    const REQUIRES_STABLE_INPUT: bool = T::REQUIRES_STABLE_INPUT;
    const REQUIRES_TIME_SORTED_INPUT: bool = T::REQUIRES_TIME_SORTED_INPUT;

    fn state_specs() -> Vec<Arc<dyn AnyStateSpec>> {
        T::State::specs()
    }

    fn timer_families() -> Vec<TimerFamilySpec> {
        T::Timer::specs()
    }

    fn side_inputs(&self) -> Vec<&dyn AnySideInput> {
        StatefulDoFn::side_inputs(self)
    }

    fn into_handler(self) -> TransformFn {
        Arc::new(StatefulDoFnHandler { func: self })
    }

    fn populate_display_data(&self, builder: &mut DisplayDataBuilder) {
        StatefulDoFn::populate_display_data(self, builder);
    }
}

/// Adapts a [`StatefulDoFn`] to the byte-level [`BundleHandler`] that a runner calls.
#[derive(Clone)]
pub(crate) struct StatefulDoFnHandler<F> {
    func: F,
}

impl<F: StatefulDoFn> StatefulDoFnHandler<F> {
    /// Binds the state of `key` and calls `process_element`.
    fn process_kv(
        &mut self,
        (key, value): (F::Key, F::Value),
        ctx: ProcessContext<'_, F::Out>,
    ) -> crate::Result {
        // Encode the key once to scope user state cells and timers for this element.
        let key_bytes = key.encode()?;
        let mut state = F::State::bind(&ctx.state_binding(&key_bytes))?;
        let mut ctx = StatefulContext::new(ctx, key_bytes);
        self.func.process_element(&key, value, &mut state, &mut ctx)
    }
}

impl<F: StatefulDoFn> BundleHandler for StatefulDoFnHandler<F> {
    fn setup(&mut self) -> Result<(), String> {
        Ok(self.func.setup()?)
    }

    fn start_bundle(&mut self) -> Result<(), String> {
        Ok(self.func.start_bundle()?)
    }

    fn process(&mut self, element: &[u8], ctx: &mut HandlerContext<'_>) -> Result<(), String> {
        process_each_window(element, ctx, true, |kv, pctx| self.process_kv(kv, pctx))
    }

    fn process_value(
        &mut self,
        element: TypedElement<'_>,
        ctx: &mut HandlerContext<'_>,
    ) -> Result<(), String> {
        process_value_each_window(element, ctx, true, |kv, pctx| self.process_kv(kv, pctx))
    }

    fn on_timer(
        &mut self,
        family: &str,
        record: &TimerRecord,
        ctx: &mut HandlerContext<'_>,
    ) -> Result<(), String> {
        let TimerAction::Set {
            fire_timestamp,
            pane,
            ..
        } = record.action
        else {
            return Ok(());
        };
        let timer = F::Timer::from_fired(family, &record.dynamic_tag)
            .ok_or_else(|| format!("A timer fired in the undeclared family '{family}'"))?;
        let key = F::Key::decode(&record.user_key)
            .map_err(|e| format!("Failed to decode the key of a fired timer: {e}"))?;
        let header = WindowedHeader::new(fire_timestamp.as_millis(), &record.windows, pane);
        let pctx = ctx
            .as_process_context::<F::Out>()
            .with_key_bytes(record.user_key.as_slice())
            .with_header(&header);
        let mut state = F::State::bind(&pctx.state_binding(&record.user_key))?;
        let mut ctx = StatefulContext::new(pctx, record.user_key.clone());
        Ok(self.func.on_timer(timer, &key, &mut state, &mut ctx)?)
    }

    fn finish_bundle(&mut self, ctx: &mut HandlerContext<'_>) -> Result<(), String> {
        let mut fctx = FinishBundleContext::new(ctx.as_process_context::<F::Out>());
        Ok(self.func.finish_bundle(&mut fctx)?)
    }

    fn teardown(&mut self) -> Result<(), String> {
        Ok(self.func.teardown()?)
    }

    fn instantiate(&self) -> HandlerInstance {
        Box::new(self.clone())
    }
}
