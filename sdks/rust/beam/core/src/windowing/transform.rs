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

//! The `WindowInto` transform and execution handlers.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use model::pipeline as proto;
use prost::Message;

use super::strategy::{
    AccumulationMode, ClosingBehavior, OnTimeBehavior, OutputTime, WindowingStrategy,
};
use super::trigger::Trigger;
use super::window_fn::{WindowFn, decode_window_fn};
use crate::coders::WindowedHeader;
use crate::internals::HandlerContext;
use crate::pipeline::constants::URN_WINDOW_INTO;
use crate::transforms::{BundleHandler, HandlerInstance, PTransform, TransformFn};
use crate::values::PCollection;

/// A PTransform that assigns the elements of a [`PCollection`] to windows with a [`WindowFn`].
#[derive(Clone, Debug)]
pub struct WindowInto<W = Arc<dyn WindowFn>> {
    name: String,
    window_fn: W,
    trigger: Trigger,
    accumulation_mode: AccumulationMode,
    output_time: OutputTime,
    closing_behavior: ClosingBehavior,
    allowed_lateness: Duration,
    on_time_behavior: OnTimeBehavior,
}

impl<W: WindowFn> WindowInto<W> {
    /// Creates a `WindowInto` transform named `name` with the specified window function.
    pub fn new(name: impl Into<String>, window_fn: W) -> Self {
        Self {
            name: name.into(),
            window_fn,
            trigger: Trigger::Default,
            accumulation_mode: AccumulationMode::Discarding,
            output_time: OutputTime::EndOfWindow,
            closing_behavior: ClosingBehavior::EmitIfNonempty,
            allowed_lateness: Duration::ZERO,
            on_time_behavior: OnTimeBehavior::FireIfNonempty,
        }
    }

    /// Sets the trigger governing when panes are fired.
    pub fn triggering(mut self, trigger: Trigger) -> Self {
        self.trigger = trigger;
        self
    }

    /// Sets the accumulation mode to accumulate elements across firings for the same window.
    pub fn accumulating_fired_panes(mut self) -> Self {
        self.accumulation_mode = AccumulationMode::Accumulating;
        self
    }

    /// Sets the accumulation mode to discard elements from previous firings.
    pub fn discarding_fired_panes(mut self) -> Self {
        self.accumulation_mode = AccumulationMode::Discarding;
        self
    }

    /// Sets the allowed lateness beyond window expiration before elements are dropped.
    pub fn with_allowed_lateness(mut self, lateness: Duration) -> Self {
        self.allowed_lateness = lateness;
        self
    }

    /// Sets the output timestamp rule for aggregated panes.
    pub fn with_output_time(mut self, output_time: OutputTime) -> Self {
        self.output_time = output_time;
        self
    }

    /// Sets the window closing behavior.
    pub fn with_closing_behavior(mut self, closing_behavior: ClosingBehavior) -> Self {
        self.closing_behavior = closing_behavior;
        self
    }

    /// Sets the on-time pane firing behavior.
    pub fn with_on_time_behavior(mut self, on_time_behavior: OnTimeBehavior) -> Self {
        self.on_time_behavior = on_time_behavior;
        self
    }
}

impl<T: 'static, W: WindowFn + Clone> PTransform<PCollection<T>> for WindowInto<W> {
    type Output = PCollection<T>;

    fn expand(&self, input: &PCollection<T>) -> PCollection<T> {
        let pipeline = input.pipeline();

        let window_coder_id =
            pipeline.register_coder(self.window_fn.window_coder_urn(), Vec::new());

        let strategy = WindowingStrategy {
            window_fn: Arc::new(self.window_fn.clone()),
            trigger: self.trigger.clone(),
            accumulation_mode: self.accumulation_mode,
            output_time: self.output_time,
            closing_behavior: self.closing_behavior,
            allowed_lateness: self.allowed_lateness,
            on_time_behavior: self.on_time_behavior,
        };

        let env_id = pipeline.lock().default_environment_id.clone();
        let proto_strategy = strategy.to_proto(&window_coder_id, &env_id);
        let ws_id = pipeline.register_windowing_strategy(proto_strategy);

        let is_bounded = pipeline
            .lock()
            .components
            .pcollections
            .get(input.id())
            .map(|p| {
                if p.is_bounded == proto::is_bounded::Enum::Unbounded as i32 {
                    crate::values::IsBounded::Unbounded
                } else {
                    crate::values::IsBounded::Bounded
                }
            })
            .unwrap_or(crate::values::IsBounded::Bounded);

        let out_pcoll = pipeline.add_pcollection_with_windowing::<T>(
            &format!("{}.out", self.name),
            input.coder_id(),
            is_bounded,
            &ws_id,
        );

        let window_fn_spec = proto::FunctionSpec {
            urn: self.window_fn.urn().to_string(),
            payload: self.window_fn.payload(),
        };
        let payload = proto::WindowIntoPayload {
            window_fn: Some(window_fn_spec.clone()),
        }
        .encode_to_vec();

        let inputs = HashMap::from([("in".to_string(), input.id().to_string())]);
        let outputs = HashMap::from([("out".to_string(), out_pcoll.id().to_string())]);

        pipeline.add_transform(&self.name, URN_WINDOW_INTO, payload, inputs, outputs);

        // The harness rebuilds the standard window functions from the payload. Register a
        // handler for any other window function, keyed by that same payload.
        if decode_window_fn(&window_fn_spec).is_err() {
            let handler: TransformFn =
                Arc::new(WindowIntoHandler::new(Arc::new(self.window_fn.clone())));
            pipeline.register_transform_handler(window_into_handler_key(&window_fn_spec), handler);
        }

        out_pcoll
    }
}

/// Returns the registration key of a `WindowInto` handler, used when [`decode_window_fn`]
/// cannot rebuild the window function. The runner passes the `FunctionSpec` back unchanged,
/// so the worker harness computes the same key from the bundle descriptor.
pub fn window_into_handler_key(window_fn: &proto::FunctionSpec) -> String {
    let payload: String = window_fn
        .payload
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    format!("{URN_WINDOW_INTO}/{}/{payload}", window_fn.urn)
}

/// Execution handler that assigns incoming elements to windows with `window_fn`. Copies share
/// the immutable window function.
#[derive(Clone)]
pub struct WindowIntoHandler {
    window_fn: Arc<dyn WindowFn>,
}

impl WindowIntoHandler {
    pub fn new(window_fn: Arc<dyn WindowFn>) -> Self {
        Self { window_fn }
    }
}

impl BundleHandler for WindowIntoHandler {
    fn instantiate(&self) -> HandlerInstance {
        Box::new(self.clone())
    }

    fn process(&mut self, element: &[u8], ctx: &mut HandlerContext<'_>) -> Result<(), String> {
        let timestamp = ctx.timestamp();
        let new_windows = self.window_fn.assign_windows_encoded(timestamp);
        let new_header =
            WindowedHeader::with_metadata(timestamp, &new_windows, ctx.pane(), &ctx.metadata());
        (0..ctx.header.window_count().max(1))
            .try_for_each(|_| ctx.sink.push_windowed(&new_header, element.to_vec()))
    }
}
