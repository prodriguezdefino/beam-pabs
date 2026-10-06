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

use super::strategy::{AccumulationMode, ClosingBehavior, OnTimeBehavior, OutputTime};
use super::trigger::Trigger;
use super::window_fn::{MergeStatus, WindowFn, decode_window_fn};
use crate::coders::WindowedHeader;
use crate::internals::HandlerContext;
use crate::pipeline::PipelineError;
use crate::pipeline::constants::{URN_WINDOW_FN_SESSION_WINDOWS, URN_WINDOW_INTO};
use crate::transforms::{BundleHandler, HandlerInstance, PTransform, TransformFn};
use crate::values::PCollection;

/// A PTransform that assigns the elements of a [`PCollection`] to windows with a [`WindowFn`].
///
/// The output strategy starts from the strategy of the input. Each field that a `with_*`
/// method does not set keeps the value of the input, so a new window function keeps the
/// trigger, the lateness and the accumulation mode of the input.
#[derive(Clone, Debug)]
pub struct WindowInto<W = Arc<dyn WindowFn>> {
    window_fn: W,
    trigger: Option<Trigger>,
    accumulation_mode: Option<AccumulationMode>,
    output_time: Option<OutputTime>,
    closing_behavior: Option<ClosingBehavior>,
    allowed_lateness: Option<Duration>,
    on_time_behavior: Option<OnTimeBehavior>,
}

impl<W: WindowFn> WindowInto<W> {
    /// Creates a `WindowInto` transform with the specified window function.
    pub fn new(window_fn: W) -> Self {
        Self {
            window_fn,
            trigger: None,
            accumulation_mode: None,
            output_time: None,
            closing_behavior: None,
            allowed_lateness: None,
            on_time_behavior: None,
        }
    }

    /// Sets the trigger that fires panes.
    pub fn with_trigger(mut self, trigger: impl Into<Trigger>) -> Self {
        self.trigger = Some(trigger.into());
        self
    }

    /// Sets the accumulation mode for pane emissions.
    pub fn with_accumulation_mode(mut self, mode: AccumulationMode) -> Self {
        self.accumulation_mode = Some(mode);
        self
    }

    /// Sets the allowed lateness beyond window expiration before elements are dropped.
    pub fn with_allowed_lateness(mut self, lateness: Duration) -> Self {
        self.allowed_lateness = Some(lateness);
        self
    }

    /// Sets the output timestamp rule for aggregated panes.
    pub fn with_output_time(mut self, output_time: OutputTime) -> Self {
        self.output_time = Some(output_time);
        self
    }

    /// Sets the window closing behavior.
    pub fn with_closing_behavior(mut self, closing_behavior: ClosingBehavior) -> Self {
        self.closing_behavior = Some(closing_behavior);
        self
    }

    /// Sets the on-time pane firing behavior.
    pub fn with_on_time_behavior(mut self, on_time_behavior: OnTimeBehavior) -> Self {
        self.on_time_behavior = Some(on_time_behavior);
        self
    }

    /// Returns the strategy of the output: the window function of this transform, the fields
    /// that it sets, and the fields of `input` for the rest.
    fn output_strategy(
        &self,
        input: proto::WindowingStrategy,
        window_coder_id: &str,
        environment_id: &str,
    ) -> proto::WindowingStrategy {
        let Self {
            window_fn,
            trigger,
            accumulation_mode,
            output_time,
            closing_behavior,
            allowed_lateness,
            on_time_behavior,
            ..
        } = self;
        proto::WindowingStrategy {
            window_fn: Some(proto::FunctionSpec {
                urn: window_fn.urn().to_string(),
                payload: window_fn.payload(),
            }),
            merge_status: proto::merge_status::Enum::from(window_fn.merge_status()) as i32,
            window_coder_id: window_coder_id.to_string(),
            assigns_to_one_window: window_fn.assigns_to_one_window(),
            environment_id: environment_id.to_string(),
            trigger: trigger.as_ref().map(proto::Trigger::from).or(input.trigger),
            accumulation_mode: accumulation_mode.map_or(input.accumulation_mode, |m| {
                proto::accumulation_mode::Enum::from(m) as i32
            }),
            output_time: output_time.map_or(input.output_time, |t| {
                proto::output_time::Enum::from(t) as i32
            }),
            closing_behavior: closing_behavior.map_or(input.closing_behavior, |c| {
                proto::closing_behavior::Enum::from(c) as i32
            }),
            allowed_lateness: allowed_lateness.map_or(input.allowed_lateness, |l| {
                i64::try_from(l.as_millis()).unwrap_or(i64::MAX)
            }),
            on_time_behavior: on_time_behavior.map_or(input.on_time_behavior, |o| {
                proto::on_time_behavior::Enum::from(o) as i32
            }),
        }
    }
}

impl<T: 'static, W: WindowFn + Clone> PTransform<PCollection<T>> for WindowInto<W> {
    type Output = PCollection<T>;

    fn expand(&self, input: &PCollection<T>) -> PCollection<T> {
        let pipeline = input.pipeline();
        let name = pipeline.primitive_name("WindowInto");

        // Only session windows merge: a runner merges them itself. `WindowFn` has no merge
        // function, so a runner cannot merge the windows of another window function.
        if let (MergeStatus::NeedsMerge, urn) =
            (self.window_fn.merge_status(), self.window_fn.urn())
            && urn != URN_WINDOW_FN_SESSION_WINDOWS
        {
            pipeline.record_construction_error(PipelineError::InvalidConstruction {
                transform_id: name.clone(),
                reason: format!(
                    "window function '{urn}' needs merging, but only session windows merge. \
                     Return MergeStatus::NonMerging from merge_status"
                ),
            });
        }

        let window_coder_id =
            pipeline.register_coder(self.window_fn.window_coder_urn(), Vec::new());

        let input_ws_id = input.windowing_strategy_id();
        let ws_id = {
            let mut lock = pipeline.lock();
            let strategies = &lock.components.windowing_strategies;
            let input_strategy = strategies
                .get(&input_ws_id)
                .or_else(|| strategies.get(&lock.default_windowing_strategy_id))
                .cloned()
                .expect("the pipeline registers its default strategy when it is created");
            let strategy = self.output_strategy(
                input_strategy,
                &window_coder_id,
                &lock.default_environment_id,
            );
            lock.register_windowing_strategy(strategy)
        };

        let is_bounded = input.is_bounded();

        let out_pcoll = pipeline.add_pcollection_with_windowing::<T>(
            &format!("{name}.out"),
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

        pipeline.add_transform(&name, URN_WINDOW_INTO, payload, inputs, outputs);

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
        let mut assigned = Vec::new();
        self.window_fn
            .assign_windows_into(crate::Instant::from_millis(timestamp), &mut assigned);
        let new_windows: Vec<Vec<u8>> = assigned
            .iter()
            .map(|w| {
                let mut buf = Vec::new();
                w.encode(&mut buf)
                    .expect("encoding a window into a Vec<u8> cannot fail");
                buf
            })
            .collect();
        let new_header =
            WindowedHeader::with_metadata(timestamp, &new_windows, ctx.pane(), &ctx.metadata());
        (0..ctx.header.window_count().max(1))
            .try_for_each(|_| ctx.sink.push_windowed(&new_header, element.to_vec()))
    }
}
