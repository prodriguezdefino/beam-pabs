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

//! The windowing strategy model and its Runner API serialization.

use std::sync::Arc;
use std::time::Duration;

use model::pipeline as proto;

use super::trigger::Trigger;
use super::window_fn::{WindowFn, decode_window_fn};

/// Whether later outputs of an aggregation replace earlier values or accumulate them.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum AccumulationMode {
    /// The aggregation is discarded when it is output. Later panes contain only new data.
    #[default]
    Discarding = 1,
    /// The aggregation is accumulated across outputs.
    Accumulating = 2,
    /// The aggregation emits retractions when it is output.
    Retracting = 3,
}

impl From<AccumulationMode> for proto::accumulation_mode::Enum {
    fn from(mode: AccumulationMode) -> Self {
        match mode {
            AccumulationMode::Discarding => proto::accumulation_mode::Enum::Discarding,
            AccumulationMode::Accumulating => proto::accumulation_mode::Enum::Accumulating,
            AccumulationMode::Retracting => proto::accumulation_mode::Enum::Retracting,
        }
    }
}

impl From<proto::accumulation_mode::Enum> for AccumulationMode {
    fn from(proto_enum: proto::accumulation_mode::Enum) -> Self {
        match proto_enum {
            proto::accumulation_mode::Enum::Accumulating => Self::Accumulating,
            proto::accumulation_mode::Enum::Retracting => Self::Retracting,
            _ => Self::Discarding,
        }
    }
}

/// When inputs are aggregated, the timestamp assigned to the resulting output.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum OutputTime {
    /// Output has the timestamp of the end of the window.
    #[default]
    EndOfWindow = 1,
    /// Output has the latest timestamp of the input elements in the pane.
    LatestInPane = 2,
    /// Output has the earliest timestamp of the input elements in the pane.
    EarliestInPane = 3,
}

impl From<OutputTime> for proto::output_time::Enum {
    fn from(ot: OutputTime) -> Self {
        match ot {
            OutputTime::EndOfWindow => proto::output_time::Enum::EndOfWindow,
            OutputTime::LatestInPane => proto::output_time::Enum::LatestInPane,
            OutputTime::EarliestInPane => proto::output_time::Enum::EarliestInPane,
        }
    }
}

impl From<proto::output_time::Enum> for OutputTime {
    fn from(proto_enum: proto::output_time::Enum) -> Self {
        match proto_enum {
            proto::output_time::Enum::LatestInPane => Self::LatestInPane,
            proto::output_time::Enum::EarliestInPane => Self::EarliestInPane,
            _ => Self::EndOfWindow,
        }
    }
}

/// Controls whether an aggregating transform outputs data when a window expires.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum ClosingBehavior {
    /// Always emit output when the window expires.
    EmitAlways = 1,
    /// Only emit output when new data has arrived since the last output.
    #[default]
    EmitIfNonempty = 2,
}

impl From<ClosingBehavior> for proto::closing_behavior::Enum {
    fn from(cb: ClosingBehavior) -> Self {
        match cb {
            ClosingBehavior::EmitAlways => proto::closing_behavior::Enum::EmitAlways,
            ClosingBehavior::EmitIfNonempty => proto::closing_behavior::Enum::EmitIfNonempty,
        }
    }
}

impl From<proto::closing_behavior::Enum> for ClosingBehavior {
    fn from(proto_enum: proto::closing_behavior::Enum) -> Self {
        match proto_enum {
            proto::closing_behavior::Enum::EmitAlways => Self::EmitAlways,
            _ => Self::EmitIfNonempty,
        }
    }
}

/// Controls whether an aggregating transform outputs data when an on-time pane is empty.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum OnTimeBehavior {
    /// Always fire the on-time pane.
    FireAlways = 1,
    /// Only fire the on-time pane if there is new data.
    #[default]
    FireIfNonempty = 2,
}

impl From<OnTimeBehavior> for proto::on_time_behavior::Enum {
    fn from(ot: OnTimeBehavior) -> Self {
        match ot {
            OnTimeBehavior::FireAlways => proto::on_time_behavior::Enum::FireAlways,
            OnTimeBehavior::FireIfNonempty => proto::on_time_behavior::Enum::FireIfNonempty,
        }
    }
}

impl From<proto::on_time_behavior::Enum> for OnTimeBehavior {
    fn from(proto_enum: proto::on_time_behavior::Enum) -> Self {
        match proto_enum {
            proto::on_time_behavior::Enum::FireAlways => Self::FireAlways,
            _ => Self::FireIfNonempty,
        }
    }
}

/// The windowing strategy of a PCollection: window assignment, merging, triggering, lateness
/// and accumulation.
#[derive(Clone, Debug)]
pub struct WindowingStrategy {
    pub window_fn: Arc<dyn WindowFn>,
    pub trigger: Trigger,
    pub accumulation_mode: AccumulationMode,
    pub output_time: OutputTime,
    pub closing_behavior: ClosingBehavior,
    pub allowed_lateness: Duration,
    pub on_time_behavior: OnTimeBehavior,
}

impl WindowingStrategy {
    /// Serializes this strategy into the Runner API [`proto::WindowingStrategy`].
    pub fn to_proto(
        &self,
        window_coder_id: &str,
        environment_id: &str,
    ) -> proto::WindowingStrategy {
        proto::WindowingStrategy {
            window_fn: Some(proto::FunctionSpec {
                urn: self.window_fn.urn().to_string(),
                payload: self.window_fn.payload(),
            }),
            merge_status: self.window_fn.merge_status() as i32,
            window_coder_id: window_coder_id.to_string(),
            trigger: Some(self.trigger.to_proto()),
            accumulation_mode: proto::accumulation_mode::Enum::from(self.accumulation_mode) as i32,
            output_time: proto::output_time::Enum::from(self.output_time) as i32,
            closing_behavior: proto::closing_behavior::Enum::from(self.closing_behavior) as i32,
            allowed_lateness: self.allowed_lateness.as_millis() as i64,
            on_time_behavior: proto::on_time_behavior::Enum::from(self.on_time_behavior) as i32,
            assigns_to_one_window: self.window_fn.assigns_to_one_window(),
            environment_id: environment_id.to_string(),
        }
    }

    /// Deserializes a [`WindowingStrategy`] from the Runner API [`proto::WindowingStrategy`].
    /// Unknown enum values and a missing trigger decode to the defaults.
    pub fn from_proto(strategy: &proto::WindowingStrategy) -> Result<Self, String> {
        let window_fn_spec = strategy
            .window_fn
            .as_ref()
            .ok_or_else(|| "WindowingStrategy missing window_fn FunctionSpec".to_string())?;
        let window_fn = decode_window_fn(window_fn_spec)?;

        let trigger = strategy
            .trigger
            .as_ref()
            .map(Trigger::from_proto)
            .transpose()?
            .unwrap_or_default();

        let accumulation_mode =
            proto::accumulation_mode::Enum::try_from(strategy.accumulation_mode)
                .map(AccumulationMode::from)
                .unwrap_or_default();

        let output_time = proto::output_time::Enum::try_from(strategy.output_time)
            .map(OutputTime::from)
            .unwrap_or_default();

        let closing_behavior = proto::closing_behavior::Enum::try_from(strategy.closing_behavior)
            .map(ClosingBehavior::from)
            .unwrap_or_default();

        let on_time_behavior = proto::on_time_behavior::Enum::try_from(strategy.on_time_behavior)
            .map(OnTimeBehavior::from)
            .unwrap_or_default();

        let allowed_lateness = Duration::from_millis(strategy.allowed_lateness.max(0) as u64);

        Ok(Self {
            window_fn,
            trigger,
            accumulation_mode,
            output_time,
            closing_behavior,
            allowed_lateness,
            on_time_behavior,
        })
    }
}

/// Whether `input` is windowed by the global window function. An unresolved strategy, which
/// only an unexpanded cross-language output has, counts as global: rejecting it would be a guess.
pub fn is_globally_windowed<T: 'static>(input: &crate::values::PCollection<T>) -> bool {
    let ws_id = input.windowing_strategy_id();
    input
        .pipeline()
        .lock()
        .components
        .windowing_strategies
        .get(&ws_id)
        .and_then(|ws| ws.window_fn.as_ref())
        .is_none_or(|spec| spec.urn == crate::pipeline::constants::URN_WINDOW_FN_GLOBAL_WINDOWS)
}
