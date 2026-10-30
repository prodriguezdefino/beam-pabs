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

//! [`GroupIntoBatches`]: keyed batching with user state and timers, across bundles.

use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;

use super::dofn::state::AnyStateSpec;
use super::dofn::timer::{TimeDomain, TimerFamilySpec};
use super::dofn::{
    BagState, DoFn, ParDo, ProcessContext, StateBinding, StateCell, StateCells, StatefulContext,
    StatefulDoFn, TimerFamilies, ValueState,
};
use super::reshuffle::random_seed;
use crate::Instant;
use crate::coders::{DefaultCoder, ShardedKey, with_scratch_buffer};
use crate::pipeline::PipelineError;
use crate::transforms::PTransform;
use crate::transforms::display_data::{DisplayDataBuilder, HasDisplayData};
use crate::values::PCollection;

/// Batches the values of each key and window, across bundles.
///
/// The input is `(K, V)`. The output is `(K, Vec<V>)`, or `(ShardedKey<K>, Vec<V>)` after
/// [`with_sharded_key`](GroupIntoBatches::with_sharded_key). A batch is emitted when it holds
/// `batch_size` values, when the encoded values reach the byte size, when the buffering
/// duration ends, or when the window ends.
///
/// A batch that a size limit emits has the timestamp of the value that filled it. A batch
/// that the end of the window emits has the minimum timestamp of its values: a timer holds
/// the output watermark there. A batch that the buffering duration emits has the timestamp
/// of its first value, which a timer also holds.
///
/// ```
/// use std::time::Duration;
/// use beam::prelude::*;
///
/// let p = Pipeline::default();
/// let batches = p
///     .apply("Create", Create::new(vec![("a".to_string(), 1i64), ("a".to_string(), 2)]))
///     .apply(
///         "Batch",
///         GroupIntoBatches::new(100).with_max_buffering_duration(Duration::from_secs(10)),
///     );
/// # let _: PCollection<(String, Vec<i64>)> = batches;
/// ```
#[derive(Clone, Debug)]
pub struct GroupIntoBatches<S = ByKey> {
    batch_size: usize,
    byte_size: Option<usize>,
    max_buffering_duration: Option<Duration>,
    _keying: PhantomData<S>,
}

/// The keying of a [`GroupIntoBatches`] that batches on the user key.
#[derive(Clone, Copy, Debug)]
pub enum ByKey {}

/// The keying of a [`GroupIntoBatches`] that batches on a [`ShardedKey`].
#[derive(Clone, Copy, Debug)]
pub enum ByShardedKey {}

impl GroupIntoBatches<ByKey> {
    /// Emits a batch when it holds `batch_size` values. `batch_size` must not be zero.
    pub fn new(batch_size: usize) -> Self {
        Self {
            batch_size,
            byte_size: None,
            max_buffering_duration: None,
            _keying: PhantomData,
        }
    }

    /// Batches on a [`ShardedKey`] of each key, so that the batches of one hot key are made
    /// in parallel. Each worker thread adds its own shard id.
    pub fn with_sharded_key(self) -> GroupIntoBatches<ByShardedKey> {
        GroupIntoBatches {
            batch_size: self.batch_size,
            byte_size: self.byte_size,
            max_buffering_duration: self.max_buffering_duration,
            _keying: PhantomData,
        }
    }
}

impl<S> GroupIntoBatches<S> {
    /// Also emits a batch when its values, encoded with the value coder, hold `bytes` bytes.
    /// `bytes` must not be zero.
    pub fn with_byte_size(mut self, bytes: usize) -> Self {
        self.byte_size = Some(bytes);
        self
    }

    /// Also emits a batch `duration` of processing time after its first value. `duration`
    /// must not be zero.
    pub fn with_max_buffering_duration(mut self, duration: Duration) -> Self {
        self.max_buffering_duration = Some(duration);
        self
    }

    /// The reason the configuration is not valid, if it is not.
    fn configuration_error(&self) -> Option<&'static str> {
        match (self.batch_size, self.byte_size, self.max_buffering_duration) {
            (0, _, _) => Some("batch_size must be greater than zero"),
            (_, Some(0), _) => Some("the byte size must be greater than zero"),
            (_, _, Some(duration)) if duration.is_zero() => {
                Some("the maximum buffering duration must be greater than zero")
            }
            _ => None,
        }
    }

    fn limits(&self) -> BatchLimits {
        let to_i64 = |n: usize| i64::try_from(n).unwrap_or(i64::MAX);
        BatchLimits {
            batch_size: to_i64(self.batch_size),
            byte_size: self.byte_size.map(to_i64),
            max_buffering_duration: self.max_buffering_duration,
        }
    }

    /// Applies `batch` to `input` in the scope of this transform, with its display data and
    /// its configuration errors.
    fn expand_with<I: 'static, O>(
        &self,
        input: &PCollection<I>,
        sharded: bool,
        batch: impl FnOnce(&PCollection<I>, BatchLimits) -> PCollection<O>,
    ) -> PCollection<O> {
        let pipeline = input.pipeline();
        let name = pipeline.scope_name().unwrap_or_default();
        if let Some(reason) = self.configuration_error() {
            pipeline.record_construction_error(PipelineError::InvalidConstruction {
                transform_id: name.clone(),
                reason: reason.to_string(),
            });
        }
        let output = batch(input, self.limits());
        let mut builder = DisplayDataBuilder::with_namespace(name);
        self.populate_display_data(&mut builder);
        builder.add_boolean("sharded_key", sharded);
        pipeline.set_scope_display_data(builder.into_proto());
        output
    }
}

impl<S> HasDisplayData for GroupIntoBatches<S> {
    fn populate_display_data(&self, builder: &mut DisplayDataBuilder) {
        builder.add_integer(
            "batch_size",
            i64::try_from(self.batch_size).unwrap_or(i64::MAX),
        );
        if let Some(bytes) = self.byte_size {
            builder.add_integer("byte_size", i64::try_from(bytes).unwrap_or(i64::MAX));
        }
        if let Some(duration) = self.max_buffering_duration {
            builder.add_text("max_buffering_duration", format!("{duration:?}"));
        }
    }
}

impl<K, V> PTransform<PCollection<(K, V)>> for GroupIntoBatches<ByKey>
where
    K: DefaultCoder + Clone,
    V: DefaultCoder,
{
    type Output = PCollection<(K, Vec<V>)>;

    fn expand(&self, input: &PCollection<(K, V)>) -> Self::Output {
        self.expand_with(input, false, |input, limits| {
            input.apply("Batch", ParDo::new(GroupIntoBatchesFn::new(limits)))
        })
    }
}

impl<K, V> PTransform<PCollection<(K, V)>> for GroupIntoBatches<ByShardedKey>
where
    K: DefaultCoder + Clone,
    V: DefaultCoder,
{
    type Output = PCollection<(ShardedKey<K>, Vec<V>)>;

    fn expand(&self, input: &PCollection<(K, V)>) -> Self::Output {
        self.expand_with(input, true, |input, limits| {
            input
                .apply("AssignShard", ParDo::new(AssignShardFn::default()))
                .apply("Batch", ParDo::new(GroupIntoBatchesFn::new(limits)))
        })
    }
}

/// Wraps each key in a [`ShardedKey`] with the shard id of this copy of the function.
struct AssignShardFn<K, V> {
    shard_id: Bytes,
    _marker: PhantomData<fn() -> (K, V)>,
}

impl<K, V> Default for AssignShardFn<K, V> {
    fn default() -> Self {
        Self {
            shard_id: Bytes::new(),
            _marker: PhantomData,
        }
    }
}

/// A copy gets its shard id in `setup`, not from the prototype.
impl<K, V> Clone for AssignShardFn<K, V> {
    fn clone(&self) -> Self {
        Self::default()
    }
}

impl<K: DefaultCoder, V: DefaultCoder> DoFn for AssignShardFn<K, V> {
    type In = (K, V);
    type Out = (ShardedKey<K>, V);

    /// Each copy runs on one worker thread, so a random id per copy spreads a key over them.
    fn setup(&mut self) -> crate::Result {
        self.shard_id = Bytes::copy_from_slice(&random_seed().to_be_bytes());
        Ok(())
    }

    fn process_element(
        &mut self,
        (key, value): Self::In,
        ctx: &mut ProcessContext<'_, Self::Out>,
    ) -> crate::Result {
        // A `Bytes` clone shares the buffer: it does not copy the shard id.
        ctx.emit((ShardedKey::new(key, self.shard_id.clone()), value))
    }
}

/// The limits of a batch, converted to the types of the batch state.
#[derive(Clone, Copy, Debug)]
struct BatchLimits {
    batch_size: i64,
    byte_size: Option<i64>,
    max_buffering_duration: Option<Duration>,
}

/// The timers of [`GroupIntoBatchesFn`].
#[derive(Clone, Copy, Debug)]
enum BatchTimer {
    /// Fires at the end of the window, so a partial batch is not lost.
    EndOfWindow,
    /// Fires the maximum buffering duration after the first value of a batch.
    Buffering,
}

const END_OF_WINDOW: &str = "end_of_window";
const BUFFERING: &str = "buffering";

impl TimerFamilies for BatchTimer {
    fn specs() -> Vec<TimerFamilySpec> {
        vec![
            TimerFamilySpec::event_time(END_OF_WINDOW),
            TimerFamilySpec::processing_time(BUFFERING),
        ]
    }

    fn family(&self) -> &'static str {
        match self {
            Self::EndOfWindow => END_OF_WINDOW,
            Self::Buffering => BUFFERING,
        }
    }

    fn tag(&self) -> &str {
        ""
    }

    fn time_domain(&self) -> TimeDomain {
        match self {
            Self::EndOfWindow => TimeDomain::EventTime,
            Self::Buffering => TimeDomain::ProcessingTime,
        }
    }

    fn from_fired(family: &str, _tag: &str) -> Option<Self> {
        match family {
            END_OF_WINDOW => Some(Self::EndOfWindow),
            BUFFERING => Some(Self::Buffering),
            _ => None,
        }
    }
}

/// The open batch of one key and window.
struct BatchState<V> {
    values: BagState<V>,
    count: ValueState<i64>,
    bytes: ValueState<i64>,
    /// The minimum timestamp of the values, which the end-of-window timer holds.
    min_timestamp: ValueState<Instant>,
}

const VALUES: &str = "values";
const COUNT: &str = "count";
const BYTES: &str = "bytes";
const MIN_TIMESTAMP: &str = "min_timestamp";

impl<V: DefaultCoder> StateCells for BatchState<V> {
    fn specs() -> Vec<Arc<dyn AnyStateSpec>> {
        vec![
            BagState::<V>::spec(VALUES),
            ValueState::<i64>::spec(COUNT),
            ValueState::<i64>::spec(BYTES),
            ValueState::<Instant>::spec(MIN_TIMESTAMP),
        ]
    }

    fn bind(binding: &StateBinding<'_>) -> crate::Result<Self> {
        Ok(Self {
            values: BagState::bind(VALUES, binding)?,
            count: ValueState::bind(COUNT, binding)?,
            bytes: ValueState::bind(BYTES, binding)?,
            min_timestamp: ValueState::bind(MIN_TIMESTAMP, binding)?,
        })
    }
}

impl<V: DefaultCoder> BatchState<V> {
    fn clear(&mut self) -> crate::Result {
        self.values.clear()?;
        self.count.clear()?;
        self.bytes.clear()?;
        self.min_timestamp.clear()
    }
}

/// Buffers the values of each key and window in state, and emits them as batches.
struct GroupIntoBatchesFn<K, V> {
    limits: BatchLimits,
    _marker: PhantomData<fn() -> (K, V)>,
}

impl<K, V> GroupIntoBatchesFn<K, V> {
    fn new(limits: BatchLimits) -> Self {
        Self {
            limits,
            _marker: PhantomData,
        }
    }
}

impl<K, V> Clone for GroupIntoBatchesFn<K, V> {
    fn clone(&self) -> Self {
        Self::new(self.limits)
    }
}

/// The size of `value` in the value coder: the size of one value in the bag.
fn encoded_size<V: DefaultCoder>(value: &V) -> crate::Result<i64> {
    with_scratch_buffer(|buf| value.encode_element(buf).map(|()| buf.len()))
        .map(|len| i64::try_from(len).unwrap_or(i64::MAX))
        .map_err(crate::Error::from)
}

impl<K, V> GroupIntoBatchesFn<K, V>
where
    K: DefaultCoder + Clone,
    V: DefaultCoder,
{
    /// Emits the buffered values, if there are any, and clears the batch state.
    fn flush(
        key: &K,
        state: &mut BatchState<V>,
        ctx: &mut StatefulContext<'_, Self>,
    ) -> crate::Result {
        let values = state.values.read()?;
        state.clear()?;
        match values.is_empty() {
            true => Ok(()),
            // The output owns its key and the callbacks borrow it: one clone per batch.
            false => ctx.emit((key.clone(), values)),
        }
    }

    fn is_full(&self, count: i64, bytes: i64) -> bool {
        count >= self.limits.batch_size || self.limits.byte_size.is_some_and(|max| bytes >= max)
    }
}

impl<K, V> StatefulDoFn for GroupIntoBatchesFn<K, V>
where
    K: DefaultCoder + Clone,
    V: DefaultCoder,
{
    type Key = K;
    type Value = V;
    type Out = (K, Vec<V>);
    type State = BatchState<V>;
    type Timer = BatchTimer;

    fn process_element(
        &mut self,
        key: &K,
        value: V,
        state: &mut BatchState<V>,
        ctx: &mut StatefulContext<'_, Self>,
    ) -> crate::Result {
        let timestamp = ctx.timestamp();
        let size = match self.limits.byte_size {
            Some(_) => encoded_size(&value)?,
            None => 0,
        };
        state.values.append(value)?;
        let count = state.count.read()?.unwrap_or(0) + 1;
        let bytes = state.bytes.read()?.unwrap_or(0).saturating_add(size);

        if self.is_full(count, bytes) {
            if count > 1 {
                ctx.timer(BatchTimer::EndOfWindow).clear()?;
                if self.limits.max_buffering_duration.is_some() {
                    ctx.timer(BatchTimer::Buffering).clear()?;
                }
            }
            return Self::flush(key, state, ctx);
        }

        state.count.write(count)?;
        if self.limits.byte_size.is_some() {
            state.bytes.write(bytes)?;
        }
        // Set the end-of-window timer again when the minimum timestamp moves back, so its
        // hold stays at or before every buffered value.
        let min_timestamp = state.min_timestamp.read()?;
        if min_timestamp.is_none_or(|min| timestamp < min) {
            state.min_timestamp.write(timestamp)?;
            ctx.timer(BatchTimer::EndOfWindow)
                .hold(timestamp)
                .at(ctx.window()?.max_timestamp())?;
        }
        match (count, self.limits.max_buffering_duration) {
            (1, Some(duration)) => ctx
                .timer(BatchTimer::Buffering)
                .hold(timestamp)
                .after(duration),
            _ => Ok(()),
        }
    }

    fn on_timer(
        &mut self,
        timer: BatchTimer,
        key: &K,
        state: &mut BatchState<V>,
        ctx: &mut StatefulContext<'_, Self>,
    ) -> crate::Result {
        match (timer, self.limits.max_buffering_duration) {
            (BatchTimer::EndOfWindow, Some(_)) => ctx.timer(BatchTimer::Buffering).clear()?,
            (BatchTimer::EndOfWindow, None) => {}
            (BatchTimer::Buffering, _) => ctx.timer(BatchTimer::EndOfWindow).clear()?,
        }
        Self::flush(key, state, ctx)
    }
}
