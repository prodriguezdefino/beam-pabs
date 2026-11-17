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

//! Gives each element the event timestamp that a function computes from the element.

use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;

use super::composite::expand_as;
use super::{DisplayDataBuilder, DoFn, PTransform, ParDo, ProcessContext};
use crate::Instant;
use crate::coders::DefaultCoder;
use crate::values::PCollection;

/// Gives each element the event timestamp that a function returns for it.
///
/// The elements stay in their windows. Apply a
/// [`WindowInto`](crate::windowing::WindowInto) after this transform to assign windows from
/// the new timestamps. The output keeps the coder of the input.
///
/// A timestamp earlier than the timestamp of the input element fails the bundle. To permit
/// it, set [`with_allowed_timestamp_skew`](Self::with_allowed_timestamp_skew).
///
/// ```
/// # use beam_core as beam;
/// use beam::prelude::*;
/// use beam::transforms::WithTimestamps;
///
/// # let p = Pipeline::default();
/// let millis = p.apply("Millis", Create::new(vec![1_000_i64, 2_000]));
/// let timestamped =
///     millis.apply("Timestamps", WithTimestamps::new(|ms: &i64| Instant::from_millis(*ms)));
/// # let _ = timestamped;
/// ```
pub struct WithTimestamps<T, F> {
    timestamp_fn: Arc<F>,
    allowed_timestamp_skew: Duration,
    _marker: PhantomData<fn(&T)>,
}

impl<T, F> Clone for WithTimestamps<T, F> {
    fn clone(&self) -> Self {
        Self {
            timestamp_fn: Arc::clone(&self.timestamp_fn),
            allowed_timestamp_skew: self.allowed_timestamp_skew,
            _marker: PhantomData,
        }
    }
}

impl<T, F> WithTimestamps<T, F>
where
    F: Fn(&T) -> Instant + Send + Sync + 'static,
{
    /// Creates a transform that gives each element the timestamp from `timestamp_fn`.
    pub fn new(timestamp_fn: F) -> Self {
        Self {
            timestamp_fn: Arc::new(timestamp_fn),
            allowed_timestamp_skew: Duration::ZERO,
            _marker: PhantomData,
        }
    }

    /// Sets how far before the timestamp of the input element a new timestamp can be. The
    /// default is zero. `Duration::MAX` permits each timestamp from [`Instant::MIN`].
    ///
    /// An element before the watermark is late, and a later step can drop it.
    pub fn with_allowed_timestamp_skew(mut self, skew: Duration) -> Self {
        self.allowed_timestamp_skew = skew;
        self
    }
}

/// The function of [`WithTimestamps`]. Copies share the closure.
struct AssignTimestampsFn<T, F> {
    timestamp_fn: Arc<F>,
    allowed_timestamp_skew: Duration,
    _marker: PhantomData<fn(&T)>,
}

impl<T, F> Clone for AssignTimestampsFn<T, F> {
    fn clone(&self) -> Self {
        Self {
            timestamp_fn: Arc::clone(&self.timestamp_fn),
            allowed_timestamp_skew: self.allowed_timestamp_skew,
            _marker: PhantomData,
        }
    }
}

impl<T: DefaultCoder, F> DoFn for AssignTimestampsFn<T, F>
where
    F: Fn(&T) -> Instant + Send + Sync + 'static,
{
    type In = T;
    type Out = T;

    fn process_element(&mut self, element: T, ctx: &mut ProcessContext<'_, T>) -> crate::Result {
        let timestamp = (self.timestamp_fn)(&element);
        ctx.output(element).at(timestamp).emit()
    }

    fn allowed_timestamp_skew(&self) -> Duration {
        self.allowed_timestamp_skew
    }

    fn populate_display_data(&self, builder: &mut DisplayDataBuilder) {
        builder.add_text("transform", "WithTimestamps");
    }
}

impl<T: DefaultCoder, F> PTransform<PCollection<T>> for WithTimestamps<T, F>
where
    F: Fn(&T) -> Instant + Send + Sync + 'static,
{
    type Output = PCollection<T>;

    fn expand(self, input: &PCollection<T>) -> PCollection<T> {
        let Self {
            timestamp_fn,
            allowed_timestamp_skew,
            _marker,
        } = self;
        let func = AssignTimestampsFn {
            timestamp_fn,
            allowed_timestamp_skew,
            _marker,
        };
        expand_as(input, ParDo::new(func)).with_coder_of(input)
    }
}
