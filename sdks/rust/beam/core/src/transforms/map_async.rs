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

//! Concurrent async element mapping backed by a bounded in-flight task window.

use std::future::Future;
use std::marker::PhantomData;
use std::sync::Arc;

use tokio::task::JoinError;

use super::composite::expand_as;
use super::{DisplayDataBuilder, DoFn, FinishBundleContext, PTransform, ParDo, ProcessContext};
use crate::coders::{DefaultCoder, WindowedHeader};
use crate::runtime::{InFlightWindow, io_runtime};
use crate::values::PCollection;

/// Default maximum number of concurrent calls per bundle.
const DEFAULT_MAX_IN_FLIGHT: usize = 64;

/// Maps each element with an async function, running up to `max_in_flight` calls at once per
/// bundle on the shared async I/O runtime.
///
/// Outputs follow input order under each element's original window, timestamp, pane, and
/// metadata. Unfinished calls abort when a call fails or the bundle ends early.
///
/// ```
/// # use beam_core as beam;
/// use beam::prelude::*;
/// use beam::transforms::MapAsync;
///
/// # let p = Pipeline::default();
/// let ids = p.apply("Ids", Create::new(vec![1_i64, 2, 3]));
/// let doubled = ids.apply(
///     "DoubleAsync",
///     MapAsync::new(|id: i64| async move { Ok(id * 2) }).with_max_in_flight(32),
/// );
/// # let _ = doubled;
/// ```
pub struct MapAsync<In, Out, F, Fut> {
    func: Arc<F>,
    max_in_flight: usize,
    _marker: PhantomData<fn(In) -> (Out, Fut)>,
}

impl<In, Out, F, Fut> Clone for MapAsync<In, Out, F, Fut> {
    fn clone(&self) -> Self {
        Self {
            func: Arc::clone(&self.func),
            max_in_flight: self.max_in_flight,
            _marker: PhantomData,
        }
    }
}

impl<In, Out, F, Fut> MapAsync<In, Out, F, Fut>
where
    F: Fn(In) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = crate::Result<Out>> + Send + 'static,
{
    /// Creates a transform that maps each element with `func`, allowing up to 64 concurrent
    /// calls per bundle.
    pub fn new(func: F) -> Self {
        Self {
            func: Arc::new(func),
            max_in_flight: DEFAULT_MAX_IN_FLIGHT,
            _marker: PhantomData,
        }
    }

    /// Sets the maximum number of concurrent calls per bundle. Must be at least 1.
    pub fn with_max_in_flight(mut self, max_in_flight: usize) -> Self {
        self.max_in_flight = max_in_flight;
        self
    }
}

impl<In, Out, F, Fut> PTransform<PCollection<In>> for MapAsync<In, Out, F, Fut>
where
    In: DefaultCoder,
    Out: DefaultCoder,
    F: Fn(In) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = crate::Result<Out>> + Send + 'static,
{
    type Output = PCollection<Out>;

    fn expand(self, input: &PCollection<In>) -> PCollection<Out> {
        if self.max_in_flight == 0 {
            input
                .pipeline()
                .record_invalid_construction("max_in_flight must be >= 1");
        }
        let func = MapAsyncFn::new(self.func, self.max_in_flight);
        expand_as(input, ParDo::new(func))
    }
}

/// The `DoFn` backing [`MapAsync`]. Each worker copy owns its own [`InFlightWindow`].
struct MapAsyncFn<In, Out, F, Fut> {
    func: Arc<F>,
    max_in_flight: usize,
    in_flight: InFlightWindow<WindowedHeader, crate::Result<Out>>,
    _marker: PhantomData<fn(In) -> Fut>,
}

impl<In, Out, F, Fut> MapAsyncFn<In, Out, F, Fut> {
    fn new(func: Arc<F>, max_in_flight: usize) -> Self {
        Self {
            func,
            max_in_flight,
            in_flight: InFlightWindow::new(max_in_flight, usize::MAX),
            _marker: PhantomData,
        }
    }
}

impl<In, Out, F, Fut> Clone for MapAsyncFn<In, Out, F, Fut> {
    fn clone(&self) -> Self {
        Self::new(Arc::clone(&self.func), self.max_in_flight)
    }
}

impl<In, Out, F, Fut> DoFn for MapAsyncFn<In, Out, F, Fut>
where
    In: DefaultCoder,
    Out: DefaultCoder,
    F: Fn(In) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = crate::Result<Out>> + Send + 'static,
{
    type In = In;
    type Out = Out;

    fn start_bundle(&mut self) -> crate::Result {
        self.in_flight.clear();
        Ok(())
    }

    fn process_element(&mut self, element: In, ctx: &mut ProcessContext<'_, Out>) -> crate::Result {
        let rt = io_runtime()?;
        // `WindowedHeader::clone` copies inline bytes or bumps an `Arc`, with no heap copy.
        let header = ctx.header().clone();
        let fut = {
            let _guard = rt.enter();
            (self.func)(element)
        };
        self.in_flight.spawn(rt.handle(), header, 0, fut);
        // A full window blocks on the oldest task, so send the buffered output first.
        if self.in_flight.is_full() {
            ctx.flush_output()?;
        }
        let drain = self
            .in_flight
            .drain_while_full(|header, result| {
                ctx.output(unwrap_task_result(result)?)
                    .windowed(&header)
                    .emit()
            })
            .and_then(|()| {
                self.in_flight.drain_ready(|header, result| {
                    ctx.output(unwrap_task_result(result)?)
                        .windowed(&header)
                        .emit()
                })
            });
        if drain.is_err() {
            self.in_flight.clear();
        }
        drain
    }

    fn finish_bundle(&mut self, ctx: &mut FinishBundleContext<'_, Out>) -> crate::Result {
        let drain = self.in_flight.drain_all(|header, result| {
            ctx.output_windowed(unwrap_task_result(result)?, &header)
                .emit()
        });
        if drain.is_err() {
            self.in_flight.clear();
        }
        drain
    }

    fn teardown(&mut self) -> crate::Result {
        self.in_flight.clear();
        Ok(())
    }

    fn populate_display_data(&self, builder: &mut DisplayDataBuilder) {
        builder.add_text("transform", "MapAsync");
        builder.add_integer(
            "max_in_flight",
            i64::try_from(self.max_in_flight).unwrap_or(i64::MAX),
        );
    }
}

fn unwrap_task_result<Out>(result: Result<crate::Result<Out>, JoinError>) -> crate::Result<Out> {
    match result {
        Ok(value) => value,
        Err(err) => Err(crate::Error::from(err).context("MapAsync task failed")),
    }
}
