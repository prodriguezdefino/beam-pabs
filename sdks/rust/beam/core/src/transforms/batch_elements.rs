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

//! Micro-batching: [`BatchElements`] groups elements into batch containers (`Vec<E>`, Arrow
//! `RecordBatch`, or custom types) and [`ExplodeBatch`] splits them back.

use std::marker::PhantomData;
use std::time::{Duration, Instant};

use crate::coders::{DefaultCoder, PaneInfo, WindowedHeader};
use crate::internals::{FastHashMap, PerWindow};
use crate::transforms::composite::expand_as;
use crate::transforms::dofn::context::{FinishBundleContext, ProcessContext};
use crate::transforms::{BatchConverter, VecBatchConverter};
use crate::transforms::{DisplayDataBuilder, PTransform};
use crate::transforms::{DoFn, ParDo};
use crate::values::PCollection;

/// Groups elements of a [`PCollection`] into batches by size and maximum duration.
///
/// Each `(window, pane)` pair has its own buffer, so a batch never mixes windows or trigger
/// firings. A batch has the minimum event timestamp of its elements, so it is never later
/// than any of them relative to the watermark. `finish_bundle` emits all open batches.
pub struct BatchElements<E, B = Vec<E>, C = VecBatchConverter<E>> {
    min_batch_size: usize,
    max_batch_size: usize,
    max_batch_duration: Option<Duration>,
    converter: C,
    _marker: PhantomData<(E, B)>,
}

impl<E: Send + Sync + 'static> BatchElements<E, Vec<E>, VecBatchConverter<E>> {
    /// Collects elements into `Vec<E>`.
    ///
    /// # Panics
    ///
    /// Panics if `min_batch_size` is zero or `max_batch_size` is less than `min_batch_size`.
    pub fn new(min_batch_size: usize, max_batch_size: usize) -> Self {
        Self::with_converter(min_batch_size, max_batch_size, VecBatchConverter::new())
    }
}

impl<E, B, C: BatchConverter<E, B>> BatchElements<E, B, C> {
    /// Like [`Self::new`], but with a custom [`BatchConverter`].
    ///
    /// # Panics
    ///
    /// Panics if `min_batch_size` is zero or `max_batch_size` is less than `min_batch_size`.
    pub fn with_converter(min_batch_size: usize, max_batch_size: usize, converter: C) -> Self {
        assert!(min_batch_size >= 1, "min_batch_size must be >= 1");
        assert!(
            max_batch_size >= min_batch_size,
            "max_batch_size must be >= min_batch_size"
        );
        Self {
            min_batch_size,
            max_batch_size,
            max_batch_duration: None,
            converter,
            _marker: PhantomData,
        }
    }

    /// Sets the maximum time that a batch stays open. The check runs only when an element arrives
    /// and the batch has `min_batch_size` elements.
    pub fn with_max_batch_duration(mut self, duration: Duration) -> Self {
        self.max_batch_duration = Some(duration);
        self
    }
}

impl<E, B, C> PTransform<PCollection<E>> for BatchElements<E, B, C>
where
    E: DefaultCoder,
    B: DefaultCoder,
    C: BatchConverter<E, B>,
{
    type Output = PCollection<B>;

    fn expand(&self, input: &PCollection<E>) -> PCollection<B> {
        let do_fn = BatchElementsFn {
            min_batch_size: self.min_batch_size,
            max_batch_size: self.max_batch_size,
            max_batch_duration: self.max_batch_duration,
            converter: self.converter.clone(),
            buffers: FastHashMap::default(),
            _marker: PhantomData,
        };
        expand_as(input, ParDo::new(do_fn))
    }
}

struct ActiveBatch<Buf> {
    buffer: Buf,
    representative_header: WindowedHeader,
    min_timestamp: i64,
    created_at: Instant,
}

struct BatchElementsFn<E, B, C: BatchConverter<E, B>> {
    min_batch_size: usize,
    max_batch_size: usize,
    max_batch_duration: Option<Duration>,
    converter: C,
    buffers: FastHashMap<Vec<u8>, FastHashMap<PaneInfo, ActiveBatch<C::Buffer>>>,
    _marker: PhantomData<(E, B)>,
}

impl<E, B, C: BatchConverter<E, B>> Clone for BatchElementsFn<E, B, C> {
    fn clone(&self) -> Self {
        Self {
            min_batch_size: self.min_batch_size,
            max_batch_size: self.max_batch_size,
            max_batch_duration: self.max_batch_duration,
            converter: self.converter.clone(),
            buffers: FastHashMap::default(),
            _marker: PhantomData,
        }
    }
}

impl<E, B, C> DoFn for BatchElementsFn<E, B, C>
where
    E: DefaultCoder,
    B: DefaultCoder,
    C: BatchConverter<E, B>,
{
    // Batches never mix windows, so a multi-window element is processed once per window.
    type In = PerWindow<E>;
    type Out = B;

    fn start_bundle(&mut self) -> crate::Result {
        self.buffers.clear();
        Ok(())
    }

    fn process_element(
        &mut self,
        PerWindow(element): Self::In,
        ctx: &mut ProcessContext<'_, Self::Out>,
    ) -> crate::Result {
        let pane = ctx.pane();
        let current_timestamp = ctx.timestamp().as_millis();
        let now = Instant::now();
        let window_bytes = ctx.header().window_bytes();

        let pane_map = match self.buffers.get_mut(window_bytes) {
            Some(map) => map,
            None => {
                self.buffers
                    .insert(window_bytes.to_vec(), FastHashMap::default());
                self.buffers
                    .get_mut(window_bytes)
                    .expect("window buffer map was just inserted")
            }
        };

        let entry = pane_map.entry(pane).or_insert_with(|| ActiveBatch {
            buffer: self.converter.create_buffer(),
            representative_header: ctx.header().clone(),
            min_timestamp: current_timestamp,
            created_at: now,
        });

        entry.min_timestamp = entry.min_timestamp.min(current_timestamp);
        self.converter.push(&mut entry.buffer, element)?;

        let current_len = self.converter.buffer_len(&entry.buffer);
        let timeout_reached = self
            .max_batch_duration
            .is_some_and(|max_dur| now.saturating_duration_since(entry.created_at) >= max_dur);

        let should_flush = current_len >= self.max_batch_size
            || (current_len >= self.min_batch_size && timeout_reached);
        if should_flush && let Some(active) = pane_map.remove(&pane) {
            let (batch, header) = Self::close_batch(&self.converter, active)?;
            ctx.output(batch).windowed(&header).emit()?;
        }
        Ok(())
    }

    fn finish_bundle(&mut self, ctx: &mut FinishBundleContext<'_, Self::Out>) -> crate::Result {
        let converter = &self.converter;
        self.buffers
            .drain()
            .flat_map(|(_, pane_map)| pane_map.into_values())
            .try_for_each(|active| {
                let (batch, header) = Self::close_batch(converter, active)?;
                ctx.output_windowed(batch, &header).emit()
            })
    }

    fn teardown(&mut self) -> crate::Result {
        self.buffers.clear();
        Ok(())
    }

    fn populate_display_data(&self, builder: &mut DisplayDataBuilder) {
        builder.add_integer("min_batch_size", self.min_batch_size as i64);
        builder.add_integer("max_batch_size", self.max_batch_size as i64);
        if let Some(dur) = self.max_batch_duration {
            builder.add_text("max_batch_duration", format!("{dur:?}"));
        }
    }
}

impl<E, B: DefaultCoder, C: BatchConverter<E, B>> BatchElementsFn<E, B, C> {
    /// The finished batch and the header it is emitted under: its window, at its earliest
    /// timestamp.
    fn close_batch(
        converter: &C,
        active: ActiveBatch<C::Buffer>,
    ) -> crate::Result<(B, WindowedHeader)> {
        let batch = converter.finish_batch(active.buffer)?;
        let header = active.representative_header.rebuilt(
            active.min_timestamp,
            active.representative_header.pane(),
            &active.representative_header.metadata(),
        );
        Ok((batch, header))
    }
}

/// Splits batch containers back into single elements.
pub struct ExplodeBatch<B, E, C = VecBatchConverter<E>> {
    converter: C,
    _marker: PhantomData<(B, E)>,
}

impl<E, B, C: BatchConverter<E, B>> ExplodeBatch<B, E, C> {
    /// Creates an `ExplodeBatch` transform with a custom [`BatchConverter`].
    pub fn new(converter: C) -> Self {
        Self {
            converter,
            _marker: PhantomData,
        }
    }
}

impl<B, E, C> PTransform<PCollection<B>> for ExplodeBatch<B, E, C>
where
    B: DefaultCoder,
    E: DefaultCoder,
    C: BatchConverter<E, B>,
{
    type Output = PCollection<E>;

    fn expand(&self, input: &PCollection<B>) -> PCollection<E> {
        let do_fn = ExplodeBatchFn {
            converter: self.converter.clone(),
            _marker: PhantomData,
        };
        expand_as(input, ParDo::new(do_fn))
    }
}

struct ExplodeBatchFn<B, E, C> {
    converter: C,
    _marker: PhantomData<(B, E)>,
}

impl<B, E, C: Clone> Clone for ExplodeBatchFn<B, E, C> {
    fn clone(&self) -> Self {
        Self {
            converter: self.converter.clone(),
            _marker: PhantomData,
        }
    }
}

impl<B, E, C> DoFn for ExplodeBatchFn<B, E, C>
where
    B: DefaultCoder,
    E: DefaultCoder,
    C: BatchConverter<E, B>,
{
    type In = B;
    type Out = E;

    fn process_element(
        &mut self,
        batch: Self::In,
        ctx: &mut ProcessContext<'_, Self::Out>,
    ) -> crate::Result {
        self.converter
            .explode(batch)?
            .into_iter()
            .try_for_each(|elem| ctx.emit(elem))
    }
}
