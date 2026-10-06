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

//! Batched DoFns and the converters between elements and batches.

use std::fmt;
use std::marker::PhantomData;

use super::context::ProcessContext;
use super::pardo::DoFn;
use crate::coders::DefaultCoder;
use crate::transforms::DisplayDataBuilder;

/// Converts between individual elements of type `E` and batch representations of type `B`.
pub trait BatchConverter<E, B>: Clone + Send + Sync + 'static {
    /// Buffer that collects elements into a batch.
    type Buffer: Send + Sync + 'static;

    fn create_buffer(&self) -> Self::Buffer;

    /// Appends one element to the buffer.
    fn push(&self, buffer: &mut Self::Buffer, element: E) -> crate::Result;

    /// Returns the number of elements in the buffer.
    fn buffer_len(&self, buffer: &Self::Buffer) -> usize;

    /// Converts the buffer into a batch of type `B`.
    fn finish_batch(&self, buffer: Self::Buffer) -> crate::Result<B>;

    /// Splits a batch `B` into individual elements of type `E`.
    fn explode(&self, batch: B) -> crate::Result<Vec<E>>;
}

/// Default batch converter that collects elements in a `Vec<E>`.
pub struct VecBatchConverter<E> {
    _marker: PhantomData<E>,
}

impl<E> VecBatchConverter<E> {
    pub const fn new() -> Self {
        Self {
            _marker: PhantomData,
        }
    }
}

impl<E> Clone for VecBatchConverter<E> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<E> Copy for VecBatchConverter<E> {}

impl<E> Default for VecBatchConverter<E> {
    fn default() -> Self {
        Self::new()
    }
}

impl<E> fmt::Debug for VecBatchConverter<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("VecBatchConverter").finish()
    }
}

impl<E: Send + Sync + 'static> BatchConverter<E, Vec<E>> for VecBatchConverter<E> {
    type Buffer = Vec<E>;

    fn create_buffer(&self) -> Self::Buffer {
        Vec::new()
    }

    fn push(&self, buffer: &mut Self::Buffer, element: E) -> crate::Result {
        buffer.push(element);
        Ok(())
    }

    fn buffer_len(&self, buffer: &Self::Buffer) -> usize {
        buffer.len()
    }

    fn finish_batch(&self, buffer: Self::Buffer) -> crate::Result<Vec<E>> {
        Ok(buffer)
    }

    fn explode(&self, batch: Vec<E>) -> crate::Result<Vec<E>> {
        Ok(batch)
    }
}

/// Processes a batch of elements at once, for SIMD, hardware acceleration or batched remote
/// service calls.
pub trait BatchedDoFn: Clone + Send + Sync + 'static {
    /// The batch type consumed by this transform.
    type InBatch: DefaultCoder;
    /// The batch type produced by this transform.
    type OutBatch: DefaultCoder;

    /// Called once when the bundle processor owning this copy is created.
    fn setup(&mut self) -> crate::Result {
        Ok(())
    }

    /// Called once before the first batch of a bundle.
    fn start_bundle(&mut self) -> crate::Result {
        Ok(())
    }

    /// Processes one batch of elements and emits zero or more output batches.
    fn process_batch(
        &mut self,
        batch: Self::InBatch,
        ctx: &mut ProcessContext<'_, Self::OutBatch>,
    ) -> crate::Result;

    /// Called once after the last batch of a bundle.
    fn finish_bundle(&mut self, ctx: &mut ProcessContext<'_, Self::OutBatch>) -> crate::Result {
        let _ = ctx;
        Ok(())
    }

    /// Called once when the bundle processor owning this copy is discarded.
    fn teardown(&mut self) -> crate::Result {
        Ok(())
    }

    /// Adds display data for runner UIs.
    fn populate_display_data(&self, _builder: &mut DisplayDataBuilder) {}
}

/// Adapts a [`BatchedDoFn`] into a [`DoFn`] whose input and output elements are batches.
#[derive(Clone, Debug)]
pub struct BatchedDoFnAdapter<F> {
    inner: F,
}

impl<F: BatchedDoFn> BatchedDoFnAdapter<F> {
    pub fn new(inner: F) -> Self {
        Self { inner }
    }

    pub fn into_inner(self) -> F {
        self.inner
    }
}

impl<F: BatchedDoFn> DoFn for BatchedDoFnAdapter<F> {
    type In = F::InBatch;
    type Out = F::OutBatch;

    fn setup(&mut self) -> crate::Result {
        self.inner.setup()
    }

    fn start_bundle(&mut self) -> crate::Result {
        self.inner.start_bundle()
    }

    fn process_element(
        &mut self,
        batch: Self::In,
        ctx: &mut ProcessContext<'_, Self::Out>,
    ) -> crate::Result {
        self.inner.process_batch(batch, ctx)
    }

    fn finish_bundle(&mut self, ctx: &mut ProcessContext<'_, Self::Out>) -> crate::Result {
        self.inner.finish_bundle(ctx)
    }

    fn teardown(&mut self) -> crate::Result {
        self.inner.teardown()
    }

    fn populate_display_data(&self, builder: &mut DisplayDataBuilder) {
        self.inner.populate_display_data(builder);
    }
}
