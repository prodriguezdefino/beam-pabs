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

//! Fluent method syntax for element-wise and collection-level transforms.

use std::collections::HashMap;

use beam::coders::DefaultCoder;
#[cfg(doc)]
use beam::transforms::Windowed;
use beam::transforms::{
    BatchElements, BatchedDoFn, BatchedDoFnAdapter, DoFn, DoFnInput, ExplodeBatch, Failure, Filter,
    FlatMap, Flatten, Inspect, Map, Outputs, ParDo, Partition, ProcessContext, Reshuffle, TryMap,
    VecBatchConverter, WithFailures, WithKeys,
};
use beam::values::{IntoSideInputs, PCollection, PCollectionList, SideInputs};
use beam::windowing::{WindowFn, WindowInto};

use crate::composite::composite;

/// Shorthand methods for element-wise and collection-level transforms.
pub trait PCollectionExt<T: DefaultCoder> {
    /// Applies a function to every element, producing one output per element.
    fn map<U, F>(&self, name: impl Into<String>, f: F) -> PCollection<U>
    where
        U: DefaultCoder,
        F: Fn(T) -> U + Send + Sync + 'static;

    /// Maps elements with a fallible function and routes `Err` inputs to a dead-letter
    /// output. Shorthand for [`TryMap::new`].
    ///
    /// On `Err`, the input and the `Display` text of the error become a [`Failure`] in
    /// [`failures`](WithFailures::failures). The bundle does not fail. For a custom
    /// failure element, use `.apply(name, TryMap::new(f).exceptions_via(handler))`.
    ///
    /// ```ignore
    /// lines
    ///     .try_map("Parse", |s: &String| s.parse::<i64>())
    ///     .failures_to("DeadLetters", dead_letter_sink)
    ///     .map("Double", |n| n * 2);
    /// ```
    fn try_map<U, E, F>(&self, name: impl Into<String>, f: F) -> WithFailures<U, Failure<T>>
    where
        U: DefaultCoder,
        E: std::fmt::Display + 'static,
        F: Fn(&T) -> Result<U, E> + Send + Sync + 'static;

    /// Applies a function producing an iterator for each element and flattens results.
    fn flat_map<U, Iter, F>(&self, name: impl Into<String>, f: F) -> PCollection<U>
    where
        U: DefaultCoder,
        Iter: IntoIterator<Item = U>,
        F: Fn(T) -> Iter + Send + Sync + 'static;

    /// Keeps elements that satisfy a predicate.
    fn filter<F>(&self, name: impl Into<String>, predicate: F) -> PCollection<T>
    where
        F: Fn(&T) -> bool + Send + Sync + 'static;

    /// Runs a custom [`DoFn`] with bundle lifecycle hooks and [`ProcessContext`]. Returns what
    /// its [`Outputs`] declare: a `PCollection` for a single output.
    fn par_do<D>(&self, name: impl Into<String>, do_fn: D) -> <D::Out as Outputs>::Collections
    where
        D: DoFn,
        D::In: DoFnInput<Value = T>;

    /// Runs a closure with [`ProcessContext`] for each element. Take [`Windowed<T>`] to also
    /// receive the window.
    fn par_do_fn<In, Out, F>(&self, name: impl Into<String>, f: F) -> Out::Collections
    where
        In: DoFnInput<Value = T>,
        Out: Outputs,
        F: Fn(In, &mut ProcessContext<'_, Out>) -> beam::Result + Send + Sync + 'static;

    /// [`par_do_fn`](Self::par_do_fn) with the values of `side_inputs`. See [`SideInputs`].
    fn par_do_fn_with_side_inputs<In, Out, V, F>(
        &self,
        name: impl Into<String>,
        side_inputs: V,
        f: F,
    ) -> Out::Collections
    where
        In: DoFnInput<Value = T>,
        Out: Outputs,
        V: IntoSideInputs,
        F: for<'s> Fn(
                In,
                <V::Owned as SideInputs>::Values<'s>,
                &mut ProcessContext<'_, Out>,
            ) -> beam::Result
            + Send
            + Sync
            + 'static;

    /// Maps each element with the values of `side_inputs`. See [`SideInputs`].
    fn map_with_side_inputs<U, V, F>(
        &self,
        name: impl Into<String>,
        side_inputs: V,
        f: F,
    ) -> PCollection<U>
    where
        U: DefaultCoder,
        V: IntoSideInputs,
        F: for<'s> Fn(T, <V::Owned as SideInputs>::Values<'s>) -> beam::Result<U>
            + Send
            + Sync
            + 'static;

    /// Inspects each element by reference without modifying elements.
    fn inspect<F>(&self, name: impl Into<String>, f: F) -> PCollection<T>
    where
        F: Fn(&T) + Send + Sync + 'static;

    /// Inserts a redistribution barrier between pipeline stages.
    fn reshuffle(&self, name: impl Into<String>) -> PCollection<T>;

    /// Partitions elements into `num_partitions` output collections.
    fn partition<F>(
        &self,
        name: impl Into<String>,
        num_partitions: usize,
        partition_fn: F,
    ) -> PCollectionList<T>
    where
        F: Fn(&T) -> usize + Send + Sync + 'static;

    /// Keys each element with `key_fn(&element)`, producing `(K, T)` pairs.
    fn key_by<K, F>(&self, name: impl Into<String>, key_fn: F) -> PCollection<(K, T)>
    where
        K: DefaultCoder,
        F: Fn(&T) -> K + Send + Sync + 'static;

    /// Assigns elements into windows using the specified [`WindowFn`].
    fn window_into<W: WindowFn + Clone>(
        &self,
        name: impl Into<String>,
        window_fn: W,
    ) -> PCollection<T>;

    /// Merges this collection with other collections into a single `PCollection<T>`.
    fn flatten<I: FlattenInputs<T>>(&self, name: impl Into<String>, others: I) -> PCollection<T>;

    /// Batches elements up to `max_batch_size` within bundle and window boundaries.
    fn batch_elements(&self, name: impl Into<String>, max_batch_size: usize) -> PCollection<Vec<T>>
    where
        Vec<T>: DefaultCoder;

    /// Batches elements up to `max_batch_size` and processes batches with a [`BatchedDoFn`].
    fn par_do_batch<OutBatch, D>(
        &self,
        name: impl Into<String>,
        max_batch_size: usize,
        do_fn: D,
    ) -> PCollection<OutBatch>
    where
        Vec<T>: DefaultCoder,
        OutBatch: DefaultCoder,
        D: BatchedDoFn<InBatch = Vec<T>, OutBatch = OutBatch>;

    /// Batches elements, runs a [`BatchedDoFn`], and explodes batches into an element stream.
    fn par_do_batch_elementwise<Out, D>(
        &self,
        name: impl Into<String>,
        max_batch_size: usize,
        do_fn: D,
    ) -> PCollection<Out>
    where
        Vec<T>: DefaultCoder,
        Vec<Out>: DefaultCoder,
        Out: DefaultCoder,
        D: BatchedDoFn<InBatch = Vec<T>, OutBatch = Vec<Out>>;
}

impl<T: DefaultCoder> PCollectionExt<T> for PCollection<T> {
    fn map<U, F>(&self, name: impl Into<String>, f: F) -> PCollection<U>
    where
        U: DefaultCoder,
        F: Fn(T) -> U + Send + Sync + 'static,
    {
        self.apply(name, Map::new(f))
    }

    fn try_map<U, E, F>(&self, name: impl Into<String>, f: F) -> WithFailures<U, Failure<T>>
    where
        U: DefaultCoder,
        E: std::fmt::Display + 'static,
        F: Fn(&T) -> Result<U, E> + Send + Sync + 'static,
    {
        self.apply(name, TryMap::new(f))
    }

    fn flat_map<U, Iter, F>(&self, name: impl Into<String>, f: F) -> PCollection<U>
    where
        U: DefaultCoder,
        Iter: IntoIterator<Item = U>,
        F: Fn(T) -> Iter + Send + Sync + 'static,
    {
        self.apply(name, FlatMap::new(f))
    }

    fn filter<F>(&self, name: impl Into<String>, predicate: F) -> PCollection<T>
    where
        F: Fn(&T) -> bool + Send + Sync + 'static,
    {
        self.apply(name, Filter::new(predicate))
    }

    fn par_do<D>(&self, name: impl Into<String>, do_fn: D) -> <D::Out as Outputs>::Collections
    where
        D: DoFn,
        D::In: DoFnInput<Value = T>,
    {
        self.apply(name, ParDo::new(do_fn))
    }

    fn par_do_fn<In, Out, F>(&self, name: impl Into<String>, f: F) -> Out::Collections
    where
        In: DoFnInput<Value = T>,
        Out: Outputs,
        F: Fn(In, &mut ProcessContext<'_, Out>) -> beam::Result + Send + Sync + 'static,
    {
        self.apply(name, ParDo::from_fn(f))
    }

    fn par_do_fn_with_side_inputs<In, Out, V, F>(
        &self,
        name: impl Into<String>,
        side_inputs: V,
        f: F,
    ) -> Out::Collections
    where
        In: DoFnInput<Value = T>,
        Out: Outputs,
        V: IntoSideInputs,
        F: for<'s> Fn(
                In,
                <V::Owned as SideInputs>::Values<'s>,
                &mut ProcessContext<'_, Out>,
            ) -> beam::Result
            + Send
            + Sync
            + 'static,
    {
        self.apply(name, ParDo::from_fn_with_side_inputs(side_inputs, f))
    }

    fn map_with_side_inputs<U, V, F>(
        &self,
        name: impl Into<String>,
        side_inputs: V,
        f: F,
    ) -> PCollection<U>
    where
        U: DefaultCoder,
        V: IntoSideInputs,
        F: for<'s> Fn(T, <V::Owned as SideInputs>::Values<'s>) -> beam::Result<U>
            + Send
            + Sync
            + 'static,
    {
        self.par_do_fn_with_side_inputs(name, side_inputs, move |elem: T, values, ctx| {
            ctx.emit(f(elem, values)?)
        })
    }

    fn inspect<F>(&self, name: impl Into<String>, f: F) -> PCollection<T>
    where
        F: Fn(&T) + Send + Sync + 'static,
    {
        self.apply(name, Inspect::new(f))
    }

    fn reshuffle(&self, name: impl Into<String>) -> PCollection<T> {
        self.apply(name, Reshuffle::new())
    }

    fn partition<F>(
        &self,
        name: impl Into<String>,
        num_partitions: usize,
        partition_fn: F,
    ) -> PCollectionList<T>
    where
        F: Fn(&T) -> usize + Send + Sync + 'static,
    {
        self.apply(name, Partition::new(num_partitions, partition_fn))
    }

    fn key_by<K, F>(&self, name: impl Into<String>, key_fn: F) -> PCollection<(K, T)>
    where
        K: DefaultCoder,
        F: Fn(&T) -> K + Send + Sync + 'static,
    {
        self.apply(name, WithKeys::new(key_fn))
    }

    fn window_into<W: WindowFn + Clone>(
        &self,
        name: impl Into<String>,
        window_fn: W,
    ) -> PCollection<T> {
        self.apply(name, WindowInto::new(window_fn))
    }

    fn flatten<I: FlattenInputs<T>>(&self, name: impl Into<String>, others: I) -> PCollection<T> {
        others.into_list(self.clone()).apply(name, Flatten::new())
    }

    fn batch_elements(&self, name: impl Into<String>, max_batch_size: usize) -> PCollection<Vec<T>>
    where
        Vec<T>: DefaultCoder,
    {
        self.apply(name, BatchElements::new(1, max_batch_size))
    }

    fn par_do_batch<OutBatch, D>(
        &self,
        name: impl Into<String>,
        max_batch_size: usize,
        do_fn: D,
    ) -> PCollection<OutBatch>
    where
        Vec<T>: DefaultCoder,
        OutBatch: DefaultCoder,
        D: BatchedDoFn<InBatch = Vec<T>, OutBatch = OutBatch>,
    {
        composite(self.pipeline(), &name.into(), main_input(self), || {
            self.apply("Batch", BatchElements::new(1, max_batch_size))
                .apply("Process", ParDo::new(BatchedDoFnAdapter::new(do_fn)))
        })
    }

    fn par_do_batch_elementwise<Out, D>(
        &self,
        name: impl Into<String>,
        max_batch_size: usize,
        do_fn: D,
    ) -> PCollection<Out>
    where
        Vec<T>: DefaultCoder,
        Vec<Out>: DefaultCoder,
        Out: DefaultCoder,
        D: BatchedDoFn<InBatch = Vec<T>, OutBatch = Vec<Out>>,
    {
        composite(self.pipeline(), &name.into(), main_input(self), || {
            self.apply("Batch", BatchElements::new(1, max_batch_size))
                .apply("Compute", ParDo::new(BatchedDoFnAdapter::new(do_fn)))
                .apply("Explode", ExplodeBatch::new(VecBatchConverter::new()))
        })
    }
}

/// Converts one `&PCollection<T>` or a slice of `&PCollection<T>` into a [`PCollectionList<T>`].
pub trait FlattenInputs<T: DefaultCoder> {
    /// Prepends `first` and returns the combined [`PCollectionList<T>`].
    fn into_list(self, first: PCollection<T>) -> PCollectionList<T>;
}

impl<T: DefaultCoder> FlattenInputs<T> for &PCollection<T> {
    fn into_list(self, first: PCollection<T>) -> PCollectionList<T> {
        PCollectionList::of(first).and(self.clone())
    }
}

impl<T: DefaultCoder> FlattenInputs<T> for &[&PCollection<T>] {
    fn into_list(self, first: PCollection<T>) -> PCollectionList<T> {
        self.iter().fold(PCollectionList::of(first), |list, col| {
            list.and((*col).clone())
        })
    }
}

impl<T: DefaultCoder, const N: usize> FlattenInputs<T> for &[&PCollection<T>; N] {
    fn into_list(self, first: PCollection<T>) -> PCollectionList<T> {
        self.as_slice().into_list(first)
    }
}

/// The inputs of a composite with one main input.
fn main_input<T: 'static>(input: &PCollection<T>) -> HashMap<String, String> {
    HashMap::from([("in".to_string(), input.id().to_string())])
}
