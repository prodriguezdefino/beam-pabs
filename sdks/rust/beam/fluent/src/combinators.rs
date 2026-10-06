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

//! Fluent method syntax and combinator extension traits for Apache Beam collections.

use std::hash::Hash;
use std::marker::PhantomData;

use beam::coders::DefaultCoder;
#[cfg(doc)]
use beam::transforms::Windowed;
use beam::transforms::{
    BatchElements, BatchedDoFn, BatchedDoFnAdapter, CombineFn, CombineGlobally, CountGlobally,
    CountPerElement, DoFn, DoFnInput, ExplodeBatch, Failure, Filter, FlatMap, Flatten, Inspect,
    Map, Outputs, ParDo, Partition, ProcessContext, Reshuffle, TryMap, VecBatchConverter,
    WithFailures,
};
use beam::values::{IntoSideInputs, PCollection, PCollectionList, SideInputs};
use beam::windowing::{WindowFn, WindowInto};

pub use crate::keyed::PCollectionKeyedExt;

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
    /// failure element, use `.apply(TryMap::new(name, f).exceptions_via(handler))`.
    ///
    /// ```ignore
    /// lines
    ///     .try_map("Parse", |s: &String| s.parse::<i64>())
    ///     .failures_to(dead_letter_sink)
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

    /// Counts occurrences of each distinct element.
    fn count_per_element(&self, name: impl Into<String>) -> PCollection<(T, i64)>
    where
        T: Eq + Hash;

    /// Combines all elements globally using an associative [`CombineFn`].
    fn combine_globally<CF>(
        &self,
        name: impl Into<String>,
        combine_fn: CF,
    ) -> PCollection<CF::Output>
    where
        CF: CombineFn<Input = T>;

    /// Combines elements globally without inserting a default value for empty inputs.
    ///
    /// Unlike [`combine_globally`](Self::combine_globally), this method works on
    /// PCollections that are not globally windowed (for example fixed or sliding windows).
    fn combine_globally_without_defaults<CF>(
        &self,
        name: impl Into<String>,
        combine_fn: CF,
    ) -> PCollection<CF::Output>
    where
        CF: CombineFn<Input = T>;

    /// Combines elements globally using an associative fold, with partial combining and
    /// combiner lifting.
    fn fold_globally<Accum, Fold, Merge>(
        &self,
        name: impl Into<String>,
        zero: Accum,
        fold_fn: Fold,
        merge_fn: Merge,
    ) -> PCollection<Accum>
    where
        Accum: DefaultCoder + Clone + Send + Sync + 'static,
        Fold: Fn(Accum, T) -> Accum + Send + Sync + 'static,
        Merge: Fn(Accum, Accum) -> Accum + Send + Sync + 'static;

    /// Counts the total number of elements in the collection globally.
    fn count_globally(&self, name: impl Into<String>) -> PCollection<i64>;

    /// Inspects each element by reference without modifying elements.
    fn inspect<F>(&self, name: impl Into<String>, f: F) -> PCollection<T>
    where
        F: Fn(&T) + Send + Sync + 'static;

    /// Inserts a redistribution barrier between pipeline stages.
    fn reshuffle(&self, name: impl Into<String>) -> PCollection<T>
    where
        T: Clone;

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

impl<T: DefaultCoder> PCollectionExt<T> for PCollection<T> {
    fn map<U, F>(&self, name: impl Into<String>, f: F) -> PCollection<U>
    where
        U: DefaultCoder,
        F: Fn(T) -> U + Send + Sync + 'static,
    {
        self.apply(Map::new(name, f))
    }

    fn try_map<U, E, F>(&self, name: impl Into<String>, f: F) -> WithFailures<U, Failure<T>>
    where
        U: DefaultCoder,
        E: std::fmt::Display + 'static,
        F: Fn(&T) -> Result<U, E> + Send + Sync + 'static,
    {
        self.apply(TryMap::new(name, f))
    }

    fn flat_map<U, Iter, F>(&self, name: impl Into<String>, f: F) -> PCollection<U>
    where
        U: DefaultCoder,
        Iter: IntoIterator<Item = U>,
        F: Fn(T) -> Iter + Send + Sync + 'static,
    {
        self.apply(FlatMap::new(name, f))
    }

    fn filter<F>(&self, name: impl Into<String>, predicate: F) -> PCollection<T>
    where
        F: Fn(&T) -> bool + Send + Sync + 'static,
    {
        self.apply(Filter::new(name, predicate))
    }

    fn par_do<D>(&self, name: impl Into<String>, do_fn: D) -> <D::Out as Outputs>::Collections
    where
        D: DoFn,
        D::In: DoFnInput<Value = T>,
    {
        self.apply(ParDo::new(name, do_fn))
    }

    fn par_do_fn<In, Out, F>(&self, name: impl Into<String>, f: F) -> Out::Collections
    where
        In: DoFnInput<Value = T>,
        Out: Outputs,
        F: Fn(In, &mut ProcessContext<'_, Out>) -> beam::Result + Send + Sync + 'static,
    {
        self.apply(ParDo::from_fn(name, f))
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
        self.apply(ParDo::from_fn_with_side_inputs(name, side_inputs, f))
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

    fn count_per_element(&self, name: impl Into<String>) -> PCollection<(T, i64)>
    where
        T: Eq + Hash,
    {
        self.apply(CountPerElement::new(name))
    }

    fn combine_globally<CF>(
        &self,
        name: impl Into<String>,
        combine_fn: CF,
    ) -> PCollection<CF::Output>
    where
        CF: CombineFn<Input = T>,
    {
        self.apply(CombineGlobally::new(name, combine_fn))
    }

    fn combine_globally_without_defaults<CF>(
        &self,
        name: impl Into<String>,
        combine_fn: CF,
    ) -> PCollection<CF::Output>
    where
        CF: CombineFn<Input = T>,
    {
        self.apply(CombineGlobally::new(name, combine_fn).without_defaults())
    }

    fn fold_globally<Accum, Fold, Merge>(
        &self,
        name: impl Into<String>,
        zero: Accum,
        fold_fn: Fold,
        merge_fn: Merge,
    ) -> PCollection<Accum>
    where
        Accum: DefaultCoder + Clone + Send + Sync + 'static,
        Fold: Fn(Accum, T) -> Accum + Send + Sync + 'static,
        Merge: Fn(Accum, Accum) -> Accum + Send + Sync + 'static,
    {
        self.combine_globally(name, FoldCombineFn::new(zero, fold_fn, merge_fn))
    }

    fn count_globally(&self, name: impl Into<String>) -> PCollection<i64> {
        self.apply(CountGlobally::new(name))
    }

    fn inspect<F>(&self, name: impl Into<String>, f: F) -> PCollection<T>
    where
        F: Fn(&T) + Send + Sync + 'static,
    {
        self.apply(Inspect::new(name, f))
    }

    fn reshuffle(&self, name: impl Into<String>) -> PCollection<T>
    where
        T: Clone,
    {
        self.apply(Reshuffle::new(name))
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
        self.apply(Partition::new(name, num_partitions, partition_fn))
    }

    fn key_by<K, F>(&self, name: impl Into<String>, key_fn: F) -> PCollection<(K, T)>
    where
        K: DefaultCoder,
        F: Fn(&T) -> K + Send + Sync + 'static,
    {
        self.map(name, move |elem: T| {
            let k = key_fn(&elem);
            (k, elem)
        })
    }

    fn window_into<W: WindowFn + Clone>(
        &self,
        name: impl Into<String>,
        window_fn: W,
    ) -> PCollection<T> {
        self.apply(WindowInto::new(name, window_fn))
    }

    fn flatten<I: FlattenInputs<T>>(&self, name: impl Into<String>, others: I) -> PCollection<T> {
        others.into_list(self.clone()).apply(Flatten::new(name))
    }

    fn batch_elements(&self, name: impl Into<String>, max_batch_size: usize) -> PCollection<Vec<T>>
    where
        Vec<T>: DefaultCoder,
    {
        self.apply(BatchElements::new(name, 1, max_batch_size))
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
        let name_str = name.into();
        let batched = self.apply(BatchElements::new(
            format!("{name_str}/Batch"),
            1,
            max_batch_size,
        ));
        batched.apply(ParDo::new(name_str, BatchedDoFnAdapter::new(do_fn)))
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
        let name_str = name.into();
        let batched = self.apply(BatchElements::new(
            format!("{name_str}/Batch"),
            1,
            max_batch_size,
        ));
        let computed = batched.apply(ParDo::new(
            format!("{name_str}/Compute"),
            BatchedDoFnAdapter::new(do_fn),
        ));
        computed.apply(ExplodeBatch::new(
            format!("{name_str}/Explode"),
            VecBatchConverter::new(),
        ))
    }
}

/// Combinator methods for collection lists.
pub trait PCollectionListExt<T: DefaultCoder> {
    /// Merges all collections in this list into a single collection.
    fn flatten(&self, name: impl Into<String>) -> PCollection<T>;
}

impl<T: DefaultCoder> PCollectionListExt<T> for PCollectionList<T> {
    fn flatten(&self, name: impl Into<String>) -> PCollection<T> {
        self.apply(Flatten::new(name))
    }
}

/// A [`CombineFn`] built from an initial accumulator value, fold closure, and merge closure.
#[derive(Clone)]
pub struct FoldCombineFn<Accum, Input, F, M> {
    zero: Accum,
    fold_fn: F,
    merge_fn: M,
    _marker: PhantomData<Input>,
}

impl<Accum, Input, F, M> FoldCombineFn<Accum, Input, F, M>
where
    Accum: DefaultCoder + Clone + Send + Sync + 'static,
    Input: DefaultCoder + Send + Sync + 'static,
    F: Fn(Accum, Input) -> Accum + Send + Sync + 'static,
    M: Fn(Accum, Accum) -> Accum + Send + Sync + 'static,
{
    /// Creates a `FoldCombineFn` from a zero accumulator, fold closure, and merge closure.
    pub fn new(zero: Accum, fold_fn: F, merge_fn: M) -> Self {
        Self {
            zero,
            fold_fn,
            merge_fn,
            _marker: PhantomData,
        }
    }
}

impl<Accum, Input, F, M> CombineFn for FoldCombineFn<Accum, Input, F, M>
where
    Accum: DefaultCoder + Clone + Send + Sync + 'static,
    Input: DefaultCoder + Send + Sync + 'static,
    F: Fn(Accum, Input) -> Accum + Send + Sync + 'static,
    M: Fn(Accum, Accum) -> Accum + Send + Sync + 'static,
{
    type Input = Input;
    type Accum = Accum;
    type Output = Accum;

    fn create_accumulator(&self) -> Self::Accum {
        self.zero.clone()
    }

    fn add_input(&self, accumulator: Self::Accum, input: Self::Input) -> Self::Accum {
        (self.fold_fn)(accumulator, input)
    }

    fn merge_accumulators(&self, accumulators: Vec<Self::Accum>) -> Self::Accum {
        accumulators
            .into_iter()
            .reduce(&self.merge_fn)
            .unwrap_or_else(|| self.create_accumulator())
    }

    fn extract_output(&self, accumulator: Self::Accum) -> Self::Output {
        accumulator
    }
}
