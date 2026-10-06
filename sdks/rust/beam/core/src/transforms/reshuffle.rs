/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

//! Reshuffle: a `GroupByKey` on random keys as a shuffle barrier. The runner cannot fuse the
//! upstream and downstream transforms, so it can autoscale and spread the downstream work.

use std::collections::HashMap;
use std::marker::PhantomData;

use super::ProcessContext;
use super::dofn::{DoFn, ParDo};
use super::group_by_key::GroupByKey;
use crate::coders::{BeamIterable, DefaultCoder};
use crate::transforms::PTransform;
use crate::transforms::display_data::{DisplayDataBuilder, HasDisplayData};
use crate::values::PCollection;

/// A composite transform that breaks runner fusion and rebalances elements across workers.
#[derive(Clone, Debug)]
pub struct Reshuffle<T> {
    name: String,
    _marker: PhantomData<T>,
}

impl<T> Reshuffle<T> {
    /// Creates a `Reshuffle` transform with the given name.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            _marker: PhantomData,
        }
    }
}

impl<T> HasDisplayData for Reshuffle<T> {
    fn populate_display_data(&self, builder: &mut DisplayDataBuilder) {
        builder.add_text("transform", "Reshuffle");
        builder.add_text("name", &self.name);
    }
}

/// Assigns a shard key to each element, so that the downstream `GroupByKey` spreads work. Each
/// instance starts from a random key and increments it. With a fixed start, every worker emits
/// the same key range and the `GroupByKey` merges them into a few very large groups.
struct AssignShardFn<T> {
    next_shard: i64,
    _marker: PhantomData<T>,
}

impl<T> Default for AssignShardFn<T> {
    fn default() -> Self {
        Self {
            next_shard: random_seed(),
            _marker: PhantomData,
        }
    }
}

/// Each copy gets its own random start key, so bundle processors emit different ranges.
impl<T> Clone for AssignShardFn<T> {
    fn clone(&self) -> Self {
        Self::default()
    }
}

/// Returns a random start key without a random number crate. The OS seeds `RandomState` per
/// process and each instance changes the seed. This is enough: keys must be well spread, not
/// unpredictable.
fn random_seed() -> i64 {
    use std::hash::{BuildHasher, Hasher};
    std::collections::hash_map::RandomState::new()
        .build_hasher()
        .finish() as i64
}

impl<T: DefaultCoder> DoFn for AssignShardFn<T> {
    type In = T;
    type Out = (i64, (i64, T));

    fn process_element(
        &mut self,
        element: Self::In,
        out: &mut ProcessContext<Self::Out>,
    ) -> crate::Result {
        let shard = self.next_shard;
        self.next_shard = shard.wrapping_add(1);
        out.emit((shard, (out.timestamp(), element)))
    }
}

struct ExpandShardFn<T> {
    _marker: PhantomData<T>,
}

impl<T> Default for ExpandShardFn<T> {
    fn default() -> Self {
        Self {
            _marker: PhantomData,
        }
    }
}

impl<T> Clone for ExpandShardFn<T> {
    fn clone(&self) -> Self {
        Self::default()
    }
}

impl<T: DefaultCoder> DoFn for ExpandShardFn<T> {
    type In = (i64, BeamIterable<(i64, T)>);
    type Out = T;

    fn process_element(
        &mut self,
        (_shard, elements): Self::In,
        out: &mut ProcessContext<Self::Out>,
    ) -> crate::Result {
        elements.try_into_iter().try_for_each(|elem| {
            let (ts, value) =
                elem.map_err(|e| format!("Reshuffle failed to read a grouped element: {e}"))?;
            out.output(value).at(ts).emit()
        })
    }
}

impl<T> PTransform<PCollection<T>> for Reshuffle<T>
where
    T: DefaultCoder + Clone,
{
    type Output = PCollection<T>;

    fn expand(&self, input: &PCollection<T>) -> PCollection<T> {
        let pipeline = input.pipeline();
        let name = pipeline.unique_transform_name(&self.name);

        let paired = input.apply(ParDo::new(
            format!("{name}/PairWithRandomKey"),
            AssignShardFn::<T>::default(),
        ));

        let grouped = paired.apply(GroupByKey::new(format!("{name}/GroupByKey")));

        let output = grouped.apply(ParDo::new(
            format!("{name}/Expand"),
            ExpandShardFn::<T>::default(),
        ));

        let paired_id = pipeline
            .producer_transform_id(paired.id())
            .expect("PairWithRandomKey transform must exist");
        let gbk_id = pipeline
            .producer_transform_id(grouped.id())
            .expect("GroupByKey transform must exist");
        let expand_id = pipeline
            .producer_transform_id(output.id())
            .expect("Expand transform must exist");

        let inputs = HashMap::from([("in".to_string(), input.id().to_string())]);
        let outputs = HashMap::from([("out".to_string(), output.id().to_string())]);
        let subtransforms = vec![paired_id, gbk_id, expand_id];

        let transform_id = pipeline.add_composite_transform(
            &name,
            None,
            Vec::new(),
            inputs,
            outputs,
            subtransforms,
        );

        let mut builder = DisplayDataBuilder::with_namespace(name);
        self.populate_display_data(&mut builder);
        pipeline.set_transform_display_data(&transform_id, builder.into_proto());

        output
    }
}
