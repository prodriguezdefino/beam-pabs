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

//! Partitioning a single PCollection into a fixed number of PCollections.

use std::sync::Arc;

use super::dofn::outputs::Indexed;
use super::{DisplayDataBuilder, DoFn, HasDisplayData, PTransform, ParDo, ProcessContext};
use crate::coders::DefaultCoder;
use crate::values::{PCollection, PCollectionList};

/// Maps an element to the index of its partition.
type PartitionFn<T> = dyn Fn(&T) -> usize + Send + Sync;

/// Internal DoFn that routes each element to an indexed partition.
struct PartitionDoFn<T> {
    name: String,
    num_partitions: usize,
    partition_fn: Arc<PartitionFn<T>>,
}

impl<T> Clone for PartitionDoFn<T> {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            num_partitions: self.num_partitions,
            partition_fn: Arc::clone(&self.partition_fn),
        }
    }
}

impl<T: DefaultCoder> DoFn for PartitionDoFn<T> {
    type In = T;
    type Out = Indexed<T>;

    fn process_element(
        &mut self,
        element: Self::In,
        out: &mut ProcessContext<'_, Self::Out>,
    ) -> crate::Result {
        let index = (self.partition_fn)(&element);
        if index >= self.num_partitions {
            return Err(format!(
                "Partition '{}' returned out-of-range index {index} (must be in 0..{})",
                self.name, self.num_partitions
            )
            .into());
        }
        out.emit(Indexed(index, element))
    }

    fn populate_display_data(&self, builder: &mut DisplayDataBuilder) {
        builder.add_integer("num_partitions", self.num_partitions as i64);
    }
}

/// Partitions a single [`PCollection`] into a fixed number of [`PCollection`]s.
///
/// The partition function maps each element to an index in `0..num_partitions`; one
/// multi-output [`ParDo`] routes it there. An index out of range fails the bundle.
pub struct Partition<T> {
    name: String,
    num_partitions: usize,
    partition_fn: Arc<PartitionFn<T>>,
}

impl<T: DefaultCoder> Partition<T> {
    /// Splits elements into `num_partitions` collections. Panics if `num_partitions` is 0.
    pub fn new<F>(name: impl Into<String>, num_partitions: usize, partition_fn: F) -> Self
    where
        F: Fn(&T) -> usize + Send + Sync + 'static,
    {
        assert!(num_partitions > 0, "num_partitions must be greater than 0");
        Self {
            name: name.into(),
            num_partitions,
            partition_fn: Arc::new(partition_fn),
        }
    }
}

impl<T> HasDisplayData for Partition<T> {
    fn populate_display_data(&self, builder: &mut DisplayDataBuilder) {
        builder.add_text("transform", "Partition");
        builder.add_text("name", &self.name);
        builder.add_integer("num_partitions", self.num_partitions as i64);
    }
}

impl<T: DefaultCoder> PTransform<PCollection<T>> for Partition<T> {
    type Output = PCollectionList<T>;

    fn expand(&self, input: &PCollection<T>) -> PCollectionList<T> {
        let do_fn = PartitionDoFn {
            name: self.name.clone(),
            num_partitions: self.num_partitions,
            partition_fn: Arc::clone(&self.partition_fn),
        };

        let parts = input
            .apply(ParDo::new(&self.name, do_fn).with_output_count(self.num_partitions))
            .into_vec()
            .into_iter()
            .map(|part| part.with_coder_of(input))
            .collect();
        PCollectionList::from_vec(input.pipeline().clone(), parts)
    }
}
