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

//! Per-PCollection element counts and sampled sizes for one bundle.

use std::collections::HashMap;

use beam::metrics::DistributionValue;

use super::build::{OperatorGraph, PCollIdx};

/// The element counts and sampled encoded sizes of one bundle, indexed by [`PCollIdx`].
pub(in crate::bundle_processor) struct PCollectionStats {
    counts: Vec<i64>,
    sizes: Vec<Option<DistributionValue>>,
}

impl PCollectionStats {
    /// Returns empty stats for the PCollections of a graph.
    pub(in crate::bundle_processor) fn new(graph: &OperatorGraph) -> Self {
        let n = graph.pcollections.len();
        Self {
            counts: vec![0; n],
            sizes: vec![None; n],
        }
    }

    /// Counts one element on `pcoll` and returns the count of the collection so far.
    pub(super) fn count(&mut self, pcoll: PCollIdx) -> i64 {
        let count = &mut self.counts[pcoll];
        *count += 1;
        *count
    }

    /// Adds one encoded element size to the distribution of `pcoll`.
    pub(super) fn record_size(&mut self, pcoll: PCollIdx, len: usize) {
        let len = len as i64;
        match &mut self.sizes[pcoll] {
            Some(sizes) => sizes.update(len),
            empty @ None => *empty = Some(DistributionValue::new(len)),
        }
    }

    /// Returns the counts of the PCollections that had elements, keyed by PCollection id.
    pub(in crate::bundle_processor) fn counts_by_id(
        &self,
        graph: &OperatorGraph,
    ) -> HashMap<String, i64> {
        self.counts
            .iter()
            .zip(&graph.pcollections)
            .filter(|(count, _)| **count > 0)
            .map(|(count, route)| (route.id.clone(), *count))
            .collect()
    }

    /// Returns the size distributions of the PCollections that have one, keyed by id.
    pub(in crate::bundle_processor) fn sizes_by_id(
        &self,
        graph: &OperatorGraph,
    ) -> HashMap<String, DistributionValue> {
        self.sizes
            .iter()
            .zip(&graph.pcollections)
            .filter_map(|(sizes, route)| Some((route.id.clone(), sizes.clone()?)))
            .collect()
    }
}
