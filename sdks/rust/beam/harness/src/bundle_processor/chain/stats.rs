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

use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

use beam::metrics::DistributionValue;

use super::build::{OperatorGraph, PCollIdx};

/// The element count and the sampled encoded sizes of one PCollection.
#[derive(Debug, Default)]
struct PCollectionCell {
    count: AtomicI64,
    size_count: AtomicI64,
    size_sum: AtomicI64,
    size_min: AtomicI64,
    size_max: AtomicI64,
}

/// Adds `n` to `cell`, which has one writer, so a load and a store take the place of a
/// read-modify-write.
fn add(cell: &AtomicI64, n: i64) -> i64 {
    let value = cell.load(Ordering::Relaxed).wrapping_add(n);
    cell.store(value, Ordering::Relaxed);
    value
}

/// The element counts and sampled encoded sizes of one bundle, indexed by [`PCollIdx`]. The
/// bundle thread is the only writer. A progress request reads the values at any time through
/// a [`PCollectionStatsReader`], and can see a size distribution that is one element behind.
pub(in crate::bundle_processor) struct PCollectionStats {
    cells: Arc<[PCollectionCell]>,
}

impl PCollectionStats {
    /// Returns empty stats for the PCollections of a graph.
    pub(in crate::bundle_processor) fn new(graph: &OperatorGraph) -> Self {
        Self {
            cells: (0..graph.pcollections.len())
                .map(|_| PCollectionCell::default())
                .collect(),
        }
    }

    /// Counts one element on `pcoll` and returns the count of the collection so far.
    pub(super) fn count(&mut self, pcoll: PCollIdx) -> i64 {
        add(&self.cells[pcoll].count, 1)
    }

    /// Adds one encoded element size to the distribution of `pcoll`.
    pub(super) fn record_size(&mut self, pcoll: PCollIdx, len: usize) {
        let len = len as i64;
        let cell = &self.cells[pcoll];
        add(&cell.size_sum, len);
        let first = cell.size_count.load(Ordering::Relaxed) == 0;
        if first || len < cell.size_min.load(Ordering::Relaxed) {
            cell.size_min.store(len, Ordering::Relaxed);
        }
        if first || len > cell.size_max.load(Ordering::Relaxed) {
            cell.size_max.store(len, Ordering::Relaxed);
        }
        add(&cell.size_count, 1);
    }

    /// Returns a reader of these stats for progress requests.
    pub(in crate::bundle_processor) fn reader(&self) -> PCollectionStatsReader {
        PCollectionStatsReader(Arc::clone(&self.cells))
    }
}

/// Reads the stats of a running bundle from another thread.
#[derive(Clone)]
pub(in crate::bundle_processor) struct PCollectionStatsReader(Arc<[PCollectionCell]>);

impl PCollectionStatsReader {
    /// Returns the element counts indexed by [`PCollIdx`].
    pub(in crate::bundle_processor) fn counts(&self) -> Vec<i64> {
        self.0
            .iter()
            .map(|cell| cell.count.load(Ordering::Relaxed))
            .collect()
    }

    /// Returns the sampled byte-size distributions indexed by [`PCollIdx`].
    pub(in crate::bundle_processor) fn sizes(&self) -> Vec<Option<DistributionValue>> {
        self.0
            .iter()
            .map(|cell| {
                let count = cell.size_count.load(Ordering::Relaxed);
                (count > 0).then(|| DistributionValue {
                    count,
                    sum: cell.size_sum.load(Ordering::Relaxed),
                    min: cell.size_min.load(Ordering::Relaxed),
                    max: cell.size_max.load(Ordering::Relaxed),
                })
            })
            .collect()
    }
}
