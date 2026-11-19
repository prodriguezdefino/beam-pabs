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

//! The pre-shuffle partial combine DoFn.

use std::hash::Hash;
use std::sync::Arc;

use super::CombineFn;
use super::table::{AccumulatorTable, GroupOutput, KeyAccumulator};
use crate::coders::{DefaultCoder, WindowedHeader};
use crate::internals::PerWindow;
use crate::transforms::{DoFn, FinishBundleContext, ProcessContext};

/// About how much memory the accumulator table of one bundle holds before it evicts. The
/// limit is per running bundle; with word-count entries it is about 800k keys.
const DEFAULT_TABLE_MAX_BYTES: usize = 64 << 20;

/// Aggregates the elements of a bundle in memory before the shuffle.
///
/// Over that memory limit, the DoFn emits the coldest accumulators; at bundle end, all
/// others. There is one accumulator per key *and* window assignment, because Beam
/// aggregates within a window. Each bundle processor has its own clone and runs one
/// bundle at a time, so the table is a plain field.
pub(super) struct PartialCombineFn<K, CF: CombineFn> {
    combine_fn: Arc<CF>,
    table: AccumulatorTable<K, CF::Accum>,
    max_bytes: usize,
}

impl<K, CF: CombineFn> PartialCombineFn<K, CF> {
    pub(super) fn new(combine_fn: Arc<CF>) -> Self {
        Self {
            combine_fn,
            table: AccumulatorTable::default(),
            max_bytes: DEFAULT_TABLE_MAX_BYTES,
        }
    }
}

/// A clone starts with an empty table, because accumulators belong to their bundle.
impl<K, CF: CombineFn> Clone for PartialCombineFn<K, CF> {
    fn clone(&self) -> Self {
        Self {
            combine_fn: Arc::clone(&self.combine_fn),
            table: AccumulatorTable::default(),
            max_bytes: self.max_bytes,
        }
    }
}

/// One accumulator and the header of the windows of its input elements, or `None` if the
/// entry holds no accumulator.
fn accumulator_output<K, A>(
    group: &GroupOutput<'_>,
    key: K,
    entry: KeyAccumulator<A>,
) -> Option<((K, A), WindowedHeader)> {
    let header = group.header_for(&entry);
    entry
        .accumulator
        .map(|accumulator| ((key, accumulator), header))
}

impl<K, CF> DoFn for PartialCombineFn<K, CF>
where
    K: DefaultCoder + Eq + Hash,
    CF: CombineFn,
{
    // Accumulators are per key and window, so a multi-window element is processed per window.
    type In = PerWindow<(K, CF::Input)>;
    type Out = (K, CF::Accum);

    fn start_bundle(&mut self) -> crate::Result {
        self.table = AccumulatorTable::default();
        Ok(())
    }

    fn process_element(
        &mut self,
        PerWindow((key, value)): Self::In,
        out: &mut ProcessContext<(K, CF::Accum)>,
    ) -> crate::Result {
        let Self {
            combine_fn,
            table,
            max_bytes,
        } = self;

        if table.estimated_bytes() >= *max_bytes {
            table.evict_coldest(|group, key, entry| {
                accumulator_output(group, key, entry).map_or(Ok(()), |(kv, header)| {
                    out.output(kv).windowed(&header).emit()
                })
            })?;
        }

        let timestamp = out.timestamp().as_millis();
        table.add(
            combine_fn.as_ref(),
            out.header().window_bytes(),
            out.header(),
            key,
            value,
            timestamp,
        )
    }

    fn finish_bundle(&mut self, out: &mut FinishBundleContext<(K, CF::Accum)>) -> crate::Result {
        std::mem::take(&mut self.table).drain(|group, key, entry| {
            accumulator_output(group, key, entry).map_or(Ok(()), |(kv, header)| {
                out.output_windowed(kv, &header).emit()
            })
        })
    }
}
