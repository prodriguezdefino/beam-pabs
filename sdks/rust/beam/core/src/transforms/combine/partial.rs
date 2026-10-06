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
use crate::coders::DefaultCoder;
use crate::transforms::{DoFn, ProcessContext};

/// About how much memory the accumulator table of one bundle holds before it evicts. The
/// limit is per running bundle; with word-count entries it is about 800k keys.
const DEFAULT_TABLE_BUDGET_BYTES: usize = 64 << 20;

/// Aggregates the elements of a bundle in memory before the shuffle.
///
/// Over the memory budget, the DoFn emits the coldest accumulators; at bundle end, all
/// others. There is one accumulator per key *and* window assignment, because Beam
/// aggregates within a window. Each bundle processor has its own clone and runs one
/// bundle at a time, so the table is a plain field.
pub(super) struct PartialCombineFn<K, CF: CombineFn> {
    combine_fn: Arc<CF>,
    table: AccumulatorTable<K, CF::Accum>,
    budget_bytes: usize,
}

impl<K, CF: CombineFn> PartialCombineFn<K, CF> {
    pub(super) fn new(combine_fn: Arc<CF>) -> Self {
        Self {
            combine_fn,
            table: AccumulatorTable::default(),
            budget_bytes: DEFAULT_TABLE_BUDGET_BYTES,
        }
    }
}

/// A clone starts with an empty table, because accumulators belong to their bundle.
impl<K, CF: CombineFn> Clone for PartialCombineFn<K, CF> {
    fn clone(&self) -> Self {
        Self {
            combine_fn: Arc::clone(&self.combine_fn),
            table: AccumulatorTable::default(),
            budget_bytes: self.budget_bytes,
        }
    }
}

/// Emits one accumulator into the windows of its input elements.
fn emit_accumulator<K, A>(
    out: &mut ProcessContext<'_, (K, A)>,
    group: &GroupOutput<'_>,
    key: K,
    entry: KeyAccumulator<A>,
) -> crate::Result
where
    (K, A): DefaultCoder,
{
    let header = group.header_for(&entry);
    let Some(accumulator) = entry.accumulator else {
        return Ok(());
    };
    out.output((key, accumulator)).windowed(&header).emit()
}

impl<K, CF> DoFn for PartialCombineFn<K, CF>
where
    K: DefaultCoder + Eq + Hash,
    CF: CombineFn,
{
    type In = (K, CF::Input);
    type Out = (K, CF::Accum);

    fn start_bundle(&mut self) -> crate::Result {
        self.table = AccumulatorTable::default();
        Ok(())
    }

    fn process_element(
        &mut self,
        (key, value): (K, CF::Input),
        out: &mut ProcessContext<(K, CF::Accum)>,
    ) -> crate::Result {
        let Self {
            combine_fn,
            table,
            budget_bytes,
        } = self;

        if table.estimated_bytes() >= *budget_bytes {
            table.evict_coldest(|group, key, entry| emit_accumulator(out, group, key, entry))?;
        }

        let timestamp = out.timestamp();
        table.add(
            combine_fn.as_ref(),
            out.window(),
            out.header(),
            key,
            value,
            timestamp,
        )
    }

    fn finish_bundle(&mut self, out: &mut ProcessContext<(K, CF::Accum)>) -> crate::Result {
        std::mem::take(&mut self.table)
            .drain(|group, key, entry| emit_accumulator(out, group, key, entry))
    }
}
