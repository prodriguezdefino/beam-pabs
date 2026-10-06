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

//! The per-bundle accumulator table of the partial combine, with its LRU eviction.

use std::hash::Hash;

use super::CombineFn;
use crate::coders::{DefaultCoder, ElementMetadata, PaneInfo, WindowedHeader};
use crate::internals::FastHashMap;

/// At the memory budget, the table emits its least recently used 1/divisor of entries.
///
/// This keeps hot keys in memory for the full bundle. On Wikipedia word counts, a full
/// flush emitted 45% more records than this policy.
const EVICTION_DIVISOR: usize = 10;

/// Number of entries to measure before the weight estimate is used.
const WEIGHER_WARMUP_SAMPLES: u32 = 16;

/// After warm-up, one in this many table operations is measured.
const WEIGHER_SAMPLE_PERIOD: u32 = 64;

/// One key's accumulator within one window assignment.
pub(super) struct KeyAccumulator<A> {
    /// `None` only while `add_input` owns the value. See `AccumulatorTable::add`.
    pub(super) accumulator: Option<A>,
    /// The earliest input timestamp, so the output does not move forward in event time.
    min_timestamp: i64,
    /// The table clock at the last use. Each use takes a new tick, so stamps are unique
    /// and the smallest belong to the coldest entries.
    last_used: u64,
}

/// The accumulators of every key seen in one window assignment.
struct WindowGroup<K, A> {
    /// A header from one input element. Output copies its encoded windows, because the
    /// window coder is not available here.
    header: WindowedHeader,
    /// Uses foldhash: SipHash took about one eighth of the CPU of a word count.
    keys: FastHashMap<K, KeyAccumulator<A>>,
}

/// The output header parts of a window group, decoded once for each group, not each key.
pub(super) struct GroupOutput<'a> {
    header: &'a WindowedHeader,
    pane: PaneInfo,
    metadata: ElementMetadata,
}

impl<'a> GroupOutput<'a> {
    fn of(header: &'a WindowedHeader) -> Self {
        Self {
            header,
            pane: header.pane(),
            metadata: header.metadata(),
        }
    }

    /// Returns the header for an accumulator: the group windows and the entry timestamp.
    pub(super) fn header_for<A>(&self, entry: &KeyAccumulator<A>) -> WindowedHeader {
        self.header
            .rebuilt(entry.min_timestamp, self.pane, &self.metadata)
    }
}

/// Counts encoded bytes without storing them.
struct ByteCounter(usize);

impl std::io::Write for ByteCounter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0 += buf.len();
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Returns the encoded size of `value`, as an estimate of its heap memory.
fn encoded_len<T: DefaultCoder>(value: &T) -> usize {
    let mut counter = ByteCounter(0);
    // Ignore the error: the value fails again, with context, when it is emitted. Its
    // weight until then is the bytes written before the failure.
    let _ = value.encode_element(&mut counter);
    counter.0
}

/// A running estimate of the memory of one table entry.
///
/// Measuring every entry costs about as much as the combine, so the weigher measures the
/// first entries, then one operation in [`WEIGHER_SAMPLE_PERIOD`]. It samples updates
/// too, because some accumulators (lists, sets, sketches) grow with input. The weight is
/// the hash table slot plus the encoded size of the key and accumulator.
pub(super) struct EntryWeigher {
    slot_bytes: usize,
    heap_bytes: usize,
    samples: u32,
    ticks: u32,
}

impl EntryWeigher {
    fn new<K, A>() -> Self {
        // A hash table fills at most 7/8 of its slots and doubles when it grows, so a live
        // entry costs about 1.5 slots plus a control byte.
        let slot = size_of::<K>() + size_of::<KeyAccumulator<A>>();
        Self {
            slot_bytes: slot + slot / 2 + 1,
            heap_bytes: 0,
            samples: 0,
            ticks: 0,
        }
    }

    fn should_sample(&mut self) -> bool {
        if self.samples < WEIGHER_WARMUP_SAMPLES {
            return true;
        }
        self.ticks = self.ticks.wrapping_add(1);
        self.ticks.is_multiple_of(WEIGHER_SAMPLE_PERIOD)
    }

    /// Adds a sample: a plain mean during warm-up, then a moving average with weight 1/16
    /// so the estimate follows accumulators that grow during the bundle.
    fn record(&mut self, heap_bytes: usize) {
        self.samples = self.samples.saturating_add(1);
        let weight = self.samples.min(WEIGHER_WARMUP_SAMPLES) as i64;
        let delta = (heap_bytes as i64 - self.heap_bytes as i64) / weight;
        self.heap_bytes = (self.heap_bytes as i64 + delta).max(0) as usize;
    }

    pub(super) fn entry_bytes(&self) -> usize {
        self.slot_bytes + self.heap_bytes
    }
}

/// The open accumulators of one bundle, grouped by encoded windows and then by key.
///
/// Window first, so a known key in a known window copies nothing, and the header is
/// stored once per window, not per key.
pub(super) struct AccumulatorTable<K, A> {
    groups: FastHashMap<Vec<u8>, WindowGroup<K, A>>,
    pub(super) len: usize,
    clock: u64,
    pub(super) weigher: EntryWeigher,
    /// Reused scratch space for the eviction cutoff.
    stamps: Vec<u64>,
}

impl<K, A> Default for AccumulatorTable<K, A> {
    fn default() -> Self {
        Self {
            groups: FastHashMap::default(),
            len: 0,
            clock: 0,
            weigher: EntryWeigher::new::<K, A>(),
            stamps: Vec::new(),
        }
    }
}

impl<K, A> AccumulatorTable<K, A>
where
    K: DefaultCoder + Eq + Hash,
    A: DefaultCoder,
{
    pub(super) fn estimated_bytes(&self) -> usize {
        self.len.saturating_mul(self.weigher.entry_bytes())
    }

    pub(super) fn add<CF: CombineFn<Accum = A>>(
        &mut self,
        combine_fn: &CF,
        window: &[u8],
        header: &WindowedHeader,
        key: K,
        value: CF::Input,
        timestamp: i64,
    ) -> crate::Result {
        let Self {
            groups,
            len,
            clock,
            weigher,
            ..
        } = self;
        *clock += 1;

        if !groups.contains_key(window) {
            groups.insert(
                window.to_vec(),
                WindowGroup {
                    header: header.clone(),
                    keys: FastHashMap::default(),
                },
            );
        }
        let Some(group) = groups.get_mut(window) else {
            return Err("Combine window group vanished after insertion".into());
        };

        match group.keys.get_mut(&key) {
            Some(entry) => {
                // `add_input` takes the accumulator by value: take it out and put it back,
                // so the key and window are not copied.
                let accumulator = entry
                    .accumulator
                    .take()
                    .unwrap_or_else(|| combine_fn.create_accumulator());
                let accumulator = combine_fn.add_input(accumulator, value);
                if weigher.should_sample() {
                    weigher.record(encoded_len(&key) + encoded_len(&accumulator));
                }
                entry.accumulator = Some(accumulator);
                entry.min_timestamp = entry.min_timestamp.min(timestamp);
                entry.last_used = *clock;
            }
            None => {
                let accumulator = combine_fn.add_input(combine_fn.create_accumulator(), value);
                if weigher.should_sample() {
                    weigher.record(encoded_len(&key) + encoded_len(&accumulator));
                }
                group.keys.insert(
                    key,
                    KeyAccumulator {
                        accumulator: Some(accumulator),
                        min_timestamp: timestamp,
                        last_used: *clock,
                    },
                );
                *len += 1;
            }
        }
        Ok(())
    }

    /// Removes the least recently used tenth of the entries and gives each to `emit`. A
    /// linear-time selection finds the cutoff, so the amortized cost per key is constant.
    pub(super) fn evict_coldest(
        &mut self,
        mut emit: impl FnMut(&GroupOutput<'_>, K, KeyAccumulator<A>) -> crate::Result,
    ) -> crate::Result {
        let Self {
            groups,
            len,
            stamps,
            ..
        } = self;
        stamps.clear();
        stamps.extend(
            groups
                .values()
                .flat_map(|group| group.keys.values().map(|entry| entry.last_used)),
        );
        if stamps.is_empty() {
            return Ok(());
        }
        let count = (stamps.len() / EVICTION_DIVISOR).max(1);
        let (_, &mut cutoff, _) = stamps.select_nth_unstable(count - 1);

        groups.values_mut().try_for_each(|group| {
            let output = GroupOutput::of(&group.header);
            group
                .keys
                .extract_if(|_, entry| entry.last_used <= cutoff)
                .try_for_each(|(key, entry)| {
                    *len -= 1;
                    emit(&output, key, entry)
                })
        })?;
        groups.retain(|_, group| !group.keys.is_empty());
        Ok(())
    }

    /// Empties the table and gives each entry to `emit`.
    pub(super) fn drain(
        self,
        mut emit: impl FnMut(&GroupOutput<'_>, K, KeyAccumulator<A>) -> crate::Result,
    ) -> crate::Result {
        self.groups.into_values().try_for_each(|group| {
            let output = GroupOutput::of(&group.header);
            group
                .keys
                .into_iter()
                .try_for_each(|(key, entry)| emit(&output, key, entry))
        })
    }
}
