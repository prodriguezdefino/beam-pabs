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

//! [`SampleCoalescer`]: folds `SampleRowKeysResponse` messages into bounded super-ranges.

use crate::bigtable::key::{compare_end, is_valid_range, split_evenly};
use crate::bigtable::proto::SampleRowKeysResponse;
use crate::bigtable::read::restriction::{
    KeyRangeRestriction, MAX_INTERIOR_TABLETS, TabletBoundary, downsample_boundaries,
};

/// Default target bundle size (`64 MiB`) when sub-splitting large sampled tablets.
const TARGET_BUNDLE_BYTES: i64 = 64 * 1024 * 1024;

/// Single-tablet byte threshold (`256 MiB`) above which a single-tablet scan is pre-split into
/// at least [`SINGLE_TABLET_SUB_SPLITS`] interpolated sub-ranges.
const LARGE_SINGLE_TABLET_BYTES: i64 = 256 * 1024 * 1024;

/// Minimum number of interpolated sub-ranges created when a single tablet exceeds
/// [`LARGE_SINGLE_TABLET_BYTES`].
const SINGLE_TABLET_SUB_SPLITS: usize = 16;

/// Pure streaming state machine that folds `SampleRowKeysResponse` messages into bounded
/// [`KeyRangeRestriction`] super-ranges.
#[derive(Clone, Debug)]
pub struct SampleCoalescer {
    req_start: Vec<u8>,
    req_end: Vec<u8>,
    max_initial_bundles: usize,
    max_interior_tablets: usize,
    sub_split_tablets: bool,
    prev_sample_key: Vec<u8>,
    prev_offset_bytes: i64,
    ranges: Vec<KeyRangeRestriction>,
}

impl SampleCoalescer {
    /// Creates a coalescer for the requested `[start_key, end_key)` scan range.
    pub fn new(
        start_key: Vec<u8>,
        end_key: Vec<u8>,
        max_initial_bundles: usize,
        sub_split_tablets: bool,
    ) -> Self {
        Self {
            req_start: start_key,
            req_end: end_key,
            max_initial_bundles: max_initial_bundles.max(1),
            max_interior_tablets: MAX_INTERIOR_TABLETS,
            sub_split_tablets,
            prev_sample_key: Vec::new(),
            prev_offset_bytes: 0,
            ranges: Vec::new(),
        }
    }

    /// Feeds one `SampleRowKeysResponse` tablet boundary into the coalescer.
    pub fn feed(&mut self, sample: SampleRowKeysResponse) {
        let sample_end = sample.row_key.to_vec();
        let offset = sample.offset_bytes.max(0);
        let tablet_size = (offset - self.prev_offset_bytes).max(1);

        let tablet_start = std::mem::replace(&mut self.prev_sample_key, sample_end.clone());
        self.prev_offset_bytes = offset.max(self.prev_offset_bytes);

        // Ignore duplicate boundaries that do not advance in key space.
        if !is_valid_range(&tablet_start, &sample_end) {
            return;
        }

        // Intersect `[tablet_start, sample_end)` with `[req_start, req_end)`.
        let clipped_start = if tablet_start.as_slice() < self.req_start.as_slice() {
            self.req_start.clone()
        } else {
            tablet_start
        };
        let clipped_end = if self.req_end.is_empty() {
            sample_end
        } else if sample_end.is_empty()
            || compare_end(&sample_end, &self.req_end) == std::cmp::Ordering::Greater
        {
            self.req_end.clone()
        } else {
            sample_end
        };

        if !is_valid_range(&clipped_start, &clipped_end) {
            return;
        }

        self.ranges.push(KeyRangeRestriction::new(
            clipped_start,
            clipped_end,
            tablet_size,
        ));

        // Keep memory bounded while streaming millions of tablets by compacting pairwise
        // whenever the buffer reaches `4 * max_initial_bundles`.
        let compact_threshold = self.max_initial_bundles.saturating_mul(4).max(2);
        if self.ranges.len() >= compact_threshold {
            self.ranges =
                compact_pairwise(std::mem::take(&mut self.ranges), self.max_interior_tablets);
        }
    }

    /// Finalizes the coalesced super-ranges and shuffles them deterministically.
    pub fn finish(mut self) -> Vec<KeyRangeRestriction> {
        if !is_valid_range(&self.req_start, &self.req_end) {
            return Vec::new();
        }

        // Cover the tail up to `req_end` if the last sample stopped before `req_end`.
        let covered_end = self.ranges.last().map(|r| r.end_key.as_slice());
        match covered_end {
            None => {
                return vec![KeyRangeRestriction::new(self.req_start, self.req_end, 1)];
            }
            Some(last_end)
                if !last_end.is_empty()
                    && compare_end(last_end, &self.req_end) == std::cmp::Ordering::Less =>
            {
                let tail_start = last_end.to_vec();
                let avg_size = self
                    .ranges
                    .iter()
                    .map(|r| r.estimated_bytes)
                    .fold(0i64, i64::saturating_add)
                    / (self.ranges.len() as i64);
                self.ranges.push(KeyRangeRestriction::new(
                    tail_start,
                    self.req_end.clone(),
                    avg_size.max(1),
                ));
            }
            Some(_) => {}
        }

        if self.sub_split_tablets && self.ranges.len() < self.max_initial_bundles {
            self.ranges = sub_split_large_tablets(self.ranges, self.max_initial_bundles);
        }

        let mut coalesced = if self.ranges.len() > self.max_initial_bundles {
            coalesce_to_limit(
                &self.ranges,
                self.max_initial_bundles,
                self.max_interior_tablets,
            )
        } else {
            self.ranges
        };

        shuffle_restrictions(&mut coalesced);
        coalesced
    }
}

fn sub_split_large_tablets(
    ranges: Vec<KeyRangeRestriction>,
    max_bundles: usize,
) -> Vec<KeyRangeRestriction> {
    let total_bytes = ranges
        .iter()
        .map(|r| r.estimated_bytes.max(1))
        .fold(0i64, i64::saturating_add);
    let desired_bundle_bytes = (total_bytes / (max_bundles as i64)).max(TARGET_BUNDLE_BYTES);
    let is_single = ranges.len() == 1;

    ranges
        .into_iter()
        .flat_map(|range| {
            if !range.tablets.is_empty() || range.estimated_bytes <= desired_bundle_bytes {
                return vec![range];
            }
            let by_size = ((range.estimated_bytes + desired_bundle_bytes - 1)
                / desired_bundle_bytes) as usize;
            let split_count = if is_single && range.estimated_bytes >= LARGE_SINGLE_TABLET_BYTES {
                by_size.max(SINGLE_TABLET_SUB_SPLITS)
            } else {
                by_size
            }
            .min(max_bundles);
            if split_count <= 1 {
                return vec![range];
            }
            let pairs = split_evenly(&range.start_key, &range.end_key, split_count);
            let per_range_bytes = (range.estimated_bytes / (pairs.len() as i64)).max(1);
            pairs
                .into_iter()
                .map(|(s, e)| KeyRangeRestriction::new(s, e, per_range_bytes))
                .collect()
        })
        .collect()
}

/// Merges two adjacent [`KeyRangeRestriction`]s into one super-range, preserving interior
/// [`TabletBoundary`] split points downsampled to `max_interior`.
fn merge_two(
    left: &KeyRangeRestriction,
    right: &KeyRangeRestriction,
    max_interior: usize,
) -> KeyRangeRestriction {
    let left_bytes = left.estimated_bytes.max(1);
    let right_bytes = right.estimated_bytes.max(1);
    let total_bytes = left_bytes.saturating_add(right_bytes);

    let mut boundaries = Vec::with_capacity(left.tablets.len() + 1 + right.tablets.len());
    boundaries.extend(left.tablets.iter().cloned());
    if !left.end_key.is_empty() {
        boundaries.push(TabletBoundary {
            end_key: left.end_key.clone(),
            offset_bytes: left_bytes,
        });
    }
    boundaries.extend(right.tablets.iter().map(|b| TabletBoundary {
        end_key: b.end_key.clone(),
        offset_bytes: left_bytes.saturating_add(b.offset_bytes),
    }));

    KeyRangeRestriction {
        start_key: left.start_key.clone(),
        end_key: right.end_key.clone(),
        estimated_bytes: total_bytes,
        tablets: downsample_boundaries(&boundaries, max_interior),
    }
}

/// Compacts `ranges` in pairs (`[0..1], [2..3], ...`), halving the length.
fn compact_pairwise(
    ranges: Vec<KeyRangeRestriction>,
    max_interior: usize,
) -> Vec<KeyRangeRestriction> {
    let mut ranges = ranges.into_iter();
    std::iter::from_fn(|| {
        let left = ranges.next()?;
        Some(match ranges.next() {
            Some(right) => merge_two(&left, &right, max_interior),
            None => left,
        })
    })
    .collect()
}

/// Coalesces `ranges` into at most `max_bundles` contiguous super-ranges balanced by byte weight.
fn coalesce_to_limit(
    ranges: &[KeyRangeRestriction],
    max_bundles: usize,
    max_interior: usize,
) -> Vec<KeyRangeRestriction> {
    if ranges.len() <= max_bundles {
        return ranges.to_vec();
    }
    let total_bytes = ranges
        .iter()
        .map(|r| r.estimated_bytes.max(1))
        .fold(0i64, i64::saturating_add);
    let target_bytes = (total_bytes / (max_bundles as i64)).max(1);

    let mut result: Vec<KeyRangeRestriction> = Vec::with_capacity(max_bundles);
    let mut current: Option<KeyRangeRestriction> = None;

    for (idx, range) in ranges.iter().enumerate() {
        let remaining_inputs = ranges.len() - idx;
        let remaining_slots = max_bundles.saturating_sub(result.len());

        current = match current.take() {
            None => Some(range.clone()),
            Some(acc) => {
                // If we have enough remaining slots to emit the rest 1-to-1, flush `acc`.
                if remaining_inputs < remaining_slots {
                    result.push(acc);
                    Some(range.clone())
                } else {
                    let merged = merge_two(&acc, range, max_interior);
                    if merged.estimated_bytes >= target_bytes && result.len() + 1 < max_bundles {
                        result.push(merged);
                        None
                    } else {
                        Some(merged)
                    }
                }
            }
        };
    }

    if let Some(last) = current {
        // With no free slot, the last range merges into the previous one.
        let full = result.len() >= max_bundles;
        let tail = match result.pop() {
            Some(prev) if full => merge_two(&prev, &last, max_interior),
            Some(prev) => {
                result.push(prev);
                last
            }
            None => last,
        };
        result.push(tail);
    }

    result
}

/// Deterministically shuffles `ranges` using a SplitMix64 PRNG so adjacent lexicographic
/// key ranges distribute across workers after `Reshuffle`.
pub fn shuffle_restrictions(ranges: &mut [KeyRangeRestriction]) {
    let len = ranges.len();
    if len <= 1 {
        return;
    }
    let mut state: u64 = 0x9E37_79B9_7F4A_7C15 ^ (len as u64);
    for i in (1..len).rev() {
        state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z = z ^ (z >> 31);
        let j = (z as usize) % (i + 1);
        ranges.swap(i, j);
    }
}
