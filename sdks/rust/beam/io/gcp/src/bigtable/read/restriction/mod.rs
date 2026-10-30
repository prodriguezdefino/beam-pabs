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

//! Key-range restriction and tablet-aware tracker for splittable Bigtable reads.

mod tracker;

use beam::coders::{Coder, CoderError, CoderRegistry, Context, DefaultCoder};
use serde::{Deserialize, Serialize};

pub use self::tracker::KeyRangeTracker;
use crate::bigtable::key;

/// Maximum number of interior tablet boundaries retained inside a coalesced super-range.
pub const MAX_INTERIOR_TABLETS: usize = 64;

/// One tablet boundary key and its byte offset relative to the start of a restriction.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TabletBoundary {
    /// Exclusive upper bound key of the tablet.
    pub end_key: Vec<u8>,
    /// Approximate byte size from the start of the restriction up to `end_key`.
    pub offset_bytes: i64,
}

/// Downsamples `boundaries` to at most `max_interior` evenly spaced entries.
pub fn downsample_boundaries(
    boundaries: &[TabletBoundary],
    max_interior: usize,
) -> Vec<TabletBoundary> {
    if max_interior == 0 {
        return Vec::new();
    }
    if boundaries.len() <= max_interior {
        return boundaries.to_vec();
    }
    let len = boundaries.len();
    (0..max_interior)
        .map(|i| {
            let idx = ((i + 1) * len) / (max_interior + 1);
            boundaries[idx.min(len - 1)].clone()
        })
        .collect()
}

/// A half-open key range `[start_key, end_key)` with interior tablet boundaries.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct KeyRangeRestriction {
    /// Inclusive lower bound key.
    pub start_key: Vec<u8>,
    /// Exclusive upper bound key. Empty means the end of the table.
    pub end_key: Vec<u8>,
    /// Approximate byte size of `[start_key, end_key)`.
    pub estimated_bytes: i64,
    /// Sorted interior tablet boundaries strictly inside `(start_key, end_key)`.
    pub tablets: Vec<TabletBoundary>,
}

type EncodedRestriction = ((Vec<u8>, Vec<u8>), (i64, Vec<(Vec<u8>, i64)>));

/// Wire coder for [`KeyRangeRestriction`].
#[derive(Clone, Copy, Debug, Default)]
pub struct KeyRangeRestrictionCoder;

impl Coder<KeyRangeRestriction> for KeyRangeRestrictionCoder {
    fn urn(&self) -> &'static str {
        beam::coders::URN_KV
    }

    fn encode(
        &self,
        value: &KeyRangeRestriction,
        writer: &mut Vec<u8>,
        _context: Context,
    ) -> Result<(), CoderError> {
        value.encode_element(writer)
    }

    fn decode(
        &self,
        reader: &mut &[u8],
        _context: Context,
    ) -> Result<KeyRangeRestriction, CoderError> {
        KeyRangeRestriction::decode_element(reader)
    }
}

impl DefaultCoder for KeyRangeRestriction {
    type Coder = KeyRangeRestrictionCoder;

    fn coder() -> Self::Coder {
        KeyRangeRestrictionCoder
    }

    fn encode_element(&self, writer: &mut Vec<u8>) -> Result<(), CoderError> {
        let tablets: Vec<(Vec<u8>, i64)> = self
            .tablets
            .iter()
            .map(|t| (t.end_key.clone(), t.offset_bytes))
            .collect();
        let tuple: EncodedRestriction = (
            (self.start_key.clone(), self.end_key.clone()),
            (self.estimated_bytes, tablets),
        );
        tuple.encode_element(writer)
    }

    fn decode_element(reader: &mut &[u8]) -> Result<Self, CoderError> {
        let ((start_key, end_key), (estimated_bytes, tablets)) =
            EncodedRestriction::decode_element(reader)?;
        Ok(Self {
            start_key,
            end_key,
            estimated_bytes,
            tablets: tablets
                .into_iter()
                .map(|(end_key, offset_bytes)| TabletBoundary {
                    end_key,
                    offset_bytes,
                })
                .collect(),
        })
    }

    fn register_coder<R: CoderRegistry + ?Sized>(registry: &R) -> String {
        EncodedRestriction::register_coder(registry)
    }
}

impl KeyRangeRestriction {
    /// Creates a restriction over `[start_key, end_key)` with `estimated_bytes` and no
    /// interior tablet boundaries.
    pub fn new(start_key: Vec<u8>, end_key: Vec<u8>, estimated_bytes: i64) -> Self {
        Self {
            start_key,
            end_key,
            estimated_bytes: estimated_bytes.max(1),
            tablets: Vec::new(),
        }
    }

    /// Sets the interior tablet boundaries of the restriction.
    pub fn with_tablets(mut self, tablets: Vec<TabletBoundary>) -> Self {
        self.tablets = tablets;
        self
    }

    /// Expands this restriction into one restriction per tablet when `tablets` is non-empty.
    pub fn expand_tablets(&self) -> Vec<Self> {
        if self.tablets.is_empty() {
            return vec![self.clone()];
        }
        let mut out = Vec::with_capacity(self.tablets.len() + 1);
        let mut current_start = self.start_key.clone();
        let mut last_offset = 0i64;
        for tablet in &self.tablets {
            if tablet.end_key > current_start
                && (self.end_key.is_empty() || tablet.end_key < self.end_key)
            {
                let bytes = (tablet.offset_bytes - last_offset).max(1);
                out.push(Self::new(current_start, tablet.end_key.clone(), bytes));
                current_start = tablet.end_key.clone();
                last_offset = tablet.offset_bytes.max(last_offset);
            }
        }
        if self.end_key.is_empty() || current_start < self.end_key {
            let bytes = (self.estimated_bytes - last_offset).max(1);
            out.push(Self::new(current_start, self.end_key.clone(), bytes));
        }
        out
    }
}

/// How [`choose_split`] divides the remaining key range `[unprocessed_start, end_key)`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SplitTarget {
    /// Checkpoint at `0.0`: split right at `unprocessed_start`.
    Checkpoint(Vec<u8>),
    /// Split at interior tablet boundary `index` (`tablets[index]`).
    Tablet {
        index: usize,
        key: Vec<u8>,
        primary_bytes: i64,
    },
    /// No interior tablet boundaries remain: interpolate a byte key at `fraction`.
    Interpolated { key: Vec<u8>, primary_bytes: i64 },
}

/// Pure function that selects a split point for `[unprocessed_start, end_key)`.
pub fn choose_split(
    unprocessed_start: &[u8],
    end_key: &[u8],
    remaining_bytes: i64,
    remaining_tablets: &[TabletBoundary],
    fraction: f64,
) -> Option<SplitTarget> {
    if !key::is_valid_range(unprocessed_start, end_key) {
        return None;
    }
    match (fraction <= 0.0, remaining_tablets) {
        (true, _) => Some(SplitTarget::Checkpoint(unprocessed_start.to_vec())),
        (false, []) => {
            let split_key = key::interpolate(unprocessed_start, end_key, fraction)?;
            let total = remaining_bytes.max(2);
            let primary_bytes = ((total as f64) * fraction).round() as i64;
            Some(SplitTarget::Interpolated {
                key: split_key,
                primary_bytes: primary_bytes.clamp(1, total - 1),
            })
        }
        (false, tablets) => {
            let total = remaining_bytes.max(2);
            let target_bytes = ((total as f64) * fraction).round() as i64;
            let (index, boundary) = tablets
                .iter()
                .enumerate()
                .filter(|(_, t)| {
                    t.end_key.as_slice() > unprocessed_start
                        && (end_key.is_empty() || t.end_key.as_slice() < end_key)
                })
                .min_by_key(|(_, t)| (t.offset_bytes - target_bytes).abs())?;
            Some(SplitTarget::Tablet {
                index,
                key: boundary.end_key.clone(),
                primary_bytes: boundary.offset_bytes.clamp(1, total - 1),
            })
        }
    }
}
