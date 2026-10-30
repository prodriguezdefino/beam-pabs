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

//! Base-256 byte-key ordering, successor, fraction estimation, and key interpolation.
//!
//! An empty end key (`b""`) represents the unbounded upper end of the table (`1.0` in
//! fractional key space).

use std::cmp::Ordering;

/// Number of significant bytes after the common prefix read into a `u128` (`15 * 8 = 120` bits).
const WINDOW_BYTES: usize = 15;

/// `1.0` in 120-bit fixed-point key space (`1 << 120`), representing an unbounded end key.
const UPPER_UNIT: u128 = 1u128 << (WINDOW_BYTES * 8);

/// Returns the smallest byte key strictly greater than `key` (`[key, &[0x00]].concat()`).
pub fn next_key(key: &[u8]) -> Vec<u8> {
    let mut next = Vec::with_capacity(key.len() + 1);
    next.extend_from_slice(key);
    next.push(0);
    next
}

/// Returns whether `key` lies in the half-open range `[start, end)`. An empty `end` is unbounded.
pub fn in_range(key: &[u8], start: &[u8], end: &[u8]) -> bool {
    key >= start && (end.is_empty() || key < end)
}

/// Compares two upper-bound keys where an empty slice represents infinity.
pub fn compare_end(left: &[u8], right: &[u8]) -> Ordering {
    match (left.is_empty(), right.is_empty()) {
        (true, true) => Ordering::Equal,
        (true, false) => Ordering::Greater,
        (false, true) => Ordering::Less,
        (false, false) => left.cmp(right),
    }
}

/// Returns whether `start` and `end` form a non-empty half-open key range `[start, end)`.
pub fn is_valid_range(start: &[u8], end: &[u8]) -> bool {
    end.is_empty() || start < end
}

/// Estimates the position of `key` within `[start, end)` as a fraction in `[0.0, 1.0]`.
pub fn estimate_fraction(start: &[u8], end: &[u8], key: &[u8]) -> f64 {
    if !is_valid_range(start, end) {
        return 0.0;
    }
    if key <= start {
        return 0.0;
    }
    if !end.is_empty() && key >= end {
        return 1.0;
    }
    let Some((prefix_len, start_val, end_val)) = window_bounds(start, end) else {
        return 0.0;
    };
    let key_val = read_window(key, prefix_len).clamp(start_val, end_val);
    let span = (end_val - start_val) as f64;
    if span <= 0.0 {
        0.0
    } else {
        ((key_val - start_val) as f64 / span).clamp(0.0, 1.0)
    }
}

/// Interpolates a split key at `fraction` (in `(0.0, 1.0)`) strictly inside `(start, end)`.
/// Returns `None` if the range cannot be divided or `fraction` is outside `(0.0, 1.0)`.
pub fn interpolate(start: &[u8], end: &[u8], fraction: f64) -> Option<Vec<u8>> {
    if !(0.0..1.0).contains(&fraction) || !is_valid_range(start, end) {
        return None;
    }
    let (prefix_len, start_val, end_val) = window_bounds(start, end)?;
    let span = end_val.checked_sub(start_val)?;
    if span <= 1 {
        return None;
    }
    let offset = ((span as f64) * fraction).round() as u128;
    let mid_val = start_val.saturating_add(offset.clamp(1, span - 1));

    let mut result = Vec::with_capacity(prefix_len + WINDOW_BYTES);
    for i in 0..prefix_len {
        let b = start.get(i).or_else(|| end.get(i)).copied().unwrap_or(0xFF);
        result.push(b);
    }
    let tail = mid_val.to_be_bytes();
    result.extend_from_slice(&tail[16 - WINDOW_BYTES..]);

    // Trim trailing zero bytes while keeping the key strictly greater than `start`.
    while result.last() == Some(&0) && result.len() > start.len() {
        let candidate_len = result.len() - 1;
        if &result[..candidate_len] > start {
            result.pop();
        } else {
            break;
        }
    }

    (result.as_slice() > start && (end.is_empty() || result.as_slice() < end)).then_some(result)
}

/// Splits `[start, end)` into `count` sub-ranges `[(k_0, k_1), ...]` of about equal width.
pub fn split_evenly(start: &[u8], end: &[u8], count: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
    if count <= 1 || !is_valid_range(start, end) {
        return vec![(start.to_vec(), end.to_vec())];
    }
    let mut ranges = Vec::with_capacity(count);
    let mut current = start.to_vec();
    for i in 1..count {
        let fraction = (i as f64) / (count as f64);
        if let Some(split) = interpolate(start, end, fraction).filter(|k| k > &current) {
            ranges.push((current, split.clone()));
            current = split;
        }
    }
    ranges.push((current, end.to_vec()));
    ranges
}

/// Finds the first byte position where `start` (right-padded with `0`) and `end` differ, and
/// returns `(prefix_len, start_window, end_window)` as 120-bit integers.
fn window_bounds(start: &[u8], end: &[u8]) -> Option<(usize, u128, u128)> {
    if end.is_empty() {
        let prefix_len = start.iter().take_while(|&&b| b == 0xFF).count();
        let start_val = read_window(start, prefix_len);
        (start_val < UPPER_UNIT).then_some((prefix_len, start_val, UPPER_UNIT))
    } else {
        let max_len = start.len().max(end.len());
        let prefix_len = (0..max_len)
            .find(|&i| start.get(i).copied().unwrap_or(0) != end.get(i).copied().unwrap_or(0))?;
        let start_val = read_window(start, prefix_len);
        let end_val = read_window(end, prefix_len);
        (end_val > start_val).then_some((prefix_len, start_val, end_val))
    }
}

/// Reads `WINDOW_BYTES` (15 bytes) from `key` starting at `offset`, right-padded with zeros.
fn read_window(key: &[u8], offset: usize) -> u128 {
    (0..WINDOW_BYTES).fold(0u128, |acc, i| {
        let b = key.get(offset + i).copied().unwrap_or(0);
        (acc << 8) | u128::from(b)
    })
}
