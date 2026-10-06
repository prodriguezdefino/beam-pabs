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

//! Dynamic work rebalancing through data channel splits.
//!
//! While a bundle runs, a runner can ask the SDK to give back part of the remaining work
//! for an idle worker. The SDK answers with a *channel split*. The split is an index in
//! the input channel. The SDK keeps the elements before it (the "primary"). The runner
//! can schedule the rest again (the "residual").
//!
//! Without splits, a runner cannot redistribute a bundle that it gave to one worker. It
//! then scales the pool down to the one worker that makes progress.
//!
//! Keep the index conventions exact. The runner compares the reported read index with
//! the split it granted. An off-by-one error silently loses or duplicates data.

/// Progress and split state for one in-flight bundle.
///
/// Keep it behind a mutex. A split decision and the position that it uses must change
/// atomically with respect to the executor.
#[derive(Debug, Clone, Copy)]
pub struct SplitState {
    /// The 0-based index of the current element, or -1 before the first element.
    ///
    /// It advances *before* the element goes downstream, so the current element is
    /// already counted. This prevents a split from giving away work in progress.
    pub index: i64,
    /// The 0-based index of the first element not to process, that is, the first residual
    /// element. `i64::MAX` until a split is granted.
    pub stop_index: i64,
}

impl Default for SplitState {
    fn default() -> Self {
        Self {
            index: -1,
            stop_index: i64::MAX,
        }
    }
}

impl SplitState {
    /// Claims the next element for processing.
    ///
    /// Returns false when the bundle is at the split boundary and must stop.
    pub fn begin_element(&mut self) -> bool {
        if self.index == self.stop_index - 1 {
            return false;
        }
        self.index += 1;
        true
    }

    /// Returns the read index for *intermediate* progress.
    ///
    /// The value is the index of the current element, or -1 before the first element.
    pub fn read_index(&self) -> i64 {
        self.index
    }

    /// Closes the channel and returns the read index for the final response: one past the
    /// last element read, so the *count* of processed elements. After a split, it is exactly
    /// the first residual index, and the runner uses it to check the split. Do not make it
    /// the same as the intermediate value from [`read_index`](Self::read_index).
    pub fn finish(&mut self) -> i64 {
        self.index += 1;
        self.stop_index = self.index;
        self.index
    }
}

/// A split point, as a (last primary, first residual) index pair.
#[derive(Debug, PartialEq, Eq)]
pub struct SplitPoint {
    pub last_primary: i64,
    pub first_residual: i64,
}

/// Chooses where to divide the input channel between primary and residual.
///
/// `fraction` is the part of the *remaining* work that the SDK keeps. 0.0 means
/// "checkpoint as soon as possible" and 1.0 means "keep everything".
///
/// Returns `None` when no useful split exists. This occurs when the channel is drained,
/// when the split does not go past the current element, or when the split is at or after
/// a boundary already granted to the runner.
pub fn compute_split(
    state: &SplitState,
    estimated_input_elements: i64,
    fraction: f64,
    allowed_split_points: &[i64],
) -> Option<SplitPoint> {
    let current_element_progress = if state.index >= 0 { 0.5 } else { 1.0 };
    compute_split_with_progress(
        state,
        estimated_input_elements,
        fraction,
        allowed_split_points,
        current_element_progress,
    )
}

/// Chooses where to divide the input channel between primary and residual.
///
/// `current_element_progress` is the progress inside the current element, from an active
/// splittable DoFn.
pub fn compute_split_with_progress(
    state: &SplitState,
    estimated_input_elements: i64,
    fraction: f64,
    allowed_split_points: &[i64],
    current_element_progress: f64,
) -> Option<SplitPoint> {
    let SplitState { index, stop_index } = *state;

    // A finished channel has no work to give back.
    if index == stop_index {
        return None;
    }

    // A delayed request can carry an estimate that disagrees with the elements already
    // seen or with a granted split. Use the SDK state in both cases.
    let total = estimated_input_elements.clamp(index + 1, stop_index);

    let remainder = total as f64 - index as f64 - current_element_progress;
    let keep = remainder * fraction;

    let advance = ((current_element_progress + keep).round() as i64).max(1);
    let new_stop = snap_to_allowed(index + advance, index, allowed_split_points);

    // Do not move a boundary outwards. Do not give away the current element.
    if new_stop < stop_index && new_stop > index {
        Some(SplitPoint {
            last_primary: new_stop - 1,
            first_residual: new_stop,
        })
    } else {
        None
    }
}

/// Moves a split index to the nearest index that the runner permits.
///
/// An empty `allowed` list means that the runner accepts any index. Otherwise, this picks
/// the nearest permitted point. It picks the point before `desired` only when that point
/// is after `index` and is strictly nearer. Callers must validate the result: when all
/// entries are at or before `index`, the result is an index that the caller rejects.
fn snap_to_allowed(desired: i64, index: i64, allowed: &[i64]) -> i64 {
    if allowed.is_empty() || allowed.contains(&desired) {
        return desired;
    }

    let mut sorted: Vec<i64> = allowed.to_vec();
    sorted.sort_unstable();

    // The position of the first permitted point that is not less than `desired`.
    let next_pos = sorted.partition_point(|&p| p < desired);

    match next_pos {
        0 => sorted[0],
        pos if pos == sorted.len() => sorted[pos - 1],
        pos => {
            let prev = sorted[pos - 1];
            let next = sorted[pos];
            if index < prev && desired - prev < next - desired {
                prev
            } else {
                next
            }
        }
    }
}
