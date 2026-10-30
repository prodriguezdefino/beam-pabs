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

//! Constructors of [`RowRange`] for split and resume ranges, in the [`RowRangeExt`] trait.

use bytes::Bytes;

use super::{RowRange, row_range};

/// Constructors of [`RowRange`] for split and resume ranges.
pub trait RowRangeExt {
    /// Builds the half-open range `[start_closed, end_open)`. An empty bound is unbounded.
    fn half_open(start_closed: Bytes, end_open: Bytes) -> Self;

    /// Builds the open-start range `(start_open, end_open)` to resume after `start_open`.
    fn open_closed(start_open: Bytes, end_open: Bytes) -> Self;
}

impl RowRangeExt for RowRange {
    fn half_open(start_closed: Bytes, end_open: Bytes) -> Self {
        Self {
            start_key: (!start_closed.is_empty())
                .then_some(row_range::StartKey::StartKeyClosed(start_closed)),
            end_key: (!end_open.is_empty()).then_some(row_range::EndKey::EndKeyOpen(end_open)),
        }
    }

    fn open_closed(start_open: Bytes, end_open: Bytes) -> Self {
        Self {
            start_key: (!start_open.is_empty())
                .then_some(row_range::StartKey::StartKeyOpen(start_open)),
            end_key: (!end_open.is_empty()).then_some(row_range::EndKey::EndKeyOpen(end_open)),
        }
    }
}
