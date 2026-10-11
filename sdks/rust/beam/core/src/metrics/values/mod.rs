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

//! The values of metric cells. Each value encodes as its Fn API payload and combines with
//! other cells of the same name. [`MetricCell`] ties a value to its URNs.

mod bounded_trie;
mod distribution;
mod gauge;
mod histogram;
mod string_set;

pub use bounded_trie::{BOUNDED_TRIE_DEFAULT_BOUND, BoundedTrieValue};
pub use distribution::DistributionValue;
pub use gauge::GaugeValue;
pub use histogram::{HistogramBuckets, HistogramValue};
pub use string_set::{STRING_SET_MAX_BYTES, StringSetValue};

use super::{
    TYPE_BOUNDED_TRIE, TYPE_DISTRIBUTION_INT64, TYPE_HISTOGRAM, TYPE_LATEST_INT64, TYPE_SET_STRING,
    TYPE_SUM_INT64, URN_USER_BOUNDED_TRIE, URN_USER_DISTRIBUTION_INT64, URN_USER_HISTOGRAM,
    URN_USER_LATEST_INT64, URN_USER_SET_STRING, URN_USER_SUM_INT64,
};
use crate::coders::VarIntCoder;

/// One kind of metric cell: its URNs, its Fn API payload, and how cells of one name combine.
/// The container, the query results and the decoding of monitoring infos all use it, so a new
/// kind is one implementation.
pub(crate) trait MetricCell: Clone + Sized {
    /// The URN of the user metric of this kind.
    const URN: &'static str;
    /// The type URN that names the payload encoding.
    const TYPE_URN: &'static str;

    fn encode_cell(&self) -> Vec<u8>;

    fn decode_cell(payload: &[u8]) -> Result<Self, String>;

    /// Combines `other`, a cell with the same name, into this one. Query results call it in
    /// transform order, so a tie between gauges goes to the last transform.
    fn combine(&mut self, other: &Self);
}

impl MetricCell for i64 {
    const URN: &'static str = URN_USER_SUM_INT64;
    const TYPE_URN: &'static str = TYPE_SUM_INT64;

    fn encode_cell(&self) -> Vec<u8> {
        let mut payload = Vec::new();
        // Invariant: encode to an in-memory vector never fails.
        let _ = VarIntCoder::encode_varint(*self, &mut payload);
        payload
    }

    fn decode_cell(payload: &[u8]) -> Result<Self, String> {
        VarIntCoder::decode_varint(&mut &payload[..])
            .map_err(|e| format!("Failed to decode a counter: {e}"))
    }

    fn combine(&mut self, other: &Self) {
        // A counter wraps on overflow, as in the container.
        *self = self.wrapping_add(*other);
    }
}

/// Implements [`MetricCell`] for a value type with `encode_payload`, `decode_payload` and
/// `merge`.
macro_rules! payload_cell {
    ($value:ty, $urn:expr, $type_urn:expr) => {
        impl MetricCell for $value {
            const URN: &'static str = $urn;
            const TYPE_URN: &'static str = $type_urn;

            fn encode_cell(&self) -> Vec<u8> {
                self.encode_payload()
            }

            fn decode_cell(payload: &[u8]) -> Result<Self, String> {
                Self::decode_payload(payload)
            }

            fn combine(&mut self, other: &Self) {
                // Query results ignore the flag for dropped values.
                self.merge(other);
            }
        }
    };
}

payload_cell!(
    DistributionValue,
    URN_USER_DISTRIBUTION_INT64,
    TYPE_DISTRIBUTION_INT64
);
payload_cell!(GaugeValue, URN_USER_LATEST_INT64, TYPE_LATEST_INT64);
payload_cell!(StringSetValue, URN_USER_SET_STRING, TYPE_SET_STRING);
payload_cell!(BoundedTrieValue, URN_USER_BOUNDED_TRIE, TYPE_BOUNDED_TRIE);
payload_cell!(HistogramValue, URN_USER_HISTOGRAM, TYPE_HISTOGRAM);
