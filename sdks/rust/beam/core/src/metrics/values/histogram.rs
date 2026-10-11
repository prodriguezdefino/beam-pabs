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

//! The value of a histogram metric and its buckets.

use model::pipeline as proto;
use prost::Message;

/// The bucket boundaries of a histogram.
#[derive(Debug, Clone, Copy)]
pub enum HistogramBuckets {
    /// `count` buckets of `width`, the first starting at `start`.
    Linear {
        start: f64,
        width: f64,
        count: usize,
    },
    /// `count` buckets that grow by `2^(2^-scale)`: `[0, g)`, then `[g^i, g^(i+1))`.
    Exponential { scale: i32, count: usize },
}

// Bitwise equality, so a value with NaN boundaries still equals itself.
impl PartialEq for HistogramBuckets {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (
                Self::Linear {
                    start,
                    width,
                    count,
                },
                Self::Linear {
                    start: other_start,
                    width: other_width,
                    count: other_count,
                },
            ) => {
                start.to_bits() == other_start.to_bits()
                    && width.to_bits() == other_width.to_bits()
                    && count == other_count
            }
            (
                Self::Exponential { scale, count },
                Self::Exponential {
                    scale: other_scale,
                    count: other_count,
                },
            ) => scale == other_scale && count == other_count,
            (Self::Linear { .. }, Self::Exponential { .. })
            | (Self::Exponential { .. }, Self::Linear { .. }) => false,
        }
    }
}

impl Eq for HistogramBuckets {}

/// Where a value falls in a histogram.
enum Bucket {
    Index(usize),
    Outside,
}

impl HistogramBuckets {
    /// `count` buckets of `width` from `start`. `width` and `count` must be positive.
    pub fn linear(start: f64, width: f64, count: usize) -> crate::Result<Self> {
        if !(width > 0.0 && width.is_finite() && start.is_finite()) || count == 0 {
            return Err(crate::Error::msg(format!(
                "Linear histogram buckets need a finite start, a positive width and a positive \
                 count, got start {start}, width {width}, count {count}"
            )));
        }
        Ok(Self::Linear {
            start,
            width,
            count,
        })
    }

    /// `count` buckets that grow by `2^(2^-scale)`. `scale` is between -3 and 3.
    pub fn exponential(scale: i32, count: usize) -> crate::Result<Self> {
        if !(-3..=3).contains(&scale) || count == 0 {
            return Err(crate::Error::msg(format!(
                "Exponential histogram buckets need a scale from -3 to 3 and a positive count, \
                 got scale {scale}, count {count}"
            )));
        }
        Ok(Self::Exponential { scale, count })
    }

    /// The number of buckets.
    pub fn count(&self) -> usize {
        match *self {
            Self::Linear { count, .. } | Self::Exponential { count, .. } => count,
        }
    }

    fn bucket(&self, value: f64) -> Bucket {
        let index = match *self {
            Self::Linear { start, width, .. } if value >= start => {
                Some(((value - start) / width).floor())
            }
            Self::Exponential { scale, .. } if value >= 0.0 => {
                let growth = 2f64.powf(2f64.powi(-scale));
                Some(if value < growth {
                    0.0
                } else {
                    (value.log2() * 2f64.powi(scale)).floor()
                })
            }
            Self::Linear { .. } | Self::Exponential { .. } => None,
        };
        match index {
            // The cast saturates, and an index past the last bucket is outside.
            Some(index) if (index as usize) < self.count() => Bucket::Index(index as usize),
            Some(_) | None => Bucket::Outside,
        }
    }

    fn to_proto(self) -> proto::histogram_value::BucketOptions {
        use proto::histogram_value::bucket_options::{Base2Exponent, BucketType, Linear};
        let count = Some(i32::try_from(self.count()).unwrap_or(i32::MAX));
        let bucket_type = match self {
            Self::Linear { start, width, .. } => BucketType::Linear(Linear {
                number_of_buckets: count,
                width: Some(width),
                start: Some(start),
            }),
            Self::Exponential { scale, .. } => BucketType::Exponential(Base2Exponent {
                number_of_buckets: count,
                scale: Some(scale),
            }),
        };
        proto::histogram_value::BucketOptions {
            bucket_type: Some(bucket_type),
        }
    }

    fn from_proto(options: &proto::histogram_value::BucketOptions) -> Result<Self, String> {
        use proto::histogram_value::bucket_options::BucketType;
        let count = |n: Option<i32>| usize::try_from(n.unwrap_or(0)).unwrap_or(0);
        match &options.bucket_type {
            Some(BucketType::Linear(linear)) => Self::linear(
                linear.start.unwrap_or(0.0),
                linear.width.unwrap_or(0.0),
                count(linear.number_of_buckets),
            ),
            Some(BucketType::Exponential(exponential)) => Self::exponential(
                exponential.scale.unwrap_or(0),
                count(exponential.number_of_buckets),
            ),
            None => return Err("A histogram has no bucket type".to_string()),
        }
        .map_err(|e| e.to_string())
    }
}

/// Counts of values per bucket. Values outside the buckets count only in the total.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistogramValue {
    buckets: HistogramBuckets,
    counts: Vec<i64>,
    total: i64,
}

impl HistogramValue {
    pub fn new(buckets: HistogramBuckets) -> Self {
        Self {
            buckets,
            counts: vec![0; buckets.count()],
            total: 0,
        }
    }

    /// Counts `value`. A NaN value is ignored.
    pub fn record(&mut self, value: f64) {
        if value.is_nan() {
            return;
        }
        self.total = self.total.saturating_add(1);
        if let Bucket::Index(index) = self.buckets.bucket(value)
            && let Some(count) = self.counts.get_mut(index)
        {
            *count = count.saturating_add(1);
        }
    }

    /// Adds the counts of `other`. Returns `false` and changes nothing if the buckets differ.
    pub fn merge(&mut self, other: &Self) -> bool {
        if self.buckets != other.buckets {
            return false;
        }
        self.counts
            .iter_mut()
            .zip(&other.counts)
            .for_each(|(ours, theirs)| *ours = ours.saturating_add(*theirs));
        self.total = self.total.saturating_add(other.total);
        true
    }

    pub fn buckets(&self) -> HistogramBuckets {
        self.buckets
    }

    /// The count of each bucket.
    pub fn bucket_counts(&self) -> &[i64] {
        &self.counts
    }

    /// The count of all values, inside and outside the buckets.
    pub fn total(&self) -> i64 {
        self.total
    }

    /// Encodes the histogram as Fn API bytes: a serialized `HistogramValue` message.
    pub fn encode_payload(&self) -> Vec<u8> {
        proto::HistogramValue {
            count: Some(self.total),
            bucket_options: Some(self.buckets.to_proto()),
            bucket_counts: self.counts.clone(),
        }
        .encode_to_vec()
    }

    /// Decodes a histogram from a serialized `HistogramValue` message.
    pub fn decode_payload(payload: &[u8]) -> Result<Self, String> {
        let histogram = proto::HistogramValue::decode(payload)
            .map_err(|e| format!("Failed to decode a histogram: {e}"))?;
        let buckets = HistogramBuckets::from_proto(
            histogram
                .bucket_options
                .as_ref()
                .ok_or("A histogram has no bucket options")?,
        )?;
        let mut counts = histogram.bucket_counts;
        counts.resize(buckets.count(), 0);
        Ok(Self {
            buckets,
            counts,
            total: histogram.count.unwrap_or(0),
        })
    }
}
