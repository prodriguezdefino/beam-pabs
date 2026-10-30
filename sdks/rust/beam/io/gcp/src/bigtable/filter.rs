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

//! Constructors for server-side [`RowFilter`] messages, in the [`RowFilterExt`] trait.
//!
//! [`RowFilterExt::value_bitmask`] keeps the cells whose value has every bit of a mask set.
//! [`RowFilterExt::column_value_bitmask`] keeps or drops whole rows on the value of one column.

use std::fmt::Write as _;

use bytes::Bytes;

use crate::bigtable::proto::{self, RowFilter, ValueBitmask, row_filter::Filter};

/// Constructors for [`proto::RowFilter`]. Import the trait to call them as
/// `RowFilter::chain(...)`.
pub trait RowFilterExt: Sized {
    /// Creates a `chain` filter that applies `filters` sequentially.
    fn chain(filters: impl Into<Vec<RowFilter>>) -> RowFilter {
        RowFilter {
            filter: Some(Filter::Chain(proto::row_filter::Chain {
                filters: filters.into(),
            })),
        }
    }

    /// Creates an `interleave` filter that unions the results of `filters`.
    fn interleave(filters: impl Into<Vec<RowFilter>>) -> RowFilter {
        RowFilter {
            filter: Some(Filter::Interleave(proto::row_filter::Interleave {
                filters: filters.into(),
            })),
        }
    }

    /// Creates a `condition` filter that evaluates `predicate` and branches to `true_filter`
    /// or `false_filter`.
    fn condition(
        predicate: RowFilter,
        true_filter: Option<RowFilter>,
        false_filter: Option<RowFilter>,
    ) -> RowFilter {
        RowFilter {
            filter: Some(Filter::Condition(Box::new(proto::row_filter::Condition {
                predicate_filter: Some(Box::new(predicate)),
                true_filter: true_filter.map(Box::new),
                false_filter: false_filter.map(Box::new),
            }))),
        }
    }

    /// Creates a `pass_all_filter`.
    fn pass_all() -> RowFilter {
        RowFilter {
            filter: Some(Filter::PassAllFilter(true)),
        }
    }

    /// Creates a `block_all_filter`.
    fn block_all() -> RowFilter {
        RowFilter {
            filter: Some(Filter::BlockAllFilter(true)),
        }
    }

    /// Creates a `strip_value_transformer` filter that replaces cell values with empty bytes.
    fn strip_values() -> RowFilter {
        RowFilter {
            filter: Some(Filter::StripValueTransformer(true)),
        }
    }

    /// Creates a `cells_per_column_limit_filter` keeping the latest `limit` cells per column.
    fn cells_per_column_limit(limit: i32) -> RowFilter {
        RowFilter {
            filter: Some(Filter::CellsPerColumnLimitFilter(limit)),
        }
    }

    /// Creates a `cells_per_row_limit_filter` keeping the first `limit` cells per row.
    fn cells_per_row_limit(limit: i32) -> RowFilter {
        RowFilter {
            filter: Some(Filter::CellsPerRowLimitFilter(limit)),
        }
    }

    /// Creates a `family_name_regex_filter`.
    fn family_name_regex(regex: impl Into<String>) -> RowFilter {
        RowFilter {
            filter: Some(Filter::FamilyNameRegexFilter(regex.into())),
        }
    }

    /// Creates a `column_qualifier_regex_filter`.
    fn column_qualifier_regex(regex: impl Into<Bytes>) -> RowFilter {
        RowFilter {
            filter: Some(Filter::ColumnQualifierRegexFilter(regex.into())),
        }
    }

    /// Creates a `row_key_regex_filter`.
    fn row_key_regex(regex: impl Into<Bytes>) -> RowFilter {
        RowFilter {
            filter: Some(Filter::RowKeyRegexFilter(regex.into())),
        }
    }

    /// Creates a `value_regex_filter`.
    fn value_regex(regex: impl Into<Bytes>) -> RowFilter {
        RowFilter {
            filter: Some(Filter::ValueRegexFilter(regex.into())),
        }
    }

    /// Creates a `timestamp_range_filter` for `[start_micros, end_micros)`.
    fn timestamp_range(start_micros: i64, end_micros: i64) -> RowFilter {
        RowFilter {
            filter: Some(Filter::TimestampRangeFilter(proto::TimestampRange {
                start_timestamp_micros: start_micros,
                end_timestamp_micros: end_micros,
            })),
        }
    }

    /// Creates a `value_bitmask_filter` that keeps the cells where `(value & mask) == mask`.
    ///
    /// The value must have the same length as `mask`, else the cell does not match. So an empty
    /// mask matches only empty values. The filter applies to each cell, not to the row.
    fn value_bitmask(mask: impl Into<Bytes>) -> RowFilter {
        RowFilter {
            filter: Some(Filter::ValueBitmaskFilter(ValueBitmask {
                mask: mask.into(),
            })),
        }
    }

    /// Creates a filter that keeps every cell of a row when the column `family:qualifier` has a
    /// cell that matches [`value_bitmask`](Self::value_bitmask). The filter drops the other rows.
    ///
    /// The family and the qualifier are literal names: the filter escapes them for RE2.
    fn column_value_bitmask(
        family: &str,
        qualifier: impl AsRef<[u8]>,
        mask: impl Into<Bytes>,
    ) -> RowFilter {
        let predicate = Self::chain([
            Self::family_name_regex(family_regex(family)),
            Self::column_qualifier_regex(qualifier_regex(qualifier.as_ref())),
            Self::value_bitmask(mask),
        ]);
        Self::condition(predicate, Some(Self::pass_all()), None)
    }

    /// Encodes this [`RowFilter`] into its wire-format protobuf bytes (`google.bigtable.v2.RowFilter`).
    fn to_proto_bytes(&self) -> Vec<u8>;
}

impl RowFilterExt for RowFilter {
    fn to_proto_bytes(&self) -> Vec<u8> {
        use prost::Message as _;
        self.encode_to_vec()
    }
}

/// Returns an RE2 regex that matches only `family`. RE2 reads a family regex as UTF-8, so
/// letters, digits, `_` and non-ASCII characters stay literal, other ASCII punctuation gets a
/// backslash, and the other ASCII characters become `\x{HH}`.
fn family_regex(family: &str) -> String {
    family.chars().fold(String::from("^"), |mut out, c| {
        match c {
            c if c.is_ascii_alphanumeric() || c == '_' || !c.is_ascii() => out.push(c),
            c if c.is_ascii_punctuation() => {
                out.push('\\');
                out.push(c);
            }
            c => {
                let _ = write!(out, "\\x{{{:02x}}}", u32::from(c));
            }
        }
        out
    }) + "$"
}

/// Returns an RE2 regex that matches only `qualifier`. RE2 reads a qualifier regex as raw
/// bytes, so every byte that is not ASCII alphanumeric becomes `\xHH`.
fn qualifier_regex(qualifier: &[u8]) -> Bytes {
    let pattern = qualifier.iter().fold(String::from("^"), |mut out, &b| {
        match b {
            b if b.is_ascii_alphanumeric() => out.push(char::from(b)),
            b => {
                let _ = write!(out, "\\x{b:02x}");
            }
        }
        out
    }) + "$";
    Bytes::from(pattern)
}
