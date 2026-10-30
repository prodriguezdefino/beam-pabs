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

//! Tests for the native `value_bitmask_filter` builders of [`RowFilter`].

use bytes::Bytes;
use gcp::bigtable::RowFilterExt as _;
use gcp::bigtable::proto::row_filter::{Chain, Condition, Filter};
use gcp::bigtable::proto::{RowFilter, ValueBitmask};

#[test]
fn value_bitmask_encodes_field_20_with_the_nested_mask() {
    let filter = RowFilter::value_bitmask(vec![0x0F, 0xF0]);
    assert_eq!(
        filter.filter,
        Some(Filter::ValueBitmaskFilter(ValueBitmask {
            mask: Bytes::from_static(&[0x0F, 0xF0]),
        }))
    );
    // Field 20 with wire type 2 is the varint 0xA2 0x01. The nested message is field 1 with
    // wire type 2 (0x0A), the mask length and the mask bytes.
    assert_eq!(
        filter.to_proto_bytes(),
        vec![0xA2, 0x01, 0x04, 0x0A, 0x02, 0x0F, 0xF0]
    );
}

#[test]
fn value_bitmask_with_an_empty_mask_encodes_an_empty_message() {
    assert_eq!(
        RowFilter::value_bitmask(Bytes::new()).to_proto_bytes(),
        vec![0xA2, 0x01, 0x00]
    );
}

#[test]
fn column_value_bitmask_keeps_whole_rows_on_one_column() {
    let filter = RowFilter::column_value_bitmask("cf", "flags", vec![0x80]);
    let expected = RowFilter {
        filter: Some(Filter::Condition(Box::new(Condition {
            predicate_filter: Some(Box::new(RowFilter {
                filter: Some(Filter::Chain(Chain {
                    filters: vec![
                        RowFilter::family_name_regex("^cf$"),
                        RowFilter::column_qualifier_regex(Bytes::from_static(b"^flags$")),
                        RowFilter::value_bitmask(vec![0x80]),
                    ],
                })),
            })),
            true_filter: Some(Box::new(RowFilter::pass_all())),
            false_filter: None,
        }))),
    };
    assert_eq!(filter, expected);
}

/// Returns the family and qualifier regexes of a `column_value_bitmask` filter.
fn column_regexes(filter: RowFilter) -> (String, Bytes) {
    let Some(Filter::Condition(condition)) = filter.filter else {
        panic!("expected a condition filter");
    };
    let Some(Filter::Chain(Chain { filters })) = condition.predicate_filter.and_then(|p| p.filter)
    else {
        panic!("expected a chain predicate");
    };
    match filters.as_slice() {
        [
            RowFilter {
                filter: Some(Filter::FamilyNameRegexFilter(family)),
            },
            RowFilter {
                filter: Some(Filter::ColumnQualifierRegexFilter(qualifier)),
            },
            _,
        ] => (family.clone(), qualifier.clone()),
        other => panic!("unexpected predicate {other:?}"),
    }
}

#[test]
fn column_value_bitmask_escapes_special_characters_in_family_and_qualifier() {
    let (family, qualifier) = column_regexes(RowFilter::column_value_bitmask(
        "my_cf.v1-x",
        b"q.*",
        vec![0x01],
    ));
    assert_eq!(family, r"^my_cf\.v1\-x$");
    assert_eq!(qualifier, Bytes::from_static(br"^q\x2e\x2a$"));

    // A space in the family becomes a hex escape. Non-ASCII characters stay
    // literal, and every byte of the qualifier that is not alphanumeric becomes `\xHH`.
    let (family, qualifier) = column_regexes(RowFilter::column_value_bitmask(
        "a b\u{e9}",
        [b'k', 0x00, 0xFF, b'_'],
        vec![0x01],
    ));
    assert_eq!(family, "^a\\x{20}b\u{e9}$");
    assert_eq!(qualifier, Bytes::from_static(br"^k\x00\xff\x5f$"));
}
