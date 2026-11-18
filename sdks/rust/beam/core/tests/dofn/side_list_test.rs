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

//! Tests for `ListEntry` and the reads of a `ListView` by index.

use beam_core::coders::DefaultCoder;
use beam_core::prelude::*;
use beam_core::transforms::sdf::OffsetRange;
use beam_core::values::{LIST_RANGES_KEY, ListEntry, SideInputKind, URN_SIDE_INPUT_MULTIMAP};

use crate::common::{MultimapCell, read_keyed};

fn list_view() -> ListView<String> {
    let p = Pipeline::new();
    p.apply("Words", Create::new(vec!["a".to_string()]))
        .apply("AsList", AsList::new())
        .as_list()
}

/// The cell of `position` with `values`.
fn at(tag: &str, position: i64, values: &[&str]) -> MultimapCell {
    (
        (tag.to_string(), position.encode().unwrap()),
        values
            .iter()
            .map(|v| ListEntry::Value(v.to_string()).encode().unwrap())
            .collect(),
    )
}

/// The cell of the offset ranges.
fn ranges(tag: &str, ranges: &[(i64, i64)]) -> MultimapCell {
    (
        (tag.to_string(), LIST_RANGES_KEY.encode().unwrap()),
        ranges
            .iter()
            .map(|&(start, end)| {
                ListEntry::<String>::Range(OffsetRange::new(start, end))
                    .encode()
                    .unwrap()
            })
            .collect(),
    )
}

#[test]
fn a_list_entry_encodes_as_a_kv_of_two_nullable_parts() {
    let value = ListEntry::Value(5_i64);
    let range = ListEntry::<i64>::Range(OffsetRange::new(1, 2));
    assert_eq!(value.encode().unwrap(), [0x01, 0x05, 0x00]);
    assert_eq!(range.encode().unwrap(), [0x00, 0x01, 0x01, 0x02]);
    assert_eq!(ListEntry::decode(&value.encode().unwrap()).unwrap(), value);
    assert_eq!(ListEntry::decode(&range.encode().unwrap()).unwrap(), range);

    let both = ListEntry::<i64>::decode(&[0x01, 0x05, 0x01, 0x01, 0x02]).unwrap_err();
    assert!(both.to_string().contains("exactly one"), "{both}");
    let neither = ListEntry::<i64>::decode(&[0x00, 0x00]).unwrap_err();
    assert!(neither.to_string().contains("exactly one"), "{neither}");
}

#[test]
fn a_list_view_reads_by_index_and_in_index_order() {
    let view = list_view();
    assert_eq!(view.kind(), SideInputKind::List);
    assert_eq!(view.access_pattern(), URN_SIDE_INPUT_MULTIMAP);
    let tag = view.tag().to_string();
    let cells = vec![
        ranges(&tag, &[(20, 21), (10, 12)]),
        at(&tag, 10, &["a"]),
        at(&tag, 11, &["b"]),
        at(&tag, 20, &["c"]),
    ];

    read_keyed(&view, cells, |list| {
        assert_eq!((list.len(), list.is_empty()), (3, false));
        let by_index = (0..3)
            .map(|i| list.get(i).unwrap().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(by_index, ["a", "b", "c"]);
        assert_eq!(list.get(3).unwrap(), None);
        assert_eq!(list.to_vec().unwrap(), ["a", "b", "c"]);
    });
}

#[test]
fn overlapping_ranges_give_one_index_for_each_value_at_a_position() {
    let view = list_view();
    let tag = view.tag().to_string();
    let cells = vec![
        ranges(&tag, &[(10, 13), (11, 12)]),
        at(&tag, 10, &["a"]),
        at(&tag, 11, &["b", "c"]),
        at(&tag, 12, &["d"]),
    ];

    read_keyed(&view, cells, |list| {
        assert_eq!(list.len(), 4);
        assert_eq!(list.get(2).unwrap().as_deref(), Some("c"));
        assert_eq!(list.get(3).unwrap().as_deref(), Some("d"));
        assert_eq!(list.to_vec().unwrap(), ["a", "b", "c", "d"]);
    });
}

#[test]
fn a_window_without_ranges_reads_as_an_empty_list() {
    let view = list_view();
    read_keyed(&view, Vec::new(), |list| {
        assert_eq!((list.len(), list.is_empty()), (0, true));
        assert_eq!(list.get(0).unwrap(), None);
        assert_eq!(list.to_vec().unwrap(), Vec::<String>::new());
    });
}
