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

//! Side input view kinds: keys and entries of keyed views, map views, list views and
//! singleton defaults.

use std::time::Duration;

use beam::prelude::*;
use beam::testing::{TestPipeline, passert};

use crate::dofns::AssignTimestampDoFn;

/// Side input events of the keyed view tests: key, value, timestamp in ms. Windows of 10 ms
/// put `a` in `[0, 10)` and `b` in `[10, 20)`.
const KEYED_SIDE: [(&str, i64, i64); 3] = [("a", 1, 1), ("a", 2, 7), ("b", 3, 14)];

/// [`KEYED_SIDE`] in fixed windows of 10 ms, as `(key, value)`.
fn windowed_keyed_side(p: &TestPipeline) -> PCollection<(String, i64)> {
    p.apply(
        "CreateSideInput",
        Create::new(
            KEYED_SIDE
                .map(|(k, v, ts)| (k.to_string(), (v, ts)))
                .to_vec(),
        ),
    )
    .par_do("TimestampSide", AssignTimestampDoFn)
    .apply(
        "SideWindowInto",
        WindowInto::new(FixedWindows::of(Duration::from_millis(10))),
    )
}

/// Main elements at 5 ms and 13 ms, one in each window of [`windowed_keyed_side`].
fn windowed_main(p: &TestPipeline) -> PCollection<(String, i64)> {
    p.apply(
        "CreateMainInput",
        Create::new(vec![
            ("apple".to_string(), (0i64, 5i64)),
            ("banana".to_string(), (0, 13)),
        ]),
    )
    .par_do("TimestampMain", AssignTimestampDoFn)
    .apply(
        "MainWindowInto",
        WindowInto::new(FixedWindows::of(Duration::from_millis(10))),
    )
}

/// Validates `keys` and `entries` of a multimap side input. The runner lists the keys with
/// the `MultimapKeysSideInput` state key.
///
/// Java: `MapViewTest.testMultimapAsEntrySetSideInput`.
pub fn build_multimap_entries_side_input(p: &TestPipeline) {
    let side = p.apply(
        "CreateSideInput",
        Create::new(vec![
            ("a".to_string(), 1i64),
            ("a".to_string(), 2),
            ("b".to_string(), 3),
        ]),
    );
    let out = p
        .apply("CreateMainInput", Create::new(vec![2i64]))
        .map_with_side_inputs("OutputSideInputs", side.as_multimap(), |x, side| {
            let mut keys = side.keys()?.collect::<Result<Vec<String>>>()?;
            keys.sort();
            let mut entries = side
                .entries()?
                .map(|entry| {
                    entry.map(|(key, mut values)| {
                        values.sort_unstable();
                        (key, values.iter().map(|v| v * x).collect::<Vec<_>>())
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            entries.sort();
            Ok((keys, entries))
        });
    passert::that("AssertOut", &out).contains_in_any_order([(
        vec!["a".to_string(), "b".to_string()],
        vec![("a".to_string(), vec![2, 4]), ("b".to_string(), vec![6])],
    )]);
}

/// Validates `entries` of a multimap side input in fixed windows: each main element sees the
/// keys of its own window only.
///
/// Java: `MapViewTest.testWindowedMultimapAsEntrySetSideInput`.
pub fn build_windowed_multimap_entries_side_input(p: &TestPipeline) {
    let side = windowed_keyed_side(p);
    let out = windowed_main(p).map_with_side_inputs(
        "OutputSideInputs",
        side.as_multimap(),
        |(fruit, _): (String, i64), side| {
            let mut entries = side
                .entries()?
                .collect::<Result<Vec<(String, Vec<i64>)>>>()?;
            entries
                .iter_mut()
                .for_each(|(_, values)| values.sort_unstable());
            entries.sort();
            Ok((fruit, entries))
        },
    );
    passert::that("AssertOut", &out).contains_in_any_order([
        ("apple".to_string(), vec![("a".to_string(), vec![1, 2])]),
        ("banana".to_string(), vec![("b".to_string(), vec![3])]),
    ]);
}

/// Validates a map side input: `get` by key and `entries`.
///
/// Java: `MapViewTest.testMapSideInput`, `MapViewTest.testMapAsEntrySetSideInput`.
pub fn build_map_side_input(p: &TestPipeline) {
    let side = p.apply(
        "CreateSideInput",
        Create::new(vec![("a".to_string(), 1i64), ("b".to_string(), 3)]),
    );
    let out = p
        .apply(
            "CreateMainInput",
            Create::new(["apple", "banana", "blackberry"].map(String::from).to_vec()),
        )
        .map_with_side_inputs("OutputSideInputs", side.as_map(), |fruit, side| {
            let key: String = fruit.chars().take(1).collect();
            let value = side.get(&key)?.unwrap_or_default();
            let mut entries = side.entries()?.collect::<Result<Vec<(String, i64)>>>()?;
            entries.sort();
            Ok((fruit, value, entries))
        });
    let entries = vec![("a".to_string(), 1), ("b".to_string(), 3)];
    passert::that("AssertOut", &out).contains_in_any_order([
        ("apple".to_string(), 1, entries.clone()),
        ("banana".to_string(), 3, entries.clone()),
        ("blackberry".to_string(), 3, entries),
    ]);
}

/// Validates a map side input in fixed windows: `get` and `entries` see the keys of the
/// window of the main element.
///
/// Java: `MapViewTest.testWindowedMapSideInput`, `MapViewTest.testWindowedMapAsEntrySetSideInput`.
pub fn build_windowed_map_side_input(p: &TestPipeline) {
    let side = windowed_keyed_side(p).sum_per_key("SumPerKey");
    let out = windowed_main(p).map_with_side_inputs(
        "OutputSideInputs",
        side.as_map(),
        |(fruit, _): (String, i64), side| {
            let key: String = fruit.chars().take(1).collect();
            let value = side.get(&key)?;
            let entries = side.entries()?.collect::<Result<Vec<(String, i64)>>>()?;
            Ok((fruit, value, entries))
        },
    );
    passert::that("AssertOut", &out).contains_in_any_order([
        ("apple".to_string(), Some(3), vec![("a".to_string(), 3)]),
        ("banana".to_string(), Some(3), vec![("b".to_string(), 3)]),
    ]);
}

/// Validates an empty map side input: no key, no entry.
///
/// Java: `MapViewTest.testEmptyMapSideInput`.
pub fn build_empty_map_side_input(p: &TestPipeline) {
    let empty = p.apply("CreateEmptyView", Create::new(Vec::<(String, i64)>::new()));
    let out = p
        .apply("Create1", Create::new(vec![1i64]))
        .map_with_side_inputs("OutputSideInputs", empty.as_map(), |x, side| {
            let keys = side.keys()?.count() as i64;
            Ok((x, keys, side.get(&"a".to_string())?.is_none()))
        });
    passert::that("AssertOut", &out).contains_in_any_order([(1, 0, true)]);
}

/// Validates that a map side input fails the bundle on a key with two values.
///
/// Java: `MapViewTest.testMapSideInputWithNullValuesCatchesDuplicates`.
pub fn build_map_side_input_fails_on_duplicate_key(p: &TestPipeline) {
    let side = p.apply(
        "CreateSideInput",
        Create::new(vec![("a".to_string(), 1i64), ("a".to_string(), 2)]),
    );
    p.apply("CreateMainInput", Create::new(vec!["apple".to_string()]))
        .map_with_side_inputs("OutputSideInputs", side.as_map(), |fruit, side| {
            Ok((fruit, side.get(&"a".to_string())?))
        });
}

/// Validates a singleton side input with a default in fixed windows: a main window whose side
/// window is empty reads the default.
///
/// Java: `ViewTest.testWindowedSideInputFixedToFixedWithDefault`.
pub fn build_windowed_singleton_with_default(p: &TestPipeline) {
    let fixed = || WindowInto::new(FixedWindows::of(Duration::from_millis(10)));
    let view = p
        .apply(
            "CreateSideInput",
            Create::new(vec![("a".to_string(), (2i64, 14i64))]),
        )
        .par_do("TimestampSide", AssignTimestampDoFn)
        .map("Value", |(_, v): (String, i64)| v)
        .apply("SideWindowInto", fixed())
        .as_singleton()
        .with_default(&0);
    let out = windowed_main(p).map_with_side_inputs(
        "OutputSideInputs",
        view,
        |(fruit, _): (String, i64), side| Ok((fruit, side)),
    );
    passert::that("AssertOut", &out)
        .contains_in_any_order([("apple".to_string(), 0), ("banana".to_string(), 2)]);
}

/// Validates a singleton side input with a `None` default on an empty input.
///
/// Java: `ViewTest.testSideInputWithNullDefault`.
pub fn build_singleton_with_none_default(p: &TestPipeline) {
    let view = p
        .apply(
            "CreateEmptySideInput",
            Create::new(Vec::<Option<String>>::new()),
        )
        .as_singleton()
        .with_default(&None);
    let out = p
        .apply("CreateMainInput", Create::new(vec![0i64]))
        .map_with_side_inputs("OutputSideInput", view, |_, side| Ok(side));
    passert::that("AssertOut", &out).contains_in_any_order([None::<String>]);
}

/// Validates a list side input read by index and in order: every main element sees the whole
/// list.
///
/// Java: `ViewTest.testListSideInput`, `ViewTest.testListWithRandomAccessSideInput`.
pub fn build_list_side_input(p: &TestPipeline) {
    let view = p
        .apply(
            "CreateSideInput",
            Create::new((0i64..100).collect::<Vec<_>>()),
        )
        .apply("AsList", AsList::new())
        .as_list();
    let out = p
        .apply("CreateMainInput", Create::new(vec![1000i64, 2000]))
        .map_with_side_inputs("OutputSideInputs", view, |x, list| {
            let mut by_index = (0..list.len())
                .map(|i| list.get(i)?.ok_or("an index below len has a value".into()))
                .collect::<Result<Vec<i64>>>()?;
            by_index.sort_unstable();
            let in_order = list.iter().sum::<Result<i64>>()?;
            Ok((x, by_index == (0..100).collect::<Vec<_>>(), x + in_order))
        });
    passert::that("AssertOut", &out)
        .contains_in_any_order([(1000, true, 1000 + 4950), (2000, true, 2000 + 4950)]);
}

/// Validates a list side input in fixed windows: each main element sees the list of its
/// window.
///
/// Java: `ViewTest.testWindowedListSideInput`.
pub fn build_windowed_list_side_input(p: &TestPipeline) {
    let view = windowed_keyed_side(p)
        .map("Value", |(_, v): (String, i64)| v)
        .apply("AsList", AsList::new())
        .as_list();
    let out = windowed_main(p).map_with_side_inputs(
        "OutputSideInputs",
        view,
        |(fruit, _): (String, i64), list| {
            let mut values = list.to_vec()?;
            values.sort_unstable();
            Ok((fruit, values))
        },
    );
    passert::that("AssertOut", &out).contains_in_any_order([
        ("apple".to_string(), vec![1, 2]),
        ("banana".to_string(), vec![3]),
    ]);
}

/// Validates an empty list side input: no element, and no value at index 0.
///
/// Java: `ViewTest.testEmptyListSideInput`.
pub fn build_empty_list_side_input(p: &TestPipeline) {
    let view = p
        .apply("CreateEmptyView", Create::new(Vec::<i64>::new()))
        .apply("AsList", AsList::new())
        .as_list();
    let out = p
        .apply("Create1", Create::new(vec![1i64]))
        .map_with_side_inputs("OutputSideInputs", view, |x, list| {
            Ok((x, list.len() as i64, list.get(0)?.is_none()))
        });
    passert::that("AssertOut", &out).contains_in_any_order([(1, 0, true)]);
}
