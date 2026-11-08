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

//! Side inputs: every view type, windowed views, side inputs in stateful DoFns, timers and
//! bundle finalization, and the singleton error paths.

use std::collections::BTreeSet;
use std::time::Duration;

use beam::io::file::get_filesystem;
use beam::prelude::*;
use beam::runners::PipelineResult;
use beam::testing::{TestPipeline, TestPipelineError, TestStream, passert};
use beam::transforms::WithFinalizer;

use super::finalization::{marker_dir, words_in, write_marker};
use crate::dofns::{
    AssignTimestampDoFn, BufferSideSingletonDoFn, BufferWithSideListDoFn, EnrichOrderDoFn,
    SideInputInTimerDoFn, SideTaggedPCollections, TaggedSideInputsDoFn,
};
use crate::{Expectation, RunnerKind};

/// Validates the singleton, iterable and multimap views, each read by the main input.
pub fn build_side_input_views(p: &TestPipeline) {
    let cart = p.apply(
        "Cart",
        Create::new(
            ["apple", "milk", "bread", "caviar"]
                .map(String::from)
                .to_vec(),
        ),
    );
    let tax_percent = p.apply("TaxPercent", Create::new(vec![20i64]));
    let exempt = p.apply(
        "Exempt",
        Create::new(["milk", "bread"].map(String::from).to_vec()),
    );
    // Two prices for bread: a multimap returns every value of a key.
    let prices = p.apply(
        "Prices",
        Create::new(vec![
            ("apple".to_string(), 100i64),
            ("milk".to_string(), 200),
            ("bread".to_string(), 150),
            ("bread".to_string(), 170),
        ]),
    );

    let singleton =
        cart.map_with_side_inputs("Singleton", tax_percent.as_singleton(), |item, tax| {
            Ok(format!("{item}:{tax}"))
        });
    passert::that("SingletonView", &singleton)
        .contains_in_any_order(["apple:20", "milk:20", "bread:20", "caviar:20"].map(String::from));

    let iterable = cart.map_with_side_inputs("Iterable", exempt.as_iter(), |item, exempt| {
        let mut sorted = exempt.to_vec()?;
        sorted.sort();
        Ok((item.clone(), (sorted.contains(&item), sorted.join("+"))))
    });
    passert::that("IterableView", &iterable).contains_in_any_order([
        ("apple".to_string(), (false, "bread+milk".to_string())),
        ("milk".to_string(), (true, "bread+milk".to_string())),
        ("bread".to_string(), (true, "bread+milk".to_string())),
        ("caviar".to_string(), (false, "bread+milk".to_string())),
    ]);

    let multimap = cart.map_with_side_inputs("Multimap", prices.as_multimap(), |item, prices| {
        let mut found = prices.get_all(&item)?.collect::<Result<Vec<_>>>()?;
        found.sort_unstable();
        Ok((item, found))
    });
    passert::that("MultimapView", &multimap).contains_in_any_order([
        ("apple".to_string(), vec![100]),
        ("milk".to_string(), vec![200]),
        ("bread".to_string(), vec![150, 170]),
        // A missing key reads as no values, not as an error.
        ("caviar".to_string(), vec![]),
    ]);
}

/// Validates an empty iterable side input: it reads as an empty list.
pub fn build_empty_iterable_side_input(p: &TestPipeline) {
    let nothing = p
        .apply("Nothing", Create::new(vec![0i64]))
        .filter("DropAll", |_: &i64| false);
    let main = p.apply("Main", Create::new(vec![1i64, 2]));
    let out = main.map_with_side_inputs("ReadEmpty", nothing.as_iter(), |x, side| {
        Ok((x, side.iter()?.count() as i64))
    });
    passert::that("AssertOut", &out).contains_in_any_order([(1, 0), (2, 0)]);
}

/// Validates windowed side inputs: each main-input element reads the side input of its
/// own window, not the whole side collection.
pub fn build_windowed_side_input(p: &TestPipeline) {
    let fixed = || WindowInto::new(FixedWindows::of(Duration::from_secs(10)));

    let rates = p
        .apply(
            "Rates",
            Create::new(vec![
                ("rate".to_string(), (10i64, 1_000i64)),
                ("rate".to_string(), (20i64, 11_000i64)),
            ]),
        )
        .par_do("TimestampRates", AssignTimestampDoFn)
        .apply("WindowRates", fixed())
        .map("RateValue", |(_, v): (String, i64)| v);

    let orders = p
        .apply(
            "Orders",
            Create::new(vec![
                ("a".to_string(), (1i64, 2_000i64)),
                ("b".to_string(), (2i64, 12_000i64)),
                ("c".to_string(), (3i64, 9_999i64)),
            ]),
        )
        .par_do("TimestampOrders", AssignTimestampDoFn)
        .apply("WindowOrders", fixed());

    let priced = orders.map_with_side_inputs("ApplyRate", rates.as_singleton(), |(k, v), rate| {
        Ok((k, v * rate))
    });
    passert::that("AssertPriced", &priced).contains_in_any_order([
        ("a".to_string(), 10),
        ("b".to_string(), 40),
        ("c".to_string(), 30),
    ]);

    let ts_data = p
        .apply(
            "CrossWindowData",
            Create::new(vec![
                ("k".to_string(), (1i64, 990i64)),
                ("k".to_string(), (2i64, 1_990i64)),
                ("k".to_string(), (3i64, 2_990i64)),
            ]),
        )
        .par_do("TimestampCrossWindow", AssignTimestampDoFn)
        .map("DropCrossKey", |(_, v): (String, i64)| v);

    let w1 = Duration::from_secs(1);
    let check_sums =
        |label: &str, main: PCollection<i64>, side: PCollection<i64>, expected: &[i64]| {
            let sums = main
                .map_with_side_inputs(
                    format!("{label}/SumSide"),
                    side.as_iter(),
                    |v, side_vals| Ok(v + side_vals.to_vec()?.into_iter().sum::<i64>()),
                )
                .apply(format!("{label}/Global"), WindowInto::new(GlobalWindows));
            passert::that(format!("{label}/Assert"), &sums)
                .contains_in_any_order(expected.iter().copied());
        };

    check_sums(
        "FixedGlobal",
        ts_data.apply("FG/Main", WindowInto::new(FixedWindows::of(w1))),
        ts_data.apply("FG/Side", WindowInto::new(GlobalWindows)),
        &[7, 8, 9],
    );
    check_sums(
        "FixedSame",
        ts_data.apply("FS/Main", WindowInto::new(FixedWindows::of(w1))),
        ts_data.apply("FS/Side", WindowInto::new(FixedWindows::of(w1))),
        &[2, 4, 6],
    );
    check_sums(
        "FixedBig",
        ts_data.apply("FB/Main", WindowInto::new(FixedWindows::of(w1))),
        ts_data.apply(
            "FB/Side",
            WindowInto::new(FixedWindows::of(Duration::from_secs(10))),
        ),
        &[7, 8, 9],
    );
    check_sums(
        "FixedSliding",
        ts_data.apply("FSl/Main", WindowInto::new(FixedWindows::of(w1))),
        ts_data.apply(
            "FSl/Side",
            WindowInto::new(SlidingWindows::of(w1 * 2).every(w1)),
        ),
        &[2, 5, 8],
    );
    check_sums(
        "SlidingFixed",
        ts_data.apply(
            "SlF/Main",
            WindowInto::new(SlidingWindows::of(w1 * 2).every(w1)),
        ),
        ts_data.apply("SlF/Side", WindowInto::new(FixedWindows::of(w1))),
        &[2, 3, 4, 5, 6, 3],
    );
}

/// Reading an empty collection as a singleton fails the pipeline, saying the singleton
/// was empty.
pub const EMPTY_SINGLETON_SIDE_INPUT_FAILS: Expectation =
    Expectation::FailsWith(&["Empty singleton side input"]);

/// Builds a pipeline reading an empty collection as a singleton; see
/// [`EMPTY_SINGLETON_SIDE_INPUT_FAILS`].
pub fn build_empty_singleton_side_input_fails(p: &TestPipeline) {
    let nothing = p
        .apply("Nothing", Create::new(vec![0i64]))
        .filter("DropAll", |_: &i64| false);
    let main = p.apply("Main", Create::new(vec![1i64]));
    let out = main.map_with_side_inputs("ReadEmpty", nothing.as_singleton(), |x, s| Ok(x + s));
    passert::that("Unreachable", &out).empty();
}

/// Reading a collection of several elements as a singleton fails the pipeline. The
/// pipeline does not pick one of the elements.
pub const MULTI_ELEMENT_SINGLETON_SIDE_INPUT_FAILS: Expectation =
    Expectation::FailsWith(&["singleton", "more than one"]);

/// Builds a pipeline reading two elements as a singleton; see
/// [`MULTI_ELEMENT_SINGLETON_SIDE_INPUT_FAILS`].
pub fn build_multi_element_singleton_side_input_fails(p: &TestPipeline) {
    let two = p.apply("Two", Create::new(vec![1i64, 2]));
    let main = p.apply("Main", Create::new(vec![10i64]));
    let out = main.map_with_side_inputs("ReadTwo", two.as_singleton(), |x, s| Ok(x + s));
    passert::that("Unreachable", &out).empty();
}

/// Validates reading three side inputs as a tuple, non-string multimap coders, and a
/// 400-entry multimap.
pub fn build_side_input_multiple_and_coders(p: &TestPipeline) {
    let factor = p.apply("Factor", Create::new(vec![10i64]));
    let offsets = p.apply("Offsets", Create::new(vec![1i64, 2, 3]));
    let catalog = p.apply(
        "Catalog",
        Create::new(vec![
            (1i64, ("a".to_string(), 5i64)),
            (1, ("b".to_string(), 7)),
            (2, ("c".to_string(), 3)),
        ]),
    );
    let keys = p.apply("Keys", Create::new(vec![1i64, 2, 99]));

    let combined = keys.map_with_side_inputs(
        "ReadAllThree",
        (
            factor.as_singleton(),
            offsets.as_iter(),
            catalog.as_multimap(),
        ),
        |key, (factor, offsets, catalog)| {
            let offset_sum: i64 = offsets.to_vec()?.into_iter().sum();
            let mut items = catalog.get_all(&key)?.collect::<Result<Vec<_>>>()?;
            items.sort();
            let adjusted: Vec<(String, i64)> = items
                .into_iter()
                .map(|(label, v)| (label, v * factor + offset_sum))
                .collect();
            Ok((key, adjusted))
        },
    );
    passert::that("AssertCombined", &combined).contains_in_any_order([
        (1i64, vec![("a".to_string(), 56i64), ("b".to_string(), 76)]),
        (2, vec![("c".to_string(), 36)]),
        (99, vec![]),
    ]);

    let large_entries: Vec<(i64, i64)> = (0..200i64)
        .flat_map(|k| [(k, k * 10 + 1), (k, k * 10 + 2)])
        .collect();
    let large_map = p.apply("LargeMap", Create::new(large_entries));
    let lookups = p.apply("Lookups", Create::new(vec![0i64, 50, 199, 250]));

    let stats = lookups.map_with_side_inputs("LookupLarge", large_map.as_multimap(), |key, map| {
        let vals = map.get_all(&key)?.collect::<Result<Vec<_>>>()?;
        let count = vals.len() as i64;
        let sum: i64 = vals.into_iter().sum();
        Ok((key, (count, sum)))
    });
    passert::that("AssertLargeMultimap", &stats).contains_in_any_order([
        (0i64, (2i64, 3i64)),
        (50, (2, 1003)),
        (199, (2, 3983)),
        (250, (0, 0)),
    ]);
}

/// The fixed windows of 10 s that the windowed builders below use.
fn fixed_10s() -> WindowInto<FixedWindows> {
    WindowInto::new(FixedWindows::of(Duration::from_secs(10)))
}

/// Validates singleton, iterable and multimap views of the global window, read from a main
/// input in fixed windows. The runner maps each main window to the global window of the view.
pub fn build_global_side_inputs_in_fixed_windows(p: &TestPipeline) {
    let main = p
        .apply(
            "Main",
            Create::new(vec![
                ("m".to_string(), (1i64, 1_000i64)),
                ("m".to_string(), (3, 11_000)),
            ]),
        )
        .par_do("AssignTimestamps", AssignTimestampDoFn)
        .apply("WindowInto", fixed_10s())
        .map("DropKey", |(_, v): (String, i64)| v);

    let iter_side = p.apply("IterSide", Create::new(vec![100i64, 200]));
    let sums = main.map_with_side_inputs("AddSide", iter_side.as_iter(), |v, side| {
        Ok(v + side.to_vec()?.into_iter().sum::<i64>())
    });
    passert::that("AssertIter", &sums).contains_in_any_order([301, 303]);

    let single_side = p.apply("SingleSide", Create::new(vec![10i64]));
    let products =
        main.map_with_side_inputs("MultiplySide", single_side.as_singleton(), |v, factor| {
            Ok(v * factor)
        });
    passert::that("AssertSingleton", &products).contains_in_any_order([10, 30]);

    let map_side = p.apply(
        "MapSide",
        Create::new(vec![(1i64, "one".to_string()), (3, "three".to_string())]),
    );
    let names = main.map_with_side_inputs("LookupSide", map_side.as_multimap(), |v, names| {
        Ok(names.get(&v)?.unwrap_or_default())
    });
    passert::that("AssertMultimap", &names)
        .contains_in_any_order(["one", "three"].map(String::from));
}

/// Validates a struct DoFn that reads two views, then a closure that reads two views. The
/// IterView is read by both transforms.
///
/// Java: `ParDoTest$MultipleInputsAndOutputTests.testSameSideInputReadTwice`.
pub fn build_side_inputs_in_dofn_and_closure(p: &TestPipeline) {
    let mult_view = p
        .apply("Multiplier", Create::new(vec![3i64]))
        .as_singleton();
    let vip_view = p
        .apply("VIPs", Create::new(vec!["alice".to_string()]))
        .as_iter();
    let orders = p.apply(
        "Orders",
        Create::new(vec![("alice".to_string(), 10i64), ("bob".to_string(), 10)]),
    );
    let enriched = orders.par_do(
        "EnrichOrders",
        EnrichOrderDoFn {
            multiplier_view: mult_view,
            vip_users_view: vip_view.clone(),
        },
    );

    let prefix_view = p
        .apply("Prefix", Create::new(vec!["[REWARD]".to_string()]))
        .as_singleton();
    let formatted = enriched.par_do_fn_with_side_inputs(
        "FormatRewards",
        (&prefix_view, &vip_view),
        |s: String, (pfx, vips), ctx| {
            let vip_count = vips.iter()?.count();
            ctx.emit(format!("{pfx} {s} ({vip_count})"))
        },
    );
    passert::that("AssertFormatted", &formatted)
        .contains_in_any_order(["[REWARD] alice=30 (1)", "[REWARD] bob=10 (1)"].map(String::from));
}

/// Validates the broadcast joins, which read the right side as a multimap side input.
pub fn build_broadcast_joins(p: &TestPipeline) {
    let events = p.apply(
        "Events",
        Create::new(vec![
            ("us".to_string(), 1i64),
            ("eu".to_string(), 2),
            ("apac".to_string(), 3),
        ]),
    );
    let regions = p.apply(
        "Regions",
        Create::new(vec![
            ("us".to_string(), "United States".to_string()),
            ("eu".to_string(), "Europe".to_string()),
        ]),
    );

    let inner = events.broadcast_inner_join("BroadcastInner", &regions);
    passert::that("AssertInner", &inner).contains_in_any_order([
        ("eu".to_string(), (2, "Europe".to_string())),
        ("us".to_string(), (1, "United States".to_string())),
    ]);

    let left = events.broadcast_left_join("BroadcastLeft", &regions);
    passert::that("AssertLeft", &left).contains_in_any_order([
        ("apac".to_string(), (3, None)),
        ("eu".to_string(), (2, Some("Europe".to_string()))),
        ("us".to_string(), (1, Some("United States".to_string()))),
    ]);
}

/// Validates a multimap side input in fixed windows: each main element reads the entries
/// of its own window only.
///
/// Java: `MapViewTest.testWindowedMultimapSideInput`.
pub fn build_windowed_multimap_side_input(p: &TestPipeline) {
    let fixed = || WindowInto::new(FixedWindows::of(Duration::from_millis(10)));
    let side = p
        .apply(
            "CreateSideInput",
            Create::new(vec![
                ("a".to_string(), (1i64, 1i64)),
                ("a".to_string(), (1, 2)),
                ("a".to_string(), (2, 7)),
                ("b".to_string(), (3, 14)),
            ]),
        )
        .par_do("TimestampSide", AssignTimestampDoFn)
        .apply("SideWindowInto", fixed());
    let main = p
        .apply(
            "CreateMainInput",
            Create::new(vec![
                ("apple".to_string(), (0i64, 5i64)),
                ("banana".to_string(), (0, 13)),
                ("blackberry".to_string(), (0, 16)),
            ]),
        )
        .par_do("TimestampMain", AssignTimestampDoFn)
        .apply("MainWindowInto", fixed());

    let out = main.map_with_side_inputs(
        "OutputSideInputs",
        side.as_multimap(),
        |(fruit, _): (String, i64), side| {
            let key: String = fruit.chars().take(1).collect();
            let mut values = side.get_all(&key)?.collect::<Result<Vec<i64>>>()?;
            values.sort_unstable();
            Ok((fruit, values))
        },
    );
    passert::that("AssertOut", &out).contains_in_any_order([
        ("apple".to_string(), vec![1, 1, 2]),
        ("banana".to_string(), vec![3]),
        ("blackberry".to_string(), vec![3]),
    ]);
}

/// Validates an empty multimap side input: every key reads as no values.
///
/// Java: `MapViewTest.testEmptyMultimapSideInput`. `SideMap` has no key iteration, so
/// the check reads two keys.
pub fn build_empty_multimap_side_input(p: &TestPipeline) {
    let empty = p.apply("CreateEmptyView", Create::new(Vec::<(String, i64)>::new()));
    let out = p
        .apply("Create1", Create::new(vec![1i64]))
        .map_with_side_inputs("OutputSideInputs", empty.as_multimap(), |x, side| {
            let all =
                side.get_all(&String::new())?.count() + side.get_all(&"a".to_string())?.count();
            Ok((x, all as i64, side.get(&"a".to_string())?.is_none()))
        });
    passert::that("AssertOut", &out).contains_in_any_order([(1, 0, true)]);
}

/// Validates a global combine of an empty input, read as a singleton side input: the side
/// input is the default value of the combine.
///
/// Java: `CombineTest$WindowingTests.testCombineGloballyAsSingletonView`.
pub fn build_combine_globally_as_singleton(p: &TestPipeline) {
    let view = p
        .apply("CreateEmptySideInput", Create::new(Vec::<i64>::new()))
        .sum_globally("SumGlobally")
        .as_singleton();
    let out = p
        .apply("CreateMainInput", Create::new(vec![0i64]))
        .map_with_side_inputs("OutputSideInput", view, |_, sum| Ok(sum));
    passert::that_singleton("AssertSum", &out).is_equal_to(0);
}

/// Validates the output of a flatten of empty inputs, read as an iterable side input.
///
/// Java: `FlattenTest.testEmptyFlattenAsSideInput`. A flatten with no input is a
/// construction error, so the flatten here has two empty inputs.
pub fn build_empty_flatten_side_input(p: &TestPipeline) {
    let first = p.apply("EmptyA", Create::new(Vec::<String>::new()));
    let second = p.apply("EmptyB", Create::new(Vec::<String>::new()));
    let view = first.flatten("Flatten", &[&second]).as_iter();
    let out = p
        .apply("CreateMain", Create::new(vec![0i64]))
        .par_do_fn_with_side_inputs(
            "OutputSideInput",
            view,
            |_: i64, side, ctx: &mut ProcessContext<'_, String>| {
                side.iter()?.try_for_each(|value| ctx.emit(value?))
            },
        );
    passert::that("AssertEmpty", &out).empty();
}

/// Validates an iterable side input whose elements are lists read from another iterable
/// side input.
///
/// Java: `ViewTest.testSideInputWithNestedIterables`.
pub fn build_nested_iterable_side_input(p: &TestPipeline) {
    let inner = p
        .apply("CreateVoid1", Create::new(vec![0i64]))
        .map("OutputOneInteger", |_: i64| 17i64)
        .as_iter();
    let outer = p
        .apply("CreateVoid2", Create::new(vec![0i64]))
        .map_with_side_inputs("OutputSideInput", inner, |_, inner| inner.to_vec())
        .as_iter();
    let out = p
        .apply("CreateVoid3", Create::new(vec![0i64]))
        .par_do_fn_with_side_inputs(
            "ReadIterableSideInput",
            outer,
            |_: i64, outer, ctx: &mut ProcessContext<'_, i64>| {
                outer
                    .iter()?
                    .try_for_each(|list| list?.into_iter().try_for_each(|value| ctx.emit(value)))
            },
        );
    passert::that("AssertOut", &out).contains_in_any_order([17]);
}

/// Validates that each element reads the same values of two singleton views, with a third
/// view in the same transform.
///
/// Java: `ParDoTest$MultipleInputsAndOutputTests.testParDoWithSideInputsIsCumulative`.
pub fn build_side_input_read_by_every_element(p: &TestPipeline) {
    let first = p
        .apply("CreateSideInput1", Create::new(vec![11i64]))
        .as_singleton();
    let other = p
        .apply("CreateSideInputUnread", Create::new(vec![-3333i64]))
        .as_singleton();
    let second = p
        .apply("CreateSideInput2", Create::new(vec![222i64]))
        .as_singleton();
    let out = p
        .apply("Create", Create::new(vec![3i64, -42, 666]))
        .map_with_side_inputs(
            "ReadSideInputs",
            (first, other, second),
            |x, (first, _, second)| Ok(format!("{x}:{first}+{second}")),
        );
    passert::that("AssertOut", &out)
        .contains_in_any_order(["3:11+222", "-42:11+222", "666:11+222"].map(String::from));
}

/// Validates a multi-output DoFn that declares three views and reads two of them.
///
/// Java: `ParDoTest$MultipleInputsAndOutputTests.testMultiOutputParDoWithSideInputs` and
/// `testMultiOutputParDoWithSideInputsIsCumulative`.
pub fn build_multi_output_side_inputs(p: &TestPipeline) {
    let side = |name: &str, value: i64| p.apply(name, Create::new(vec![value])).as_singleton();
    let SideTaggedPCollections { main, extra } =
        p.apply("Create", Create::new(vec![3i64, -42, 666])).par_do(
            "TaggedSideInputs",
            TaggedSideInputsDoFn {
                first: side("CreateSideInput1", 11),
                unread: side("CreateSideInputUnread", -3333),
                second: side("CreateSideInput2", 222),
            },
        );
    passert::that("AssertMain", &main)
        .contains_in_any_order(["3:11+222", "-42:11+222", "666:11+222"].map(String::from));
    passert::that("AssertExtra", &extra).empty();
}

/// Validates a stateful DoFn that reads an iterable side input after its bag state holds
/// four values.
///
/// Java: `ParDoTest$StateTests.testBagStateSideInput`.
pub fn build_bag_state_side_input(p: &TestPipeline) {
    let side = p
        .apply("CreateList", Create::new(vec![2i64, 1, 0]))
        .as_iter();
    let out = p
        .apply(
            "CreateMain",
            Create::new(
                [97i64, 42, 84, 12]
                    .map(|v| ("hello".to_string(), v))
                    .to_vec(),
            ),
        )
        .apply(
            "BufferWithSideList",
            ParDo::new(BufferWithSideListDoFn { side }),
        );
    passert::that("AssertOut", &out).contains_in_any_order([vec![12, 42, 84, 97], vec![0, 1, 2]]);
}

/// Validates a stateful DoFn that writes and reads bag state with a singleton side input.
///
/// Java: `ParDoTest$StateTests.testStateSideInput`.
pub fn build_state_side_input(p: &TestPipeline) {
    let side = p
        .apply("CreateSideInput1", Create::new(vec![2i64]))
        .as_singleton();
    let out = p
        .apply("Create", Create::new(vec![(1i64, 10i64), (1, 20)]))
        .apply(
            "BufferSideSingleton",
            ParDo::new(BufferSideSingletonDoFn { side }),
        );
    passert::that("AssertOut", &out).contains_in_any_order([34]);
}

/// How long the side input of [`build_side_input_not_ready_timer`] waits before it emits.
/// It is longer than the main input takes to reach the DoFn, so the side input is not ready.
const SIDE_INPUT_DELAY: Duration = Duration::from_secs(5);

/// Validates a singleton side input that a slow transform makes, read from
/// `process_element`, from a timer and from `on_window_expiration`.
///
/// Java: `ParDoTest$StateTests.testSideInputNotReadyTimer`.
pub fn build_side_input_not_ready_timer(p: &TestPipeline) {
    let side = p
        .apply("CreateSideSource", Create::new(vec![0i64]))
        .map("DelaySideInput", |_: i64| {
            std::thread::sleep(SIDE_INPUT_DELAY);
            "side-value".to_string()
        })
        .as_singleton();
    let out = p
        .apply(
            "CreateMainKV",
            Create::new(vec![("key".to_string(), "main-elem".to_string())]),
        )
        .apply(
            "SideInputInTimer",
            ParDo::new(SideInputInTimerDoFn { side }),
        );
    passert::that("AssertOut", &out).contains_in_any_order(["side-value"; 3].map(String::from));
}

/// Builds a pipeline that reads a singleton over discarding panes with three elements;
/// see [`MULTI_ELEMENT_SINGLETON_SIDE_INPUT_FAILS`].
///
/// Java: `ViewTest.testDiscardingNonSingletonSideInput`.
pub fn build_discarding_singleton_side_input_fails(p: &TestPipeline) {
    let one_two_three = p.apply("Create", Create::new(vec![1i64, 2, 3]));
    let view = one_two_three
        .apply(
            "Discarding",
            WindowInto::new(GlobalWindows).with_accumulation_mode(AccumulationMode::Discarding),
        )
        .as_singleton();
    let out = one_two_three.map_with_side_inputs("OutputSideInputs", view, |_, s| Ok(s));
    passert::that("Unreachable", &out).empty();
}

/// The words of [`build_bundle_finalization_with_side_input`].
const FINALIZED_WORDS: [&str; 3] = ["red", "green", "blue"];

/// Reads a singleton side input and registers, for each word, a callback that writes
/// `side-<word>-<side input>`.
#[derive(Clone)]
pub struct SideInputFinalizingDoFn {
    side: SingletonView<String>,
}

impl DoFn for SideInputFinalizingDoFn {
    type In = (String, String);
    type Out = WithFinalizer<String>;

    fn side_inputs(&self) -> Vec<&dyn AnySideInput> {
        vec![&self.side]
    }

    fn process_element(
        &mut self,
        (dir, word): (String, String),
        ctx: &mut ProcessContext<'_, Self::Out>,
    ) -> Result {
        let side = ctx.side_input(&self.side)?;
        let marker = format!("side-{word}-{side}");
        ctx.finalizer(move || write_marker(&dir, &marker))
            .register()?;
        ctx.emit(word.into())
    }
}

/// Builds a DoFn that reads a side input and registers bundle finalizers. See
/// [`check_bundle_finalization_with_side_input`] for the markers.
///
/// Java: `ParDoTest$BundleFinalizationTests.testBundleFinalizationWithSideInputs`.
pub fn build_bundle_finalization_with_side_input(p: &TestPipeline) {
    let dir = marker_dir("side-input");
    let side = p
        .apply("SideInput", Create::new(vec!["value".to_string()]))
        .as_singleton();
    let words = words_in(p, &dir, &FINALIZED_WORDS)
        .apply("Finalizing", ParDo::new(SideInputFinalizingDoFn { side }));
    passert::that("AssertWords", &words).contains_in_any_order(FINALIZED_WORDS.map(String::from));
}

/// Checks the markers of [`build_bundle_finalization_with_side_input`] after a successful
/// run. Dataflow can skip a finalization, so there it expects one marker or more.
pub fn check_bundle_finalization_with_side_input(
    runner: RunnerKind,
    result: std::result::Result<PipelineResult, TestPipelineError>,
) {
    result.expect("the pipeline should succeed");
    let dir = marker_dir("side-input");
    let markers: BTreeSet<String> = get_filesystem(&dir)
        .and_then(|fs| fs.match_files(&format!("{dir}/side-*")))
        .expect("list the markers")
        .iter()
        .filter_map(|path| path.rsplit('/').next().map(str::to_string))
        .collect();
    let expected: BTreeSet<String> = FINALIZED_WORDS
        .iter()
        .map(|word| format!("side-{word}-value"))
        .collect();
    match runner {
        RunnerKind::Dataflow => assert!(
            !markers.is_empty() && markers.is_subset(&expected),
            "side input finalization markers {markers:?}"
        ),
        RunnerKind::Prism => assert_eq!(markers, expected, "side input finalization markers"),
    }
}

/// Builds a TestStream pipeline whose main window has no side input data: the iterable view
/// reads as empty in that window. Prism only.
///
/// Java: `ViewTest.testWindowedSideInputNotPresent`. A singleton view has no default value,
/// so the view here is an iterable and the main input emits its sum.
pub fn build_windowed_side_input_not_present(p: &TestPipeline) {
    let input = p.apply(
        "TestStream",
        TestStream::new()
            .advance_watermark_to(Instant::EPOCH)
            .add_timestamped_elements([((1_000i64, 1_000i64), Instant::from_millis(1_000))])
            .advance_watermark_to(Instant::from_millis(20_000))
            .advance_watermark_to_infinity(),
    );
    // The sum lands in the 10 s window that holds the end of its 100 s window.
    let view = input
        .map("Values", |(_, v): (i64, i64)| v)
        .apply(
            "SideWindowInto",
            WindowInto::new(FixedWindows::of(Duration::from_secs(100))),
        )
        .combine_globally_without_defaults("ViewCombine", Sum::new())
        .apply("Rewindow", fixed_10s())
        .as_iter();
    let out = input
        .apply("MainWindowInto", fixed_10s())
        .group_by_key("GroupByKey")
        .map_with_side_inputs("OutputSideInputs", view, |_, side| {
            side.iter()?.sum::<Result<i64>>()
        });
    passert::that("AssertNoSideData", &out)
        .in_window(IntervalWindow {
            start: Instant::EPOCH,
            end: Instant::from_millis(10_000),
        })
        .contains_in_any_order([0]);
}

/// Side input events of [`build_triggered_side_input`]: key, value, timestamp in ms.
const TRIGGERED_SIDE: [(&str, i64, i64); 4] = [
    ("a", 10, 1_000),
    ("a", 20, 2_000),
    ("b", 5, 3_000),
    ("a", 40, 6_000),
];

/// Validates a triggered side input in fixed windows (`AfterWatermark` with early `AfterCount`,
/// `Discarding`) read as a multimap and an iterable of the same collection in one `ParDo`.
///
/// A runner can read the side window after any of its panes, so the checks hold for every
/// set of fired panes. [`build_test_stream_triggered_side_input`] checks exact pane values.
///
/// Java: `ViewTest.testTriggeredLatestSingleton`.
pub fn build_triggered_side_input(p: &TestPipeline) {
    let w5 = Duration::from_secs(5);
    let side = p
        .apply(
            "SideEvents",
            Create::new(
                TRIGGERED_SIDE
                    .iter()
                    .map(|&(k, v, ts)| (k.to_string(), (v, ts)))
                    .collect::<Vec<_>>(),
            ),
        )
        .par_do("TimestampSide", AssignTimestampDoFn)
        .apply(
            "WindowSide",
            WindowInto::new(FixedWindows::of(w5))
                .with_trigger(
                    Trigger::after_end_of_window().with_early_firings(Trigger::after_count(1)),
                )
                .with_accumulation_mode(AccumulationMode::Discarding),
        )
        .combine_per_key("SumPerPane", Sum::new());

    let main = p
        .apply(
            "MainEvents",
            Create::new(vec![
                ("a".to_string(), (0i64, 2_000i64)),
                ("b".to_string(), (0, 4_000)),
                ("c".to_string(), (0, 4_000)),
                ("a".to_string(), (0, 7_000)),
            ]),
        )
        .par_do("TimestampMain", AssignTimestampDoFn)
        .apply("WindowMain", WindowInto::new(FixedWindows::of(w5)))
        .map("MainKey", |(k, _): (String, i64)| k);

    let reads = main.map_with_side_inputs(
        "ReadTriggeredViews",
        (side.as_multimap(), side.as_iter()),
        |key, (by_key, all)| {
            let mut key_panes = by_key.get_all(&key)?.collect::<Result<Vec<i64>>>()?;
            key_panes.sort_unstable();
            let has_first = by_key.get(&key)?.is_some();
            let window_sum: i64 = all.iter()?.map(|kv| Ok(kv?.1)).sum::<Result<i64>>()?;
            Ok((key, key_panes, has_first, window_sum))
        },
    );

    passert::that_windowed("AssertTriggeredSide", &reads).satisfies(check_triggered_reads);
}

/// One read of [`build_triggered_side_input`]: key, pane sums of the key, whether
/// `SideMap::get` found a value, and the sum of the iterable view.
type TriggeredRead = (String, Vec<i64>, bool, i64);

/// Checks the reads of [`build_triggered_side_input`] for any set of fired side panes.
fn check_triggered_reads(reads: &[(TriggeredRead, IntervalWindow)]) -> Result {
    let mut seen: Vec<(&str, Instant)> = reads
        .iter()
        .map(|((key, ..), w)| (key.as_str(), w.start))
        .collect();
    seen.sort_unstable();
    let five_s = Instant::from_millis(5_000);
    let expected = [
        ("a", Instant::EPOCH),
        ("a", five_s),
        ("b", Instant::EPOCH),
        ("c", Instant::EPOCH),
    ];
    if seen != expected {
        return Err(format!("main elements per window: {seen:?}, expected {expected:?}").into());
    }
    reads
        .iter()
        .try_for_each(|((key, panes, has_first, window_sum), w)| {
            let in_window =
                |ts: i64| w.start <= Instant::from_millis(ts) && Instant::from_millis(ts) < w.end;
            let key_inputs: Vec<i64> = TRIGGERED_SIDE
                .iter()
                .filter(|&&(k, _, ts)| k == key.as_str() && in_window(ts))
                .map(|&(_, v, _)| v)
                .collect();
            let window_total: i64 = TRIGGERED_SIDE
                .iter()
                .filter(|&&(_, _, ts)| in_window(ts))
                .map(|&(_, v, _)| v)
                .sum();
            // A discarding pane holds a subset of the inputs; the on-time pane can be empty (0).
            let pane_sums = subset_sums(&key_inputs);
            let context = format!("key {key} in {w:?}: panes {panes:?}, window sum {window_sum}");
            if key_inputs.is_empty() && !panes.is_empty() {
                return Err(format!("{context}: a key with no side data has panes").into());
            }
            if panes.iter().any(|v| !pane_sums.contains(v)) {
                return Err(format!("{context}: a pane is not a sum of the key inputs").into());
            }
            if panes.iter().sum::<i64>() > key_inputs.iter().sum::<i64>() {
                return Err(format!("{context}: panes add up to more than the key inputs").into());
            }
            if *has_first == panes.is_empty() {
                return Err(format!("{context}: get disagrees with get_all").into());
            }
            if !(0..=window_total).contains(window_sum) {
                return Err(
                    format!("{context}: iterable sum is outside 0..={window_total}").into(),
                );
            }
            Ok(())
        })
}

/// All sums of subsets of `values`, the empty subset (0) included.
fn subset_sums(values: &[i64]) -> BTreeSet<i64> {
    values.iter().fold(BTreeSet::from([0]), |sums, v| {
        sums.iter().flat_map(|s| [*s, s + v]).collect()
    })
}

/// Validates a multi-triggered `CombinePerKey` side input driven by `TestStream` across fixed
/// windows: early and on-time panes both materialize in the window view. Prism only.
///
/// Python: `SideInputsTest.test_multi_triggered_gbk_side_input`.
pub fn build_test_stream_triggered_side_input(p: &TestPipeline) {
    let w5 = Duration::from_secs(5);
    let side_ev = |ts_ms: i64, v: i64| ((true, "k".to_string(), v), Instant::from_millis(ts_ms));
    let main_ev = |ts_ms: i64, label: &str| {
        (
            (false, label.to_string(), 0i64),
            Instant::from_millis(ts_ms),
        )
    };

    let events = p.apply(
        "TestStream",
        TestStream::new()
            .add_timestamped_elements([side_ev(1_000, 100)])
            .advance_watermark_to(Instant::from_millis(3_000))
            .add_timestamped_elements([main_ev(3_000, "m1"), side_ev(4_000, 50)])
            .advance_watermark_to(Instant::from_millis(6_000))
            .add_timestamped_elements([side_ev(7_000, 400)])
            .advance_watermark_to(Instant::from_millis(8_000))
            .add_timestamped_elements([main_ev(8_000, "m2")])
            .advance_watermark_to_infinity(),
    );

    let side_view = events
        .filter("KeepSide", |(is_side, _, _): &(bool, String, i64)| *is_side)
        .map("SideKV", |(_, k, v): (bool, String, i64)| (k, v))
        .apply(
            "SideWindow",
            WindowInto::new(FixedWindows::of(w5))
                .with_trigger(
                    Trigger::after_end_of_window().with_early_firings(Trigger::after_count(1)),
                )
                .with_accumulation_mode(AccumulationMode::Discarding),
        )
        .combine_per_key("SumSidePanes", Sum::new())
        .map("PaneValue", |(_, v): (String, i64)| v)
        .as_iter();

    let out = events
        .filter("KeepMain", |(is_side, _, _): &(bool, String, i64)| {
            !*is_side
        })
        .map("MainLabel", |(_, label, _): (bool, String, i64)| label)
        .apply(
            "MainWindow",
            WindowInto::new(FixedWindows::of(w5))
                .with_accumulation_mode(AccumulationMode::Discarding),
        )
        .map_with_side_inputs("ReadPanes", side_view, |label, panes| {
            let mut values = panes.to_vec()?;
            values.sort_unstable();
            Ok((label, values))
        });

    let w0 = IntervalWindow::new(Instant::EPOCH, Instant::from_millis(5_000));
    let w1 = IntervalWindow::new(Instant::from_millis(5_000), Instant::from_millis(10_000));
    passert::that_windowed("AssertTriggeredPanes", &out).contains_in_any_order([
        (("m1".to_string(), vec![50i64, 100]), w0),
        (("m2".to_string(), vec![400]), w1),
    ]);
}
