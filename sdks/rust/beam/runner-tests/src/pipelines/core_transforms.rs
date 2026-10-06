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

//! Element-wise, grouping and combining transforms, plus DoFn lifecycle and DAG shape.
//!
//! Every check is an in-graph [`passert`] assertion, so the validators run on any runner,
//! not only on a loopback one.

use beam::coders::DefaultCoder;
use beam::prelude::*;
use beam::testing::{TestPipeline, passert};
use beam::transforms::{Max, Min, Sum};

use crate::Expectation;
use crate::dofns::*;

/// Validates element-wise `map` and `filter` operations.
pub fn build_map_and_filter(p: &TestPipeline) {
    let out = p
        .apply(Create::new("Create", vec![1i64, 2, 3, 4, 5]))
        .map("TimesTen", |x: i64| x * 10)
        .filter("GreaterThanTwenty", |x: &i64| *x > 20);
    passert::that("AssertOut", &out).contains_in_any_order([30, 40, 50]);
}

/// Validates 1-to-many `flat_map` tokenization, including an element producing nothing.
pub fn build_flat_map(p: &TestPipeline) {
    let words = p
        .apply(Create::new(
            "Create",
            vec![
                "hello world".to_string(),
                String::new(),
                "beam rust beam".to_string(),
            ],
        ))
        .flat_map("Tokenize", |line: String| {
            line.split_whitespace()
                .map(String::from)
                .collect::<Vec<_>>()
        });
    passert::that("AssertWords", &words)
        .contains_in_any_order(["hello", "world", "beam", "rust", "beam"].map(String::from));
}

/// Validates shuffle and grouping with `group_by_key`: exact groups, not only sums.
pub fn build_group_by_key(p: &TestPipeline) {
    let grouped = p
        .apply(Create::new(
            "Create",
            vec![
                ("k1".to_string(), 1i64),
                ("k2".to_string(), 10i64),
                ("k1".to_string(), 2i64),
                ("k2".to_string(), 20i64),
                ("k3".to_string(), 7i64),
            ],
        ))
        .group_by_key("Group");
    passert::that("PAssert", &sorted_groups(&grouped, "SortGroups")).contains_in_any_order([
        ("k1".to_string(), vec![1, 2]),
        ("k2".to_string(), vec![10, 20]),
        ("k3".to_string(), vec![7]),
    ]);
}

/// Validates associative aggregation with `combine_per_key` (including combine lifting).
pub fn build_combine_per_key(p: &TestPipeline) {
    let sums = p
        .apply(Create::new(
            "Create",
            vec![
                ("a".to_string(), 1i64),
                ("b".to_string(), 10i64),
                ("a".to_string(), 2i64),
                ("b".to_string(), 20i64),
                ("a".to_string(), 3i64),
            ],
        ))
        .combine_per_key("SumByKey", Sum);
    passert::that("AssertSums", &sums)
        .contains_in_any_order([("a".to_string(), 6), ("b".to_string(), 30)]);
}

/// Validates `Max` and `Min`, per key and globally.
///
/// All values are negative, to catch an accumulator that starts at zero instead of at the
/// identity (`i64::MIN` / `i64::MAX`). The empty global inputs check the identity itself.
pub fn build_combine_max_min(p: &TestPipeline) {
    let values = p.apply(Create::new(
        "Values",
        vec![
            ("a".to_string(), -5i64),
            ("a".to_string(), -1),
            ("a".to_string(), -9),
            ("b".to_string(), -30),
        ],
    ));
    passert::that("MaxPerKey", &values.combine_per_key("MaxPerKey", Max))
        .contains_in_any_order([("a".to_string(), -1), ("b".to_string(), -30)]);
    passert::that("MinPerKey", &values.combine_per_key("MinPerKey", Min))
        .contains_in_any_order([("a".to_string(), -9), ("b".to_string(), -30)]);

    let flat = values.map("DropKeys", |(_, v): (String, i64)| v);
    passert::that_singleton("MaxGlobally", &flat.combine_globally("MaxGlobally", Max))
        .is_equal_to(-1);
    passert::that_singleton("MinGlobally", &flat.combine_globally("MinGlobally", Min))
        .is_equal_to(-30);

    let nothing = flat.filter("DropAll", |_: &i64| false);
    passert::that_singleton("MaxOfEmpty", &nothing.combine_globally("MaxOfEmpty", Max))
        .is_equal_to(i64::MIN);
    passert::that_singleton("MinOfEmpty", &nothing.combine_globally("MinOfEmpty", Min))
        .is_equal_to(i64::MAX);
}

/// Distinct keys that the lifted combine buffers per bundle before it flushes. Mirrors
/// `DEFAULT_MAX_ACCUMULATORS` in core's `combine.rs`.
const LIFTED_COMBINE_MAX_ACCUMULATORS: i64 = 100_000;

/// Validates the lifted combine's memory bound: a full table flushes and starts over, and
/// the partial accumulators of a key flushed twice are merged after the shuffle.
///
/// Each key appears twice, more than a table's capacity of keys apart, so it crosses the
/// shuffle as two accumulators. Every key must come out with a count of exactly 2.
pub fn build_combine_flushes_accumulators_at_capacity(p: &TestPipeline) {
    const KEYS: i64 = LIFTED_COMBINE_MAX_ACCUMULATORS + 1;

    let counts = p
        .apply(
            // One restriction, so a runner that does not split sees one large bundle.
            GenerateSequence::new("TwoOfEachKey", 0)
                .with_end(2 * KEYS)
                .with_split_size(2 * KEYS),
        )
        .map("KeyModulo", |i: i64| (i % KEYS, 1i64))
        .combine_per_key("CountPerKey", Sum);

    // Summarise instead of asserting on 100k elements: a histogram of the counts.
    let histogram = counts
        .map("CountOnly", |(_, count): (i64, i64)| count)
        .count_per_element("Histogram");
    passert::that("AssertHistogram", &histogram).contains_in_any_order([(2i64, KEYS)]);
}

/// Validates `count_per_element`.
pub fn build_count_per_element(p: &TestPipeline) {
    let counts = p
        .apply(Create::new(
            "Create",
            ["x", "y", "x", "z", "x", "y"].map(String::from).to_vec(),
        ))
        .count_per_element("Count");
    passert::that("AssertCounts", &counts).contains_in_any_order([
        ("x".to_string(), 3),
        ("y".to_string(), 2),
        ("z".to_string(), 1),
    ]);
}

/// Validates global associative aggregation with `combine_globally`.
pub fn build_combine_globally(p: &TestPipeline) {
    let total = p
        .apply(Create::new("Create", vec![10i64, 25, 3, 42, 15]))
        .combine_globally("GlobalSum", Sum);
    passert::that_singleton("AssertTotal", &total).is_equal_to(95);
}

/// Validates `count_globally`, including the zero of an empty input.
pub fn build_count_globally(p: &TestPipeline) {
    let words = p.apply(Create::new(
        "Create",
        ["alpha", "beta", "gamma", "delta"]
            .map(String::from)
            .to_vec(),
    ));
    passert::that_singleton("CountAll", &words.count_globally("CountAll")).is_equal_to(4);
    let none = words.filter("DropAll", |_: &String| false);
    passert::that_singleton("CountNone", &none.count_globally("CountNone")).is_equal_to(0);
}

/// Validates `fold_values` (sequential, non-lifted), `fold_per_key` and
/// `fold_globally` (both built on `FoldCombineFn`).
///
/// The per-key folds build a string (accumulator type differs from input type) and sort it
/// in the merge, so the result does not depend on bundle order.
pub fn build_folds(p: &TestPipeline) {
    let scores = p.apply(Create::new(
        "Scores",
        vec![
            ("a".to_string(), 3i64),
            ("b".to_string(), 10),
            ("a".to_string(), 4),
            ("a".to_string(), 5),
        ],
    ));

    // fold_values sees every value of a key in one call; the product proves every
    // value was folded exactly once, starting from the zero.
    let products = scores.fold_values("ProductValues", 1i64, |acc, v| acc * v);
    passert::that("FoldValues", &products)
        .contains_in_any_order([("a".to_string(), 60), ("b".to_string(), 10)]);

    let listed = scores.fold_per_key(
        "ListPerKey",
        Vec::<i64>::new(),
        |mut acc, v| {
            acc.push(v);
            acc
        },
        |mut left, right| {
            left.extend(right);
            left
        },
    );
    let listed = listed.map("SortLists", |(k, mut v): (String, Vec<i64>)| {
        v.sort_unstable();
        (k, v)
    });
    passert::that("FoldPerKey", &listed).contains_in_any_order([
        ("a".to_string(), vec![3, 4, 5]),
        ("b".to_string(), vec![10]),
    ]);

    // (count, sum): a merge that dropped a partial accumulator would lose count.
    let stats = scores
        .map("Values", |(_, v): (String, i64)| v)
        .fold_globally(
            "CountAndSum",
            (0i64, 0i64),
            |(n, s), v| (n + 1, s + v),
            |(n1, s1), (n2, s2)| (n1 + n2, s1 + s2),
        );
    passert::that_singleton("FoldGlobally", &stats).is_equal_to((4, 22));
}

/// Validates that `DoFn` lifecycle methods are called in order, once per element.
///
/// [`LifecycleDoFn`] fails the bundle on an out-of-order call and reports the element count
/// of each bundle. Each element is processed once and the per-bundle counts add up.
pub fn build_dofn_lifecycle(p: &TestPipeline) {
    let out = p
        .apply(Create::new(
            "Create",
            vec!["elem1".to_string(), "elem2".to_string()],
        ))
        .apply(ParDo::new("Lifecycle", LifecycleDoFn::default()));
    passert::that("AssertOut", &out).satisfies(|lines: &[String]| {
        let mut processed: Vec<&str> = lines
            .iter()
            .filter_map(|l| l.strip_prefix("process:"))
            .collect();
        processed.sort_unstable();
        if processed != ["elem1", "elem2"] {
            return Err(format!("each element must be processed once: {lines:?}").into());
        }
        let finished: Vec<usize> = lines
            .iter()
            .filter_map(|l| l.strip_prefix("finish:"))
            .map(|n| n.parse().map_err(|e| format!("bad finish line {n}: {e}")))
            .collect::<Result<_, _>>()?;
        if finished.is_empty() || finished.iter().sum::<usize>() != 2 {
            return Err(format!(
                "finish_bundle must report every processed element once: {lines:?}"
            )
            .into());
        }
        if processed.len() + finished.len() != lines.len() {
            return Err(format!("unexpected output: {lines:?}").into());
        }
        Ok(())
    });
}

/// Validates diamond DAGs where a single PCollection is consumed by multiple downstream
/// branches, which then join again.
pub fn build_diamond_dag(p: &TestPipeline) {
    let source = p.apply(Create::new("Create", vec![1i64, 2, 3]));
    let doubled = source.map("Double", |x: i64| x * 2);
    let tripled = source.map("Triple", |x: i64| x * 3);
    passert::that("Doubled", &doubled).contains_in_any_order([2, 4, 6]);
    passert::that("Tripled", &tripled).contains_in_any_order([3, 6, 9]);
    passert::that("Rejoined", &doubled.flatten("Rejoin", &tripled))
        .contains_in_any_order([2, 4, 6, 3, 6, 9]);
}

/// Validates multi-output ParDo emitting to multiple tagged PCollections.
pub fn build_multi_output_pardo(p: &TestPipeline) {
    let numbers = p.apply(Create::new("Create", vec![1i64, 2, 3, 4, 5]));
    let outputs = numbers.par_do_multi_tags("SplitTags", &["evens", "odds"], SplitTagsDoFn);
    passert::that("Evens", &outputs[0])
        .contains_in_any_order(["even:2", "even:4"].map(String::from));
    passert::that("Odds", &outputs[1])
        .contains_in_any_order(["odd:1", "odd:3", "odd:5"].map(String::from));
}

/// Validates `Flatten` of several inputs, including an empty one and duplicates across
/// inputs, which must all be kept.
pub fn build_flatten_many(p: &TestPipeline) {
    let a = p.apply(Create::new("A", vec![1i64, 2]));
    let b = p.apply(Create::new("B", vec![2i64, 3]));
    let c = p.apply(Create::new("C", vec![4i64]));
    let empty = p
        .apply(Create::new("Empty", vec![0i64]))
        .filter("DropAll", |_: &i64| false);

    let merged = a.flatten("MergeAll", &[&b, &c, &empty]);
    passert::that("AssertMerged", &merged).contains_in_any_order([1, 2, 2, 3, 4]);
}

/// Validates that `Reshuffle` redistributes elements without dropping, duplicating or
/// altering any, for both keyed and unkeyed input.
pub fn build_reshuffle(p: &TestPipeline) {
    let numbers: Vec<i64> = (0..200).chain([7, 7]).collect();
    let reshuffled = p
        .apply(Create::new("Numbers", numbers.clone()))
        .reshuffle("Reshuffle");
    passert::that("Unkeyed", &reshuffled).contains_in_any_order(numbers);

    let keyed = p
        .apply(Create::new(
            "Keyed",
            vec![
                ("a".to_string(), (1i64, 1_000i64)),
                ("a".to_string(), (1, 2_000)),
                ("b".to_string(), (2, 3_000)),
            ],
        ))
        .par_do("TimestampKeyed", ValidatesAssignTimestampDoFn)
        .reshuffle("ReshuffleKeyed")
        .par_do_fn("ReadTimestamp", |(k, v): (String, i64), ctx| {
            ctx.emit((k, (v, ctx.timestamp())))
        });
    passert::that("Keyed", &keyed).contains_in_any_order([
        ("a".to_string(), (1, 1_000)),
        ("a".to_string(), (1, 2_000)),
        ("b".to_string(), (2, 3_000)),
    ]);
}

/// Validates `Partition` routing: each element lands in exactly the partition its
/// function names, and a partition nothing is routed to is empty.
pub fn build_partition(p: &TestPipeline) {
    let parts = p
        .apply(Create::new("Create", (0..10i64).collect::<Vec<_>>()))
        .partition("ByRemainder", 4, |x: &i64| match x % 3 {
            0 => 0,
            1 => 1,
            _ => 3,
        });
    assert_eq!(parts.len(), 4);
    passert::that("Partition0", &parts[0]).contains_in_any_order([0, 3, 6, 9]);
    passert::that("Partition1", &parts[1]).contains_in_any_order([1, 4, 7]);
    passert::that("Partition2", &parts[2]).empty();
    passert::that("Partition3", &parts[3]).contains_in_any_order([2, 5, 8]);
}

/// Validates `Partition` routing with twelve partitions, element `i` going to partition `i`.
/// The tags `"10"` and `"11"` sort before `"2"`, which catches ordering by tag string.
pub fn build_partition_many(p: &TestPipeline) {
    const PARTITIONS: usize = 12;
    let parts = p
        .apply(Create::new(
            "Create",
            (0..PARTITIONS as i64).collect::<Vec<_>>(),
        ))
        .partition("Identity", PARTITIONS, |x: &i64| *x as usize);
    assert_eq!(parts.len(), PARTITIONS);
    for i in 0..PARTITIONS {
        passert::that(format!("Partition{i}"), &parts[i]).contains_in_any_order([i as i64]);
    }
}

/// A partition function returning an index outside `0..num_partitions` fails the pipeline.
/// The error names the transform and the bad index.
pub const PARTITION_OUT_OF_RANGE_FAILS: Expectation = Expectation::FailsWith(&["OutOfRange", "5"]);

/// Builds a pipeline whose partition function returns an out-of-range index; see
/// [`PARTITION_OUT_OF_RANGE_FAILS`].
pub fn build_partition_out_of_range_fails(p: &TestPipeline) {
    let parts =
        p.apply(Create::new("Create", vec![1i64, 5]))
            .partition("OutOfRange", 2, |x: &i64| *x as usize);
    // Only reached if routing succeeds: element 5 must not appear anywhere.
    passert::that("Partition0", &parts[0]).empty();
    passert::that("Partition1", &parts[1]).contains_in_any_order([1]);
}

/// Validates a bounded `GenerateSequence` split into many small restrictions: every value
/// is emitted exactly once.
pub fn build_generate_sequence(p: &TestPipeline) {
    let doubled = p
        .apply(
            GenerateSequence::new("GenerateSequence", 1)
                .with_end(21)
                .with_split_size(3),
        )
        .map("Double", |x: i64| x * 2);
    passert::that("AssertDoubled", &doubled).contains_in_any_order((1..=20).map(|x| x * 2));
}

/// Validates `PeriodicImpulse` with a limit: it emits `0..limit` once each and then stops,
/// so the global window closes and the assertions run.
pub fn build_periodic_impulse(p: &TestPipeline) {
    let ticks = p.apply(
        PeriodicImpulse::new("PeriodicImpulse", std::time::Duration::from_millis(10)).with_limit(5),
    );
    passert::that("AssertTicks", &ticks).contains_in_any_order([0, 1, 2, 3, 4]);
}

/// Flattening a single PCollection produces an output collection with identical elements.
pub fn build_flatten_singleton_list(p: &TestPipeline) {
    let input = p.apply(Create::new("Create", vec![1i64, 2, 3]));
    let output = input.flatten("FlattenSingle", &[]);
    passert::that("AssertSingleFlatten", &output).contains_in_any_order([1, 2, 3]);
}

/// Flattening multiple collections and following immediately with a ParDo elementwise transform.
pub fn build_flatten_then_pardo(p: &TestPipeline) {
    let a = p.apply(Create::new("A", vec![1i64, 2]));
    let b = p.apply(Create::new("B", vec![3i64, 4]));
    let merged = a.flatten("Merge", &[&b]).map("Double", |x: i64| x * 2);

    passert::that("AssertMergedDoubled", &merged).contains_in_any_order([2, 4, 6, 8]);
}

/// Flattening the exact same PCollection with itself duplicates every element.
pub fn build_flatten_multiple_copies(p: &TestPipeline) {
    let a = p.apply(Create::new("A", vec![10i64, 20]));
    let duplicated = a.flatten("DuplicateSelf", &[&a]);

    passert::that("AssertDuplicated", &duplicated).contains_in_any_order([10, 10, 20, 20]);
}

/// Creating an empty collection produces zero elements and downstream asserts pass.
pub fn build_create_empty(p: &TestPipeline) {
    let empty: PCollection<String> = p.apply(Create::new("Empty", Vec::<String>::new()));
    passert::that("AssertEmpty", &empty).empty();
}

/// Swapping key and value on a KV pair collection.
pub fn build_kv_swap(p: &TestPipeline) {
    let swapped = p
        .apply(Create::new(
            "Create",
            vec![("a".to_string(), 1i64), ("b".to_string(), 2i64)],
        ))
        .map("Swap", |(k, v): (String, i64)| (v, k));

    passert::that("AssertSwapped", &swapped)
        .contains_in_any_order([(1i64, "a".to_string()), (2i64, "b".to_string())]);
}

/// Sorts the values of every group, so grouped output can be compared exactly.
pub(super) fn sorted_groups<K, V>(
    grouped: &PCollection<(K, BeamIterable<V>)>,
    name: &str,
) -> PCollection<(K, Vec<V>)>
where
    K: DefaultCoder,
    V: DefaultCoder + Ord,
    Vec<V>: DefaultCoder,
    (K, Vec<V>): DefaultCoder,
{
    grouped.par_do_fn(name, |(k, values): (K, BeamIterable<V>), ctx| {
        let mut values = values.into_vec().map_err(|e| e.to_string())?;
        values.sort();
        ctx.emit((k, values))
    })
}
