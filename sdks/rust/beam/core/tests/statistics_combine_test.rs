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

//! Tests for the `Mean`, `Top`, `Sample` and `AnySample` combine functions, and for `Decimal`
//! in the numeric combine functions.

mod combine_support;

use combine_support::combine_in_parts;
use std::collections::HashSet;

use beam::prelude::*;
use beam::transforms::{AnySample, CombineFn, Max, Mean, Min, Sample, Sum, Top};
use rust_decimal::Decimal;

#[test]
fn mean_of_integers_is_an_f64() -> Result {
    assert_eq!(combine_in_parts(&Mean::new(), vec![1i64, 2, 3, 4], 2)?, 2.5);
    assert_eq!(combine_in_parts(&Mean::new(), vec![-3i32, 4], 2)?, 0.5);
    assert_eq!(combine_in_parts(&Mean::new(), vec![1.5f64, 2.5], 1)?, 2.0);
    Ok(())
}

/// The integer mean adds `f64` values, so values near the integer limit do not overflow.
#[test]
fn mean_of_large_integers_does_not_overflow() -> Result {
    let mean = combine_in_parts(&Mean::new(), vec![i64::MAX, i64::MAX], 2)?;
    assert_eq!(mean, i64::MAX as f64);
    Ok(())
}

#[test]
fn mean_of_decimals_is_exact() -> Result {
    let values = vec![
        Decimal::new(10, 1),
        Decimal::new(20, 1),
        Decimal::new(40, 1),
    ];
    // (1.0 + 2.0 + 4.0) / 3, exact to the precision of `Decimal`.
    let mean = combine_in_parts(&Mean::new(), values, 2)?;
    assert_eq!(mean, Decimal::new(7, 0) / Decimal::new(3, 0));
    Ok(())
}

#[test]
fn mean_of_no_values_is_nan_or_zero() -> Result {
    assert!(combine_in_parts(&Mean::<i64>::new(), Vec::new(), 1)?.is_nan());
    assert_eq!(
        combine_in_parts(&Mean::<Decimal>::new(), Vec::new(), 1)?,
        Decimal::ZERO
    );
    Ok(())
}

#[test]
fn top_keeps_the_greatest_in_descending_order() -> Result {
    let values: Vec<i64> = (0..100).map(|i| (i * 37) % 101).collect();
    assert_eq!(
        combine_in_parts(&Top::largest(3), values.clone(), 4)?,
        vec![100, 99, 98]
    );
    assert_eq!(
        combine_in_parts(&Top::smallest(3), values, 4)?,
        vec![0, 1, 2]
    );
    Ok(())
}

#[test]
fn top_of_uses_the_ordering() -> Result {
    let words = ["a", "abcd", "ab", "abcdef", "abc"]
        .map(String::from)
        .to_vec();
    let longest = Top::of(2, |a: &String, b: &String| a.len().cmp(&b.len()));
    assert_eq!(
        combine_in_parts(&longest, words, 2)?,
        vec!["abcdef", "abcd"]
    );
    Ok(())
}

#[test]
fn top_with_fewer_elements_or_a_zero_count() -> Result {
    assert_eq!(
        combine_in_parts(&Top::largest(5), vec![2i64, 1], 2)?,
        vec![2, 1]
    );
    assert_eq!(
        combine_in_parts(&Top::largest(0), vec![2i64, 1], 2)?,
        Vec::<i64>::new()
    );
    assert_eq!(
        combine_in_parts(&Top::<i64, _>::largest(3), Vec::new(), 1)?,
        Vec::<i64>::new()
    );
    Ok(())
}

/// An accumulator never holds more than twice the count, whatever the input size.
#[test]
fn top_accumulator_stays_bounded() -> Result {
    let top = Top::largest(4);
    let accumulator =
        (0..10_000i64).try_fold(top.create_accumulator()?, |acc, x| top.add_input(acc, x))?;
    assert!(accumulator.len() <= 8, "{}", accumulator.len());
    assert_eq!(
        top.extract_output(accumulator)?,
        vec![9999, 9998, 9997, 9996]
    );
    Ok(())
}

#[test]
fn sample_keeps_count_distinct_input_elements() -> Result {
    let sample = combine_in_parts(&Sample::new(10), (0..1000i64).collect(), 4)?;
    assert_eq!(sample.len(), 10);
    let distinct: HashSet<i64> = sample.iter().copied().collect();
    assert_eq!(distinct.len(), 10);
    assert!(sample.iter().all(|x| (0..1000).contains(x)));

    let all = combine_in_parts(&Sample::new(10), (0..3i64).collect(), 2)?;
    assert_eq!(
        all.into_iter().collect::<HashSet<_>>(),
        HashSet::from([0, 1, 2])
    );
    Ok(())
}

/// Over many runs each element has the same chance, so the mean of the sampled values is
/// near the mean of the input. A sample biased to the first or the last partial fails.
#[test]
fn sample_is_uniform() -> Result {
    let runs = 400;
    let total: i64 = (0..runs)
        .map(|_| {
            combine_in_parts(&Sample::new(5), (0..100i64).collect(), 4)
                .map(|s| s.iter().sum::<i64>())
        })
        .sum::<Result<i64>>()?;
    let mean = total as f64 / f64::from(runs * 5);
    assert!((mean - 49.5).abs() < 4.0, "mean of sampled values: {mean}");
    Ok(())
}

#[test]
fn any_sample_keeps_at_most_count_elements() -> Result {
    let sample = combine_in_parts(&AnySample::new(3), (0..100i64).collect(), 4)?;
    assert_eq!(sample.len(), 3);
    assert!(sample.iter().all(|x| (0..100).contains(x)));
    assert_eq!(
        combine_in_parts(&AnySample::new(3), vec![7i64], 2)?,
        vec![7]
    );
    Ok(())
}

#[test]
fn top_and_mean_compose() -> Result {
    let stats = Compose::new((
        Mean::new().on(|x: &i64| *x),
        Top::largest(2).on(|x: &i64| *x),
    ));
    assert_eq!(
        combine_in_parts(&stats, vec![1, 5, 3, 9], 2)?,
        (4.5, vec![9, 5])
    );
    Ok(())
}

#[test]
fn numeric_combine_fns_accept_decimal() -> Result {
    let values = [
        Decimal::new(1050, 2),
        Decimal::new(-325, 2),
        Decimal::new(2, 0),
    ];
    let fold = |combine_fn: &dyn Fn(Decimal, Decimal) -> Result<Decimal>, init| {
        values.iter().try_fold(init, |acc, &v| combine_fn(acc, v))
    };
    let sum = Sum::new();
    assert_eq!(
        fold(&|a, v| sum.add_input(a, v), sum.create_accumulator()?)?,
        Decimal::new(925, 2)
    );
    let max = Max::new();
    assert_eq!(
        fold(&|a, v| max.add_input(a, v), max.create_accumulator()?)?,
        Decimal::new(1050, 2)
    );
    let min = Min::<Decimal>::new();
    assert_eq!(min.create_accumulator()?, Decimal::MAX);

    let err = sum
        .add_input(Decimal::MAX, Decimal::ONE)
        .expect_err("decimal overflow");
    assert!(err.to_string().contains("overflowed Decimal"), "{err}");
    Ok(())
}
