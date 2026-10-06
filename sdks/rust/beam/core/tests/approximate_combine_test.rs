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

//! Tests for `ApproximateQuantiles` and `ApproximateUnique`. The quantile cases and the
//! buffer table are the reference values of the Java SDK, so both SDKs give the same results.

mod combine_support;

use beam::coders::DefaultCoder;
use beam::prelude::*;
use beam::transforms::{ApproximateQuantiles, ApproximateUnique, Compare};
use combine_support::combine_in_parts;

/// Runs `combine_fn` with 1, 2 and 7 partial accumulators, so the result does not depend on
/// how the runner splits the input.
fn quantiles<T: DefaultCoder + Clone + Ord>(
    combine_fn: &ApproximateQuantiles<T, impl Compare<T>>,
    inputs: &[T],
) -> Result<Vec<Vec<T>>> {
    [1, 2, 7]
        .into_iter()
        .map(|parts| combine_in_parts(combine_fn, inputs.to_vec(), parts))
        .collect()
}

fn assert_quantiles(num_quantiles: usize, inputs: &[i64], expected: &[i64]) -> Result {
    for result in quantiles(&ApproximateQuantiles::new(num_quantiles)?, inputs)? {
        assert_eq!(result, expected);
    }
    Ok(())
}

#[test]
fn quantiles_of_a_range_are_exact() -> Result {
    assert_quantiles(5, &(0..101).collect::<Vec<_>>(), &[0, 25, 50, 75, 100])
}

#[test]
fn quantiles_of_one_element_repeat_it() -> Result {
    assert_quantiles(5, &[389], &[389, 389, 389, 389, 389])
}

#[test]
fn quantiles_of_no_elements_are_empty() -> Result {
    assert_quantiles(5, &[], &[])
}

#[test]
fn quantiles_with_duplicates() -> Result {
    let tenfold: Vec<i64> = (0..10).flat_map(|_| 0..101).collect();
    assert_quantiles(5, &tenfold, &[0, 25, 50, 75, 100])?;

    let skewed: Vec<i64> = std::iter::once(1)
        .chain(std::iter::repeat_n(2, 299))
        .chain(std::iter::repeat_n(3, 700))
        .collect();
    assert_quantiles(5, &skewed, &[1, 2, 3, 3, 3])
}

#[test]
fn quantiles_of_skewed_distributions() -> Result {
    let log: Vec<i64> = (1..1000).map(|i| (i as f64).ln() as i64).collect();
    assert_quantiles(5, &log, &[0, 5, 6, 6, 6])?;

    let zipf: Vec<i64> = (1..1000).map(|i| 1000 / i).collect();
    assert_quantiles(5, &zipf, &[1, 1, 2, 4, 1000])
}

#[test]
fn quantiles_follow_the_ordering() -> Result {
    let words = ["aa", "aaa", "aaaa", "b", "ccccc", "dddd", "zz"].map(String::from);
    for result in quantiles(&ApproximateQuantiles::new(3)?, &words)? {
        assert_eq!(result, ["aa", "b", "zz"]);
    }
    let by_length = |a: &String, b: &String| a.len().cmp(&b.len()).then_with(|| a.cmp(b));
    for result in quantiles(&ApproximateQuantiles::of(3, by_length)?, &words)? {
        assert_eq!(result, ["b", "aaa", "ccccc"]);
    }
    let descending = |a: &i64, b: &i64| b.cmp(a);
    let range: Vec<i64> = (0..101).collect();
    for result in quantiles(&ApproximateQuantiles::of(5, descending)?, &range)? {
        assert_eq!(result, [100, 75, 50, 25, 0]);
    }
    Ok(())
}

/// Each inner quantile is within `tolerance` of the exact one, and the ends are exact.
fn assert_near(size: i64, num_quantiles: usize, epsilon: Option<f64>, tolerance: i64) -> Result {
    let combine_fn = match epsilon {
        Some(epsilon) => ApproximateQuantiles::new(num_quantiles)?.with_epsilon(epsilon)?,
        None => ApproximateQuantiles::new(num_quantiles)?,
    };
    let inputs: Vec<i64> = (0..size).collect();
    for result in quantiles(&combine_fn, &inputs)? {
        assert_eq!(result.len(), num_quantiles);
        assert_eq!(result.first(), Some(&0));
        assert_eq!(result.last(), Some(&(size - 1)));
        for (k, &actual) in result.iter().enumerate() {
            let exact = (size - 1) * k as i64 / (num_quantiles as i64 - 1);
            assert!(
                (actual - exact).abs() <= tolerance,
                "quantile {k}: {actual} vs {exact}"
            );
        }
    }
    Ok(())
}

#[test]
fn quantiles_stay_within_the_error_bound() -> Result {
    assert_near(5000, 37, None, 20)?;
    assert_near(10001, 50, None, 20)?;
    assert_near(10001, 10, Some(0.01), 5)
}

/// The buffer count and size for each epsilon and input size: the reference table of the
/// Munro-Paterson paper.
#[test]
fn buffer_layout_matches_the_reference_table() -> Result {
    let epsilons = [0.1, 0.05, 0.01, 0.005, 0.001];
    let num_buffers = [
        [11, 14, 17, 21, 24],
        [11, 14, 17, 20, 23],
        [9, 11, 14, 17, 21],
        [8, 11, 14, 17, 20],
        [6, 9, 11, 14, 17],
    ];
    let buffer_sizes = [
        [98, 123, 153, 96, 120],
        [98, 123, 153, 191, 239],
        [391, 977, 1221, 1526, 954],
        [782, 977, 1221, 1526, 1908],
        [3125, 3907, 9766, 12208, 15259],
    ];
    for (i, epsilon) in epsilons.into_iter().enumerate() {
        for (j, exponent) in (5..=9u32).enumerate() {
            let combine_fn = ApproximateQuantiles::<i64, _>::new(10)?
                .with_epsilon(epsilon)?
                .with_max_input_size(10u64.pow(exponent))?;
            assert_eq!(
                combine_fn.buffer_layout(),
                (num_buffers[i][j], buffer_sizes[i][j]),
                "epsilon {epsilon}, 10^{exponent} elements"
            );
        }
    }
    Ok(())
}

#[test]
fn quantiles_reject_bad_settings() {
    assert!(ApproximateQuantiles::<i64, _>::new(1).is_err());
    let quantiles = || ApproximateQuantiles::<i64, _>::new(5).expect("5 quantiles are valid");
    assert!(quantiles().with_epsilon(0.0).is_err());
    assert!(quantiles().with_epsilon(f64::NAN).is_err());
    assert!(quantiles().with_max_input_size(0).is_err());
}

#[test]
fn unique_is_exact_below_the_sample_size() -> Result {
    let unique = ApproximateUnique::with_sample_size(100)?;
    let inputs: Vec<i64> = (0..10).flat_map(|_| 0..50).collect();
    assert_eq!(combine_in_parts(&unique, inputs, 3)?, 50);
    assert_eq!(combine_in_parts(&unique, Vec::<i64>::new(), 1)?, 0);
    Ok(())
}

/// The estimate is within three times the expected error, `2 / sqrt(sample_size)`, for
/// several input sizes, and duplicates do not change it.
#[test]
fn unique_estimate_is_near_the_distinct_count() -> Result {
    for (sample_size, distinct) in [(16usize, 1_000i64), (100, 10_000), (1_000, 100_000)] {
        let unique = ApproximateUnique::with_sample_size(sample_size)?;
        let inputs: Vec<String> = (0..2 * distinct)
            .map(|i| format!("key-{}", i % distinct))
            .collect();
        let estimate = combine_in_parts(&unique, inputs, 4)?;
        let error = (estimate - distinct).abs() as f64 / distinct as f64;
        let bound = 3.0 * 2.0 / (sample_size as f64).sqrt();
        assert!(
            error < bound,
            "{distinct} distinct: estimate {estimate}, error {error}"
        );
    }
    Ok(())
}

/// The hash depends only on the element, so splits and merges give the same estimate.
#[test]
fn unique_estimate_does_not_depend_on_the_split() -> Result {
    let unique = ApproximateUnique::with_sample_size(64)?;
    let inputs: Vec<i64> = (0..5000).collect();
    let estimates = [1, 2, 9]
        .into_iter()
        .map(|parts| combine_in_parts(&unique, inputs.clone(), parts))
        .collect::<Result<Vec<_>>>()?;
    assert!(
        estimates.windows(2).all(|pair| pair[0] == pair[1]),
        "{estimates:?}"
    );
    Ok(())
}

#[test]
fn unique_settings() -> Result {
    assert!(ApproximateUnique::<i64>::with_sample_size(15).is_err());
    assert!(ApproximateUnique::<i64>::with_max_error(0.005).is_err());
    assert!(ApproximateUnique::<i64>::with_max_error(0.6).is_err());
    // An error of 0.1 needs 4 / 0.1^2 = 400 hashes, so 300 distinct elements are exact.
    let unique = ApproximateUnique::with_max_error(0.1)?;
    assert_eq!(combine_in_parts(&unique, (0..300i64).collect(), 2)?, 300);
    Ok(())
}
