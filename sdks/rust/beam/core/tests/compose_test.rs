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

//! Tests for the `CombineFn` adapters: `on`, `map_output` and `Compose`.

mod combine_support;

use beam::prelude::*;
use beam::schema::BeamRow;
use beam::transforms::{CombineFn, CombinePerKey, Compose, Max, Min, Sum};
use combine_support::combine_in_parts;

#[derive(Clone, Debug, PartialEq, BeamRow)]
#[beam(crate = "::beam")]
struct Trip {
    miles: f64,
    seconds: i64,
}

fn trips() -> Vec<Trip> {
    [(1.5, 300), (4.0, 900), (0.5, 120), (2.0, 600)]
        .into_iter()
        .map(|(miles, seconds)| Trip { miles, seconds })
        .collect()
}

/// Runs `combine_fn` over two partial accumulators, so the merge of every part runs.
fn combine<CF: CombineFn>(combine_fn: &CF, inputs: Vec<CF::Input>) -> Result<CF::Output> {
    combine_in_parts(combine_fn, inputs, 2)
}

#[test]
fn on_combines_one_field_of_a_record() -> Result {
    assert_eq!(
        combine(&Sum::new().on(|trip: &Trip| trip.seconds), trips())?,
        1920
    );
    assert_eq!(
        combine(&Max::new().on(|trip: &Trip| trip.miles), trips())?,
        4.0
    );
    Ok(())
}

#[test]
fn map_output_converts_the_result() -> Result {
    let minutes = Sum::new()
        .on(|trip: &Trip| trip.seconds)
        .map_output(|seconds| seconds / 60);
    assert_eq!(combine(&minutes, trips())?, 32);
    Ok(())
}

/// Each part sees every element, and the results come back in the order of the parts.
#[test]
fn compose_runs_each_part_over_every_element() -> Result {
    let stats = Compose::new((
        Sum::new().on(|_: &Trip| 1i64),
        Sum::new().on(|trip: &Trip| trip.miles),
        Max::new().on(|trip: &Trip| trip.seconds),
        Min::new().on(|trip: &Trip| trip.seconds),
    ));
    assert_eq!(combine(&stats, trips())?, (4, 8.0, 900, 120));
    Ok(())
}

/// A `Compose` can be a part of another `Compose`, for more than 6 results. The nested
/// parts read the same borrowed element.
#[test]
fn compose_nests_for_more_than_six_results() -> Result {
    let stats = Compose::new((
        Compose::new((
            Sum::new().on(|_: &Trip| 1i64),
            Sum::new().on(|trip: &Trip| trip.miles),
            Min::new().on(|trip: &Trip| trip.miles),
            Max::new().on(|trip: &Trip| trip.miles),
        )),
        Compose::new((
            Sum::new().on(|trip: &Trip| trip.seconds),
            Min::new().on(|trip: &Trip| trip.seconds),
            Max::new().on(|trip: &Trip| trip.seconds),
            Sum::new().on(|trip: &Trip| i64::from(trip.seconds > 500)),
        )),
    ));
    assert_eq!(
        combine(&stats, trips())?,
        ((4, 8.0, 0.5, 4.0), (1920, 120, 900, 2))
    );
    Ok(())
}

#[test]
fn compose_of_no_elements_gives_the_identity_of_each_part() -> Result {
    let stats = Compose::new((
        Sum::new().on(|_: &Trip| 1i64),
        Max::new().on(|trip: &Trip| trip.seconds),
    ));
    assert_eq!(combine(&stats, Vec::new())?, (0, i64::MIN));
    Ok(())
}

#[test]
fn compose_with_map_output_builds_a_record() -> Result {
    #[derive(Debug, PartialEq, BeamRow)]
    #[beam(crate = "::beam")]
    struct Summary {
        trips: i64,
        mean_miles: f64,
    }

    let summary = Compose::new((
        Sum::new().on(|_: &Trip| 1i64),
        Sum::new().on(|trip: &Trip| trip.miles),
    ))
    .map_output(|(trips, miles)| Summary {
        trips,
        mean_miles: miles / trips as f64,
    });
    assert_eq!(
        combine(&summary, trips())?,
        Summary {
            trips: 4,
            mean_miles: 2.0
        }
    );
    Ok(())
}

/// A part error fails the combine, as it does for the part alone.
#[test]
fn a_part_error_fails_the_composed_function() {
    let stats = Compose::new((
        Sum::new().on(|_: &Trip| 1i64),
        Sum::new().on(|trip: &Trip| {
            if trip.seconds > 600 {
                i64::MAX
            } else {
                trip.seconds
            }
        }),
    ));
    let err = combine(&stats, trips()).expect_err("the second part overflows");
    assert!(err.to_string().contains("overflowed i64"), "{err}");
}

/// The composed accumulator is a tuple, so `CombinePerKey` registers its coder and builds.
#[test]
fn compose_applies_in_combine_per_key() {
    let p = Pipeline::new();
    let keyed = p.apply(
        "Create",
        Create::new(
            trips()
                .into_iter()
                .map(|trip| ("driver".to_string(), trip))
                .collect::<Vec<_>>(),
        ),
    );
    let _stats: PCollection<(String, (i64, f64, i64))> = keyed.apply(
        "Stats",
        CombinePerKey::new(Compose::new((
            Sum::new().on(|_: &Trip| 1i64),
            Sum::new().on(|trip: &Trip| trip.miles),
            Max::new().on(|trip: &Trip| trip.seconds),
        ))),
    );
    p.validate().unwrap();
}
