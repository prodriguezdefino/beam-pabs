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

//! The boundedness that each transform gives its output PCollections in the pipeline proto.

use std::collections::BTreeSet;
use std::time::Duration;

use beam::internals::PipelineInternals;
use beam::prelude::*;
use beam::transforms::sdf::{
    OffsetRange, OffsetRangeTracker, ProcessContinuation, RestrictionTracker, SplittableDoFn,
    SplittableParDo,
};
use beam::values::IsBounded;
use model::pipeline as proto;
use model::pipeline::is_bounded::Enum as Bounded;

/// Emits `0..element`. `boundedness` sets whether the work of one element is bounded.
#[derive(Clone)]
struct Expand {
    boundedness: IsBounded,
}

impl SplittableDoFn for Expand {
    type In = i64;
    type Out = i64;
    type Restriction = OffsetRange;
    type Tracker = OffsetRangeTracker;

    fn boundedness(&self) -> IsBounded {
        self.boundedness
    }

    fn initial_restriction(&self, element: &i64) -> Result<OffsetRange> {
        Ok(OffsetRange::new(0, *element))
    }

    fn create_tracker(&self, restriction: &OffsetRange) -> OffsetRangeTracker {
        OffsetRangeTracker::new(*restriction)
    }

    fn process_element(
        &mut self,
        _element: i64,
        tracker: &OffsetRangeTracker,
        ctx: &mut ProcessContext<'_, i64>,
    ) -> Result<ProcessContinuation> {
        let start = tracker.current_restriction().start;
        (start..)
            .take_while(|position| tracker.try_claim(position))
            .try_for_each(|position| ctx.emit(position))?;
        Ok(ProcessContinuation::Stop)
    }
}

/// The PCollections of each step of a pipeline that starts with an SDF of `boundedness`.
struct Steps {
    p: Pipeline,
    impulse: PCollection<Vec<u8>>,
    seed: PCollection<i64>,
    source: PCollection<i64>,
    keyed: PCollection<(String, i64)>,
    grouped: PCollection<(String, BeamIterable<i64>)>,
    summed: PCollection<(String, i64)>,
    reshuffled: PCollection<(String, i64)>,
    combined: PCollection<(String, i64)>,
    values: PCollection<i64>,
    windowed: PCollection<i64>,
    bounded_sdf: PCollection<i64>,
    created: PCollection<i64>,
    flattened: PCollection<i64>,
}

fn steps(boundedness: IsBounded) -> Steps {
    let p = Pipeline::new();
    let impulse = p.apply("Impulse", beam::transforms::Impulse::new());
    let seed = impulse.apply("Seed", Map::new(|_: Vec<u8>| 3_i64));
    let source = seed.apply("Source", SplittableParDo::new(Expand { boundedness }));
    let keyed = source.apply("Key", Map::new(|n: i64| ((n % 2).to_string(), n)));
    let grouped = keyed.apply("Group", GroupByKey::new());
    let summed = grouped.apply(
        "SumGroup",
        Map::new(|(key, values): (String, BeamIterable<i64>)| {
            (
                key,
                values
                    .into_iter()
                    .sum::<Result<i64>>()
                    .expect("group values decode"),
            )
        }),
    );
    let reshuffled = summed.apply("Reshuffle", Reshuffle::new());
    let combined = reshuffled.apply("Combine", CombinePerKey::new(Sum::new()));
    let values = combined.apply("Values", Map::new(|(_, n): (String, i64)| n));
    let windowed = values.apply(
        "Window",
        WindowInto::new(FixedWindows::of(Duration::from_secs(60))),
    );
    let bounded_sdf = windowed.apply(
        "BoundedSdf",
        SplittableParDo::new(Expand {
            boundedness: IsBounded::Bounded,
        }),
    );
    let created = p.apply("Create", Create::new(vec![1_i64, 2])).apply(
        "WindowCreated",
        WindowInto::new(FixedWindows::of(Duration::from_secs(60))),
    );
    let flattened = PCollectionList::of(bounded_sdf.clone())
        .and(created.clone())
        .apply("Flatten", Flatten::new());
    Steps {
        p,
        impulse,
        seed,
        source,
        keyed,
        grouped,
        summed,
        reshuffled,
        combined,
        values,
        windowed,
        bounded_sdf,
        created,
        flattened,
    }
}

/// The `is_bounded` of each named PCollection in the pipeline proto.
fn marks(components: &proto::Components, named: &[(&str, &str)]) -> Vec<(String, Bounded)> {
    named
        .iter()
        .map(|(name, id)| (name.to_string(), components.pcollections[*id].is_bounded()))
        .collect()
}

/// The ids of `start` and of every PCollection that a transform derives from it.
fn downstream(components: &proto::Components, start: &str) -> BTreeSet<String> {
    let mut reached = BTreeSet::from([start.to_string()]);
    loop {
        let next: BTreeSet<String> = components
            .transforms
            .values()
            .filter(|t| t.inputs.values().any(|id| reached.contains(id)))
            .flat_map(|t| t.outputs.values().cloned())
            .filter(|id| !reached.contains(id))
            .collect();
        if next.is_empty() {
            return reached;
        }
        reached.extend(next);
    }
}

#[test]
fn every_pcollection_after_an_unbounded_sdf_is_unbounded() {
    let s = steps(IsBounded::Unbounded);
    let components = s.p.to_proto().components.unwrap();
    let named = [
        ("impulse", s.impulse.id()),
        ("seed", s.seed.id()),
        ("source", s.source.id()),
        ("keyed", s.keyed.id()),
        ("grouped", s.grouped.id()),
        ("summed", s.summed.id()),
        ("reshuffled", s.reshuffled.id()),
        ("combined", s.combined.id()),
        ("values", s.values.id()),
        ("windowed", s.windowed.id()),
        ("bounded_sdf", s.bounded_sdf.id()),
        ("created", s.created.id()),
        ("flattened", s.flattened.id()),
    ];
    let expected = [
        ("impulse", Bounded::Bounded),
        ("seed", Bounded::Bounded),
        ("source", Bounded::Unbounded),
        ("keyed", Bounded::Unbounded),
        ("grouped", Bounded::Unbounded),
        ("summed", Bounded::Unbounded),
        ("reshuffled", Bounded::Unbounded),
        ("combined", Bounded::Unbounded),
        ("values", Bounded::Unbounded),
        ("windowed", Bounded::Unbounded),
        // A bounded SDF keeps the boundedness of an unbounded input.
        ("bounded_sdf", Bounded::Unbounded),
        ("created", Bounded::Bounded),
        // One unbounded input makes the output of Flatten unbounded.
        ("flattened", Bounded::Unbounded),
    ]
    .map(|(name, mark)| (name.to_string(), mark));
    assert_eq!(marks(&components, &named), expected);

    // The internal PCollections of the composites (Reshuffle, CombinePerKey) are unbounded too.
    let unexpected: Vec<(String, Bounded)> = downstream(&components, s.source.id())
        .into_iter()
        .map(|id| {
            let mark = components.pcollections[&id].is_bounded();
            (id, mark)
        })
        .filter(|(_, mark)| *mark != Bounded::Unbounded)
        .collect();
    assert_eq!(unexpected, []);
}

#[test]
fn a_bounded_pipeline_stays_bounded() {
    let s = steps(IsBounded::Bounded);
    let components = s.p.to_proto().components.unwrap();
    let unbounded: Vec<&String> = components
        .pcollections
        .iter()
        .filter(|(_, p)| p.is_bounded() != Bounded::Bounded)
        .map(|(id, _)| id)
        .collect();
    assert_eq!(unbounded, Vec::<&String>::new());
    assert_eq!(s.flattened.is_bounded(), IsBounded::Bounded);
}

/// A globally windowed PCollection of `boundedness`.
fn source(p: &Pipeline, boundedness: IsBounded) -> PCollection<i64> {
    p.apply("Impulse", beam::transforms::Impulse::new())
        .apply("Seed", Map::new(|_: Vec<u8>| 3_i64))
        .apply("Source", SplittableParDo::new(Expand { boundedness }))
}

#[derive(Outputs)]
enum Parity {
    Even(i64),
    Odd(i64),
}

#[derive(Clone)]
struct SplitParity;

impl DoFn for SplitParity {
    type In = i64;
    type Out = Parity;

    fn process_element(&mut self, n: i64, ctx: &mut ProcessContext<'_, Parity>) -> Result {
        ctx.emit(match n % 2 {
            0 => Parity::Even(n),
            _ => Parity::Odd(n),
        })
    }
}

#[test]
fn multi_output_cogroup_and_global_combine_outputs_take_the_input_boundedness() {
    [IsBounded::Bounded, IsBounded::Unbounded]
        .into_iter()
        .for_each(|boundedness| {
            let p = Pipeline::new();
            let input = source(&p, boundedness);
            let ParityPCollections { even, odd } =
                input.apply("SplitParity", ParDo::new(SplitParity));
            let parts = input
                .apply(
                    "Partition",
                    Partition::new(2, |n: &i64| n.unsigned_abs() as usize % 2),
                )
                .into_vec();
            let keyed = input.apply("Key", Map::new(|n: i64| (n % 2, n)));
            let cogrouped = KeyedPCollectionTuple::of("left", &keyed)
                .and("right", &keyed)
                .apply("CoGroup", CoGroupByKey::new());
            let combined = input.apply(
                "CombineGlobally",
                CombineGlobally::new(Sum::new()).without_defaults(),
            );
            let counted = input.apply("CountGlobally", CountGlobally::new().without_defaults());

            let outputs = [
                ("even", even.is_bounded()),
                ("odd", odd.is_bounded()),
                ("partition_0", parts[0].is_bounded()),
                ("partition_1", parts[1].is_bounded()),
                ("cogrouped", cogrouped.is_bounded()),
                ("combined", combined.is_bounded()),
                ("counted", counted.is_bounded()),
            ];
            assert_eq!(outputs, outputs.map(|(name, _)| (name, boundedness)));

            // The internal PCollections of the composites have the same boundedness.
            let components = p.to_proto().components.unwrap();
            let expected = Bounded::from(boundedness);
            let unexpected: Vec<String> = downstream(&components, input.id())
                .into_iter()
                .filter(|id| components.pcollections[id].is_bounded() != expected)
                .collect();
            assert_eq!(unexpected, Vec::<String>::new(), "{boundedness:?}");
        });
}

#[test]
fn global_combines_with_defaults_of_a_bounded_input_are_bounded() {
    let p = Pipeline::new();
    let input = source(&p, IsBounded::Bounded);
    let combined = input.apply("CombineGlobally", CombineGlobally::new(Sum::new()));
    let counted = input.apply("CountGlobally", CountGlobally::new());
    assert_eq!(
        [combined.is_bounded(), counted.is_bounded()],
        [IsBounded::Bounded; 2]
    );
}

#[test]
fn combine_globally_with_defaults_of_an_unbounded_input_is_a_construction_error() {
    let p = Pipeline::new();
    source(&p, IsBounded::Unbounded).apply("CombineGlobally", CombineGlobally::new(Sum::new()));
    let err = p.validate().unwrap_err();
    assert!(err.to_string().contains("needs a bounded input"), "{err}");
}

#[test]
fn count_globally_with_defaults_of_an_unbounded_input_is_a_construction_error() {
    let p = Pipeline::new();
    source(&p, IsBounded::Unbounded).apply("CountGlobally", CountGlobally::new());
    let err = p.validate().unwrap_err();
    assert!(err.to_string().contains("needs a bounded input"), "{err}");
}

/// Flatten rejects an empty list, so its output always has an input to take the boundedness of.
#[test]
fn flatten_of_no_inputs_is_a_construction_error() {
    let p = Pipeline::new();
    PCollectionList::<i64>::empty(p.clone()).apply("Flatten", Flatten::new());
    let err = p.validate().unwrap_err();
    assert!(
        err.to_string()
            .contains("Flatten requires at least one input PCollection"),
        "{err}"
    );
}

#[test]
fn is_bounded_reads_the_mark_of_the_graph() {
    let p = Pipeline::new();
    let coder = p.register_coder(beam::coders::URN_BYTES, Vec::new());
    let cases = [
        ("bounded", IsBounded::Bounded),
        ("unbounded", IsBounded::Unbounded),
    ];
    cases.into_iter().for_each(|(name, mark)| {
        let pcoll = p.add_pcollection::<Vec<u8>>(name, &coder, mark);
        assert_eq!(pcoll.is_bounded(), mark, "{name}");
    });

    // An unspecified mark on the wire counts as bounded.
    let unspecified = p.add_pcollection::<Vec<u8>>("unspecified", &coder, IsBounded::Unbounded);
    p.lock()
        .components
        .pcollections
        .get_mut(unspecified.id())
        .unwrap()
        .is_bounded = Bounded::Unspecified as i32;
    assert_eq!(unspecified.is_bounded(), IsBounded::Bounded);
}

#[test]
fn input_boundedness_combines_to_unbounded_if_one_is_unbounded() {
    let cases = [
        (vec![], IsBounded::Bounded),
        (
            vec![IsBounded::Bounded, IsBounded::Bounded],
            IsBounded::Bounded,
        ),
        (
            vec![IsBounded::Bounded, IsBounded::Unbounded],
            IsBounded::Unbounded,
        ),
        (vec![IsBounded::Unbounded], IsBounded::Unbounded),
    ];
    cases.into_iter().for_each(|(inputs, expected)| {
        assert_eq!(
            inputs.iter().copied().collect::<IsBounded>(),
            expected,
            "{inputs:?}"
        );
    });
}
