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

//! `#[derive(Register)]`: a registered `DoFn` travels in the graph, so the worker needs no
//! rebuilt pipeline for it.

use std::collections::HashMap;

use beam::pipeline::{PipelineError, URN_PAR_DO, URN_RUST_DOFN, URN_RUST_DOFN_REGISTERED};
use beam::prelude::*;
use beam::transforms::sdf::{
    OffsetRange, OffsetRangeTracker, ProcessContinuation, SplittableDoFn, SplittableParDo,
};
use model::pipeline as proto;
use prost::Message;
use serde::{Deserialize, Serialize};
use testing::{TestPipeline, passert};

/// Multiplies by `factor` and adds `offset`. `offset` is not serialized, so the worker
/// gets its `Default`.
#[derive(Clone, Serialize, Deserialize, Register)]
struct ScaleBy {
    factor: i64,
    #[serde(skip)]
    offset: i64,
}

impl DoFn for ScaleBy {
    type In = i64;
    type Out = i64;

    fn process_element(&mut self, element: i64, ctx: &mut ProcessContext<'_, i64>) -> Result {
        ctx.emit(element * self.factor + self.offset)
    }
}

/// The same `DoFn` with no registration.
#[derive(Clone)]
struct Unregistered;

impl DoFn for Unregistered {
    type In = i64;
    type Out = i64;

    fn process_element(&mut self, element: i64, ctx: &mut ProcessContext<'_, i64>) -> Result {
        ctx.emit(element)
    }
}

/// Two types that share an id.
#[derive(Clone, Serialize, Deserialize, Register)]
#[beam(id = "test::Shared")]
struct SharedA;

#[derive(Clone, Serialize, Deserialize, Register)]
#[beam(id = "test::Shared")]
struct SharedB;

/// Fields that `serde_json` cannot write: map keys must be strings.
#[derive(Clone, Serialize, Deserialize, Register)]
struct NotSerializable {
    table: HashMap<(i64, i64), i64>,
}

macro_rules! identity_dofn {
    ($($t:ty),*) => {$(
        impl DoFn for $t {
            type In = i64;
            type Out = i64;

            fn process_element(&mut self, element: i64, ctx: &mut ProcessContext<'_, i64>) -> Result {
                ctx.emit(element)
            }
        }
    )*};
}
identity_dofn!(SharedA, SharedB, NotSerializable);

/// Adds a `ParDo` named `Identity` to a new pipeline.
type BuildIdentity = fn(&Pipeline);

/// The `do_fn` spec of the transform named `name`.
fn do_fn_spec(p: &Pipeline, name: &str) -> proto::FunctionSpec {
    let transform = p
        .to_proto()
        .components
        .expect("components")
        .transforms
        .into_values()
        .find(|t| t.unique_name == name)
        .expect("transform exists");
    let spec = transform.spec.expect("spec");
    assert_eq!(spec.urn, URN_PAR_DO);
    proto::ParDoPayload::decode(spec.payload.as_slice())
        .expect("ParDoPayload")
        .do_fn
        .expect("do_fn")
}

#[tokio::test]
async fn registered_dofn_runs_on_prism_without_a_registered_handler() {
    let p = TestPipeline::new();
    let out = p
        .apply(Create::new("Create", vec![1i64, 2, 3]))
        .apply(ParDo::new(
            "Scale",
            ScaleBy {
                factor: 10,
                offset: 5,
            },
        ));
    passert::that("Scaled", &out).contains_in_any_order([10i64, 20, 30]);

    assert_eq!(
        do_fn_spec(p.pipeline(), "Scale").urn,
        URN_RUST_DOFN_REGISTERED
    );
    // The assertions register closure handlers; the registered DoFn adds none.
    assert!(!p.pipeline().transform_handlers().contains_key("Scale"));
    p.run().await.expect("pipeline and assertions pass");
}

/// Emits `offset + i` for every `i` in `0..n`, splitting the range into pieces of 2.
#[derive(Clone, Serialize, Deserialize, Register)]
struct RangeFrom {
    offset: i64,
}

impl SplittableDoFn for RangeFrom {
    type In = i64;
    type Out = i64;
    type Restriction = OffsetRange;
    type Tracker = OffsetRangeTracker;

    fn initial_restriction(&self, n: &i64) -> OffsetRange {
        OffsetRange::new(0, *n)
    }

    fn split_restriction(&self, _n: &i64, restriction: &OffsetRange) -> Vec<OffsetRange> {
        restriction.sized_splits(2)
    }

    fn create_tracker(&self, restriction: &OffsetRange) -> OffsetRangeTracker {
        OffsetRangeTracker::new(*restriction)
    }

    fn process_element(
        &mut self,
        _n: i64,
        tracker: &OffsetRangeTracker,
        ctx: &mut ProcessContext<'_, i64>,
    ) -> Result<ProcessContinuation> {
        let mut i = tracker.current_restriction().start;
        while tracker.try_claim(&i) {
            ctx.emit(self.offset + i)?;
            i += 1;
        }
        Ok(ProcessContinuation::stop())
    }
}

#[tokio::test]
async fn registered_splittable_dofn_runs_on_prism_without_a_registered_handler() {
    let p = TestPipeline::new();
    let out = p
        .apply(Create::new("Create", vec![3i64, 2]))
        .apply(SplittableParDo::new("Range", RangeFrom { offset: 100 }));
    passert::that("Ranged", &out).contains_in_any_order([100i64, 101, 102, 100, 101]);

    assert_eq!(
        do_fn_spec(p.pipeline(), "Range").urn,
        URN_RUST_DOFN_REGISTERED
    );
    assert!(!p.pipeline().transform_handlers().contains_key("Range"));
    p.run().await.expect("pipeline and assertions pass");
}

#[test]
fn the_default_id_is_the_module_path_and_the_type_name() {
    #[derive(Message)]
    struct RegisteredSpec {
        #[prost(string, tag = "1")]
        id: String,
    }

    let p = Pipeline::new();
    let _ = p.apply(Create::new("Create", vec![1i64])).apply(ParDo::new(
        "Scale",
        ScaleBy {
            factor: 1,
            offset: 0,
        },
    ));
    let spec = do_fn_spec(&p, "Scale");
    let id = RegisteredSpec::decode(spec.payload.as_slice())
        .expect("spec")
        .id;
    assert_eq!(id, "registered_dofn_test::ScaleBy");
}

#[test]
fn a_dofn_without_the_derive_stays_on_the_rebuild_path() {
    let p = Pipeline::new();
    let _ = p
        .apply(Create::new("Create", vec![1i64]))
        .apply(ParDo::new("Identity", Unregistered));
    assert_eq!(do_fn_spec(&p, "Identity").urn, URN_RUST_DOFN);
    assert!(p.transform_handlers().contains_key("Identity"));
    assert_eq!(p.validate(), Ok(()));
}

#[test]
fn a_registration_error_fails_validate_and_keeps_the_rebuild_path() {
    let cases: [(&str, BuildIdentity); 2] = [
        (
            "more than one function type registered as 'test::Shared'",
            |p| {
                let _ = p
                    .apply(Create::new("Create", vec![1i64]))
                    .apply(ParDo::new("Identity", SharedA));
            },
        ),
        ("do not serialize", |p| {
            let table = HashMap::from([((1, 2), 3)]);
            let _ = p
                .apply(Create::new("Create", vec![1i64]))
                .apply(ParDo::new("Identity", NotSerializable { table }));
        }),
    ];
    for (expected, build) in cases {
        let p = Pipeline::new();
        build(&p);
        let error = p.validate().expect_err(expected);
        assert!(
            matches!(
                &error,
                PipelineError::InvalidConstruction { transform_id, reason }
                    if transform_id == "Identity" && reason.contains(expected)
            ),
            "{error}"
        );
        assert_eq!(do_fn_spec(&p, "Identity").urn, URN_RUST_DOFN, "{expected}");
    }
}

/// Emits each value times `step`, and counts the values of each key in state.
#[derive(Clone, Serialize, Deserialize, Register)]
struct StepPerKey {
    step: i64,
}

#[derive(State)]
struct Count {
    seen: ValueState<i64>,
}

impl StatefulDoFn for StepPerKey {
    type Key = String;
    type Value = i64;
    type Out = (String, i64);
    type State = Count;
    type Timer = NoTimers;

    fn process_element(
        &mut self,
        key: &String,
        value: i64,
        state: &mut Count,
        ctx: &mut StatefulContext<'_, Self>,
    ) -> Result {
        let seen = state.seen.read()?.unwrap_or(0);
        state.seen.write(seen + 1)?;
        ctx.emit((key.clone(), value * self.step))
    }
}

#[tokio::test]
async fn registered_stateful_dofn_runs_on_prism_without_a_registered_handler() {
    let p = TestPipeline::new();
    let input = [("a", 1i64), ("b", 2), ("a", 3)]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v))
        .collect::<Vec<_>>();
    let out = p
        .apply(Create::new("Create", input))
        .apply(ParDo::new("Step", StepPerKey { step: 10 }));
    passert::that("Stepped", &out).contains_in_any_order([
        ("a".to_string(), 10i64),
        ("b".to_string(), 20),
        ("a".to_string(), 30),
    ]);

    assert_eq!(
        do_fn_spec(p.pipeline(), "Step").urn,
        URN_RUST_DOFN_REGISTERED
    );
    assert!(!p.pipeline().transform_handlers().contains_key("Step"));
    p.run().await.expect("pipeline and assertions pass");
}

#[test]
fn an_unknown_id_does_not_build() {
    #[derive(Message)]
    struct RegisteredSpec {
        #[prost(string, tag = "1")]
        id: String,
    }

    let payload = RegisteredSpec {
        id: "nowhere::Missing".to_string(),
    }
    .encode_to_vec();
    let error = beam::internals::registered_handler(&payload)
        .err()
        .expect("no such registration");
    assert!(
        error.contains("no function type registered as 'nowhere::Missing'"),
        "{error}"
    );
}
