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

use beam::pipeline::{PipelineError, URN_PAR_DO, URN_RUST_DOFN};
use beam::prelude::*;
use model::pipeline as proto;
use prost::Message;
use serde::{Deserialize, Serialize};

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
