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

//! Construction checks of `ToRows` and `FromRows`.

use beam::pipeline::{Pipeline, PipelineError};
use beam::schema::BeamRow;
use beam::transforms::{Create, FromRows, ToRows};
use beam::values::PCollection;

#[derive(Clone, Debug, PartialEq, BeamRow)]
#[beam(crate = "::beam")]
struct IdOnly {
    id: i64,
}

#[derive(Clone, Debug, PartialEq, BeamRow)]
#[beam(crate = "::beam")]
struct IdAndName {
    id: i64,
    name: String,
    note: Option<String>,
}

#[test]
fn from_rows_with_a_missing_required_field_fails_validation() {
    let p = Pipeline::new();
    let rows = p
        .apply("Create", Create::new(vec![IdOnly { id: 1 }]))
        .apply("ToRows", ToRows::new());
    let _typed: PCollection<IdAndName> = rows.apply("FromRows", FromRows::new());

    let err = p
        .validate()
        .expect_err("a missing required field must fail validation");
    assert!(
        matches!(&err, PipelineError::InvalidConstruction { reason, .. } if reason.contains("name")),
        "unexpected error: {err:?}"
    );
}

#[test]
fn from_rows_with_a_missing_nullable_field_validates() {
    #[derive(Clone, Debug, PartialEq, BeamRow)]
    #[beam(crate = "::beam")]
    struct NameAndId {
        name: String,
        id: i64,
    }

    let p = Pipeline::new();
    let rows = p
        .apply(
            "Create",
            Create::new(vec![NameAndId {
                name: "a".to_string(),
                id: 1,
            }]),
        )
        .apply("ToRows", ToRows::new());
    let _typed: PCollection<IdAndName> = rows.apply("FromRows", FromRows::new());

    p.validate()
        .expect("a reordered schema with a missing nullable field is valid");
}

#[test]
fn as_row_elements_needs_the_row_coder_of_the_type() {
    let p = Pipeline::new();
    let ids = p.apply("Create", Create::new(vec![IdOnly { id: 1 }]));
    let raw = ids
        .as_row_elements()
        .expect("the coder of a BeamRow type is its row coder");
    assert_eq!(raw.id(), ids.id());
    assert_eq!(raw.coder_id(), ids.coder_id());

    let other: PCollection<IdAndName> =
        PCollection::new(ids.id().to_string(), ids.coder_id().to_string(), p);
    assert!(other.as_row_elements().is_none());
}
