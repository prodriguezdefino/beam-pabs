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

//! The boundedness of the placeholder outputs of a cross-language transform.

use beam::coders::URN_BYTES;
use beam::internals::PipelineInternals;
use beam::pipeline::{ExpansionMode, Pipeline, URN_EXPANSION_SCHEMA_TRANSFORM};
use beam::values::IsBounded;
use external::*;
use model::pipeline::is_bounded::Enum as Bounded;

/// The marks of the outputs of a placeholder transform with one input of each boundedness.
fn output_marks(inputs: &[IsBounded]) -> Vec<Bounded> {
    let pipeline = Pipeline::new().with_expansion_mode(ExpansionMode::Placeholder);
    let coder = pipeline.register_coder(URN_BYTES, Vec::new());
    let read = ExternalTransform::new(
        "Read",
        URN_EXPANSION_SCHEMA_TRANSFORM,
        "http://127.0.0.1:1",
        Vec::new(),
    )
    .with_namespace("ns_read")
    .with_output_tags(["output", "errors"]);
    let transform = inputs
        .iter()
        .enumerate()
        .fold(read, |transform, (index, mark)| {
            let tag = format!("input_{index}");
            let input = pipeline.add_pcollection::<Vec<u8>>(&tag, &coder, *mark);
            transform.with_input(tag, input.id())
        });
    let (_, expanded) = transform
        .expand_with_pipeline(&pipeline)
        .expect("placeholder expansion does not call a service");
    let lock = pipeline.lock();
    expanded
        .outputs
        .values()
        .map(|id| lock.components.pcollections[id].is_bounded())
        .collect()
}

#[test]
fn placeholder_outputs_take_the_boundedness_of_the_inputs() {
    let cases = [
        (vec![], Bounded::Bounded),
        (vec![IsBounded::Bounded], Bounded::Bounded),
        (vec![IsBounded::Unbounded], Bounded::Unbounded),
        (
            vec![IsBounded::Bounded, IsBounded::Unbounded],
            Bounded::Unbounded,
        ),
    ];
    cases.into_iter().for_each(|(inputs, expected)| {
        assert_eq!(output_marks(&inputs), [expected; 2], "{inputs:?}");
    });
}
