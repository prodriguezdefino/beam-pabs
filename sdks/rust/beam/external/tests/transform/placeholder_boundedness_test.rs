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

use std::sync::Arc;

use beam::coders::URN_BYTES;
use beam::internals::PipelineInternals;
use beam::pipeline::{ExpansionMode, Pipeline, URN_EXPANSION_SCHEMA_TRANSFORM};
use beam::schema::{FieldType, Row, Schema};
use beam::transforms::{Map, PTransform};
use beam::values::{IsBounded, PCollection};
use beam_external::*;
use beam_model::pipeline::is_bounded::Enum as Bounded;
use beam_test_utils::xlang::MockExpansionService;

use crate::common::{Shape, shape, worker_pipeline};

/// The marks of the outputs of a placeholder transform with one input of each boundedness.
fn output_marks(inputs: &[IsBounded]) -> Vec<Bounded> {
    let pipeline = Pipeline::new().with_expansion_mode(ExpansionMode::Placeholder);
    let coder = pipeline.register_coder(URN_BYTES, Vec::new());
    let read = ExternalTransform::new(
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

const SOURCE: &str = "beam:schematransform:test:source:v1";

/// Adds a second step when its input is unbounded, as the sinks that pick a streaming shape do.
struct Branch;

impl PTransform<PCollection<Row>> for Branch {
    type Output = PCollection<Row>;

    fn expand(self, input: &PCollection<Row>) -> PCollection<Row> {
        let mapped = input.apply("Map", Map::new(|row: Row| row));
        match input.is_bounded() {
            IsBounded::Bounded => mapped,
            IsBounded::Unbounded => mapped.apply("Unbounded", Map::new(|row: Row| row)),
        }
    }
}

/// Reads the source of `endpoint` and applies `Branch`. Returns the boundedness of the read
/// and the transforms of `Branch`.
fn read_and_branch(pipeline: &Pipeline, endpoint: &str) -> (IsBounded, Shape) {
    let schema = Arc::new(Schema::builder().field("name", FieldType::string()).build());
    let config = Row::builder(schema).with_value("events").build().unwrap();
    let source = ExternalSource::new(
        ExternalTransform::schema_transform(SOURCE, endpoint, &config).unwrap(),
    );
    let read = pipeline.try_apply("Read", source).unwrap();
    let _ = read.apply("Branch", Branch);
    (read.is_bounded(), shape(pipeline, "Branch"))
}

#[test]
fn a_worker_builds_the_launcher_graph_after_a_cross_language_source() {
    [IsBounded::Bounded, IsBounded::Unbounded]
        .into_iter()
        .for_each(|boundedness| {
            let server = MockExpansionService::new()
                .with_source(SOURCE, boundedness)
                .start();
            let launcher = Pipeline::new();
            let (launcher_mark, launcher_shape) = read_and_branch(&launcher, server.endpoint());
            assert_eq!(launcher_mark, boundedness);
            assert_eq!(
                launcher_shape.keys().any(|id| id.contains("Unbounded")),
                boundedness == IsBounded::Unbounded,
                "{launcher_shape:?}"
            );

            // The worker has no service: an unroutable endpoint fails the test if it is called.
            let worker = worker_pipeline(&launcher);
            let (worker_mark, worker_shape) = read_and_branch(&worker, "http://127.0.0.1:1");
            assert_eq!(worker_mark, boundedness);
            assert_eq!(worker_shape, launcher_shape, "{boundedness:?}");
        });
}
