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

//! The graph check of a worker after a cross-language transform that it does not expand.

use std::sync::Arc;

use beam::internals::PipelineInternals;
use beam::pipeline::Pipeline;
use beam::schema::{FieldType, Row, Schema};
use beam::transforms::Map;
use beam::values::IsBounded;
use beam_external::*;
use beam_test_utils::xlang::MockExpansionService;

use crate::common::worker_pipeline;

const SOURCE: &str = "beam:schematransform:test:source:v1";

/// Reads the source of `endpoint` and maps each row.
fn read_and_map(pipeline: &Pipeline, endpoint: &str) {
    let schema = Arc::new(Schema::builder().field("name", FieldType::string()).build());
    let config = Row::builder(schema).with_value("events").build().unwrap();
    let source = ExternalSource::new(
        ExternalTransform::schema_transform(SOURCE, endpoint, &config).unwrap(),
    );
    let _ = pipeline
        .try_apply("Read", source)
        .unwrap()
        .apply("Map", Map::new(|row: Row| row));
}

#[test]
fn a_step_after_a_placeholder_output_matches_the_launcher_input() {
    let server = MockExpansionService::new()
        .with_source(SOURCE, IsBounded::Bounded)
        .start();
    let launcher = Pipeline::new();
    read_and_map(&launcher, server.endpoint());
    launcher.record_rebuild_graph(&launcher.options()).unwrap();

    let worker = worker_pipeline(&launcher);
    read_and_map(&worker, "http://127.0.0.1:1");

    let input = |pipeline: &Pipeline| {
        let lock = pipeline.lock();
        lock.components.transforms["Map"].inputs.clone()
    };
    assert_ne!(input(&launcher), input(&worker));
    worker.check_rebuild_graph(&worker.options()).unwrap();
}
