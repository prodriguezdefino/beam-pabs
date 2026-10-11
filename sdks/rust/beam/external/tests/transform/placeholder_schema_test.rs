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

//! The row schema of the placeholder outputs of a cross-language transform.

use std::sync::Arc;

use beam::pipeline::{ExpansionMode, Pipeline, UNEXPANDED_PLACEHOLDER_ID};
use beam::schema::{FieldType, Row, Schema};
use beam::transforms::{Map, PTransform};
use beam::values::PCollection;
use beam_external::*;
use beam_test_utils::xlang::{MockExpansionService, Route};

use crate::common::{Shape, shape, worker_pipeline};

const SOURCE: &str = "beam:schematransform:test:rows:v1";

/// The worker has no service: an unroutable endpoint fails the test if it is called.
const UNROUTABLE: &str = "http://127.0.0.1:1";

fn output_schema() -> Schema {
    Schema::builder()
        .field("id", FieldType::int64())
        .field("name", FieldType::string())
        .build()
}

/// Adds a step only when its input has a row schema, as the sinks that build a row codec do.
struct Probe;

impl PTransform<PCollection<Row>> for Probe {
    type Output = PCollection<Row>;

    fn expand(self, input: &PCollection<Row>) -> PCollection<Row> {
        match input.row_schema() {
            Some(schema) => input
                .apply("Encode", Map::new(|row: Row| row))
                .with_row_schema(&schema),
            None => {
                input
                    .pipeline()
                    .record_invalid_construction("the input has no row schema");
                input.clone()
            }
        }
    }
}

/// Reads the source of `endpoint` and applies `Probe`. Returns the schema and the coder of the
/// read, and the transforms of `Probe`.
fn read_and_probe(pipeline: &Pipeline, endpoint: &str) -> (Option<Arc<Schema>>, String, Shape) {
    let config_schema = Arc::new(Schema::builder().field("name", FieldType::string()).build());
    let config = Row::builder(config_schema)
        .with_value("events")
        .build()
        .unwrap();
    let source = ExternalSource::new(
        ExternalTransform::schema_transform(SOURCE, endpoint, &config).unwrap(),
    );
    let read = pipeline.try_apply("Read", source).unwrap();
    let _ = read.apply("Probe", Probe);
    (
        read.row_schema(),
        read.coder_id().to_string(),
        shape(pipeline, "Probe"),
    )
}

#[test]
fn a_worker_gets_the_row_schema_of_a_cross_language_output() {
    let server = MockExpansionService::new()
        .with_route(SOURCE, Route::source().with_schema(&output_schema()))
        .start();
    let launcher = Pipeline::new();
    let (launcher_schema, launcher_coder, launcher_shape) =
        read_and_probe(&launcher, server.endpoint());
    assert_eq!(launcher_schema.as_deref(), Some(&output_schema()));
    assert!(
        launcher_shape.keys().any(|id| id.contains("Encode")),
        "{launcher_shape:?}"
    );

    let worker = worker_pipeline(&launcher);
    let (worker_schema, worker_coder, worker_shape) = read_and_probe(&worker, UNROUTABLE);
    assert_eq!(worker_schema, launcher_schema);
    assert_eq!(worker_coder, launcher_coder);
    assert_eq!(worker_shape, launcher_shape);
}

#[test]
fn a_worker_without_the_launcher_options_has_no_row_schema() {
    let worker = Pipeline::new().with_expansion_mode(ExpansionMode::Placeholder);
    let (schema, coder, probe) = read_and_probe(&worker, UNROUTABLE);
    assert_eq!(schema, None);
    assert_eq!(coder, UNEXPANDED_PLACEHOLDER_ID);
    assert!(!probe.keys().any(|id| id.contains("Encode")), "{probe:?}");
}
