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

//! Multi-output expansion: declared output tags, [`ExternalOutputs`], and verification of
//! the declaration against the expansion service's response.

use beam::internals::PipelineInternals;
use beam::pipeline::{
    ExpansionMode, Pipeline, UNEXPANDED_PLACEHOLDER_ID, URN_EXPANSION_SCHEMA_TRANSFORM,
};
use external::*;
use testutils::xlang::{MockExpansionServer, MockExpansionService, OutputIds, Route};

/// Starts a mock expansion service whose every transform produces the fixed set of `tags`,
/// the way a Kafka read with error handling produces `output` plus its error tag. Returns its
/// endpoint and the server, which stops when dropped.
fn start_service(tags: Vec<&'static str>) -> (String, MockExpansionServer) {
    let server = MockExpansionService::new()
        .for_any_target(Route::source().with_outputs(tags))
        .with_output_ids(OutputIds::Underscore)
        .start();
    (server.endpoint().to_string(), server)
}

fn transform(name: &str, endpoint: &str) -> ExternalTransform {
    ExternalTransform::new(name, URN_EXPANSION_SCHEMA_TRANSFORM, endpoint, Vec::new())
}

/// A placeholder pipeline with one row-typed input, for exercising sinks offline.
fn placeholder_input(pipeline: &Pipeline) -> beam::values::PCollection<beam::schema::Row> {
    pipeline.begin().apply(ExternalSource::new(
        transform("Input", "http://127.0.0.1:1").with_namespace("ns_input"),
    ))
}

#[test]
fn placeholder_source_emits_a_pcollection_per_declared_tag() {
    let pipeline = Pipeline::new().with_expansion_mode(ExpansionMode::Placeholder);

    let outputs = pipeline.begin().apply(
        ExternalSource::new(
            transform("Read", "http://127.0.0.1:1")
                .with_namespace("ns_read")
                .with_output_tags(["output", "errors"]),
        )
        .all_outputs(),
    );

    assert_eq!(outputs.tags().collect::<Vec<_>>(), ["errors", "output"]);
    // The main output keeps the ID without a suffix, so existing worker graphs do not change.
    assert_eq!(outputs.try_get("output").unwrap().id(), "ns_read_out");
    assert_eq!(
        outputs.try_get("errors").unwrap().id(),
        "ns_read_out_errors"
    );

    let lock = pipeline.lock();
    let errors = lock
        .components
        .pcollections
        .get("ns_read_out_errors")
        .expect("placeholder error output recorded");
    assert_eq!(errors.coder_id, UNEXPANDED_PLACEHOLDER_ID);
    assert_eq!(errors.unique_name, "ns_read.out_errors");
}

#[test]
fn placeholder_sink_exposes_declared_outputs() {
    let pipeline = Pipeline::new().with_expansion_mode(ExpansionMode::Placeholder);
    let input = placeholder_input(&pipeline);

    let outputs = input.apply(
        ExternalSink::new(
            transform("Write", "http://127.0.0.1:1")
                .with_namespace("ns_write")
                .with_output_tags(["snapshots"]),
        )
        .all_outputs(),
    );

    assert_eq!(
        outputs.try_get("snapshots").unwrap().id(),
        "ns_write_out_snapshots"
    );
    let lock = pipeline.lock();
    let write = lock.components.transforms.get("Write").unwrap();
    assert_eq!(
        write.inputs.get("input").map(String::as_str),
        Some("ns_input_out")
    );
    assert!(write.outputs.contains_key("snapshots"));
}

#[test]
fn placeholder_without_declared_tags_is_unchanged() {
    let pipeline = Pipeline::new().with_expansion_mode(ExpansionMode::Placeholder);

    let outputs = pipeline.begin().apply(
        ExternalSource::new(transform("Read", "http://127.0.0.1:1").with_namespace("ns_plain"))
            .all_outputs(),
    );

    assert_eq!(outputs.tags().collect::<Vec<_>>(), ["output"]);
    assert_eq!(outputs.try_get("output").unwrap().id(), "ns_plain_out");
}

#[test]
fn remote_source_returns_every_output_with_its_coder() {
    let (endpoint, _service) = start_service(vec!["output", "errors"]);
    let pipeline = Pipeline::new();

    let outputs = pipeline
        .try_apply(
            ExternalSource::new(
                transform("KafkaRead", &endpoint)
                    .with_namespace("ns_kafka")
                    .with_output_tags(["errors"]),
            )
            .all_outputs(),
        )
        .unwrap();

    assert_eq!(outputs.len(), 2);
    let errors = outputs.try_get("errors").unwrap();
    assert_eq!(errors.id(), "ns_kafka_errors");
    assert_eq!(errors.coder_id(), "ns_kafka_coder");
}

#[test]
fn remote_source_main_output_still_resolves_with_extra_outputs() {
    let (endpoint, _service) = start_service(vec!["errors", "output"]);
    let pipeline = Pipeline::new();

    let main = pipeline
        .try_apply(ExternalSource::new(
            transform("KafkaRead", &endpoint).with_namespace("ns_main"),
        ))
        .unwrap();

    assert_eq!(main.id(), "ns_main_output");
}

#[test]
fn remote_sink_returns_its_outputs() {
    let (endpoint, _service) = start_service(vec!["snapshots"]);
    let pipeline = Pipeline::new().with_expansion_mode(ExpansionMode::Placeholder);
    let input = placeholder_input(&pipeline);
    pipeline.lock().expansion_mode = ExpansionMode::Remote;

    let outputs = input
        .try_apply(
            ExternalSink::new(
                transform("IcebergWrite", &endpoint)
                    .with_namespace("ns_iceberg")
                    .with_output_tags(["snapshots"]),
            )
            .all_outputs(),
        )
        .unwrap();

    assert_eq!(outputs.tags().collect::<Vec<_>>(), ["snapshots"]);
    assert_eq!(
        outputs.try_get("snapshots").unwrap().id(),
        "ns_iceberg_snapshots"
    );
}

#[test]
fn remote_expansion_rejects_a_missing_declared_tag() {
    let (endpoint, _service) = start_service(vec!["output"]);
    let pipeline = Pipeline::new();

    let err = pipeline
        .try_apply(
            ExternalSource::new(
                transform("KafkaRead", &endpoint)
                    .with_namespace("ns_typo")
                    .with_output_tags(["erorrs"]),
            )
            .all_outputs(),
        )
        .expect_err("a declared tag the service does not produce must fail expansion");

    let message = err.to_string();
    assert!(
        message.contains("erorrs"),
        "names the missing tag: {message}"
    );
    assert!(
        message.contains("output"),
        "names the produced tags: {message}"
    );
    // Rejected before splicing, so the pipeline carries no half-applied transform.
    assert!(
        !pipeline
            .lock()
            .components
            .transforms
            .contains_key("KafkaRead")
    );
}

#[test]
fn try_get_on_an_unknown_tag_lists_the_available_ones() {
    let pipeline = Pipeline::new().with_expansion_mode(ExpansionMode::Placeholder);
    let outputs = pipeline.begin().apply(
        ExternalSource::new(transform("Read", "http://127.0.0.1:1").with_output_tags(["errors"]))
            .all_outputs(),
    );

    assert!(outputs.get("snapshots").is_none());
    let message = outputs.try_get("snapshots").unwrap_err().to_string();
    assert!(message.contains("snapshots"), "{message}");
    assert!(message.contains("errors"), "{message}");
    assert!(message.contains("output"), "{message}");
}

#[test]
fn with_output_tags_accumulates_and_dedups() {
    let t = transform("T", "http://127.0.0.1:1")
        .with_output_tags(["a", "b"])
        .with_output_tags(vec!["b".to_string(), "c".to_string()]);

    assert_eq!(t.output_tags().iter().collect::<Vec<_>>(), ["a", "b", "c"]);
}

/// With no explicit namespace, the namespace comes from the transform name and not from a
/// process counter. So a worker that rebuilds the pipeline gets the IDs of the driver.
#[test]
fn the_default_namespace_comes_from_the_transform_name() {
    let placeholder_ids = || {
        let pipeline = Pipeline::new().with_expansion_mode(ExpansionMode::Placeholder);
        let outputs = pipeline.begin().apply(
            ExternalSource::new(
                transform("Read Kafka/v1", "http://127.0.0.1:1").with_output_tags(["errors"]),
            )
            .all_outputs(),
        );
        let ids: Vec<String> = outputs
            .into_map()
            .values()
            .map(|p| p.id().to_string())
            .collect();
        assert!(
            pipeline
                .lock()
                .components
                .transforms
                .contains_key("Read Kafka/v1")
        );
        ids
    };
    let first = placeholder_ids();
    assert_eq!(
        first,
        [
            "xlang_Read_20Kafka_2fv1_out_errors",
            "xlang_Read_20Kafka_2fv1_out"
        ]
    );
    assert_eq!(placeholder_ids(), first);

    // Remote expansion sends the same namespace to the service.
    let (endpoint, _service) = start_service(vec!["output", "errors"]);
    let outputs = Pipeline::new()
        .try_apply(ExternalSource::new(transform("KafkaRead", &endpoint)).all_outputs())
        .unwrap();
    assert_eq!(
        outputs.try_get("errors").unwrap().id(),
        "xlang_KafkaRead_errors"
    );

    // Names that differ only in punctuation get different namespaces.
    let namespace_of = |name: &str| {
        let pipeline = Pipeline::new().with_expansion_mode(ExpansionMode::Placeholder);
        let output = pipeline
            .begin()
            .apply(ExternalSource::new(transform(name, "http://127.0.0.1:1")));
        output.id().to_string()
    };
    assert_ne!(namespace_of("a b"), namespace_of("a_b"));
}

/// Two transforms with the same name get the unique names that the pipeline gives them, and
/// so distinct namespaces, IDs and outputs. A second pipeline built the same way gets the same
/// IDs, as a worker does.
#[test]
fn same_named_transforms_get_distinct_deterministic_ids() {
    let build = || {
        let pipeline = Pipeline::new().with_expansion_mode(ExpansionMode::Placeholder);
        let outputs: Vec<String> = (0..2)
            .map(|_| {
                pipeline
                    .begin()
                    .apply(ExternalSource::new(transform("Read", "http://127.0.0.1:1")))
                    .id()
                    .to_string()
            })
            .collect();
        let mut transforms: Vec<String> = pipeline
            .lock()
            .components
            .transforms
            .keys()
            .cloned()
            .collect();
        transforms.sort();
        (transforms, outputs)
    };
    let (transforms, outputs) = build();
    assert_eq!(transforms, ["Read", "Read_2"]);
    assert_eq!(outputs, ["xlang_Read_out", "xlang_Read_5f2_out"]);
    assert_eq!(build(), (transforms, outputs));
}
