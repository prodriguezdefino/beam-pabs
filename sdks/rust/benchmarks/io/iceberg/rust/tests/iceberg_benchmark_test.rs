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

//! Tests of the Iceberg benchmark in a Rust pipeline. They need no Kafka cluster or catalog.

use beam::options::{ParseError, PipelineOptions, try_parse_from};
use beam_benchmark_iceberg::{
    Args, DEFAULT_LOCATION, DeviceMetric, Shape, build_pipeline, check_shape, parse_device_metric,
};
use beam_benchmark_io::Connector;
use chrono::DateTime;

/// Parses the catalog flags and then `flags`.
fn try_parse_shape(flags: &[&str]) -> Result<(PipelineOptions, Args), ParseError> {
    let args = [
        "apache-beam-benchmark-iceberg",
        "--warehouse=bl://projects/p/catalogs/c",
        "--table=iceberg_bench.ingest",
    ]
    .into_iter()
    .chain(flags.iter().copied());
    try_parse_from::<Args, _, _>(args)
}

/// Parses the flags of `ingest` and then `flags`.
fn try_parse(flags: &[&str]) -> Result<(PipelineOptions, Args), ParseError> {
    let ingest = [
        "--writer=rust",
        "--bootstrap_servers=localhost:9092",
        "--topic=device-metrics",
    ];
    try_parse_shape(&[ingest.as_slice(), flags].concat())
}

#[test]
fn flags_have_the_defaults_of_the_launcher() {
    let (_, args) = try_parse(&[]).expect("valid flags");
    assert_eq!(args.writer, Some(Connector::Rust));
    assert_eq!(args.shape, Shape::Ingest);
    assert_eq!(args.triggering_frequency_seconds, 60);
    assert_eq!(args.num_shards, None);
    assert!(args.with_gcp_adc);
    assert!(!args.object_storage);
}

#[test]
fn a_record_parses_into_a_metric() {
    let record =
        br#"{"id":7,"timestamp":1700000000123,"device_id":"d-1","payload":1.5,"location":"eu"}"#;
    assert_eq!(
        parse_device_metric(record),
        Some(DeviceMetric {
            id: 7,
            timestamp_ms: DateTime::from_timestamp_millis(1_700_000_000_123).expect("a time"),
            device_id: "d-1".to_string(),
            payload: 1.5,
            location: "eu".to_string(),
        })
    );
}

#[test]
fn a_record_with_no_location_gets_the_default() {
    let record = br#"{"id":7,"timestamp":1700000000123,"device_id":"d-1","payload":1}"#;
    let metric = parse_device_metric(record).expect("a metric");
    assert_eq!(metric.location, DEFAULT_LOCATION);
    assert_eq!(metric.payload, 1.0);
}

#[test]
fn a_bad_record_gives_no_metric() {
    assert_eq!(parse_device_metric(b"not json"), None);
    assert_eq!(parse_device_metric(br#"{"id":"x","timestamp":1}"#), None);
}

#[test]
fn the_pipeline_builds_and_validates() {
    let (options, args) = try_parse(&["--num_shards=4", "--user_project=p"]).expect("valid flags");
    let p = build_pipeline(&options, &args).expect("a pipeline");
    p.validate().expect("a valid pipeline");
}

#[test]
fn a_local_run_with_an_end_builds_a_bounded_write() {
    let (options, args) = try_parse(&["--max_read_seconds=30"]).expect("valid flags");
    let p = build_pipeline(&options, &args).expect("a pipeline");
    p.validate().expect("a valid pipeline");
}

#[test]
fn each_shape_takes_only_its_flags() {
    let check = |flags: &[&str]| {
        let (_, args) = try_parse_shape(flags).expect("valid flags");
        check_shape(&args).map_err(|e| e.to_string())
    };
    assert_eq!(check(&["--shape=count", "--reader=rust"]), Ok(()));
    let load = ["--shape=load", "--writer=rust", "--source_table=p:d.t"];
    assert_eq!(check(&load), Ok(()));
    let errors = [
        check(&["--shape=count"]),
        check(&["--shape=count", "--reader=rust", "--writer=rust"]),
        check(&["--shape=load", "--writer=rust"]),
        check(&["--shape=ingest", "--writer=rust"]),
        check(&["--shape=ingest", "--reader=rust"]),
    ];
    assert!(errors.iter().all(Result::is_err), "{errors:?}");
}

#[tokio::test]
async fn benchmark_expansion_service_discovers_kafka_and_iceberg_providers() {
    use beam::model::expansion::DiscoverSchemaTransformRequest;
    use beam::model::expansion::expansion_service_server::ExpansionService;
    use beam_expansion::ExpansionServiceServer;
    use beam_io_iceberg_expansion::{URN_ICEBERG_READ, URN_ICEBERG_WRITE};
    use beam_io_kafka_expansion::URN_KAFKA_READ;
    use tonic::Request;

    let resp = ExpansionServiceServer::new()
        .discover_schema_transform(Request::new(DiscoverSchemaTransformRequest {}))
        .await
        .expect("discovery succeeds")
        .into_inner();
    assert_eq!(resp.error, "");
    assert!(resp.schema_transform_configs.contains_key(URN_KAFKA_READ));
    assert!(
        resp.schema_transform_configs
            .contains_key(URN_ICEBERG_WRITE)
    );
    assert!(resp.schema_transform_configs.contains_key(URN_ICEBERG_READ));
}
