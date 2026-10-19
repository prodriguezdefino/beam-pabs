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

//! Tests of the Kafka benchmark in a Rust pipeline. They need no Kafka cluster.

use std::time::Duration;

use beam::options::{ParseError, PipelineOptions, try_parse_from};
use kafka_benchmark::{
    Args, AutoOffsetReset, Connector, Format, LATENCY_BOUNDS_MS, Shape, bucket, bucket_names,
    build_pipeline, formats, parse_consumer_pool_size, parse_property, read_options, write_options,
};
use kafka_io::native::format::{ReadFormat, WriteFormat};
use kafka_io::native::read::{ConsumerPool, PoolSize, ReadOptions};

/// Parses the required flags and then `flags`.
fn try_parse(flags: &[&str]) -> Result<(PipelineOptions, Args), ParseError> {
    let args = [
        "kafka_benchmark",
        "--bootstrap_servers=localhost:9092",
        "--topic=in",
        "--destination_topic=out",
    ]
    .into_iter()
    .chain(flags.iter().copied());
    try_parse_from::<Args, _, _>(args)
}

fn parse(flags: &[&str]) -> (PipelineOptions, Args) {
    try_parse(flags).expect("valid command line")
}

#[test]
fn bucket_includes_the_bound() {
    assert_eq!(bucket(0), 0);
    assert_eq!(bucket(50), 0);
    assert_eq!(bucket(51), 1);
    assert_eq!(bucket(1_000), 4);
    assert_eq!(bucket(300_000), 11);
    assert_eq!(bucket(300_001), LATENCY_BOUNDS_MS.len());
}

#[test]
fn there_is_one_bucket_for_each_bound_and_inf() {
    let names = bucket_names().collect::<Vec<_>>();
    assert_eq!(names.len(), LATENCY_BOUNDS_MS.len() + 1);
    assert_eq!(names[0], "latency_le_50");
    assert_eq!(names[bucket(300_000)], "latency_le_300000");
    assert_eq!(names[bucket(300_001)], "latency_le_inf");
}

#[test]
fn formats_are_json_and_raw() {
    assert!(matches!(
        formats(Format::Json),
        Ok((ReadFormat::Json(_), WriteFormat::Json))
    ));
    assert!(matches!(
        formats(Format::Raw),
        Ok((ReadFormat::Raw, WriteFormat::Raw))
    ));
}

#[test]
fn flags_have_the_defaults_of_the_launcher() {
    let (_, args) = parse(&[]);
    assert_eq!(args.reader, Connector::Rust);
    assert_eq!(args.writer, Connector::Rust);
    assert_eq!(args.shape, Shape::Copy);
    assert_eq!(args.format, Format::Json);
    assert_eq!(args.auto_offset_reset, AutoOffsetReset::Earliest);
    assert_eq!(args.group_id, None);
    assert_eq!(args.consumer_pool_size, None);
    assert!(args.with_gcp_adc);
    assert_eq!(args.max_read_seconds, None);
    assert_eq!(args.max_read_time_per_call_ms, None);

    let (_, local) = parse(&[
        "--with_gcp_adc=false",
        "--format=raw",
        "--auto_offset_reset=latest",
        "--group_id=bench",
        "--consumer_pool_size=4",
    ]);
    assert!(!local.with_gcp_adc);
    assert_eq!(local.format, Format::Raw);
    assert_eq!(local.auto_offset_reset, AutoOffsetReset::Latest);
    assert_eq!(local.group_id.as_deref(), Some("bench"));
    assert_eq!(local.consumer_pool_size, Some(4));
}

#[test]
fn pipeline_builds_for_json_and_raw() {
    for format in ["--format=JSON", "--format=RAW"] {
        let (options, args) = parse(&[format]);
        let pipeline = build_pipeline(&options, &args).expect("the pipeline builds");
        let names = pipeline
            .to_proto()
            .components
            .expect("components")
            .transforms
            .into_values()
            .map(|transform| transform.unique_name)
            .collect::<Vec<_>>();
        for step in ["Read", "Touch", "Write"] {
            assert!(names.iter().any(|name| name == step), "{step} in {names:?}");
        }
    }
}

#[test]
fn flags_reject_other_variants() {
    for flags in [
        &["--shape=load"][..],
        &["--reader=java-native"],
        &["--writer=java"],
        &["--format=AVRO"],
        &["--auto_offset_reset=none"],
    ] {
        assert!(try_parse(flags).is_err(), "{flags:?}");
    }
}

#[test]
fn consumer_properties_are_key_value_pairs() {
    let (_, args) = parse(&["--consumer_properties=fetch.min.bytes=1,client.id=a=b"]);
    assert_eq!(
        args.consumer_properties,
        [
            ("fetch.min.bytes".to_string(), "1".to_string()),
            ("client.id".to_string(), "a=b".to_string()),
        ]
    );
    let (_, none) = parse(&[]);
    assert!(none.consumer_properties.is_empty());
    assert_eq!(
        parse_property("linger.ms="),
        Ok(("linger.ms".to_string(), String::new()))
    );
}

#[test]
fn a_bad_consumer_property_fails_at_parse() {
    for pair in ["no_equals", "=1", " fetch.min.bytes=1", "fetch min=1"] {
        assert!(parse_property(pair).is_err(), "{pair}");
        let flag = format!("--consumer_properties={pair}");
        assert!(try_parse(&[&flag]).is_err(), "{flag}");
    }
}

#[test]
fn pipeline_builds_with_consumer_properties() {
    let (options, args) = parse(&["--consumer_properties=fetch.min.bytes=1,max.poll.records=500"]);
    build_pipeline(&options, &args).expect("the pipeline builds");
}

#[test]
fn consumer_pool_size_sets_read_options_and_rejects_zero() {
    let (_, default_args) = parse(&[]);
    assert_eq!(
        read_options(&default_args, ReadFormat::Raw)
            .unwrap()
            .consumer_pool,
        ConsumerPool::OnePerPartition
    );

    let (_, single_args) = parse(&["--consumer_pool_size=1"]);
    assert_eq!(
        read_options(&single_args, ReadFormat::Raw)
            .unwrap()
            .consumer_pool,
        ConsumerPool::Size(PoolSize::try_from(1).unwrap())
    );

    for bad in ["0", "17", "-1", "abc"] {
        assert!(parse_consumer_pool_size(bad).is_err(), "{bad}");
        let flag = format!("--consumer_pool_size={bad}");
        assert!(try_parse(&[&flag]).is_err(), "{flag}");
    }
}

#[test]
fn max_read_time_per_call_sets_read_options_and_rejects_zero() {
    let (_, default_args) = parse(&[]);
    assert_eq!(
        read_options(&default_args, ReadFormat::Raw)
            .unwrap()
            .max_read_time_per_call,
        ReadOptions::new("b", "t", ReadFormat::Raw).max_read_time_per_call
    );
    let (_, args) = parse(&["--max_read_time_per_call_ms=1500"]);
    assert_eq!(
        read_options(&args, ReadFormat::Raw)
            .unwrap()
            .max_read_time_per_call,
        Duration::from_millis(1_500)
    );
    assert!(try_parse(&["--max_read_time_per_call_ms=0"]).is_err());
}

#[test]
fn producer_properties_set_write_options() {
    let (_, default_args) = parse(&[]);
    assert!(
        write_options(&default_args, WriteFormat::Raw)
            .connection
            .properties
            .is_empty()
    );

    let (_, args) = parse(&["--producer_properties=linger.ms=5,acks=all"]);
    let write = write_options(&args, WriteFormat::Raw);
    assert_eq!(write.connection.properties["linger.ms"], "5");
    assert_eq!(write.connection.properties["acks"], "all");
}
