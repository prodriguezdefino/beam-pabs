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

//! Tests of the BigQuery benchmark in a Rust pipeline. They need no BigQuery.

use beam::io::gcp::bigquery::storage::read::{ArrowCompression, ReadOptions};
use beam::io::gcp::bigquery::storage::write::{self, WriteOptions};
use beam::options::{ParseError, PipelineOptions, try_parse_from};
use beam_benchmark_bigquery::{Args, Semantics, Shape, check_shape, read_options, write_options};
use beam_benchmark_io::Connector;

const TABLE: &str = "test-project:test_dataset.wikipedia";
const DESTINATION: &str = "test-project.test_dataset.copy";

fn try_parse(flags: &[&str]) -> Result<(PipelineOptions, Args), ParseError> {
    let args = ["apache-beam-benchmark-bigquery"]
        .into_iter()
        .chain(flags.iter().copied());
    try_parse_from::<Args, _, _>(args)
}

fn parse(flags: &[&str]) -> Args {
    try_parse(flags).expect("valid command line").1
}

fn copy_flags() -> Vec<String> {
    vec![
        "--shape=copy".into(),
        "--writer=rust".into(),
        format!("--table={TABLE}"),
        format!("--destination={DESTINATION}"),
    ]
}

fn parse_owned(flags: &[String]) -> Args {
    parse(&flags.iter().map(String::as_str).collect::<Vec<_>>())
}

#[test]
fn count_has_the_defaults_of_the_launcher() {
    let args = parse(&[&format!("--table={TABLE}")]);
    assert_eq!(args.reader, Connector::Rust);
    assert_eq!(args.writer, None);
    assert_eq!(args.shape, Shape::Count);
    assert_eq!(args.semantics, Semantics::ExactlyOnce);
    assert!(args.selected_fields.is_empty());
    check_shape(&args).expect("count fits");
}

#[test]
fn table_or_query_is_required_and_exclusive() {
    assert!(try_parse(&[]).is_err());
    assert!(try_parse(&["--table=a.b", "--query=SELECT 1"]).is_err());
    let args = parse(&["--query=SELECT 1"]);
    assert_eq!(
        read_options(&args).expect("query read"),
        ReadOptions::from_query("SELECT 1")
    );
}

#[test]
fn read_options_take_the_table_flags() {
    let args = parse(&[
        &format!("--table={TABLE}"),
        "--selected_fields=title,id",
        "--row_restriction=wp_namespace = 0",
        "--compression=lz4_frame",
    ]);
    let expected = ReadOptions::new(TABLE)
        .with_selected_fields(["title", "id"])
        .with_row_restriction("wp_namespace = 0")
        .with_compression(ArrowCompression::Lz4Frame);
    assert_eq!(read_options(&args).expect("table read"), expected);
}

#[test]
fn unknown_compression_fails() {
    let args = parse(&[&format!("--table={TABLE}"), "--compression=gzip"]);
    assert!(read_options(&args).is_err());
}

#[test]
fn copy_writes_to_the_destination() {
    let args = parse_owned(&copy_flags());
    check_shape(&args).expect("copy fits");
    assert_eq!(
        write_options(&args).expect("write options"),
        WriteOptions::new(DESTINATION).with_semantics(write::Semantics::ExactlyOnce)
    );
}

#[test]
fn copy_takes_at_least_once() {
    let mut flags = copy_flags();
    flags.push("--semantics=at_least_once".into());
    let args = parse_owned(&flags);
    assert_eq!(
        write_options(&args).expect("write options"),
        WriteOptions::new(DESTINATION).with_semantics(write::Semantics::AtLeastOnce)
    );
}

#[test]
fn copy_needs_a_writer_and_a_destination() {
    let args = parse(&["--shape=copy", &format!("--table={TABLE}")]);
    assert!(check_shape(&args).is_err());
    assert!(write_options(&args).is_err());
}

#[test]
fn copy_reads_whole_rows() {
    let mut flags = copy_flags();
    flags.push("--selected_fields=title".into());
    assert!(check_shape(&parse_owned(&flags)).is_err());
}

#[test]
fn count_takes_no_writer() {
    let args = parse(&[&format!("--table={TABLE}"), "--writer=rust"]);
    assert!(check_shape(&args).is_err());
    let args = parse(&[
        &format!("--table={TABLE}"),
        &format!("--destination={DESTINATION}"),
    ]);
    assert!(check_shape(&args).is_err());
}
