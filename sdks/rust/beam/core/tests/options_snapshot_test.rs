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

//! Tests for the typed options snapshot. Workers receive the snapshot. Display data and the
//! portable pipeline options derive from it.

use beam::options::{
    OptionsError, OptionsSnapshot, PipelineOptionGroup, PipelineOptions, SDK_OPTIONS_OPTION,
    WorkerOptions, try_parse_from,
};
use clap::Args;
use prost_types::value::Kind;
use serde::{Deserialize, Serialize};

#[derive(Args, Serialize, Deserialize, Clone, Debug, PartialEq)]
struct AppArgs {
    /// A string option whose value looks like a number.
    #[arg(long, default_value = "out")]
    output: String,

    #[arg(long, default_value_t = 32)]
    batch_size: usize,

    #[arg(long, default_value_t = false)]
    verbose: bool,
}

impl PipelineOptionGroup for AppArgs {}

/// Redefines `output` to test conflict detection against [`AppArgs`].
#[derive(Args, Serialize, Deserialize, Clone, Debug, PartialEq, Default)]
struct OtherArgs {
    #[arg(long = "other_output")]
    output: Option<String>,
}

impl PipelineOptionGroup for OtherArgs {}

fn parse(args: &[&str]) -> PipelineOptions {
    try_parse_from::<AppArgs, _, _>(args.iter().copied())
        .expect("valid command line")
        .0
}

#[test]
fn snapshot_round_trips_through_its_encoding() {
    let options = parse(&["app", "--output=1", "--batch_size=64", "--num_workers=3"]);
    let snapshot = options.snapshot().expect("snapshot");

    let decoded = OptionsSnapshot::decode(&snapshot.encode()).expect("decodes");
    assert_eq!(decoded, snapshot);

    let restored = PipelineOptions::from_snapshot(decoded, Default::default()).expect("restores");
    let args: AppArgs = restored.view_as().expect("restored");
    assert_eq!(args.batch_size, 64);
    let worker: WorkerOptions = restored.view_as().expect("restored");
    assert_eq!(worker.num_workers, Some(3));
}

#[test]
fn snapshot_records_registered_groups_that_were_never_read() {
    let options = parse(&["app", "--num_workers=3"]);
    assert!(!options.contains::<WorkerOptions>());

    let snapshot = options.snapshot().expect("snapshot");

    assert!(options.contains::<WorkerOptions>());
    assert!(
        snapshot
            .flat_options()
            .expect("no conflicts")
            .get("num_workers")
            .is_some_and(|value| value == 3)
    );
}

#[test]
fn harness_flags_are_not_part_of_the_snapshot() {
    let options = parse(&[
        "app",
        "--control_endpoint=localhost:1",
        "--semi_persist_dir=/x",
    ]);
    let flat = options
        .snapshot()
        .expect("snapshot")
        .flat_options()
        .expect("no conflicts");

    assert!(!flat.contains_key("control_endpoint"));
    assert!(!flat.contains_key("semi_persist_dir"));
    assert!(!flat.contains_key("worker"));
}

#[test]
fn display_data_is_typed_by_value_not_by_spelling() {
    let items = parse(&["app", "--output=1", "--batch_size=8", "--verbose"])
        .display_data()
        .expect("display data");
    let item = |key: &str| {
        items
            .iter()
            .find(|item| item.key == key)
            .unwrap_or_else(|| panic!("no display data for {key}"))
    };

    assert_eq!(item("output").item_type, "STRING");
    assert_eq!(item("output").value, "1");
    assert_eq!(item("batch_size").item_type, "INTEGER");
    assert_eq!(item("verbose").item_type, "BOOLEAN");
    assert_eq!(item("runner").value, "prism");
}

#[test]
fn unset_options_have_no_display_data() {
    let items = parse(&["app"]).display_data().expect("display data");

    assert!(!items.iter().any(|item| item.key == "job_name"));
    assert!(!items.iter().any(|item| item.key == "experiments"));
}

#[test]
fn groups_that_disagree_on_an_option_are_a_conflict() {
    let options = parse(&["app", "--output=a", "--other_output=b"]);
    let _: OtherArgs = options.view_as().expect("parses");

    let err = options
        .snapshot()
        .expect("snapshot")
        .flat_options()
        .expect_err("ambiguous option");
    assert!(matches!(err, OptionsError::Conflict { key, .. } if key == "output"));
}

#[test]
fn groups_that_agree_on_an_option_are_not_a_conflict() {
    let options = parse(&["app", "--output=same", "--other_output=same"]);
    let _: OtherArgs = options.view_as().expect("parses");

    let flat = options
        .snapshot()
        .expect("snapshot")
        .flat_options()
        .expect("no conflict");
    assert_eq!(
        flat.get("output").and_then(|value| value.as_str()),
        Some("same")
    );
}

#[test]
fn proto_struct_carries_flat_urns_and_the_encoded_snapshot() {
    let options = parse(&["app", "--batch_size=64", "--num_workers=3"]);
    let fields = options.to_proto_struct().expect("proto struct").fields;

    assert!(matches!(
        fields.get("beam:option:num_workers:v1").and_then(|v| v.kind.as_ref()),
        Some(Kind::NumberValue(n)) if *n == 3.0
    ));
    // The snapshot travels as a string, so integers stay integers on the worker.
    let Some(Kind::StringValue(encoded)) = fields
        .get(&format!("beam:option:{SDK_OPTIONS_OPTION}:v1"))
        .and_then(|v| v.kind.as_ref())
    else {
        panic!("the snapshot is a string field");
    };
    let restored = PipelineOptions::from_snapshot(
        OptionsSnapshot::decode(encoded).expect("decodes"),
        Default::default(),
    )
    .expect("restores");
    assert_eq!(
        restored.view_as::<AppArgs>().expect("restored").batch_size,
        64
    );
}
