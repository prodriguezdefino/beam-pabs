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

//! Declared outputs: error handling and provider-specific output tags.

use beam::pipeline::ExpansionMode;
use beam::prelude::*;
use beam_io_managed::{self as managed, ManagedRead, ManagedWrite, urns};
use serde_json::json;

use crate::common::config_json;

#[test]
fn test_error_handling_is_configured_and_declared() {
    let read = ManagedRead::new(managed::KAFKA)
        .with_config_entry("topic", "events")
        .with_error_handling("errors");

    assert_eq!(read.error_output(), Some("errors"));
    assert_eq!(read.declared_outputs(), ["errors"]);
    let config = config_json(&read.build_config_row().unwrap());
    assert_eq!(config["error_handling"], json!({"output": "errors"}));
    assert_eq!(config["topic"], json!("events"));
}

#[test]
fn test_error_handling_set_through_config_is_declared_too() {
    let write = ManagedWrite::new(managed::BIGQUERY)
        .with_config(json!({"table": "p.d.t", "error_handling": {"output": "bad_rows"}}));

    assert_eq!(write.error_output(), Some("bad_rows"));
    assert_eq!(write.declared_outputs(), ["bad_rows"]);
}

#[test]
fn test_known_outputs_match_the_java_providers() {
    assert_eq!(
        managed::known_outputs(urns::ICEBERG_WRITE),
        [managed::SNAPSHOTS]
    );
    // Configuration-dependent outputs are never declared implicitly.
    [
        urns::ICEBERG_READ,
        urns::KAFKA_READ,
        urns::KAFKA_WRITE,
        urns::BIGQUERY_WRITE,
    ]
    .into_iter()
    .for_each(|urn| assert!(managed::known_outputs(urn).is_empty(), "{urn}"));

    let iceberg = ManagedWrite::new(managed::ICEBERG).with_error_handling("errors");
    assert_eq!(iceberg.declared_outputs(), ["snapshots", "errors"]);
    assert!(
        ManagedRead::new(managed::KAFKA)
            .declared_outputs()
            .is_empty()
    );
}

/// Inside a worker there is no expansion service, so every declared output must still
/// exist for downstream transforms to attach to.
#[test]
fn test_placeholder_expansion_exposes_declared_outputs() {
    let p = Pipeline::new().with_expansion_mode(ExpansionMode::Placeholder);

    let read = p
        .try_apply(
            "Managed Read(KAFKA)",
            ManagedRead::new(managed::KAFKA)
                .with_config_entry("topic", "events")
                .with_error_handling("errors")
                .with_all_outputs(),
        )
        .unwrap();
    assert_eq!(read.tags().collect::<Vec<_>>(), ["errors", "output"]);

    let written = read
        .try_get(managed::OUTPUT)
        .unwrap()
        .try_apply(
            "Managed Write(ICEBERG)",
            ManagedWrite::new(managed::ICEBERG)
                .with_config_entry("table", "db.t")
                .with_all_outputs(),
        )
        .unwrap();
    assert_eq!(written.tags().collect::<Vec<_>>(), ["output", "snapshots"]);
    assert!(written.try_get(managed::SNAPSHOTS).is_ok());
}

#[test]
fn test_yaml_config_declares_error_output() {
    let read = ManagedRead::new(managed::KAFKA)
        .with_yaml_config("topic: events\nerror_handling:\n  output: bad_rows\n");
    assert_eq!(read.error_output(), Some("bad_rows"));
    assert_eq!(read.declared_outputs(), ["bad_rows"]);
    let row = read.build_config_row().unwrap();
    assert_eq!(
        row.get_string("config").unwrap(),
        Some("topic: events\nerror_handling:\n  output: bad_rows\n")
    );
    assert_eq!(row.get_string("config_url").unwrap(), None);

    let write = ManagedWrite::new(managed::ICEBERG)
        .with_yaml_config("table: db.t\nerror_handling: {output: failed}\n");
    assert_eq!(write.error_output(), Some("failed"));
    assert_eq!(write.declared_outputs(), ["snapshots", "failed"]);

    let plain = ManagedRead::new(managed::KAFKA).with_yaml_config("topic: events\n");
    assert_eq!(plain.error_output(), None);
    assert!(plain.declared_outputs().is_empty());

    let p = Pipeline::new().with_expansion_mode(ExpansionMode::Placeholder);
    let expanded = p
        .try_apply("Managed Read(KAFKA)", read.with_all_outputs())
        .unwrap();
    assert_eq!(expanded.tags().collect::<Vec<_>>(), ["bad_rows", "output"]);
}

#[test]
fn test_config_url_allows_error_output_declaration() {
    let url_first = ManagedRead::new(managed::KAFKA)
        .with_config_url("gs://bucket/kafka.yaml")
        .with_error_handling("errors");
    let error_first = ManagedRead::new(managed::KAFKA)
        .with_error_handling("errors")
        .with_config_url("gs://bucket/kafka.yaml");

    for read in [&url_first, &error_first] {
        assert_eq!(read.error_output(), Some("errors"));
        assert_eq!(read.declared_outputs(), ["errors"]);
        let row = read.build_config_row().unwrap();
        assert_eq!(row.get_string("config").unwrap(), None);
        assert_eq!(
            row.get_string("config_url").unwrap(),
            Some("gs://bucket/kafka.yaml")
        );
    }

    let p = Pipeline::new().with_expansion_mode(ExpansionMode::Placeholder);
    let expanded = p
        .try_apply("Managed Read(KAFKA)", url_first.with_all_outputs())
        .unwrap();
    assert_eq!(expanded.tags().collect::<Vec<_>>(), ["errors", "output"]);

    // Adding an inline config entry alongside `config_url` is still rejected.
    let conflicting = ManagedRead::new(managed::KAFKA)
        .with_config_url("gs://bucket/kafka.yaml")
        .with_error_handling("errors")
        .with_config_entry("topic", "events")
        .build_config_row();
    assert!(conflicting.is_err());
}
