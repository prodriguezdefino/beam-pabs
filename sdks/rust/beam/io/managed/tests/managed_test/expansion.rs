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

//! Remote expansion against the recording mock Managed expansion service.

use beam::pipeline::ExpansionMode;
use beam::prelude::*;
use managed_io::{self as managed, ManagedRead, ManagedWrite, urns};

use crate::common::start_mock_service;

#[test]
fn test_managed_pipeline_integration() {
    let (endpoint, seen, shutdown_tx) = start_mock_service();
    let p = Pipeline::new();

    let rows = p.apply(
        ManagedRead::new("Managed Read(ICEBERG)", managed::ICEBERG)
            .with_config_entry("table", "db.in")
            .with_expansion_service(&endpoint),
    );
    rows.apply(
        ManagedWrite::new("WriteIceberg", urns::ICEBERG_WRITE)
            .with_config_entry("table", "db.out")
            .with_expansion_service(&endpoint),
    );

    let _ = shutdown_tx.send(());

    assert_eq!(
        *seen.lock().unwrap(),
        [
            (
                urns::ICEBERG_READ.to_string(),
                Some(r#"{"table":"db.in"}"#.to_string())
            ),
            (
                urns::ICEBERG_WRITE.to_string(),
                Some(r#"{"table":"db.out"}"#.to_string())
            ),
        ]
    );
    let lock = p.lock();
    let read = &lock.components.transforms["Managed Read(ICEBERG)"];
    let write = &lock.components.transforms["WriteIceberg"];
    assert_eq!(read.environment_id, "env_mock");
    assert_eq!(write.environment_id, "env_mock");
    // The read's returned output (and its coder) is exactly what the write consumes.
    assert!(read.inputs.is_empty());
    assert_eq!(
        read.outputs.get("output").map(String::as_str),
        Some(rows.id())
    );
    assert!(
        rows.coder_id().ends_with("/row_coder"),
        "{}",
        rows.coder_id()
    );
    assert_eq!(write.inputs.len(), 1);
    assert_eq!(write.inputs["input"], rows.id());
}

#[test]
fn test_remote_expansion_returns_snapshots_and_error_outputs() {
    let (endpoint, seen, shutdown_tx) = start_mock_service();
    let p = Pipeline::new();

    let read = p.apply(
        ManagedRead::new("Managed Read(KAFKA)", managed::KAFKA)
            .with_config_entry("topic", "events")
            .with_error_handling("errors")
            .with_expansion_service(&endpoint)
            .with_all_outputs(),
    );
    let records = read.expect(managed::OUTPUT).unwrap();
    let errors = read.expect("errors").unwrap();
    assert_ne!(records.id(), errors.id());
    assert!(errors.coder_id().ends_with("/row_coder"));

    let written = records.apply(
        ManagedWrite::new("Managed Write(ICEBERG)", managed::ICEBERG)
            .with_config_entry("table", "db.t")
            .with_expansion_service(&endpoint)
            .with_outputs(),
    );
    assert_eq!(written.tags().collect::<Vec<_>>(), ["snapshots"]);

    let kafka_errors = records.apply(
        ManagedWrite::new("Managed Write(KAFKA)", managed::KAFKA)
            .with_config_entry("topic", "out")
            .with_error_handling("write_errors")
            .with_expansion_service(&endpoint)
            .with_outputs(),
    );
    assert!(kafka_errors.expect("write_errors").is_ok());
    let _ = shutdown_tx.send(());

    let requests = seen.lock().unwrap();
    assert_eq!(requests.len(), 3);
    assert!(
        requests[0]
            .1
            .as_deref()
            .is_some_and(|c| c.contains(r#""error_handling":{"output":"errors"}"#)),
        "{:?}",
        requests[0]
    );
}

#[test]
fn test_plain_write_still_returns_pdone_when_outputs_exist() {
    let (endpoint, seen, shutdown_tx) = start_mock_service();
    let p = Pipeline::new().with_expansion_mode(ExpansionMode::Placeholder);
    let rows = p.apply(
        ManagedRead::new("Managed Read(ICEBERG)", managed::ICEBERG)
            .with_config_entry("table", "db.in"),
    );
    p.lock().expansion_mode = ExpansionMode::Remote;

    // Compile-time check: the plain write is still a `PDone` transform even though
    // Iceberg writes produce `snapshots`.
    let _done: PDone = rows.apply(
        ManagedWrite::new("Managed Write(ICEBERG)", managed::ICEBERG)
            .with_config_entry("table", "db.out")
            .with_expansion_service(&endpoint),
    );
    let _ = shutdown_tx.send(());

    // Only the (remote) write reached the service; the placeholder read did not.
    assert_eq!(
        *seen.lock().unwrap(),
        [(
            urns::ICEBERG_WRITE.to_string(),
            Some(r#"{"table":"db.out"}"#.to_string())
        )]
    );
    let lock = p.lock();
    let write = &lock.components.transforms["Managed Write(ICEBERG)"];
    assert_eq!(write.environment_id, "env_mock");
    assert_eq!(write.inputs.len(), 1);
    assert_eq!(write.inputs["input"], rows.id());
}
