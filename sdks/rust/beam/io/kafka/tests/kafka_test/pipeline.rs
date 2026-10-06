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

//! Remote expansion against a strict, recording mock expansion service.

use beam::prelude::*;
use external::{ExpansionError, URN_EXPANSION_SCHEMA_TRANSFORM};
use kafka_io::{KafkaFormat, KafkaRead, KafkaWrite, OffsetReset, URN_KAFKA_READ, URN_KAFKA_WRITE};
use managed_io::URN_MANAGED;

use crate::common::{Role, assert_spliced, start_mock};

#[test]
fn test_kafka_pipeline_integration() {
    let mock = start_mock(&[
        (URN_KAFKA_READ, Role::Source),
        (URN_KAFKA_WRITE, Role::Sink),
    ]);
    let p = Pipeline::new();

    let records = p.apply(
        KafkaRead::new("KafkaRead", "b:9092", "in")
            .with_format(KafkaFormat::String)
            .with_auto_offset_reset(OffsetReset::Earliest)
            .with_max_read_time_seconds(5)
            .with_expansion_service(&mock.endpoint),
    );
    records.apply(
        KafkaWrite::new("KafkaWrite", "b:9092", "out")
            .with_producer_config("acks", "all")
            .with_expansion_service(&mock.endpoint),
    );

    let seen = mock.seen();
    assert_eq!(seen.len(), 2, "{seen:?}");
    let (read, write) = (&seen[0], &seen[1]);

    // Both go through Managed; nulls are dropped and keys are sorted (serde_json has no
    // `preserve_order` in this workspace), so the config strings are exact.
    assert_eq!(read.unique_name, "KafkaRead");
    assert_eq!(read.spec_urn, URN_EXPANSION_SCHEMA_TRANSFORM);
    assert_eq!(read.identifier, URN_MANAGED);
    assert_eq!(read.target, URN_KAFKA_READ);
    assert_eq!(
        read.managed_config.as_deref(),
        Some(
            r#"{"auto_offset_reset_config":"earliest","bootstrap_servers":"b:9092","format":"STRING","max_read_time_seconds":5,"topic":"in"}"#
        )
    );
    assert!(read.inputs.is_empty());

    assert_eq!(write.unique_name, "KafkaWrite");
    assert_eq!(write.identifier, URN_MANAGED);
    assert_eq!(write.target, URN_KAFKA_WRITE);
    assert_eq!(
        write.managed_config.as_deref(),
        Some(
            r#"{"bootstrap_servers":"b:9092","format":"RAW","producer_config_updates":{"acks":"all"},"topic":"out"}"#
        )
    );

    let read_out = format!("{}/output", read.namespace);
    assert_eq!(records.id(), read_out);
    assert_eq!(records.coder_id(), format!("{}/row_coder", read.namespace));
    assert_eq!(write.inputs["input"], read_out);
    assert_spliced(&p, read, &[], &[("output", &read_out)]);
    assert_spliced(&p, write, &[("input", &read_out)], &[]);
}

/// The mock is strict, so a green pipeline test means the URN really matched.
#[test]
fn test_kafka_mock_rejects_unrouted_urn() {
    let mock = start_mock(&[(URN_KAFKA_READ, Role::Source)]);
    let p = Pipeline::new();
    let records =
        p.apply(KafkaRead::new("KafkaRead", "b:9092", "in").with_expansion_service(&mock.endpoint));
    let err = KafkaWrite::new("KafkaWrite", "b:9092", "out")
        .with_expansion_service(&mock.endpoint)
        .try_expand(&records)
        .expect_err("write URN is not routed");
    assert_eq!(
        err,
        ExpansionError::Rpc(format!("unexpected URN {URN_KAFKA_WRITE}"))
    );
}
