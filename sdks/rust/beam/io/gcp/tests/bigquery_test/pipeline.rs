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

//! Remote expansion tests using the recording mock service.

use beam::prelude::*;
use external::URN_EXPANSION_SCHEMA_TRANSFORM;
use gcp::bigquery::{
    BigQueryRead, BigQueryWrite, CreateDisposition, URN_BIGQUERY_STORAGE_READ,
    URN_BIGQUERY_STORAGE_WRITE, WriteDisposition, WriteMethod,
};
use managed_io::URN_MANAGED;

use crate::common::{Role, assert_spliced, describe, fixture, start_mock, values};
use crate::fixtures::{BIGQUERY_WRITE_CONFIG, WriteExpect};

#[test]
fn test_bigquery_pipeline_integration() {
    let mock = start_mock(&[
        (URN_BIGQUERY_STORAGE_READ, Role::Source),
        (URN_BIGQUERY_STORAGE_WRITE, Role::Sink),
    ]);
    let p = Pipeline::new();
    let rows = p.apply(
        BigQueryRead::new("BigQueryRead")
            .with_table("bigquery-public-data:samples.wikipedia")
            .with_selected_fields(["title", "views"])
            .with_expansion_service(&mock.endpoint),
    );
    rows.apply(
        BigQueryWrite::new("BigQueryWrite", "my-project:dataset.aggregated_wikipedia")
            .with_create_disposition(CreateDisposition::CreateIfNeeded)
            .with_write_disposition(WriteDisposition::WriteTruncate)
            .with_method(WriteMethod::StorageWriteApi)
            .with_auto_sharding(true)
            .with_expansion_service(&mock.endpoint),
    );

    let seen = mock.seen();
    assert_eq!(seen.len(), 2, "{seen:?}");
    let (read, write) = (&seen[0], &seen[1]);

    // The read transform routes through Managed I/O with configuration as JSON.
    assert_eq!(read.unique_name, "BigQueryRead");
    assert_eq!(read.spec_urn, URN_EXPANSION_SCHEMA_TRANSFORM);
    assert_eq!(read.identifier, URN_MANAGED);
    assert_eq!(read.target, URN_BIGQUERY_STORAGE_READ);
    assert_eq!(
        read.managed_config.as_deref(),
        Some(
            r#"{"selected_fields":["title","views"],"table_spec":"bigquery-public-data:samples.wikipedia"}"#
        )
    );
    assert!(read.inputs.is_empty());

    // StorageWriteApi invokes the provider directly with a typed configuration row.
    assert_eq!(write.unique_name, "BigQueryWrite");
    assert_eq!(write.identifier, URN_BIGQUERY_STORAGE_WRITE);
    assert_eq!(write.managed_config, None);
    assert_eq!(
        describe(write.config.schema()),
        fixture(BIGQUERY_WRITE_CONFIG)
    );
    assert_eq!(
        values(&write.config),
        WriteExpect {
            auto_sharding: Some(true),
            create_disposition: Some("CREATE_IF_NEEDED"),
            table: "my-project:dataset.aggregated_wikipedia",
            write_disposition: Some("WRITE_TRUNCATE"),
            ..Default::default()
        }
        .values()
    );

    let read_out = format!("{}/output", read.namespace);
    assert_eq!(rows.id(), read_out);
    assert_eq!(rows.coder_id(), format!("{}/row_coder", read.namespace));
    assert_eq!(write.inputs["input"], read_out);
    assert_spliced(&p, read, &[], &[("output", &read_out)]);
    assert_spliced(&p, write, &[("input", &read_out)], &[]);
}
