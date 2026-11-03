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

//! Table names, partition fields and input types that `WriteOptions::validate` accepts and
//! rejects.

use beam::schema::{Field, FieldType, Schema};
use iceberg_io::{PartitionField, Transform};

use crate::common::{device_metric, options};

#[test]
fn the_target_job_options_are_valid() {
    let options = options("iceberg_poc_db.device_metrics")
        .with_partition_fields([PartitionField::new("timestamp_ms", Transform::Hour)])
        .with_table_property("write.parquet.compression-codec", "zstd");
    options
        .validate(&device_metric())
        .expect("the options are valid");
    let ident = options.table_ident().expect("a valid table name");
    assert_eq!(
        ident.namespace().as_ref(),
        &vec!["iceberg_poc_db".to_string()]
    );
    assert_eq!(ident.name(), "device_metrics");
}

#[test]
fn a_nested_namespace_is_valid() {
    let ident = options("a.b.table")
        .table_ident()
        .expect("a valid table name");
    assert_eq!(
        ident.namespace().as_ref(),
        &vec!["a".to_string(), "b".to_string()]
    );
    assert_eq!(ident.name(), "table");
}

#[test]
fn a_table_without_a_namespace_or_with_an_empty_part_is_invalid() {
    for table in ["table", "", "ns.", ".table", "a..table"] {
        let error = options(table)
            .validate(&device_metric())
            .expect_err("the table name is invalid");
        assert!(
            error.to_string().contains("namespace.name"),
            "{table}: {error}"
        );
    }
}

#[test]
fn a_partition_source_must_be_a_field() {
    let error = options("ns.t")
        .with_partition_fields([PartitionField::new("missing", Transform::Identity)])
        .validate(&device_metric())
        .expect_err("the source is not a field");
    assert!(
        error.to_string().contains("'missing' is not a field"),
        "{error}"
    );
}

#[test]
fn a_bucket_count_or_truncate_width_of_zero_is_invalid() {
    for transform in [Transform::Bucket(0), Transform::Truncate(0)] {
        let error = options("ns.t")
            .with_partition_fields([PartitionField::new("device_id", transform)])
            .validate(&device_metric())
            .expect_err("0 is invalid");
        assert!(error.to_string().contains("greater than 0"), "{error}");
    }
}

#[test]
fn a_transform_must_accept_the_source_type() {
    let error = options("ns.t")
        .with_partition_fields([PartitionField::new("device_id", Transform::Hour)])
        .validate(&device_metric())
        .expect_err("hour does not take a string");
    assert!(error.to_string().contains("device_id_hour"), "{error}");
}

#[test]
fn unsupported_input_types_are_invalid() {
    for field_type in [FieldType::byte(), FieldType::int16(), decimal()] {
        let schema = Schema::new(vec![Field::new("value", field_type.clone())]);
        let error = options("ns.t")
            .validate(&schema)
            .expect_err("the type is not supported");
        assert!(
            error.to_string().contains("'value'"),
            "{field_type}: {error}"
        );
    }
}

fn decimal() -> FieldType {
    FieldType::logical(beam::schema::URN_DECIMAL, Vec::new(), FieldType::bytes())
}
