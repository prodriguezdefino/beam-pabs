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

//! Partition expression parsing tests for [`IcebergWriteConfig`].

use beam::io::iceberg::{Catalog, PartitionField, PartitionTransform, WriteOptions};
use beam_io_iceberg_expansion::IcebergWriteConfig;

use crate::common::{CATALOG_URI, base_config};

#[test]
fn partition_expressions_parse_all_supported_forms() {
    let specs = vec![
        "device_id",
        "  identity( region ) ",
        "IDENTITY(tenant)",
        "hour(ts_hour)",
        "DAY(ts_day)",
        "Month(ts_month)",
        "year(ts_year)",
        "bucket(id, 16)",
        "BUCKET[ 32 ]( user_id )",
        "truncate(code, 4)",
        "Truncate[8](name)",
    ]
    .into_iter()
    .map(str::to_string)
    .collect();

    let options = IcebergWriteConfig {
        partition_fields: Some(specs),
        ..base_config("db.events")
    }
    .write_options(false)
    .unwrap();

    let expected = WriteOptions::new(Catalog::rest(CATALOG_URI), "db.events")
        .with_partition_fields([
            PartitionField::new("device_id", PartitionTransform::Identity),
            PartitionField::new("region", PartitionTransform::Identity),
            PartitionField::new("tenant", PartitionTransform::Identity),
            PartitionField::new("ts_hour", PartitionTransform::Hour),
            PartitionField::new("ts_day", PartitionTransform::Day),
            PartitionField::new("ts_month", PartitionTransform::Month),
            PartitionField::new("ts_year", PartitionTransform::Year),
            PartitionField::new("id", PartitionTransform::Bucket(16)),
            PartitionField::new("user_id", PartitionTransform::Bucket(32)),
            PartitionField::new("code", PartitionTransform::Truncate(4)),
            PartitionField::new("name", PartitionTransform::Truncate(8)),
        ]);

    assert_eq!(options, expected);
}

#[test]
fn invalid_partition_expressions_fail_with_clear_errors() {
    let cases = [
        ("", "empty"),
        ("   ", "empty"),
        ("void(id)", "void"),
        ("VOID(id)", "void"),
        ("void[1](id)", "void"),
        ("century(ts)", "century"),
        ("bucket(id)", "bucket"),
        ("truncate(name)", "truncate"),
        ("bucket(id, 0)", "positive"),
        ("bucket[0](id)", "positive"),
        ("truncate(name, 0)", "positive"),
        ("truncate[0](name)", "positive"),
        ("bucket(id, -4)", "positive"),
        ("bucket(id, sixteen)", "positive"),
        ("bucket[16]", "Invalid"),
        ("bucket[16](id, 16)", "column"),
        ("hour[4](ts)", "hour"),
        ("hour()", "column"),
        ("hour(a, b)", "column"),
        ("id asc", "Invalid"),
        ("hour(ts", "Malformed"),
        ("hour(a(b))", "Malformed"),
    ];

    for (spec, expected) in cases {
        let error = IcebergWriteConfig {
            partition_fields: Some(vec![spec.to_string()]),
            ..base_config("db.events")
        }
        .write_options(false)
        .unwrap_err();
        assert!(error.contains(expected), "spec '{spec}': {error}");
    }
}
