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

//! Tests for `BigQueryRead` configuration rows and Managed I/O payloads.

use external::ExpansionError;
use gcp::bigquery::{BigQueryRead, URN_BIGQUERY_STORAGE_READ};

use crate::common::{describe, fixture, s, str_array, values};
use crate::fixtures::{BIGQUERY_READ_CONFIG, managed_target, sorted};

#[test]
fn test_bigquery_read_table_config_row() {
    let row = BigQueryRead::new("BigQueryRead")
        .with_table("bigquery-public-data:samples.wikipedia")
        .with_selected_fields(["title", "id", "language"])
        .with_row_restriction("wp_namespace = 0")
        .with_kms_key("projects/test/locations/global/keyRings/kr/cryptoKeys/k")
        .build_config_row()
        .expect("build config row");

    assert_eq!(
        sorted(describe(row.schema())),
        fixture(BIGQUERY_READ_CONFIG)
    );
    assert_eq!(
        sorted(values(&row)),
        vec![
            (
                "kms_key".to_string(),
                s("projects/test/locations/global/keyRings/kr/cryptoKeys/k")
            ),
            ("query".to_string(), None),
            ("row_restriction".to_string(), s("wp_namespace = 0")),
            (
                "selected_fields".to_string(),
                str_array(&["title", "id", "language"])
            ),
            (
                "table_spec".to_string(),
                s("bigquery-public-data:samples.wikipedia")
            ),
        ]
    );
}

#[test]
fn test_bigquery_read_query_config() {
    let query_sql = "SELECT title, COUNT(*) FROM `dataset.table` GROUP BY title";
    let source = BigQueryRead::new("BigQueryRead")
        .with_query(query_sql)
        .build()
        .unwrap();
    let (urn, config) = managed_target(&source.transform().payload);
    assert_eq!(urn, URN_BIGQUERY_STORAGE_READ);
    assert_eq!(
        config,
        r#"{"query":"SELECT title, COUNT(*) FROM `dataset.table` GROUP BY title"}"#
    );
}

#[test]
fn test_bigquery_read_requires_table_or_query() {
    let err = BigQueryRead::new("BigQueryRead")
        .build()
        .expect_err("no table or query");
    assert_eq!(
        err,
        ExpansionError::InvalidResponse("BigQueryRead requires either table or query".into())
    );
}
