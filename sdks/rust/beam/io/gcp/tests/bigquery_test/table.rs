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

//! Parsing of BigQuery table specs.

use gcp::bigquery::table::TableRef;

fn table(project: Option<&str>, dataset: &str, table: &str) -> TableRef {
    TableRef {
        project: project.map(str::to_string),
        dataset: dataset.to_string(),
        table: table.to_string(),
    }
}

#[test]
fn table_specs_parse_in_every_form() {
    assert_eq!(
        TableRef::parse("bigquery-public-data:samples.wikipedia").unwrap(),
        table(Some("bigquery-public-data"), "samples", "wikipedia")
    );
    assert_eq!(
        TableRef::parse("bigquery-public-data.samples.wikipedia").unwrap(),
        table(Some("bigquery-public-data"), "samples", "wikipedia")
    );
    assert_eq!(
        TableRef::parse("samples.wikipedia").unwrap(),
        table(None, "samples", "wikipedia")
    );
    assert_eq!(
        TableRef::parse("example.com:project:dataset.table").unwrap(),
        table(Some("example.com:project"), "dataset", "table")
    );
    assert_eq!(
        TableRef::parse("example.com:project.dataset.table").unwrap(),
        table(Some("example.com:project"), "dataset", "table")
    );
    for invalid in [
        "wikipedia",
        "samples.",
        ".wikipedia",
        ":samples.wikipedia",
        "",
    ] {
        assert!(TableRef::parse(invalid).is_err(), "{invalid}");
    }
}
