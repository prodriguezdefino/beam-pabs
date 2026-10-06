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

//! Builder defaults and getter tests for `BigQueryRead` and `BigQueryWrite`.

use gcp::bigquery::{
    BigQueryRead, BigQueryWrite, CreateDisposition, DEFAULT_EXPANSION_SERVICE,
    URN_BIGQUERY_STORAGE_READ, URN_BIGQUERY_WRITE, WriteDisposition, WriteMethod,
};

use crate::common::{describe, fixture, s, values};
use crate::fixtures::{BIGQUERY_WRITE_CONFIG, WriteExpect, managed_target, sorted};

/// `(table, query, selected_fields, row_restriction, expansion_service)`.
type ReadGetters<'a> = (
    Option<&'a str>,
    Option<&'a str>,
    Option<&'a [String]>,
    Option<&'a str>,
    &'a str,
);

/// `(table, method, create_disposition, write_disposition, error_output, expansion_service)`.
type WriteGetters<'a> = (
    &'a str,
    WriteMethod,
    Option<CreateDisposition>,
    Option<WriteDisposition>,
    Option<&'a str>,
    &'a str,
);

/// With only required fields, both go through Managed to the default expansion service.
#[test]
fn test_bigquery_default_payloads() {
    // Read: unset fields are null; Managed omits them, so Java applies its defaults.
    let read = BigQueryRead::new("BigQueryRead").with_table("p:d.t");
    let row = read.build_config_row().unwrap();
    assert_eq!(
        sorted(values(&row)),
        vec![
            ("kms_key".to_string(), None),
            ("query".to_string(), None),
            ("row_restriction".to_string(), None),
            ("selected_fields".to_string(), None),
            ("table_spec".to_string(), s("p:d.t")),
        ]
    );
    let source = read.build().unwrap();
    assert_eq!(source.transform().endpoint, DEFAULT_EXPANSION_SERVICE);
    assert_eq!(source.transform().name, "BigQueryRead");
    assert_eq!(
        managed_target(&source.transform().payload),
        (
            URN_BIGQUERY_STORAGE_READ.to_string(),
            r#"{"table_spec":"p:d.t"}"#.to_string()
        )
    );

    // Write: CREATE_IF_NEEDED / WRITE_APPEND are sent explicitly and the rest is null.
    let write = BigQueryWrite::new("BigQueryWrite", "p:d.t");
    let row = write.build_config_row().unwrap();
    assert_eq!(describe(row.schema()), fixture(BIGQUERY_WRITE_CONFIG));
    assert_eq!(
        values(&row),
        WriteExpect {
            table: "p:d.t",
            create_disposition: Some("CREATE_IF_NEEDED"),
            write_disposition: Some("WRITE_APPEND"),
            ..Default::default()
        }
        .values()
    );
    let sink = write.build().unwrap();
    assert_eq!(sink.transform().endpoint, DEFAULT_EXPANSION_SERVICE);
    assert_eq!(
        managed_target(&sink.transform().payload),
        (
            URN_BIGQUERY_WRITE.to_string(),
            r#"{"create_disposition":"CREATE_IF_NEEDED","table":"p:d.t","write_disposition":"WRITE_APPEND"}"#
                .to_string()
        )
    );
}

fn read_getters(read: &BigQueryRead) -> ReadGetters<'_> {
    (
        read.table(),
        read.query(),
        read.selected_fields(),
        read.row_restriction(),
        read.expansion_service(),
    )
}

#[test]
fn test_bigquery_read_getters() {
    let fields = ["a".to_string(), "b".to_string()];
    let cases: [(&str, BigQueryRead, ReadGetters<'_>); 3] = [
        (
            "defaults",
            BigQueryRead::new("r"),
            (None, None, None, None, DEFAULT_EXPANSION_SERVICE),
        ),
        (
            "table with options",
            BigQueryRead::new("r")
                .with_table("p:d.t")
                .with_selected_fields(["a", "b"])
                .with_row_restriction("x > 0")
                .with_kms_key("k")
                .with_expansion_service("host:1"),
            (Some("p:d.t"), None, Some(&fields), Some("x > 0"), "host:1"),
        ),
        (
            "query",
            BigQueryRead::new("r").with_query("SELECT 1"),
            (
                None,
                Some("SELECT 1"),
                None,
                None,
                DEFAULT_EXPANSION_SERVICE,
            ),
        ),
    ];
    for (name, read, expected) in &cases {
        assert_eq!(read_getters(read), *expected, "{name}");
    }
}

fn write_getters(write: &BigQueryWrite) -> WriteGetters<'_> {
    (
        write.table(),
        write.method(),
        write.create_disposition(),
        write.write_disposition(),
        write.error_output(),
        write.expansion_service(),
    )
}

#[test]
fn test_bigquery_write_getters() {
    let cases: [(&str, BigQueryWrite, WriteGetters<'_>); 2] = [
        (
            "defaults",
            BigQueryWrite::new("w", "p:d.t"),
            (
                "p:d.t",
                WriteMethod::Auto,
                Some(CreateDisposition::CreateIfNeeded),
                Some(WriteDisposition::WriteAppend),
                None,
                DEFAULT_EXPANSION_SERVICE,
            ),
        ),
        (
            "every field",
            BigQueryWrite::new("w", "p:d.other")
                .with_method(WriteMethod::FileLoads)
                .with_create_disposition(CreateDisposition::CreateNever)
                .with_write_disposition(WriteDisposition::WriteTruncate)
                .with_error_handling("failed")
                .with_expansion_service("host:2"),
            (
                "p:d.other",
                WriteMethod::FileLoads,
                Some(CreateDisposition::CreateNever),
                Some(WriteDisposition::WriteTruncate),
                Some("failed"),
                "host:2",
            ),
        ),
    ];
    for (name, write, expected) in &cases {
        assert_eq!(write_getters(write), *expected, "{name}");
    }
}
