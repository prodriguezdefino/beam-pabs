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

//! Expected Java configuration schemas and row-value builders for BigQuery tests.

use beam::schema::FieldValue;
use managed_io::URN_MANAGED;

use crate::common::{decode_payload, error_handling, s};

/// Java schema of `BigQueryDirectReadSchemaTransformConfiguration`: names from
/// `AutoValueSchema(getters).sorted().toSnakeCase()`, every getter `@Nullable`. The Rust row
/// is unsorted, which is fine because Managed forwards it as a name-keyed map, so the test
/// sorts the Rust fields.
pub const BIGQUERY_READ_CONFIG: &[(&str, &str)] = &[
    ("kms_key", "STRING?"),
    ("query", "STRING?"),
    ("row_restriction", "STRING?"),
    ("selected_fields", "ARRAY<STRING>?"),
    ("table_spec", "STRING?"),
];

/// Java schema of `BigQueryWriteConfiguration`, shared by Managed `bigquery_write:v1`,
/// `bigquery_storage_write:v2` and `bigquery_fileloads:v1`. Only `table` and
/// `ErrorHandling.output` are non-nullable. Java maps `Long` to INT64, `Integer` to INT32 and
/// `Map<String,String>` to MAP.
pub const BIGQUERY_WRITE_CONFIG: &[(&str, &str)] = &[
    ("auto_sharding", "BOOLEAN?"),
    ("big_lake_configuration", "MAP<STRING, STRING>?"),
    ("clustering_fields", "ARRAY<STRING>?"),
    ("create_disposition", "STRING?"),
    ("drop", "ARRAY<STRING>?"),
    ("error_handling", "ROW<output: STRING>?"),
    ("keep", "ARRAY<STRING>?"),
    ("kms_key", "STRING?"),
    ("num_streams", "INT32?"),
    ("only", "STRING?"),
    ("primary_key", "ARRAY<STRING>?"),
    ("table", "STRING"),
    ("triggering_frequency_seconds", "INT64?"),
    ("use_at_least_once_semantics", "BOOLEAN?"),
    ("use_cdc_writes", "BOOLEAN?"),
    ("write_disposition", "STRING?"),
];

/// Sorts `(field name, _)` pairs by field name.
pub fn sorted<T>(mut v: Vec<(String, T)>) -> Vec<(String, T)> {
    v.sort_by(|a, b| a.0.cmp(&b.0));
    v
}

/// The fields a write row sets; all other fields in [`BIGQUERY_WRITE_CONFIG`] must be null.
#[derive(Default)]
pub struct WriteExpect {
    pub auto_sharding: Option<bool>,
    pub create_disposition: Option<&'static str>,
    pub error_handling: Option<&'static str>,
    pub kms_key: Option<&'static str>,
    pub num_streams: Option<i32>,
    pub table: &'static str,
    pub triggering_frequency_seconds: Option<i64>,
    pub use_at_least_once_semantics: Option<bool>,
    pub write_disposition: Option<&'static str>,
}

impl WriteExpect {
    pub fn values(&self) -> Vec<(String, Option<FieldValue>)> {
        let st = |v: Option<&str>| v.and_then(s);
        vec![
            (
                "auto_sharding".into(),
                self.auto_sharding.map(FieldValue::Boolean),
            ),
            ("big_lake_configuration".into(), None),
            ("clustering_fields".into(), None),
            ("create_disposition".into(), st(self.create_disposition)),
            ("drop".into(), None),
            (
                "error_handling".into(),
                self.error_handling.and_then(error_handling),
            ),
            ("keep".into(), None),
            ("kms_key".into(), st(self.kms_key)),
            (
                "num_streams".into(),
                self.num_streams.map(FieldValue::Int32),
            ),
            ("only".into(), None),
            ("primary_key".into(), None),
            ("table".into(), s(self.table)),
            (
                "triggering_frequency_seconds".into(),
                self.triggering_frequency_seconds.map(FieldValue::Int64),
            ),
            (
                "use_at_least_once_semantics".into(),
                self.use_at_least_once_semantics.map(FieldValue::Boolean),
            ),
            ("use_cdc_writes".into(), None),
            ("write_disposition".into(), st(self.write_disposition)),
        ]
    }
}

/// Decodes a Managed payload into the underlying URN and inline JSON configuration.
///
/// The workspace `serde_json` does not enable `preserve_order`, so Managed emits the keys
/// in sorted order.
pub fn managed_target(payload: &[u8]) -> (String, String) {
    let (identifier, row) = decode_payload(payload);
    assert_eq!(identifier, URN_MANAGED);
    (
        row.get_string("transform_identifier")
            .expect("transform_identifier")
            .expect("transform_identifier")
            .to_string(),
        row.get_string("config")
            .expect("config")
            .expect("inline config")
            .to_string(),
    )
}
