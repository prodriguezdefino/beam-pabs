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

//! Expected Java configuration schemas and row-value builders.

use beam::schema::FieldValue;

use crate::common::{error_handling, s};

/// Configuration schema Java derives for `KafkaReadSchemaTransformConfiguration`: the
/// AutoValue getter names sorted, then snake_cased. Only `getBootstrapServers()` and
/// `getTopic()` are non-null; `Integer` -> INT32, `Map<String,String>` -> MAP, and
/// `ErrorHandling` has a non-null `getOutput()`.
pub const KAFKA_READ_CONFIG: &[(&str, &str)] = &[
    ("allow_duplicates", "BOOLEAN?"),
    ("auto_offset_reset_config", "STRING?"),
    ("bootstrap_servers", "STRING"),
    ("confluent_schema_registry_subject", "STRING?"),
    ("confluent_schema_registry_url", "STRING?"),
    ("consumer_config_updates", "MAP<STRING, STRING>?"),
    ("error_handling", "ROW<output: STRING>?"),
    ("file_descriptor_path", "STRING?"),
    ("format", "STRING?"),
    ("max_read_time_seconds", "INT32?"),
    ("message_name", "STRING?"),
    ("offset_deduplication", "BOOLEAN?"),
    ("redistribute_by_record_key", "BOOLEAN?"),
    ("redistribute_num_keys", "INT32?"),
    ("redistributed", "BOOLEAN?"),
    ("schema", "STRING?"),
    ("topic", "STRING"),
];

/// Configuration schema Java derives for `KafkaWriteSchemaTransformConfiguration` (same
/// naming rules). `getFormat()`, `getTopic()` and `getBootstrapServers()` are non-null.
pub const KAFKA_WRITE_CONFIG: &[(&str, &str)] = &[
    ("bootstrap_servers", "STRING"),
    ("error_handling", "ROW<output: STRING>?"),
    ("file_descriptor_path", "STRING?"),
    ("format", "STRING"),
    ("message_name", "STRING?"),
    ("producer_config_updates", "MAP<STRING, STRING>?"),
    ("schema", "STRING?"),
    ("topic", "STRING"),
];

pub fn string_map(entries: &[(&str, &str)]) -> Option<FieldValue> {
    Some(FieldValue::Map(
        entries
            .iter()
            .map(|(k, v)| (FieldValue::String(k.to_string()), s(v)))
            .collect(),
    ))
}

/// Fields a read row sets; everything else in [`KAFKA_READ_CONFIG`] must be null.
#[derive(Default)]
pub struct ReadExpect {
    pub allow_duplicates: Option<bool>,
    pub auto_offset_reset_config: Option<&'static str>,
    pub bootstrap_servers: &'static str,
    pub registry_subject: Option<&'static str>,
    pub registry_url: Option<&'static str>,
    pub consumer_config_updates: Option<FieldValue>,
    pub error_handling: Option<&'static str>,
    pub file_descriptor_path: Option<&'static str>,
    pub format: &'static str,
    pub max_read_time_seconds: Option<i32>,
    pub message_name: Option<&'static str>,
    pub offset_deduplication: Option<bool>,
    pub redistribute_by_record_key: Option<bool>,
    pub redistribute_num_keys: Option<i32>,
    pub redistributed: Option<bool>,
    pub schema: Option<&'static str>,
    pub topic: &'static str,
}

impl ReadExpect {
    pub fn values(self) -> Vec<(String, Option<FieldValue>)> {
        let st = |v: Option<&str>| v.and_then(s);
        let b = |v: Option<bool>| v.map(FieldValue::Boolean);
        let i = |v: Option<i32>| v.map(FieldValue::Int32);
        [
            ("allow_duplicates", b(self.allow_duplicates)),
            (
                "auto_offset_reset_config",
                st(self.auto_offset_reset_config),
            ),
            ("bootstrap_servers", s(self.bootstrap_servers)),
            (
                "confluent_schema_registry_subject",
                st(self.registry_subject),
            ),
            ("confluent_schema_registry_url", st(self.registry_url)),
            ("consumer_config_updates", self.consumer_config_updates),
            (
                "error_handling",
                self.error_handling.and_then(error_handling),
            ),
            ("file_descriptor_path", st(self.file_descriptor_path)),
            ("format", s(self.format)),
            ("max_read_time_seconds", i(self.max_read_time_seconds)),
            ("message_name", st(self.message_name)),
            ("offset_deduplication", b(self.offset_deduplication)),
            (
                "redistribute_by_record_key",
                b(self.redistribute_by_record_key),
            ),
            ("redistribute_num_keys", i(self.redistribute_num_keys)),
            ("redistributed", b(self.redistributed)),
            ("schema", st(self.schema)),
            ("topic", s(self.topic)),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v))
        .collect()
    }
}

/// Fields a write row sets; everything else in [`KAFKA_WRITE_CONFIG`] must be null.
#[derive(Default)]
pub struct WriteExpect {
    pub bootstrap_servers: &'static str,
    pub error_handling: Option<&'static str>,
    pub file_descriptor_path: Option<&'static str>,
    pub format: &'static str,
    pub message_name: Option<&'static str>,
    pub producer_config_updates: Option<FieldValue>,
    pub schema: Option<&'static str>,
    pub topic: &'static str,
}

impl WriteExpect {
    pub fn values(self) -> Vec<(String, Option<FieldValue>)> {
        let st = |v: Option<&str>| v.and_then(s);
        [
            ("bootstrap_servers", s(self.bootstrap_servers)),
            (
                "error_handling",
                self.error_handling.and_then(error_handling),
            ),
            ("file_descriptor_path", st(self.file_descriptor_path)),
            ("format", s(self.format)),
            ("message_name", st(self.message_name)),
            ("producer_config_updates", self.producer_config_updates),
            ("schema", st(self.schema)),
            ("topic", s(self.topic)),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v))
        .collect()
    }
}
