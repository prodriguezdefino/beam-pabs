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

//! Schemas and options that several tests use.

use beam::schema::{Field, FieldType, Schema, URN_DATE, URN_MILLIS_INSTANT};
use iceberg_io::{Catalog, WriteOptions};

/// The row of the target job: `{id, timestamp_ms, device_id, payload, location}`.
pub fn device_metric() -> Schema {
    Schema::new(vec![
        Field::new("id", FieldType::int64()),
        Field::new("timestamp_ms", millis_instant()),
        Field::new("device_id", FieldType::string()),
        Field::new("payload", FieldType::double()),
        Field::nullable("location", FieldType::string()),
    ])
}

pub fn millis_instant() -> FieldType {
    FieldType::logical(URN_MILLIS_INSTANT, Vec::new(), FieldType::int64())
}

pub fn date() -> FieldType {
    FieldType::logical(URN_DATE, Vec::new(), FieldType::int64())
}

pub fn options(table: &str) -> WriteOptions {
    WriteOptions::new(Catalog::rest("http://localhost:8181"), table)
}
