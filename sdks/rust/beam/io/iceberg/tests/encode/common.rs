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

//! The row of the target job, its table schema, and rows of it.

use std::sync::Arc;

use beam_io_arrow::millis_instant_type;
use beam::schema::{Field, FieldType, FieldValue, Row, Schema};
use iceberg::spec::Schema as TableSchema;
use beam_io_iceberg::schema::iceberg_schema;

pub(crate) fn device_metric() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", FieldType::int64()),
        Field::new("timestamp_ms", millis_instant_type()),
        Field::new("device_id", FieldType::string()),
        Field::new("payload", FieldType::double()),
        Field::nullable("location", FieldType::string()),
    ]))
}

/// The table that the write creates for `schema`.
pub(crate) fn table(schema: &Schema) -> TableSchema {
    iceberg_schema(schema).expect("a supported schema")
}

pub(crate) fn metric(schema: &Arc<Schema>, id: i64, millis: i64, location: Option<&str>) -> Row {
    Row::new(
        Arc::clone(schema),
        vec![
            Some(FieldValue::Int64(id)),
            Some(FieldValue::Int64(millis)),
            Some(FieldValue::String(format!("device-{id}"))),
            Some(FieldValue::Double(id as f64 / 2.0)),
            location.map(|l| FieldValue::String(l.to_string())),
        ],
    )
    .expect("a valid row")
}
