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

//! A memory catalog on a temp warehouse, and the row of the target job.

use std::collections::HashMap;
use std::sync::Arc;

use beam::schema::{Field, FieldType, Schema, URN_MILLIS_INSTANT};
use iceberg::CatalogBuilder;
use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalog, MemoryCatalogBuilder};
use iceberg_io::storage::FileSystemStorageFactory;
use iceberg_io::{Catalog, WriteOptions};
use tempfile::TempDir;

/// A memory catalog whose metadata files go to a temp directory through the SDK storage.
pub async fn memory_catalog(warehouse: &TempDir) -> MemoryCatalog {
    MemoryCatalogBuilder::default()
        .with_storage_factory(Arc::new(FileSystemStorageFactory))
        .load(
            "test",
            HashMap::from([(
                MEMORY_CATALOG_WAREHOUSE.to_string(),
                format!("file://{}", warehouse.path().display()),
            )]),
        )
        .await
        .expect("a memory catalog")
}

pub fn device_metric() -> Schema {
    Schema::new(vec![
        Field::new("id", FieldType::int64()),
        Field::new(
            "timestamp_ms",
            FieldType::logical(URN_MILLIS_INSTANT, Vec::new(), FieldType::int64()),
        ),
        Field::new("device_id", FieldType::string()),
        Field::new("payload", FieldType::double()),
        Field::nullable("location", FieldType::string()),
    ])
}

/// Options whose catalog is not used: the tests pass the memory catalog to `prepare`.
pub fn options(table: &str) -> WriteOptions {
    WriteOptions::new(Catalog::rest("http://unused"), table)
}
