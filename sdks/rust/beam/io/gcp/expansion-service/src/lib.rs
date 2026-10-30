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

//! SchemaTransform providers for the native Google Cloud connectors.
//!
//! The configuration of each provider has the field names and types of the Java provider for
//! the same service, so a caller can send the same configuration to either. Options that only
//! Rust has are extra nullable fields.

use beam::schema::BeamRow;

pub mod bigquery;
pub mod bigtable;

pub use self::bigquery::{
    BigQueryStorageReadConfig, BigQueryStorageReadProvider, BigQueryStorageWriteConfig,
    BigQueryStorageWriteProvider, URN_BIGQUERY_STORAGE_READ, URN_BIGQUERY_STORAGE_WRITE,
};
pub use self::bigtable::{
    BigtableReadConfig, BigtableReadProvider, BigtableWriteConfig, BigtableWriteProvider,
    URN_BIGTABLE_READ, URN_BIGTABLE_WRITE,
};

/// Where the rows that a connector rejects go: the output tag of the transform.
#[derive(Clone, Debug, Default, PartialEq, BeamRow)]
pub struct ErrorHandling {
    pub output: String,
}
