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

//! Google Cloud BigQuery connectors on the Storage APIs.
//!
//! [`read`] reads tables and queries through the Storage Read API. [`write`] writes rows
//! through the Storage Write API. [`schema`] maps types between BigQuery and Beam.
//!
//! The Storage Write API cannot replace the rows of a table. To write with `WRITE_TRUNCATE`
//! or with load jobs, use Managed I/O (`apache-beam-io-managed`).

mod clients;
pub mod read;
pub mod schema;
pub mod table;
pub mod write;

/// Metric namespace of the BigQuery connector.
#[doc(hidden)]
pub const METRICS: &str = "bigquery";
