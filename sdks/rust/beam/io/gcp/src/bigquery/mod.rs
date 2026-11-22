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
//! | Transform | Input → Output |
//! |---|---|
//! | [`Read<T>`] / [`ReadWithFailures<T>`] | `PBegin` → `PCollection<T>` or [`WithFailures<T, Row>`](beam::transforms::WithFailures) |
//! | [`ReadRows`] / [`ReadRowsWithFailures`] | `PBegin` → `PCollection<Row>` or [`WithFailures<Row, Row>`](beam::transforms::WithFailures) |
//! | [`Write<T>`] | `PCollection<T>` → [`WriteResult`] |
//! | [`WriteRows`] | `PCollection<Row>` → [`WriteResult`] |
//!
//! `T` is any `#[derive(BeamRow)]` type. [`Read<T>`] and [`ReadRows`] read tables and queries
//! through the Storage Read API in Arrow format. [`Write<T>`] and [`WriteRows`] write to a
//! table through the Storage Write API in Arrow format, at least once or exactly once.
//! [`schema`] maps types between BigQuery and Beam.
//!
//! The Storage Write API cannot replace the rows of a table. To write with `WRITE_TRUNCATE`
//! or with load jobs, use Managed I/O (`apache-beam-io-managed`).
//!
//! ```no_run
//! use beam::prelude::*;
//! use beam::schema::BeamRow;
//! use beam_io_gcp::bigquery::{ArrowCompression, Read, ReadOptions, Write, WriteOptions};
//!
//! #[derive(Clone, Debug, BeamRow)]
//! struct WordCount {
//!     word: String,
//!     word_count: i64,
//! }
//!
//! let p = Pipeline::new();
//! let counts = p.apply(
//!     "ReadCounts",
//!     Read::<WordCount>::new(
//!         ReadOptions::new("bigquery-public-data.samples.shakespeare")
//!             .with_billing_project("my-project")
//!             .with_row_restriction("corpus = 'hamlet'")
//!             .with_compression(ArrowCompression::Lz4Frame),
//!     ),
//! );
//! let written = counts.apply(
//!     "WriteCounts",
//!     Write::<WordCount>::new(WriteOptions::new("my-project.my_dataset.hamlet_counts")),
//! );
//! let _failures = written.failures;
//! ```

mod clients;
#[doc(hidden)]
pub mod read;
pub mod schema;
pub mod table;
#[doc(hidden)]
pub mod write;

pub use read::{
    ArrowCompression, QueryPriority, Read, ReadOptions, ReadRows, ReadRowsWithFailures,
    ReadWithFailures, read_failure_schema,
};
pub use write::{
    CreateDisposition, Semantics, Write, WriteDisposition, WriteOptions, WriteResult, WriteRows,
    write_failure_schema,
};

/// Metric namespace of the BigQuery connector.
#[doc(hidden)]
pub const METRICS: &str = "bigquery";
