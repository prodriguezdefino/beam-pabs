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

//! Apache Iceberg connector for the Apache Beam Rust SDK.
//!
//! The connector reads snapshots of Iceberg tables of a REST catalog, and appends rows to them.
//! The workers read and write Parquet data files through the SDK file systems and commit
//! snapshots to the catalog.
//!
//! [`Read`] and [`Write`] take a `#[derive(BeamRow)]` struct. [`ReadRows`] and [`WriteRows`]
//! take rows with a row schema. A bounded write commits one snapshot at the end. An unbounded
//! write commits one snapshot for each table and triggering period.
//!
//! ```no_run
//! # use beam::pipeline::Pipeline;
//! # use beam::schema::BeamRow;
//! # use beam::transforms::Create;
//! use beam_io_iceberg::{Catalog, PartitionField, PartitionTransform, Write, WriteOptions};
//!
//! #[derive(BeamRow, Clone, Debug)]
//! struct Metric {
//!     id: i64,
//!     device: String,
//! }
//!
//! let options = WriteOptions::new(
//!     Catalog::rest("https://biglake.googleapis.com/iceberg/v1/restcatalog")
//!         .with_warehouse("bl://projects/my-project/catalogs/my_catalog")
//!         .with_google_auth()
//!         .with_header("x-goog-user-project", "my-project")
//!         .with_vended_credentials(),
//!     "db.metrics",
//! )
//! .with_partition_fields([PartitionField::new("device", PartitionTransform::Identity)]);
//! let pipeline = Pipeline::new();
//! let written = pipeline
//!     .apply("Create", Create::new(vec![Metric { id: 1, device: "a".to_string() }]))
//!     .apply("WriteToIceberg", Write::<Metric>::new(options));
//! // `written.failures` holds the rows that do not convert, `written.snapshots` the commits.
//! ```

#[doc(hidden)]
pub mod arrow;
#[doc(hidden)]
pub mod catalog;
#[doc(hidden)]
pub mod read;
#[doc(hidden)]
pub mod storage;
#[doc(hidden)]
pub mod write;

pub use catalog::Catalog;
pub use read::options::{DEFAULT_READ_BATCH_SIZE, ReadOptions};
pub use read::reader::read_failure_schema;
pub use read::{Read, ReadRows, ReadRowsWithFailures, ReadWithFailures};
pub use write::commit::SnapshotInfo;
pub use write::encode::write_failure_schema;
pub use write::options::{PartitionField, PartitionTransform, WriteOptions};
pub use write::{DEFAULT_TRIGGERING_FREQUENCY, Write, WriteResult, WriteRows};

/// Metric namespace of the Iceberg connector.
#[doc(hidden)]
pub const METRICS: &str = "iceberg";
