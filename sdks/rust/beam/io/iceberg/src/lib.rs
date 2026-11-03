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

//! Native Apache Iceberg write connector for the Apache Beam Rust SDK.
//!
//! The connector appends rows to Iceberg tables of a REST catalog. The workers write Parquet
//! data files through the SDK file systems and commit snapshots to the catalog.
//!
//! [`Write`] takes a `#[derive(BeamRow)]` struct and [`WriteRows`] takes rows with a row
//! schema. A bounded input commits one snapshot at the end. An unbounded input commits one
//! snapshot for each table and triggering period.
//!
//! ```no_run
//! # use beam::pipeline::Pipeline;
//! # use beam::schema::BeamRow;
//! # use beam::transforms::Create;
//! use iceberg_io::{Catalog, PartitionField, Transform, Write, WriteOptions};
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
//! .with_partition_fields([PartitionField::new("device", Transform::Identity)]);
//! let pipeline = Pipeline::new();
//! let written = pipeline
//!     .apply("Create", Create::new(vec![Metric { id: 1, device: "a".to_string() }]))
//!     .apply("WriteToIceberg", Write::<Metric>::new(options));
//! // `written.errors` holds the rows that do not convert, `written.snapshots` the commits.
//! ```

#[doc(hidden)]
pub mod arrow;
#[doc(hidden)]
pub mod catalog;
#[doc(hidden)]
pub mod commit;
#[cfg(feature = "gcp")]
pub(crate) mod credentials;
#[doc(hidden)]
pub mod data_file;
#[doc(hidden)]
pub mod encode;
#[doc(hidden)]
pub mod location;
pub(crate) mod options;
#[doc(hidden)]
pub mod parquet;
#[doc(hidden)]
pub mod schema;
mod sink;
#[doc(hidden)]
pub mod storage;
#[doc(hidden)]
pub mod table;
#[doc(hidden)]
pub mod write;

pub use commit::SnapshotInfo;
pub use options::{Catalog, PartitionField, Transform, WriteOptions};
pub use sink::{DEFAULT_TRIGGERING_FREQUENCY, Write, WriteResult, WriteRows};

/// The namespace of the metrics of the write.
pub(crate) const METRICS: &str = "iceberg_io";
