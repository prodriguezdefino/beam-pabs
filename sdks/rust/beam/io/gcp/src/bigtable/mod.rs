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

//! Google Cloud Bigtable connectors: [`Read`], [`ReadRows`], [`Write`], [`WriteRows`],
//! and server-side [`proto::RowFilter`] builders (including [`RowFilterExt::value_bitmask`]).
//!
//! # Read (`ReadRows`)
//! ```no_run
//! use beam::prelude::*;
//! use beam_io_gcp::bigtable::{ReadOptions, ReadRows};
//!
//! let p = Pipeline::new();
//! let options = ReadOptions::from_table("my-project", "my-instance", "my-table")
//!     .with_app_profile_id("data-boost-profile");
//! let rows: PCollection<Row> = p.apply("ReadBigtable", ReadRows::new(options));
//! ```
//!
//! # Write (`WriteRows`)
//! ```no_run
//! use beam::prelude::*;
//! use beam_io_gcp::bigtable::{BigtableMutation, WriteOptions, WriteRows, mutation_schema};
//!
//! let p = Pipeline::new();
//! let mutations = p
//!     .apply(
//!         "Create",
//!         Create::new(vec![Row::from(BigtableMutation::set_cell(
//!             "row-1", "cf", "greeting", "hello",
//!         ))]),
//!     )
//!     .with_row_schema(&mutation_schema());
//! let written = mutations.apply(
//!     "WriteBigtable",
//!     WriteRows::new(WriteOptions::from_table(
//!         "my-project",
//!         "my-instance",
//!         "my-table",
//!     )),
//! );
//! // `written.failures` holds rejected mutation rows.
//! ```

#[doc(hidden)]
pub mod client;
mod filter;
#[doc(hidden)]
pub mod key;
pub mod proto;
#[doc(hidden)]
pub mod read;
#[doc(hidden)]
pub mod retry;
mod schema;
#[doc(hidden)]
pub mod target;
#[doc(hidden)]
pub mod write;
pub mod xlang;

pub use self::filter::RowFilterExt;
pub use self::read::{
    DEFAULT_MAX_INITIAL_BUNDLES, DEFAULT_MAX_SILENT_SCAN_DURATION, DEFAULT_READ_ATTEMPT_TIMEOUT,
    Read, ReadOptions, ReadRows, ReadRowsWithFailures, ReadWithFailures, read_failure_schema,
};
pub use self::schema::{
    BigtableCell, BigtableColumn, BigtableMutation, BigtableReadShape, MUTATION_DELETE_FROM_COLUMN,
    MUTATION_DELETE_FROM_FAMILY, MUTATION_DELETE_FROM_ROW, MUTATION_SET_CELL,
    batched_mutation_schema, cell_schema, flattened_row_schema, mutation_schema, nested_row_schema,
};
pub use self::target::{ReadTarget, WriteTarget};
pub use self::write::{
    DEFAULT_MAX_BYTES_PER_BATCH, DEFAULT_MAX_ELEMENTS_PER_BATCH, DEFAULT_MAX_IN_FLIGHT_BYTES,
    DEFAULT_MAX_IN_FLIGHT_REQUESTS, DEFAULT_WRITE_ATTEMPT_TIMEOUT, Write, WriteOptions,
    WriteResult, WriteRows, write_failure_schema,
};

/// Metrics namespace of the Bigtable read and write connectors.
#[doc(hidden)]
pub const METRICS: &str = "bigtable";
