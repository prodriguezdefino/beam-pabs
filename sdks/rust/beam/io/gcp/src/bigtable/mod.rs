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

//! Google Cloud Bigtable I/O connectors: native Rust [`read::ReadRows`] and
//! [`write::WriteMutations`], server-side [`proto::RowFilter`] builders (including the native
//! [`RowFilterExt::value_bitmask`] filter), and cross-language SchemaTransform wrappers
//! ([`BigtableRead`], [`BigtableWrite`]).
//!
//! # Native Read (`ReadRows`)
//! ```no_run
//! use beam::prelude::*;
//! use beam_io_gcp::bigtable::read::{ReadOptions, ReadRows};
//!
//! let p = Pipeline::new();
//! let options = ReadOptions::from_table("my-project", "my-instance", "my-table")
//!     .with_app_profile_id("data-boost-profile");
//! let rows: PCollection<Row> = p.apply("ReadBigtable", ReadRows::new(options));
//! ```
//!
//! # Native Write (`WriteMutations`)
//! ```no_run
//! use beam::prelude::*;
//! use beam_io_gcp::bigtable::write::{WriteMutations, WriteOptions};
//! use beam_io_gcp::bigtable::{BigtableMutation, mutation_schema};
//!
//! let p = Pipeline::new();
//! let mutations = p
//!     .apply("Create", Create::new(vec![
//!         Row::from(BigtableMutation::set_cell("row-1", "cf", "greeting", "hello")),
//!     ]))
//!     .with_row_schema(&mutation_schema());
//! let rejected = mutations.apply(
//!     "WriteBigtable",
//!     WriteMutations::new(WriteOptions::from_table(
//!         "my-project",
//!         "my-instance",
//!         "my-table",
//!     )),
//! );
//! ```

#[doc(hidden)]
pub mod client;
mod external;
mod filter;
#[doc(hidden)]
pub mod key;
pub mod proto;
pub mod read;
#[doc(hidden)]
pub mod retry;
mod schema;
#[doc(hidden)]
pub mod target;
pub mod write;

pub use self::external::{BigtableRead, BigtableWrite, URN_BIGTABLE_READ, URN_BIGTABLE_WRITE};
pub use self::filter::RowFilterExt;
pub use self::schema::{
    BigtableCell, BigtableColumn, BigtableMutation, BigtableReadShape, MUTATION_DELETE_FROM_COLUMN,
    MUTATION_DELETE_FROM_FAMILY, MUTATION_DELETE_FROM_ROW, MUTATION_SET_CELL,
    batched_mutation_schema, cell_schema, flattened_row_schema, mutation_schema, nested_row_schema,
};
pub use self::target::{ReadTarget, WriteTarget};
