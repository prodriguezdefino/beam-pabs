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

//! Apache Beam Rust SDK
//!
//! A native Rust SDK for writing portable [Apache Beam](https://beam.apache.org/) pipelines.
//!
//! # Getting Started
//!
//! One dependency is enough. Select the runner as a feature. The connectors, the
//! Fn API worker harness and the schema derive macros come with it:
//!
//! ```toml
//! [dependencies]
//! beam = { package = "apache-beam", version = "0.1", features = ["prism"] }
//! ```
//!
//! Writing a pipeline using `beam::prelude::*`:
//!
//! ```rust
//! use beam::prelude::*;
//!
//! let p = Pipeline::new();
//!
//! p.apply(textio::Read::new("TextIO.Read", "input.txt"))
//!     .flat_map("ExtractWords", |line: String| {
//!         line.split_whitespace().map(String::from).collect::<Vec<_>>()
//!     })
//!     .count_per_element("CountWords")
//!     .map("FormatCounts", |(word, count)| format!("{word}: {count}"))
//!     .apply(textio::Write::new("TextIO.Write", "counts.txt"));
//! ```
//!
//! # Link-time registration
//!
//! Filesystems (`gs://`) and runners register through [`inventory`], which sees only
//! the crates that the linker keeps. Rust drops an rlib that nothing refers to, so this
//! crate holds a `use ... as _` for each optional dependency. Then you do not have to
//! name `beam-io-gcp` or `beam-harness` only to keep them linked.
//!
//! [`inventory`]: https://docs.rs/inventory

pub use core::*;
pub use model;

#[cfg(feature = "fluent")]
pub use fluent;

#[cfg(feature = "external")]
pub use external;

#[cfg(feature = "harness")]
pub use harness;

#[cfg(feature = "expansion")]
pub use expansion;

#[cfg(feature = "testing")]
pub use testing;

#[cfg(feature = "ml")]
pub use ml;

pub mod io {
    #[cfg(feature = "io-file")]
    pub use file;

    #[cfg(feature = "io-file")]
    pub use file::*;

    #[cfg(feature = "gcs")]
    pub use gcp;

    /// Beam schema and `Row` bridge to Apache Arrow.
    #[cfg(feature = "arrow")]
    pub use arrow_io as arrow;

    /// Parquet I/O. The transforms are in `beam::io::parquet::parquetio`.
    #[cfg(feature = "parquet")]
    pub use parquet_io as parquet;

    /// Avro I/O. The transforms are in `beam::io::avro::avroio`.
    #[cfg(feature = "avro")]
    pub use avro_io as avro;

    /// Cross-language Kafka I/O: `beam::io::kafka::KafkaRead` and `KafkaWrite`.
    #[cfg(feature = "kafka")]
    pub use kafka_io as kafka;

    /// Managed I/O: `beam::io::ManagedRead::new("Managed Read(ICEBERG)", managed::ICEBERG)`.
    #[cfg(feature = "managed")]
    pub use managed_io as managed;
}

pub mod runners {
    pub use core::runners::*;

    #[cfg(feature = "dataflow")]
    pub use dataflow;
    #[cfg(feature = "prism")]
    pub use prism;
}

/// Process-wide allocator for binaries that link the facade.
///
/// Declared here so that users get it by default. To install a different allocator, set
/// `default-features = false` and enable again the features that you need.
#[cfg(feature = "mimalloc")]
#[global_allocator]
static GLOBAL_ALLOCATOR: mimalloc::MiMalloc = mimalloc::MiMalloc;

/// Force-links optional dependencies so that their `inventory` registrations stay.
///
/// See the crate-level docs. These imports exist only for their link-time effect.
mod link {
    #[cfg(feature = "io-file")]
    pub use file as _;

    #[cfg(feature = "gcs")]
    pub use gcp as _;

    #[cfg(feature = "harness")]
    pub use harness as _;

    #[cfg(feature = "dataflow")]
    pub use dataflow as _;

    #[cfg(feature = "prism")]
    pub use prism as _;

    #[cfg(feature = "expansion")]
    pub use expansion as _;

    #[cfg(feature = "ml")]
    pub use ml as _;
}

/// All that an ordinary pipeline needs, in both styles.
///
/// Holds the core prelude (`apply` style: `pcoll.apply(Map::new(..))`), the fluent
/// extension traits (method style: `pcoll.map(..)`), [`textio`](file::textio), and
/// [`run`](core::runners::run). Import other connectors and RunInference from their
/// own modules: `beam::io::parquet::parquetio`, `beam::io::avro::avroio`, `beam::ml`,
/// and so on.
pub mod prelude {
    pub use core::prelude::*;

    #[cfg(feature = "fluent")]
    pub use fluent::prelude::*;

    #[cfg(feature = "io-file")]
    pub use file::textio;

    pub use core::runners::run;
}
