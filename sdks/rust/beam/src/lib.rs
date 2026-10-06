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
//! p.apply("TextIO.Read", textio::Read::new("input.txt"))
//!     .flat_map("ExtractWords", |line: String| {
//!         line.split_whitespace().map(String::from).collect::<Vec<_>>()
//!     })
//!     .count_per_element("CountWords")
//!     .map("FormatCounts", |(word, count)| format!("{word}: {count}"))
//!     .apply("TextIO.Write", textio::Write::new("counts.txt"));
//! ```
//!
//! # Link-time registration
//!
//! Filesystems and runners register through [`inventory`], which sees only the crates
//! that the linker keeps. Rust drops an rlib that nothing refers to, so this crate holds
//! a `use ... as _` for each optional dependency. Then you do not have to name those
//! crates only to keep them linked.
//!
//! [`inventory`]: https://docs.rs/inventory

pub use beam_core::*;
pub use beam_model as model;

#[cfg(feature = "fluent")]
pub use beam_fluent as fluent;

#[cfg(feature = "external")]
pub use beam_external as external;

/// Logging setup for pipeline binaries.
#[cfg(feature = "harness")]
pub mod logging {
    pub use beam_harness::logging::init_logging;
}

#[cfg(feature = "expansion")]
pub use beam_expansion as expansion;

#[cfg(feature = "testing")]
pub use beam_testing as testing;

pub mod io {
    #[cfg(feature = "io-file")]
    pub use beam_io_file as file;

    #[cfg(any(
        feature = "gcp-gcs",
        feature = "gcp-bigquery",
        feature = "gcp-bigtable",
        feature = "gcp-pubsub"
    ))]
    pub use beam_io_gcp as gcp;

    /// Beam schema and `Row` bridge to Apache Arrow.
    #[cfg(feature = "arrow")]
    pub use beam_io_arrow as arrow;

    /// Parquet I/O. The transforms are in `beam::io::parquet::parquetio`.
    #[cfg(feature = "parquet")]
    pub use beam_io_parquet as parquet;

    /// Avro I/O. The transforms are in `beam::io::avro::avroio`.
    #[cfg(feature = "avro")]
    pub use beam_io_avro as avro;

    /// Cross-language Kafka I/O: `beam::io::kafka::KafkaRead` and `KafkaWrite`.
    #[cfg(feature = "kafka")]
    pub use beam_io_kafka as kafka;

    /// Managed I/O: `beam::io::managed::ManagedRead::new(managed::ICEBERG)`.
    #[cfg(feature = "managed")]
    pub use beam_io_managed as managed;
}

pub mod runners {
    pub use beam_core::runners::*;

    #[cfg(feature = "dataflow")]
    pub use beam_runner_dataflow as dataflow;
    #[cfg(feature = "prism")]
    pub use beam_runner_prism as prism;
}

/// Force-links optional dependencies so that their `inventory` registrations stay.
///
/// Downstream crates often import only `beam::prelude::*` and omit direct references
/// to `beam::io::*`. This module forces the linker to retain the registration symbols
/// from enabled feature crates.
mod link {
    #[cfg(feature = "io-file")]
    pub use beam_io_file as _;

    #[cfg(any(
        feature = "gcp-gcs",
        feature = "gcp-bigquery",
        feature = "gcp-bigtable",
        feature = "gcp-pubsub"
    ))]
    pub use beam_io_gcp as _;

    #[cfg(feature = "harness")]
    pub use beam_harness as _;

    #[cfg(feature = "dataflow")]
    pub use beam_runner_dataflow as _;

    #[cfg(feature = "prism")]
    pub use beam_runner_prism as _;

    #[cfg(feature = "expansion")]
    pub use beam_expansion as _;
}

/// All that an ordinary pipeline needs, in both styles.
///
/// Holds the core prelude (`apply` style: `pcoll.apply(.., Map::new())`), the fluent
/// extension traits (method style: `pcoll.map(..)`), [`textio`](beam_io_file::textio), and
/// [`run`](beam_core::runners::run).
pub mod prelude {
    pub use beam_core::prelude::*;

    #[cfg(feature = "fluent")]
    pub use beam_fluent::prelude::*;

    #[cfg(feature = "io-file")]
    pub use beam_io_file::textio;

    pub use beam_core::runners::run;
}
