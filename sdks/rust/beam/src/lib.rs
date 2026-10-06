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
//! Filesystems and runners register through [`inventory`], which sees only the crates
//! that the linker keeps. Rust drops an rlib that nothing refers to, so this crate holds
//! a `use ... as _` for each optional dependency. Then you do not have to name those
//! crates only to keep them linked.
//!
//! [`inventory`]: https://docs.rs/inventory

pub use core::*;
pub use model;

#[cfg(feature = "fluent")]
pub use fluent;

#[cfg(feature = "harness")]
pub use harness;

#[cfg(feature = "testing")]
pub use testing;

pub mod io {
    #[cfg(feature = "io-file")]
    pub use file;

    #[cfg(feature = "io-file")]
    pub use file::*;
}

pub mod runners {
    pub use core::runners::*;

    #[cfg(feature = "prism")]
    pub use prism;
}

/// Force-links optional dependencies so that their `inventory` registrations stay.
///
/// See the crate-level docs. These imports exist only for their link-time effect.
mod link {
    #[cfg(feature = "io-file")]
    pub use file as _;

    #[cfg(feature = "harness")]
    pub use harness as _;

    #[cfg(feature = "prism")]
    pub use prism as _;
}

/// All that an ordinary pipeline needs, in both styles.
///
/// Holds the core prelude (`apply` style: `pcoll.apply(Map::new(..))`), the fluent
/// extension traits (method style: `pcoll.map(..)`), [`textio`](file::textio), and
/// [`run`](core::runners::run).
pub mod prelude {
    pub use core::prelude::*;

    #[cfg(feature = "fluent")]
    pub use fluent::prelude::*;

    #[cfg(feature = "io-file")]
    pub use file::textio;

    pub use core::runners::run;
}
