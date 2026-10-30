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

//! Apache Kafka I/O connectors for the Apache Beam Rust SDK.
//!
//! The crate has two connectors:
//!
//! - [`native`] (feature `native`): reads and writes Kafka in the Rust worker, on librdkafka.
//!   The Kafka expansion service serves it to Python, Java and Go pipelines.
//! - The Java connector through cross-language expansion: [`KafkaRead`] and [`KafkaWrite`]
//!   expand on the Java expansion service, through Managed I/O.
//!
//! Pipelines use the crate as `beam::io::kafka`: the `beam` facade enables it with the
//! feature `kafka`, and the native connector with `kafka-native`.
//!
//! # Reading from Kafka through Java
//! ```no_run
//! use beam::prelude::*;
//! use kafka_io::{KafkaFormat, KafkaRead, OffsetReset};
//!
//! let p = Pipeline::new();
//! // Each element is a row `(payload: STRING)`.
//! let lines: PCollection<Row> = p.apply(
//!     "KafkaRead",
//!     KafkaRead::new("broker-1:9092,broker-2:9092", "events")
//!         .with_format(KafkaFormat::String)
//!         .with_auto_offset_reset(OffsetReset::Earliest)
//!         // Bounded: stop after 60s. Omit for an unbounded, streaming read.
//!         .with_max_read_time_seconds(60),
//! );
//! ```
//!
//! # Writing to Kafka through Java
//! ```no_run
//! use beam::prelude::*;
//! use kafka_io::{KafkaWrite, raw_bytes_row, raw_bytes_schema};
//!
//! # let p = Pipeline::new();
//! p.apply("Create", Create::new(vec![raw_bytes_row(b"hello".to_vec())]))
//!     .with_row_schema(&raw_bytes_schema())
//!     .apply(
//!         "KafkaWrite",
//!         KafkaWrite::new("broker-1:9092", "events"),
//!     );
//! ```

#[cfg(feature = "native")]
pub mod native;
mod xlang;

pub use xlang::*;
