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

//! The Apache Kafka I/O connector of the Apache Beam Rust SDK, on librdkafka.
//!
//! The connector reads Kafka in the Rust worker. [`read`] reads the records of a topic.
//! [`codec`] maps record values to elements and back. [`client`] holds the connection to a
//! cluster, and [`auth`] the Google authentication of Google Cloud Managed Service for Apache
//! Kafka. [`xlang`] has the cross-language Kafka transforms.
//!
//! Pipelines use the crate as `beam::io::kafka` with the facade feature `kafka`. For a feature
//! that the connector does not have, use the Kafka transforms of the Managed API
//! (`beam::io::managed`, feature `managed`).
//!
//! The consumer and producer properties are librdkafka properties, not the properties of the
//! Java Kafka client. For SASL, set `sasl.mechanism`, `sasl.username` and `sasl.password`:
//! librdkafka has no `sasl.jaas.config`.
//!
//! # Reading from Kafka
//! ```no_run
//! use std::time::Duration;
//!
//! use beam::prelude::*;
//! use beam_io_kafka::codec::Utf8;
//! use beam_io_kafka::{Read, ReadOptions, StartOffset};
//!
//! let p = Pipeline::new();
//! let lines: PCollection<String> = p.apply(
//!     "ReadFromKafka",
//!     Read::new(
//!         ReadOptions::new("broker-1:9092,broker-2:9092", "events")
//!             .with_start_offset(StartOffset::Earliest)
//!             // Bounded: stop after 60s. Omit for an unbounded, streaming read.
//!             .with_max_read_time(Duration::from_secs(60)),
//!     )
//!     .with_decoder(Utf8),
//! );
//! ```

use beam::schema::{FieldType, Schema};

pub mod auth;
pub mod client;
pub mod codec;
pub mod context;
#[doc(hidden)]
pub mod read;
#[doc(hidden)]
pub mod write;
pub mod xlang;

pub use read::{
    ConsumerPool, PartitionStart, PoolSize, Read, ReadOptions, ReadWithFailures, Redistribute,
    StartOffset,
};

/// The schema of the failure output of a read: the record value (empty for a record with no
/// value) and the reason.
pub fn read_failure_schema() -> Schema {
    codec::failure_schema(FieldType::bytes())
}

/// The schema of the failure output of a write whose encoder rejects elements as
/// `failed_type`: the element and the reason.
pub fn write_failure_schema(failed_type: FieldType) -> Schema {
    codec::failure_schema(failed_type)
}

/// Metric namespace of the Kafka connector.
#[doc(hidden)]
pub const METRICS: &str = "kafka";
