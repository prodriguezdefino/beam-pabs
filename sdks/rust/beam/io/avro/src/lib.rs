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

//! [Apache Avro](https://avro.apache.org/) object container file I/O, built on `arrow-avro`.
//!
//! | Transform | Input → Output |
//! |---|---|
//! | [`avroio::Read<T>`] | `PBegin` → `PCollection<T>` for a file pattern |
//! | [`avroio::ReadFiles<T>`] | `PCollection<String>` paths → `PCollection<T>` |
//! | [`avroio::ReadRows`] / [`avroio::ReadRowFiles`] | as above, producing schema-aware `Row`s |
//! | [`avroio::Write<T>`] | `PCollection<T>` → `PCollection<String>` written file names |
//!
//! `T` is any `#[derive(BeamRow)]` type; record fields match its fields by name through
//! `arrow_io`. The written Avro schema is derived from the Beam schema; nullable fields
//! become `["null", T]` unions. Reads split at data-block sync markers.

mod error;
mod sink;
mod source;
mod write;

/// Avro read and write transforms.
pub mod avroio {
    pub use crate::error::AvroIoError;
    pub use crate::sink::{AvroSink, DEFAULT_BLOCK_SIZE};
    pub use crate::source::{
        AvroRecordReader, DEFAULT_READ_BATCH_SIZE, Read, ReadFiles, ReadRowFiles, ReadRows,
        schema_of,
    };
    pub use crate::write::Write;
    pub use arrow_avro::compression::CompressionCodec;
}

pub use arrow_avro;
