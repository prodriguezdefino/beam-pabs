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

//! [Apache Parquet](https://parquet.apache.org/) I/O, built on arrow-rs.
//!
//! | Transform | Input → Output |
//! |---|---|
//! | [`parquetio::Read<T>`] | `PBegin` → `PCollection<T>` for a file pattern |
//! | [`parquetio::ReadFiles<T>`] | `PCollection<String>` paths → `PCollection<T>` |
//! | [`parquetio::ReadRows`] / [`parquetio::ReadRowFiles`] | as above, producing schema-aware `Row`s |
//! | [`parquetio::Write<T>`] | `PCollection<T>` → `PCollection<String>` written file names |
//!
//! `T` is any `#[derive(BeamRow)]` type; columns match its fields by name through
//! `arrow_io`. Reads split per row group and use ranged reads, so a row group of a remote
//! object fetches only the footer and its column chunks. [`parquetio::ParquetSink`] works
//! with any `FileSink` consumer.

mod chunk;
mod error;
mod sink;
mod source;
mod write;

/// Parquet read and write transforms.
pub mod parquetio {
    pub use crate::chunk::FileSystemChunkReader;
    pub use crate::error::ParquetIoError;
    pub use crate::sink::{DEFAULT_WRITE_BATCH_SIZE, ParquetSink};
    pub use crate::source::{
        DEFAULT_READ_BATCH_SIZE, ParquetRecordReader, Read, ReadFiles, ReadRowFiles, ReadRows,
        schema_of,
    };
    pub use crate::write::Write;
    pub use parquet::basic::{Compression, ZstdLevel};
}

pub use parquet;
