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

//! [TFRecord](https://www.tensorflow.org/tutorials/load_data/tfrecord) file I/O.
//!
//! | Transform | Input → Output |
//! |---|---|
//! | [`tfrecordio::Read`] | `PBegin` → `PCollection<Vec<u8>>` for a file pattern |
//! | [`tfrecordio::ReadFiles`] | `PCollection<String>` paths → `PCollection<Vec<u8>>` |
//! | [`tfrecordio::Write`] | `PCollection<Vec<u8>>` → `PCollection<String>` written file names |
//!
//! Each element is the bytes of one record, for example a serialized `tf.train.Example`
//! ([`example::Example`]).
//! Reads check the masked CRC32C of each record and fail the bundle on a corrupt record.
//! Uncompressed, Gzip and Deflate (zlib) files are byte compatible with TensorFlow and the
//! Java and Python SDKs. Each file is one split. Reads detect compression from the file
//! extension by default, or use an explicit [`Compression`](file::Compression) codec; writes
//! accept a codec through [`tfrecordio::Write::with_compression`].
//!
//! ```
//! use beam::prelude::*;
//! use tfrecord_io::tfrecordio;
//!
//! let pipeline = Pipeline::new();
//! pipeline
//!     .apply("ReadExamples", tfrecordio::Read::new("/data/*.tfrecord"))
//!     .apply("WriteExamples", tfrecordio::Write::new("/tmp/out.tfrecord"));
//! ```

pub mod example;
mod format;
mod read;
mod write;

/// TFRecord read and write transforms, and the record framing.
pub mod tfrecordio {
    pub use crate::format::{RECORD_OVERHEAD, RecordReader, masked_crc32c, write_record};
    pub use crate::read::{Read, ReadFiles, TFRecordReader};
    pub use crate::write::{TFRecordFormat, Write};
}
