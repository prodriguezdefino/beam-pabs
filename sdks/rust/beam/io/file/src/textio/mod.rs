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

//! Reading and writing newline-delimited text files.
//!
//! ```
//! use file::textio;
//! use beam::prelude::*;
//! use beam::transforms::Map;
//!
//! let pipeline = Pipeline::new();
//! pipeline
//!     .apply("TextIO.Read", textio::Read::new("/data/*.txt"))
//!     .apply("Upper", Map::new(|line: String| line.to_uppercase()))
//!     .apply("TextIO.Write", textio::Write::new("/tmp/out.txt"));
//! ```
//!
//! Paths use the [`FileSystem`](super::filesystem::FileSystem) registered for their scheme.
//! Large files are split into byte ranges and reshuffled across workers.

pub(crate) mod read;
pub(crate) mod split;
pub(crate) mod write;

pub use read::{Read, ReadFiles, ReadFilesWithFilename, ReadWithFilename};
pub use split::{
    DEFAULT_SPLIT_SIZE, TextLineReader, TextLineWithFilenameReader, read_file_lines_with_tracker,
};
#[doc(hidden)]
pub use split::{ReadFileLinesFn, ReadFileLinesWithFilenameFn};
pub use write::Write;
