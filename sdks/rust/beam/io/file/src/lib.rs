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

//! Filesystem abstraction and the built-in file connectors.
//!
//! This crate holds the filesystem registry, the local filesystem, and generic file and
//! text connectors. Connectors that require external clients—such as object stores,
//! message brokers, or databases—belong in separate crates.

pub(crate) mod composite;
pub(crate) mod filebasedsource;
pub mod fileio;
pub(crate) mod filename_policy;
pub(crate) mod filesystem;
pub(crate) mod sink;
pub mod textio;
pub(crate) mod write_files;

pub use filebasedsource::{
    DEFAULT_SPLIT_SIZE, FileBasedSource, FileBasedSourceFn, FileRecordReader,
    ReadAllViaFileBasedSource, file_initial_restriction, file_split_restriction,
};
pub use filename_policy::{
    DefaultFilenamePolicy, FileNamingContext, FilenamePolicy, Windowing, format_shard_template,
};
pub use filesystem::{
    FileSystem, FileSystemRegistration, LocalFileSystem, exists, get_filesystem, read_to_bytes,
    read_to_string, register_filesystem, write_bytes,
};
/// Glob matching for filesystem implementations in other crates.
pub use filesystem::{GlobError, GlobMatcher};
#[doc(hidden)]
pub use filesystem::{glob_match, parse_scheme};
pub use sink::{FileFormat, FileSink, FileSinkWriter, FormatSink, TextFormat};
pub use write_files::{DEFAULT_SHARD_TEMPLATE, WriteFiles};
#[doc(hidden)]
pub use write_files::{FileResult, Finalizer, RoundRobinShards, WriteFilesOptions, WriterConfig};
