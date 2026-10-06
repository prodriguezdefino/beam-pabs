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
//! Connectors live in core when they depend on nothing outside the standard
//! library, which is true of the local filesystem and text files. Connectors
//! requiring external clients — object stores, message brokers, databases —
//! belong in their own crates so that pipelines only pay for what they use.

pub mod filebasedsource;
pub mod fileio;
pub mod filename_policy;
pub mod filesystem;
pub mod sink;
pub mod textio;
pub mod write_files;

pub use filebasedsource::{
    DEFAULT_SPLIT_SIZE, FileBasedSource, FileBasedSourceFn, FileRecordReader,
    ReadAllViaFileBasedSource, file_initial_restriction, file_split_restriction,
};
pub use fileio::{EmptyMatchTreatment, FileMetadata, Match, ReadMatches, ReadableFile};
pub use filename_policy::{
    DefaultFilenamePolicy, FileNamingContext, FilenamePolicy, format_shard_template,
};
pub use filesystem::{
    FileSystem, FileSystemRegistration, LocalFileSystem, exists, get_filesystem, read_to_bytes,
    read_to_string, register_filesystem, write_bytes,
};
pub use sink::{FileFormat, FileSink, FileSinkWriter, FormatSink, TextFormat};
pub use write_files::{DEFAULT_SHARD_TEMPLATE, WriteFiles};
