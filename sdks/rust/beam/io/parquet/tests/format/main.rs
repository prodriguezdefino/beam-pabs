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

//! Checks the Parquet files on disk independently of our reader and of the
//! `arrow_io` codec:
//!
//! - the file schema, key-value metadata, compression and raw column values
//!   written by [`ParquetSink`] are read back with the parquet crate's
//!   low-level metadata and record APIs;
//! - a file written here with the parquet crate's low-level column writers
//!   (dictionary-encoded strings, `DATE`, `TIMESTAMP(MICROS|MILLIS)`, 3-level
//!   `LIST` with an `element` child) is decoded by our reader into exact values;
//! - truncated and corrupt files are rejected with the expected errors.
#![expect(clippy::unwrap_used, reason = "test fixtures panic on setup failure")]

mod common;
mod corrupt;
mod file_layout;
mod golden;
mod write_transform;
