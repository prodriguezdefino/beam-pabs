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

//! Checks the Avro bytes on disk against the Avro specification, independently
//! of our reader and of arrow-avro:
//!
//! - the object container header written by [`AvroSink`] is parsed by hand and
//!   its writer schema JSON and codec are compared to golden values;
//! - an uncompressed data block written by the sink is compared byte-for-byte
//!   with a hand-encoded record;
//! - a container file hand-encoded in this test (never produced by our sink) is
//!   decoded by our reader into exact values.
#![expect(clippy::unwrap_used, reason = "test fixtures panic on setup failure")]

mod block_scan;
mod common;
mod golden;
mod write_transform;
mod writer;
