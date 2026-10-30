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

//! Tests for Google Cloud Bigtable: cross-language SchemaTransforms, key arithmetic, target
//! routing headers (Tables, Materialized Views, Authorized Views, Data Boost), server-side
//! bitmask `RowFilter` builders, read/write unit tests, and end-to-end mock gRPC tests.

mod construction;
mod external;
mod filter;
mod key_and_target;
mod live;
mod mock_service;
mod read_units;
mod retry;
mod write_units;
