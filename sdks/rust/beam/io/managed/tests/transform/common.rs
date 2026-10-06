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

//! Shared helpers for the Managed tests: config decoding and the mock Managed expansion
//! service.

// Each test binary uses a different subset of these helpers.
#![allow(
    dead_code,
    reason = "each test binary uses a different subset of these helpers"
)]

use beam::prelude::*;
use managed_io::{SNAPSHOTS, urns};
use serde_json::Value;
use testutils::xlang::{MockExpansionServer, MockExpansionService, Route};

pub fn config_json(row: &Row) -> Value {
    let config = row
        .get_string("config")
        .expect("valid test payload")
        .expect("inline config present");
    serde_json::from_str(config).expect("valid test payload")
}

/// Starts a mock of the Managed expansion service. Reads return `output`, an Iceberg write
/// returns `snapshots`, and each transform also returns the output that its
/// `error_handling` config names.
pub fn start_mock_service() -> MockExpansionServer {
    MockExpansionService::new()
        .with_route(urns::ICEBERG_READ, Route::source())
        .with_route(urns::KAFKA_READ, Route::source())
        .with_route(urns::ICEBERG_WRITE, Route::sink().with_outputs([SNAPSHOTS]))
        .with_route(urns::KAFKA_WRITE, Route::sink())
        .with_error_output_from_managed_config()
        .start()
}

/// (underlying transform URN, inline config) of each expansion request that `server` saw.
pub fn seen_targets(server: &MockExpansionServer) -> Vec<(String, Option<String>)> {
    server
        .seen()
        .into_iter()
        .map(|seen| (seen.target, seen.managed_config))
        .collect()
}
