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

//! The checks of the read options of the native Storage Read connector, which make no call
//! to BigQuery.

use gcp::bigquery::storage::read::ReadOptions;

#[test]
fn invalid_session_settings_fail_before_any_call() {
    let cases = [
        (
            ReadOptions::new("d.t")
                .with_max_stream_count(10)
                .with_preferred_min_stream_count(20),
            "preferred_min_stream_count 20 is greater than max_stream_count 10",
        ),
        (
            ReadOptions::new("d.t").with_snapshot_time("yesterday"),
            "snapshot_time 'yesterday' is not RFC 3339",
        ),
        (
            ReadOptions::from_query("SELECT 1").with_snapshot_time("2024-05-01T00:00:00Z"),
            "apply to table reads only",
        ),
    ];
    for (options, expected) in cases {
        let error = options.validate().unwrap_err().to_string();
        assert!(error.contains(expected), "{options:?}: {error}");
    }
}

#[test]
fn valid_session_settings_pass() {
    let options = ReadOptions::new("d.t")
        .with_snapshot_time("2024-05-01T12:30:00.5Z")
        .with_preferred_min_stream_count(4);
    options.validate().unwrap();
    assert!(options.snapshot_timestamp().unwrap().is_some());
    ReadOptions::new("d.t")
        .with_preferred_min_stream_count(20)
        .validate()
        .unwrap();
}
