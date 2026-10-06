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

//! Tests for [`TEST_TIMEOUT_ENV`] parsing in [`test_timeout`].
//!
//! The variable is process-global. So each case runs this binary again as a child
//! process with its own environment, marked by [`CHILD_ENV`].

use std::process::Command;
use std::time::Duration;

use testutils::{DEFAULT_TEST_TIMEOUT, TEST_TIMEOUT_ENV, test_timeout};

/// Set in the environment of the child. The child prints [`test_timeout`] and exits.
const CHILD_ENV: &str = "BEAM_TEST_TIMEOUT_ENV_TEST_CHILD";
const TEST: &str = "the_timeout_override_accepts_only_positive_whole_seconds";
/// Prefix of the line on which the child prints its result.
const MARKER: &str = "test_timeout=";

/// Runs this test as a child with `value` in [`TEST_TIMEOUT_ENV`] (`None` leaves it unset).
/// Returns the timeout that the child read, or `Err(output)` if the child panicked.
fn child_timeout(value: Option<&str>) -> Result<Duration, String> {
    let exe = std::env::current_exe().expect("path of the running test binary");
    let mut cmd = Command::new(exe);
    cmd.args(["--exact", TEST, "--nocapture", "--test-threads=1"])
        .env(CHILD_ENV, "1");
    match value {
        Some(v) => cmd.env(TEST_TIMEOUT_ENV, v),
        None => cmd.env_remove(TEST_TIMEOUT_ENV),
    };
    let out = cmd.output().expect("run the child test process");
    let stdout = String::from_utf8_lossy(&out.stdout);
    if !out.status.success() {
        return Err(format!("{stdout}{}", String::from_utf8_lossy(&out.stderr)));
    }
    // libtest prints "test <name> ... " on the same line before the output of the child.
    let secs = stdout
        .lines()
        .find_map(|l| l.split_once(MARKER).map(|(_, secs)| secs.trim()))
        .unwrap_or_else(|| panic!("child printed no {MARKER} line:\n{stdout}"));
    Ok(Duration::from_secs(secs.parse().expect("whole seconds")))
}

#[test]
fn the_timeout_override_accepts_only_positive_whole_seconds() {
    if std::env::var_os(CHILD_ENV).is_some() {
        println!("{MARKER}{}", test_timeout().as_secs());
        return;
    }

    let accepted = [
        (None, DEFAULT_TEST_TIMEOUT),
        (Some("1"), Duration::from_secs(1)),
        (Some(" 45 "), Duration::from_secs(45)),
    ];
    for (value, expected) in accepted {
        assert_eq!(
            child_timeout(value),
            Ok(expected),
            "{TEST_TIMEOUT_ENV}={value:?}"
        );
    }

    for value in ["0", "-3", "1.5", "abc", ""] {
        let output = child_timeout(Some(value)).expect_err(value);
        assert!(
            output.contains("is not a positive whole number of seconds"),
            "{TEST_TIMEOUT_ENV}={value:?} should be rejected, got:\n{output}"
        );
    }
}
