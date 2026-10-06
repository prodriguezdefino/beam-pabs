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

//! Tests that need external resources named by environment variables.
//!
//! The Rust test harness has no runtime "skipped" outcome. A test that returns early
//! when, for example, `BEAM_TEST_GCS_BUCKET` is unset reports PASS, the same as a real
//! pass. [`require_env!`](crate::require_env) keeps the early return, but makes it
//! visible and controllable:
//!
//! - It prints a `SKIP` line with the test name and the missing variable.
//! - If [`STRICT_ENV_VAR`] is set to a value other than empty, `0` or `false`, it
//!   panics instead, so a CI job that must have the resources fails, not skips.
//!
//! ```no_run
//! #[test]
//! fn reads_from_gcs() {
//!     let bucket = testing::require_env!("BEAM_TEST_GCS_BUCKET");
//!     // ... use `bucket`
//! }
//!
//! #[test]
//! fn returns_a_result() -> Result<(), String> {
//!     let project = testing::require_env!("BEAM_TEST_GCP_PROJECT", Ok(()));
//!     // ... use `project`
//!     Ok(())
//! }
//! ```

/// Environment variable that converts missing required variables into test failures.
pub const STRICT_ENV_VAR: &str = "BEAM_TEST_REQUIRE_ENV";

/// Result of [`check_env`]: the value, or a decision to skip.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EnvRequirement {
    /// Variable is set to a non-empty value.
    Present(String),
    /// Variable is unset or empty and missing variables are allowed. Holds the `SKIP` message.
    Skip(String),
}

/// Reads `name`, treating empty values as unset.
///
/// Returns [`EnvRequirement::Skip`] with a `SKIP` message if the variable is missing.
/// Panics instead when [`STRICT_ENV_VAR`] asks for missing variables to fail.
pub fn check_env(name: &str) -> EnvRequirement {
    match std::env::var(name) {
        Ok(value) if !value.is_empty() => EnvRequirement::Present(value),
        _ => {
            let test = std::thread::current()
                .name()
                .unwrap_or("<unnamed test>")
                .to_string();
            if strict() {
                panic!(
                    "{test}: required environment variable {name} is not set, and \
                     {STRICT_ENV_VAR} forbids skipping"
                );
            }
            EnvRequirement::Skip(format!(
                "SKIP {test}: environment variable {name} is not set"
            ))
        }
    }
}

/// Returns the value of `name`, or `None` after printing a `SKIP` line to stderr.
///
/// Panics instead of skipping when [`STRICT_ENV_VAR`] is set. See
/// [`require_env!`](crate::require_env).
pub fn env_or_skip(name: &str) -> Option<String> {
    match check_env(name) {
        EnvRequirement::Present(value) => Some(value),
        EnvRequirement::Skip(message) => {
            eprintln!("{message}");
            None
        }
    }
}

fn strict() -> bool {
    std::env::var(STRICT_ENV_VAR)
        .is_ok_and(|v| !v.is_empty() && v != "0" && !v.eq_ignore_ascii_case("false"))
}

/// Evaluates to the value of an environment variable, or returns from the enclosing
/// test after printing a `SKIP` line.
///
/// On skip, `require_env!("VAR")` returns `()`, for tests that return nothing.
/// `require_env!("VAR", value)` returns `value`, for tests that return a `Result`.
/// Panics instead of skipping when [`STRICT_ENV_VAR`](crate::STRICT_ENV_VAR) is set.
#[macro_export]
macro_rules! require_env {
    ($name:expr) => {
        $crate::require_env!($name, ())
    };
    ($name:expr, $skipped:expr) => {
        match $crate::env_or_skip($name) {
            ::std::option::Option::Some(value) => value,
            ::std::option::Option::None => return $skipped,
        }
    };
}
