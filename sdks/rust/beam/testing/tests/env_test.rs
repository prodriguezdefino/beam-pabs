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

//! Tests for `require_env!` and friends.
//!
//! A single test, in a binary of its own, because it mutates the process environment.

use testing::{EnvRequirement, STRICT_ENV_VAR, check_env, env_or_skip, require_env};

const VAR: &str = "TESTUTILS_ENV_TEST_VARIABLE";

fn set(name: &str, value: &str) {
    // SAFETY: This is the only test in this binary. No other thread uses the environment.
    unsafe { std::env::set_var(name, value) };
}

fn unset(name: &str) {
    // SAFETY: Same as in `set`.
    unsafe { std::env::remove_var(name) };
}

fn value_or_skipped() -> String {
    let value = require_env!(VAR, "skipped".to_string());
    format!("got {value}")
}

fn unit_skip(reached: &mut bool) {
    let _value = require_env!(VAR);
    *reached = true;
}

#[test]
fn require_env_skips_visibly_and_fails_when_strict() {
    unset(STRICT_ENV_VAR);

    // Unset and empty variables both skip, with a message that names the test and the variable.
    unset(VAR);
    let expected = format!(
        "SKIP require_env_skips_visibly_and_fails_when_strict: environment variable {VAR} \
         is not set"
    );
    assert_eq!(check_env(VAR), EnvRequirement::Skip(expected.clone()));
    set(VAR, "");
    assert_eq!(check_env(VAR), EnvRequirement::Skip(expected));
    assert_eq!(env_or_skip(VAR), None);
    assert_eq!(value_or_skipped(), "skipped");
    let mut reached = false;
    unit_skip(&mut reached);
    assert!(
        !reached,
        "require_env! must return from the test when skipping"
    );

    // Present variable.
    set(VAR, "bucket");
    assert_eq!(
        check_env(VAR),
        EnvRequirement::Present("bucket".to_string())
    );
    assert_eq!(env_or_skip(VAR).as_deref(), Some("bucket"));
    assert_eq!(value_or_skipped(), "got bucket");
    unit_skip(&mut reached);
    assert!(reached);

    // Strict mode panics on missing variables.
    unset(VAR);
    set(STRICT_ENV_VAR, "1");
    let panic = std::panic::catch_unwind(value_or_skipped).expect_err("strict mode must panic");
    let message = panic
        .downcast_ref::<String>()
        .expect("panic payload should be a String");
    assert!(
        message.contains(&format!(
            "required environment variable {VAR} is not set, and {STRICT_ENV_VAR} forbids \
             skipping"
        )),
        "{message}"
    );

    // "0", "false" (any case) and an empty value leave strict mode off.
    for off in ["0", "false", "FALSE", ""] {
        set(STRICT_ENV_VAR, off);
        assert_eq!(value_or_skipped(), "skipped", "{STRICT_ENV_VAR}={off:?}");
    }
    unset(STRICT_ENV_VAR);
}
