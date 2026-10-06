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

use external::expansionx::*;
use std::cell::RefCell;

/// Scripted startup attempt. A test cannot cause a real port race, so this makes "one attempt
/// fails, a later one binds" testable.
struct ScriptedAttempt {
    port: u16,
    binds: bool,
}

impl StartupAttempt for ScriptedAttempt {
    /// Bound port, which shows which attempt succeeded.
    type Output = u16;

    fn port(&self) -> u16 {
        self.port
    }

    fn poll(&mut self) -> Readiness {
        if self.binds {
            Readiness::Ready
        } else {
            Readiness::Failed(AutoServiceError::SpawnFailed(format!(
                "scripted failure on port {}",
                self.port
            )))
        }
    }

    fn into_ready(self) -> u16 {
        self.port
    }
}

/// Records allocated ports and binds only on `bind_on` (0-based attempt index). Each attempt
/// gets a different port, as in the real driver.
fn script(
    bind_on: u32,
    ports: &RefCell<Vec<u16>>,
) -> impl FnMut() -> Result<ScriptedAttempt, AutoServiceError> + '_ {
    let mut index = 0u32;
    move || {
        let port = 20_000 + index as u16;
        ports.borrow_mut().push(port);
        let attempt = ScriptedAttempt {
            port,
            binds: index == bind_on,
        };
        index += 1;
        Ok(attempt)
    }
}

/// The first attempt fails, the next one binds.
#[test]
fn startup_recovers_when_a_later_attempt_binds() {
    let ports = RefCell::new(Vec::new());

    let bound = drive_startup_blocking(script(1, &ports)).expect("second attempt binds");

    assert_eq!(
        bound, 20_001,
        "the second attempt's port should be returned"
    );
    assert_eq!(
        ports.into_inner(),
        vec![20_000, 20_001],
        "exactly two attempts, each on its own port"
    );
}

/// Binding on the last permitted attempt succeeds ("three attempts", not "three retries").
#[test]
fn startup_succeeds_on_the_final_permitted_attempt() {
    let ports = RefCell::new(Vec::new());

    let bound = drive_startup_blocking(script(MAX_STARTUP_ATTEMPTS - 1, &ports))
        .expect("the last permitted attempt binds");

    assert_eq!(bound, 20_000 + (MAX_STARTUP_ATTEMPTS as u16 - 1));
    assert_eq!(
        ports.into_inner().len(),
        MAX_STARTUP_ATTEMPTS as usize,
        "the budget should allow exactly MAX_STARTUP_ATTEMPTS attempts"
    );
}

/// Startup fails, and does not loop forever, once the retry budget is spent.
#[test]
fn startup_gives_up_once_the_budget_is_spent() {
    let ports = RefCell::new(Vec::new());

    let err = drive_startup_blocking(script(MAX_STARTUP_ATTEMPTS, &ports))
        .expect_err("binding one attempt too late must not be reached");

    assert!(
        matches!(err, AutoServiceError::SpawnFailed(_)),
        "the last failure should be reported, got {err:?}"
    );
    assert_eq!(
        ports.into_inner().len(),
        MAX_STARTUP_ATTEMPTS as usize,
        "no more than MAX_STARTUP_ATTEMPTS attempts should be made"
    );
}

/// The async driver also recovers when a later attempt binds.
#[tokio::test]
async fn async_startup_recovers_when_a_later_attempt_binds() {
    let ports = RefCell::new(Vec::new());

    let bound = drive_startup(script(1, &ports))
        .await
        .expect("second attempt binds");

    assert_eq!(bound, 20_001);
    assert_eq!(ports.into_inner(), vec![20_000, 20_001]);
}
