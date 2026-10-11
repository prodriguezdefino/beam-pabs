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

//! A Java expansion service whose launcher exits early leaves no process of its group alive.
//! This binary sets `JAVA_HOME`, so it holds only this test.

#![cfg(unix)]

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::process::Command;
use std::time::{Duration, Instant};

use beam_external::{JavaExpansionServer, MAX_STARTUP_ATTEMPTS};

/// Whether the process `pid` exists. A killed child of the launcher stays a zombie until
/// init reaps it, so the caller waits for that.
fn is_alive(pid: &str) -> bool {
    Command::new("kill")
        .args(["-0", pid])
        .status()
        .is_ok_and(|status| status.success())
}

#[test]
fn an_early_exit_of_the_launcher_stops_the_jvm_children_of_its_group() {
    let dir = tempfile::tempdir().unwrap();
    let pid_file = dir.path().join("children");
    let bin = dir.path().join("bin");
    fs::create_dir(&bin).unwrap();
    // A fake `java` that starts a child in its process group and exits before it listens.
    let java = bin.join("java");
    fs::write(
        &java,
        format!(
            "#!/bin/sh\nsleep 600 &\necho $! >> '{}'\nexit 1\n",
            pid_file.display()
        ),
    )
    .unwrap();
    fs::set_permissions(&java, fs::Permissions::from_mode(0o755)).unwrap();
    // SAFETY: this binary has one test, and it sets the variable before it starts a thread.
    unsafe { std::env::set_var("JAVA_HOME", dir.path()) };

    let jar = dir.path().join("service.jar");
    fs::write(&jar, b"").unwrap();
    assert!(JavaExpansionServer::start_blocking_with_jar(&jar).is_err());

    let children = fs::read_to_string(&pid_file).unwrap();
    let pids: Vec<&str> = children.lines().collect();
    assert_eq!(pids.len(), MAX_STARTUP_ATTEMPTS as usize, "{children}");
    let deadline = Instant::now() + Duration::from_secs(30);
    while pids.iter().any(|pid| is_alive(pid)) {
        assert!(Instant::now() < deadline, "a child of {pids:?} still runs");
        std::thread::sleep(Duration::from_millis(50));
    }
}
