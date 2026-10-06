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

//! `init_logging` and `set_global_client`: the `RUST_LOG` level, delivery to the Fn API
//! client, and stdout only while no client is set. Both are process-global, so each case
//! re-runs this binary as a child, marked by [`CHILD_ENV`], with its own `RUST_LOG`.

use std::process::Command;

use harness::logging::{LOG_QUEUE_BATCHES, LoggingClient, init_logging, set_global_client};
use tokio::sync::mpsc;

/// Set in the child's environment; the child logs and prints what reached the client.
const CHILD_ENV: &str = "BEAM_LOGGING_INIT_TEST_CHILD";
const TEST: &str = "rust_log_sets_the_level_and_stdout_is_used_only_without_a_client";
/// Prefix of the lines on which the child reports entries the client received.
const SENT: &str = "SENT ";
const LEVELS: [&str; 5] = ["trace", "debug", "info", "warn", "error"];

/// Emits one event per level, each message tagged with `phase` and its level.
fn emit(phase: &str) {
    tracing::trace!("{phase}-trace");
    tracing::debug!("{phase}-debug");
    tracing::info!("{phase}-info");
    tracing::warn!("{phase}-warn");
    tracing::error!("{phase}-error");
}

/// Child side: logs before and after a client is set, then prints each entry it received.
async fn act() {
    init_logging();
    emit("before");
    let (tx, mut rx) = mpsc::channel(LOG_QUEUE_BATCHES);
    let client = LoggingClient::new(tx);
    set_global_client(client.clone());
    emit("after");
    client.flush().await;
    while let Ok(batch) = rx.try_recv() {
        for entry in batch.log_entries {
            println!("{SENT}{}", entry.message);
        }
    }
}

/// Re-runs this test as a child with `rust_log` (`None` leaves it unset); returns stdout.
fn child_stdout(rust_log: Option<&str>) -> String {
    let exe = std::env::current_exe().expect("path of the running test binary");
    let mut cmd = Command::new(exe);
    cmd.args(["--exact", TEST, "--nocapture", "--test-threads=1"])
        .env(CHILD_ENV, "1");
    match rust_log {
        Some(level) => cmd.env("RUST_LOG", level),
        None => cmd.env_remove("RUST_LOG"),
    };
    let out = cmd.output().expect("run the child test process");
    let stdout = String::from_utf8_lossy(&out.stdout).into_owned();
    assert!(
        out.status.success(),
        "child failed:\n{stdout}{}",
        String::from_utf8_lossy(&out.stderr)
    );
    stdout
}

#[tokio::test]
async fn rust_log_sets_the_level_and_stdout_is_used_only_without_a_client() {
    if std::env::var_os(CHILD_ENV).is_some() {
        act().await;
        return;
    }

    let cases = [
        (Some("trace"), "trace"),
        (Some("debug"), "debug"),
        (None, "info"),
        (Some("warn"), "warn"),
        (Some("error"), "error"),
    ];
    for (rust_log, lowest) in cases {
        let stdout = child_stdout(rust_log);
        let enabled = &LEVELS[LEVELS.iter().position(|l| *l == lowest).expect("a level")..];
        let (sent, printed): (Vec<&str>, Vec<&str>) =
            stdout.lines().partition(|l| l.starts_with(SENT));
        for phase in ["before", "after"] {
            for level in LEVELS {
                let message = format!("{phase}-{level}");
                assert_eq!(
                    sent.iter().any(|l| l.ends_with(&message)),
                    enabled.contains(&level),
                    "RUST_LOG={rust_log:?}: {message} sent to the client"
                );
            }
        }
        // Only this test's messages: `trace` also prints the runtime's own events.
        let printed_before = format!("before-{lowest}");
        assert!(
            printed.iter().any(|l| l.ends_with(&printed_before)),
            "RUST_LOG={rust_log:?}: {printed_before} printed while no client is set:\n{stdout}"
        );
        assert!(
            !printed.iter().any(|l| l.contains("after-")),
            "RUST_LOG={rust_log:?}: nothing printed once a client is set:\n{stdout}"
        );
    }
}
