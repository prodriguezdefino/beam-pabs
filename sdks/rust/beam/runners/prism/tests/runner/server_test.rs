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

//! Tests for Prism binary discovery (version ordering, cache selection) and attaching to an
//! already-running JobService.

use std::path::PathBuf;

use beam_runner_prism::{PrismServer, job_port_in_log_line, newest_prism_in, version_key};

/// A fresh, empty directory under the system temp dir, removed on drop.
struct TempDir(PathBuf);

impl TempDir {
    fn new(tag: &str) -> Self {
        let dir = std::env::temp_dir().join(format!(
            "beam_prism_server_test_{tag}_{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        Self(dir)
    }

    fn touch(&self, name: &str) -> PathBuf {
        let path = self.0.join(name);
        std::fs::write(&path, b"").unwrap();
        path
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[test]
fn version_key_extracts_numeric_runs() {
    assert_eq!(
        version_key("apache_beam-v2.60.0-prism-darwin-arm64"),
        vec![2, 60, 0, 64]
    );
    assert_eq!(version_key("prism"), Vec::<u64>::new());
    assert_eq!(version_key("v10"), vec![10]);
}

#[test]
fn version_key_orders_numerically_not_lexically() {
    let v2_9 = version_key("apache_beam-v2.9.0-prism-linux-amd64");
    let v2_10 = version_key("apache_beam-v2.10.0-prism-linux-amd64");
    assert!(v2_9 < v2_10, "{v2_9:?} should sort below {v2_10:?}");
    // A name with no digits loses to any versioned name.
    assert!(version_key("prism") < v2_9);
}

#[test]
fn newest_prism_in_picks_highest_version_and_skips_archives() {
    let dir = TempDir::new("newest");
    dir.touch("apache_beam-v2.9.0-prism-darwin-arm64");
    let newest = dir.touch("apache_beam-v2.10.0-prism-darwin-arm64");
    // Higher-versioned archives, non-prism files and directories must not be chosen.
    dir.touch("apache_beam-v2.99.0-prism-darwin-arm64.zip");
    dir.touch("apache_beam-v2.98.0-prism-darwin-arm64.tar.gz");
    dir.touch("apache_beam-v3.0.0-other-tool");
    std::fs::create_dir(dir.0.join("apache_beam-v4.0.0-prism-dir")).unwrap();

    assert_eq!(newest_prism_in(&dir.0), Some(newest));
}

#[test]
fn newest_prism_in_returns_none_without_candidates() {
    let dir = TempDir::new("empty");
    assert_eq!(newest_prism_in(&dir.0), None);
    dir.touch("apache_beam-v2.60.0-prism-darwin-arm64.zip");
    assert_eq!(newest_prism_in(&dir.0), None);
    assert_eq!(newest_prism_in(&dir.0.join("missing")), None);
}

#[tokio::test]
async fn start_or_connect_attaches_to_existing_server_on_requested_port() {
    // Anything accepting TCP on the requested port is treated as a running JobService.
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();

    let server = PrismServer::start_or_connect(Some(port)).await.unwrap();
    assert_eq!(server.port(), port);
    assert_eq!(server.endpoint(), format!("http://127.0.0.1:{port}"));

    // Dropping the handle must not affect a server it did not start.
    drop(server);
    assert!(std::net::TcpStream::connect(("127.0.0.1", port)).is_ok());
}

#[test]
fn job_port_in_log_line_reads_the_serving_line() {
    let line = "time=2026-11-15T22:05:09.577-08:00 level=INFO msg=\"Serving JobManagement\" \
                endpoint=localhost:49967";
    assert_eq!(job_port_in_log_line(line), Some(49967));
    assert_eq!(
        job_port_in_log_line("level=INFO msg=\"Serving JobManagement\" endpoint=\"[::1]:8073\""),
        Some(8073)
    );
    let other = "level=INFO msg=\"shutting down after being idle\" endpoint=localhost:1";
    assert_eq!(job_port_in_log_line(other), None);
    assert_eq!(
        job_port_in_log_line("level=INFO msg=\"Serving JobManagement\""),
        None
    );
}

#[tokio::test]
async fn a_spawned_prism_logs_its_own_port() {
    // A port that nothing serves, so the handle spawns Prism on it and reads it back.
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let server = PrismServer::start_or_connect(Some(port)).await.unwrap();
    assert_eq!(server.port(), port);
    drop(server);
    assert!(std::net::TcpStream::connect(("127.0.0.1", port)).is_err());
}

#[cfg(unix)]
#[test]
fn dropping_prism_child_kills_process_immediately() {
    use std::io::Read;
    use std::process::{Command, Stdio};
    use std::time::{Duration, Instant};

    use beam_runner_prism::PrismChild;

    let mut child = Command::new("sleep")
        .arg("60")
        .stdout(Stdio::piped())
        .spawn()
        .expect("spawn sleep");
    let mut stdout = child.stdout.take().expect("piped stdout");
    let guard = PrismChild::new(child);

    let start = Instant::now();
    drop(guard);

    let mut buf = Vec::new();
    stdout.read_to_end(&mut buf).expect("read closed pipe");
    assert!(buf.is_empty());
    assert!(
        start.elapsed() < Duration::from_secs(5),
        "child should exit immediately on drop, took {:?}",
        start.elapsed()
    );
}

#[cfg(unix)]
#[tokio::test]
async fn cancelling_future_holding_prism_child_kills_process() {
    use std::io::Read;
    use std::process::{Command, Stdio};
    use std::time::{Duration, Instant};

    use beam_runner_prism::PrismChild;

    let mut child = Command::new("sleep")
        .arg("60")
        .stdout(Stdio::piped())
        .spawn()
        .expect("spawn sleep");
    let mut stdout = child.stdout.take().expect("piped stdout");
    let guard = PrismChild::new(child);

    let startup = async move {
        let _guard = guard;
        std::future::pending::<()>().await;
    };
    let start = Instant::now();
    let timed_out = tokio::time::timeout(Duration::from_millis(20), startup).await;
    assert!(timed_out.is_err());

    let mut buf = Vec::new();
    stdout.read_to_end(&mut buf).expect("read closed pipe");
    assert!(buf.is_empty());
    assert!(
        start.elapsed() < Duration::from_secs(5),
        "child should exit immediately on cancel, took {:?}",
        start.elapsed()
    );
}
