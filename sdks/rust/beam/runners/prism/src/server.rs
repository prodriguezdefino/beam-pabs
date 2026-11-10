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

//! Prism server process management and health detection.

use std::io::{BufRead, BufReader, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStderr, Command, ExitStatus, Stdio};
use std::sync::{Arc, Mutex, Weak, mpsc};
use std::time::{Duration, Instant};
use thiserror::Error;
use tracing::{debug, info, warn};

pub(crate) const DEFAULT_PRISM_PORT: u16 = 8073;

#[derive(Error, Debug)]
pub enum PrismServerError {
    #[error("Prism binary not found. Set BEAM_PRISM_PATH or install Prism.")]
    BinaryNotFound,
    #[error("Failed to spawn Prism process: {0}")]
    SpawnFailed(#[from] std::io::Error),
    #[error("Timed out waiting for Prism to become ready on {0}")]
    Timeout(String),
}

/// Reads the Prism executable path from the `BEAM_PRISM_PATH` environment variable.
fn prism_from_env() -> Option<PathBuf> {
    let p = PathBuf::from(std::env::var("BEAM_PRISM_PATH").ok()?);
    p.is_file().then_some(p)
}

/// Looks for a downloaded Prism executable in `~/.apache_beam/cache/prism/bin/`.
///
/// The directory keeps one binary per downloaded release, named
/// `apache_beam-v<version>-prism-<os>-<arch>`. The newest wins, independent of directory
/// order. Set `BEAM_PRISM_PATH` to pin one.
fn prism_from_cache() -> Option<PathBuf> {
    let home = std::env::var_os("HOME")?;
    newest_prism_in(&Path::new(&home).join(".apache_beam/cache/prism/bin"))
}

/// Picks the newest Prism executable in `cache_dir`, skipping archives.
#[doc(hidden)]
pub fn newest_prism_in(cache_dir: &Path) -> Option<PathBuf> {
    let entries = std::fs::read_dir(cache_dir).ok()?;

    entries
        .flatten()
        .map(|entry| entry.path())
        .filter(|path| {
            let extension = path.extension().and_then(|e| e.to_str());
            path.is_file()
                && !matches!(extension, Some("zip" | "gz"))
                && path
                    .file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|name| name.contains("prism"))
        })
        .max_by_key(|path| {
            path.file_name()
                .and_then(|n| n.to_str())
                .map(version_key)
                .unwrap_or_default()
        })
}

/// Orders release names by version, so `v2.9.0` sorts below `v2.10.0`. Each run of digits
/// is one component; a name with no digits sorts below any name that has one.
#[doc(hidden)]
pub fn version_key(name: &str) -> Vec<u64> {
    name.split(|c: char| !c.is_ascii_digit())
        .filter_map(|part| part.parse().ok())
        .collect()
}

/// Looks for a Prism executable on the system `PATH`.
fn prism_from_path() -> Option<PathBuf> {
    let output = Command::new("which").arg("prism").output().ok()?;
    if !output.status.success() {
        return None;
    }
    let path_str = String::from_utf8_lossy(&output.stdout).trim().to_string();
    let p = PathBuf::from(path_str);
    p.is_file().then_some(p)
}

/// Locates the Prism executable on the local system.
pub(crate) fn find_prism_binary() -> Result<PathBuf, PrismServerError> {
    prism_from_env()
        .or_else(prism_from_cache)
        .or_else(prism_from_path)
        .ok_or(PrismServerError::BinaryNotFound)
}

/// Tests if a TCP endpoint is currently accepting connections.
pub(crate) fn is_endpoint_ready(addr: &SocketAddr) -> bool {
    TcpStream::connect_timeout(addr, Duration::from_millis(100)).is_ok()
}

/// Sets `DOCKER_HOST`, if unset, when Docker uses a non-standard socket path (such as
/// Colima, Docker Desktop, or OrbStack on macOS).
fn configure_docker_host(cmd: &mut Command) {
    if std::env::var("DOCKER_HOST").is_ok() {
        return;
    }
    if Path::new("/var/run/docker.sock").exists() {
        return;
    }
    let socket = std::env::var_os("HOME")
        .map(PathBuf::from)
        .and_then(|home| {
            [
                home.join(".colima/default/docker.sock"),
                home.join(".docker/run/docker.sock"),
                home.join(".orbstack/run/docker.sock"),
            ]
            .into_iter()
            .find(|candidate| candidate.exists())
        });
    if let Some(socket) = socket {
        cmd.env("DOCKER_HOST", format!("unix://{}", socket.display()));
    }
}

/// Guard for a spawned Prism child process that kills and reaps it on drop.
#[doc(hidden)]
pub struct PrismChild(Option<Child>);

impl PrismChild {
    /// Wraps `child` so dropping this guard kills and reaps the process.
    pub fn new(child: Child) -> Self {
        Self(Some(child))
    }

    fn take_stderr(&mut self) -> Option<ChildStderr> {
        self.0.as_mut().and_then(|child| child.stderr.take())
    }

    fn try_wait(&mut self) -> std::io::Result<Option<ExitStatus>> {
        match self.0.as_mut() {
            Some(child) => child.try_wait(),
            None => Ok(None),
        }
    }
}

impl Drop for PrismChild {
    fn drop(&mut self) {
        if let Some(mut child) = self.0.take() {
            debug!("Stopping Prism runner child process (pid: {})", child.id());
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

/// Manages a running Prism process.
pub struct PrismServer {
    endpoint: String,
    port: u16,
    /// The process that this handle spawned. The lock lets a shared handle check that the
    /// process still runs.
    child: Option<Mutex<PrismChild>>,
}

/// Serializes auto-start so two pipelines do not race for a port. Holds the child process
/// in the same lock so the second pipeline reuses the active Prism server. The [`Weak`]
/// stops the child when the last pipeline drops its handle.
static AUTO_STARTED_SERVER: tokio::sync::Mutex<Weak<PrismServer>> =
    tokio::sync::Mutex::const_new(Weak::new());

/// How long a spawned Prism has to log its job port and accept connections on it.
const READY_TIMEOUT: Duration = Duration::from_secs(20);
const READY_POLL_INTERVAL: Duration = Duration::from_millis(50);

/// Where a Prism JobService should be reached.
enum PortChoice {
    /// A port that may already be served: connect to it if something answers.
    Existing(u16),
    /// No server answers: spawn Prism on a port that it picks itself.
    Spawn,
}

/// Picks the port to use for a Prism JobService.
fn select_port(requested_port: Option<u16>) -> PortChoice {
    match requested_port {
        Some(port) => PortChoice::Existing(port),
        None if is_endpoint_ready(&local_addr(DEFAULT_PRISM_PORT)) => {
            PortChoice::Existing(DEFAULT_PRISM_PORT)
        }
        None => PortChoice::Spawn,
    }
}

fn local_addr(port: u16) -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], port))
}

/// Reads the job port from the `Serving JobManagement` line that Prism logs with
/// `--log_kind=text`.
#[doc(hidden)]
pub fn job_port_in_log_line(line: &str) -> Option<u16> {
    if !line.contains("msg=\"Serving JobManagement\"") {
        return None;
    }
    line.split_whitespace()
        .find_map(|field| field.strip_prefix("endpoint="))
        .and_then(|endpoint| endpoint.trim_matches('"').rsplit_once(':'))
        .and_then(|(_, port)| port.parse().ok())
}

/// Copies the stderr of Prism to the stderr of this process. Sends the job port when Prism
/// logs it.
fn forward_stderr(stderr: ChildStderr) -> std::io::Result<mpsc::Receiver<u16>> {
    let (port_tx, port_rx) = mpsc::channel();
    std::thread::Builder::new()
        .name("prism-stderr".to_string())
        .spawn(move || {
            let mut out = std::io::stderr();
            BufReader::new(stderr)
                .lines()
                .map_while(Result::ok)
                .for_each(|line| {
                    let _ = writeln!(out, "{line}");
                    if let Some(port) = job_port_in_log_line(&line) {
                        let _ = port_tx.send(port);
                    }
                });
        })?;
    Ok(port_rx)
}

/// Waits until `child` logs its job port and accepts connections on it.
async fn wait_for_job_port(
    child: &mut PrismChild,
    port_rx: mpsc::Receiver<u16>,
) -> Result<u16, PrismServerError> {
    let deadline = Instant::now() + READY_TIMEOUT;
    let mut port = None;
    while Instant::now() < deadline {
        if let Some(status) = child.try_wait()? {
            return Err(PrismServerError::SpawnFailed(std::io::Error::other(
                format!("Prism process exited prematurely with status: {status}"),
            )));
        }
        port = port.or_else(|| port_rx.try_recv().ok());
        if let Some(port) = port.filter(|&port| is_endpoint_ready(&local_addr(port))) {
            return Ok(port);
        }
        tokio::time::sleep(READY_POLL_INTERVAL).await;
    }
    Err(PrismServerError::Timeout(port.map_or_else(
        || "a job port that Prism did not log".to_string(),
        |port| format!("http://127.0.0.1:{port}"),
    )))
}

impl PrismServer {
    /// Connects to an existing Prism server or starts a new local instance.
    ///
    /// When `requested_port` is `None` the handle may be shared with other pipelines in this
    /// process. A pinned port is not shared.
    pub async fn start_or_connect(
        requested_port: Option<u16>,
    ) -> Result<Arc<Self>, PrismServerError> {
        if requested_port.is_some() {
            return Self::connect_or_spawn(requested_port).await;
        }

        let mut auto_started = AUTO_STARTED_SERVER.lock().await;
        if let Some(server) = auto_started.upgrade().filter(|s| s.is_serving()) {
            debug!(
                "Reusing Prism runner already started on {}",
                server.endpoint
            );
            return Ok(server);
        }

        let server = Self::connect_or_spawn(None).await?;
        *auto_started = Arc::downgrade(&server);
        Ok(server)
    }

    /// Connects to whatever is serving the selected port, or spawns Prism.
    async fn connect_or_spawn(requested_port: Option<u16>) -> Result<Arc<Self>, PrismServerError> {
        let max_attempts = if requested_port.is_some() { 1 } else { 3 };
        let mut last_err = None;

        for attempt in 0..max_attempts {
            let job_port = match select_port(requested_port) {
                PortChoice::Existing(port) if is_endpoint_ready(&local_addr(port)) => {
                    let endpoint = format!("http://127.0.0.1:{port}");
                    info!("Connected to existing Prism runner on {}", endpoint);
                    return Ok(Arc::new(Self {
                        endpoint,
                        port,
                        child: None,
                    }));
                }
                PortChoice::Existing(port) => port,
                // Prism binds port 0 and logs the port that it got. No other process can take
                // that port between the choice and the bind.
                PortChoice::Spawn => 0,
            };

            let prism_bin = find_prism_binary()?;
            info!(
                "Spawning Prism runner ({}) with --job_port={} (attempt {}/{})",
                prism_bin.display(),
                job_port,
                attempt + 1,
                max_attempts
            );

            let mut cmd = Command::new(&prism_bin);
            configure_docker_host(&mut cmd);
            cmd.arg(format!("--job_port={job_port}"))
                .arg("--log_kind=text")
                .arg("--serve_http=false")
                .arg("--idle_shutdown_timeout=60s")
                .stdout(Stdio::null())
                .stderr(Stdio::piped());

            let mut child = match cmd.spawn() {
                Ok(child) => PrismChild::new(child),
                Err(e) => {
                    warn!("Failed to spawn Prism runner: {e}");
                    last_err = Some(PrismServerError::SpawnFailed(e));
                    continue;
                }
            };
            let ready = match child.take_stderr().map(forward_stderr) {
                Some(Ok(port_rx)) => wait_for_job_port(&mut child, port_rx).await,
                Some(Err(e)) => Err(PrismServerError::SpawnFailed(e)),
                None => Err(PrismServerError::SpawnFailed(std::io::Error::other(
                    "Prism stderr is not piped",
                ))),
            };
            match ready {
                Ok(port) => {
                    let endpoint = format!("http://127.0.0.1:{port}");
                    info!("Prism runner is ready on {}", endpoint);
                    return Ok(Arc::new(Self {
                        endpoint,
                        port,
                        child: Some(Mutex::new(child)),
                    }));
                }
                Err(e) => {
                    warn!("Prism runner did not start: {e}");
                    drop(child);
                    last_err = Some(e);
                }
            }
        }

        Err(last_err.unwrap_or_else(|| PrismServerError::Timeout("unknown".to_string())))
    }

    /// Whether this handle still reaches its Prism. A spawned child can exit when idle or
    /// crash, and then another process can get its port, so the child must still run.
    fn is_serving(&self) -> bool {
        let child_runs = self.child.as_ref().is_none_or(|child| {
            child
                .lock()
                .is_ok_and(|mut child| matches!(child.try_wait(), Ok(None)))
        });
        child_runs && is_endpoint_ready(&local_addr(self.port))
    }

    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    pub fn port(&self) -> u16 {
        self.port
    }
}
