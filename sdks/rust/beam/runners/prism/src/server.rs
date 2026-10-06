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

use std::net::{SocketAddr, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};
use thiserror::Error;
use tracing::{debug, info, warn};

pub const DEFAULT_PRISM_PORT: u16 = 8073;

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
pub fn find_prism_binary() -> Result<PathBuf, PrismServerError> {
    prism_from_env()
        .or_else(prism_from_cache)
        .or_else(prism_from_path)
        .ok_or(PrismServerError::BinaryNotFound)
}

/// Tests if a TCP endpoint is currently accepting connections.
pub fn is_endpoint_ready(addr: &SocketAddr) -> bool {
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

/// Manages a running Prism process.
pub struct PrismServer {
    endpoint: String,
    port: u16,
    child: Option<Child>,
}

/// Serializes auto-start, so two pipelines do not race for a port, and caches the process
/// it produced in the same lock, so the second pipeline reuses the first one's Prism. The
/// [`Weak`] stops the child when the last pipeline drops its handle.
static AUTO_STARTED_SERVER: tokio::sync::Mutex<Weak<PrismServer>> =
    tokio::sync::Mutex::const_new(Weak::new());

/// Where a Prism JobService should be reached.
enum PortChoice {
    /// A port that may already be served: connect to it if something answers.
    Existing(u16),
    /// A reserved ephemeral port and its listener. The OS can give the port to another
    /// process once the listener closes, so the listener lives until Prism is spawned.
    Reserved(u16, std::net::TcpListener),
}

/// Picks the port to use for a Prism JobService.
fn select_port(requested_port: Option<u16>) -> PortChoice {
    if let Some(port) = requested_port {
        return PortChoice::Existing(port);
    }

    let default_addr = SocketAddr::from(([127, 0, 0, 1], DEFAULT_PRISM_PORT));
    if is_endpoint_ready(&default_addr) {
        return PortChoice::Existing(DEFAULT_PRISM_PORT);
    }

    std::net::TcpListener::bind("127.0.0.1:0")
        .and_then(|listener| Ok((listener.local_addr()?.port(), listener)))
        .map_or(PortChoice::Existing(DEFAULT_PRISM_PORT), |(port, l)| {
            PortChoice::Reserved(port, l)
        })
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

    /// Connects to whatever is serving the selected port, or spawns Prism on it.
    async fn connect_or_spawn(requested_port: Option<u16>) -> Result<Arc<Self>, PrismServerError> {
        let max_attempts = if requested_port.is_some() { 1 } else { 3 };
        let mut last_err = None;

        for attempt in 0..max_attempts {
            // Kept alive until the child is spawned; see `PortChoice::Reserved`.
            let reservation;
            let port = match select_port(requested_port) {
                PortChoice::Existing(port) => {
                    let addr = SocketAddr::from(([127, 0, 0, 1], port));
                    if is_endpoint_ready(&addr) {
                        let endpoint = format!("http://127.0.0.1:{port}");
                        info!("Connected to existing Prism runner on {}", endpoint);
                        return Ok(Arc::new(Self {
                            endpoint,
                            port,
                            child: None,
                        }));
                    }
                    reservation = None;
                    port
                }
                PortChoice::Reserved(port, listener) => {
                    reservation = Some(listener);
                    port
                }
            };

            let addr = SocketAddr::from(([127, 0, 0, 1], port));
            let endpoint = format!("http://127.0.0.1:{port}");

            let prism_bin = find_prism_binary()?;
            info!(
                "Spawning Prism runner ({}) on port {} (attempt {}/{})",
                prism_bin.display(),
                port,
                attempt + 1,
                max_attempts
            );

            let mut cmd = Command::new(&prism_bin);
            configure_docker_host(&mut cmd);
            cmd.arg(format!("--job_port={port}"))
                .arg("--serve_http=false")
                .arg("--idle_shutdown_timeout=60s")
                .stdout(Stdio::null())
                .stderr(Stdio::inherit());

            // Release the reserved port as late as possible, so no other process claims it.
            drop(reservation);
            let child = match cmd.spawn() {
                Ok(c) => c,
                Err(e) => {
                    warn!("Failed to spawn Prism runner on port {port}: {e}");
                    last_err = Some(PrismServerError::SpawnFailed(e));
                    continue;
                }
            };

            let mut server = Self {
                endpoint: endpoint.clone(),
                port,
                child: Some(child),
            };

            // Wait for port to become active (generous timeout under parallel load)
            let start = Instant::now();
            let timeout = Duration::from_secs(20);
            let poll_interval = Duration::from_millis(50);
            let mut ready = false;

            while start.elapsed() < timeout {
                // Check if child exited prematurely
                let exit_status = server
                    .child
                    .as_mut()
                    .and_then(|c| c.try_wait().ok().flatten());
                if let Some(status) = exit_status {
                    warn!(
                        "Prism process exited prematurely on port {} with status: {status}, retrying on new port",
                        port
                    );
                    last_err = Some(PrismServerError::SpawnFailed(std::io::Error::other(
                        format!("Prism process exited prematurely with status: {status}"),
                    )));
                    break;
                }

                if is_endpoint_ready(&addr) {
                    info!("Prism runner is ready on {}", endpoint);
                    ready = true;
                    break;
                }

                tokio::time::sleep(poll_interval).await;
            }

            if ready {
                return Ok(Arc::new(server));
            } else if last_err.is_none() {
                last_err = Some(PrismServerError::Timeout(endpoint));
            }
        }

        Err(last_err.unwrap_or_else(|| PrismServerError::Timeout("unknown".to_string())))
    }

    /// Whether the JobService still accepts connections. A cached handle can outlive its
    /// child: Prism exits when idle and can crash.
    fn is_serving(&self) -> bool {
        is_endpoint_ready(&SocketAddr::from(([127, 0, 0, 1], self.port)))
    }

    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    pub fn port(&self) -> u16 {
        self.port
    }
}

impl Drop for PrismServer {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            debug!(
                "Terminating Prism runner child process (pid: {})",
                child.id()
            );
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}
