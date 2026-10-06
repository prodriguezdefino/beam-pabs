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

//! Starts and stops Java expansion services for automated targets such as
//! `autojava::sdks:java:io:google-cloud-platform:expansion-service:runExpansionService`.
//!
//! The JAR comes from a local Beam build, else `~/.apache_beam/cache/jars/`, else Maven
//! Central. The service runs as `java -jar <jar> <port>` and is ready when the port accepts
//! connections. [`Drop`] kills and reaps the process.

use crate::artifact::*;
use std::io;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use tracing::{debug, info, warn};

/// Locates the `java` binary, checking `JAVA_HOME` first.
pub fn find_java_executable() -> Result<PathBuf, AutoServiceError> {
    std::env::var("JAVA_HOME")
        .ok()
        .map(|home| PathBuf::from(home).join("bin/java"))
        .filter(|p| p.is_file())
        .or_else(|| {
            Command::new("which")
                .arg("java")
                .output()
                .ok()
                .filter(|out| out.status.success())
                .map(|out| PathBuf::from(String::from_utf8_lossy(&out.stdout).trim()))
                .filter(|p| p.is_file())
        })
        .or_else(|| {
            Command::new("java")
                .arg("-version")
                .output()
                .ok()
                .map(|_| PathBuf::from("java"))
        })
        .ok_or(AutoServiceError::JavaNotFound)
}

/// Time a spawned service has to accept connections.
const STARTUP_TIMEOUT: Duration = Duration::from_secs(30);

const READINESS_POLL_INTERVAL: Duration = Duration::from_millis(100);

pub const MAX_STARTUP_ATTEMPTS: u32 = 3;

/// Picks a free port on `127.0.0.1` and releases it. Another process can take the port before
/// the JVM binds it, and `java -jar` cannot take a bound socket, so
/// [`JavaExpansionServer::start`] retries with a new port.
fn reserve_ephemeral_port() -> Result<u16, io::Error> {
    TcpListener::bind("127.0.0.1:0").and_then(|l| l.local_addr().map(|addr| addr.port()))
}

fn is_endpoint_ready(addr: &SocketAddr) -> bool {
    TcpStream::connect_timeout(addr, Duration::from_millis(100)).is_ok()
}

/// Spawns the Java expansion service process on the given port.
pub fn spawn_expansion_process(
    jar_path: &Path,
    port: u16,
) -> Result<(Child, SocketAddr, String), AutoServiceError> {
    let java_path = find_java_executable()?;
    let endpoint = format!("127.0.0.1:{port}");
    let socket_addr = SocketAddr::from(([127, 0, 0, 1], port));

    info!(
        "Starting Java Expansion Service on {} from {}",
        endpoint,
        jar_path.display()
    );

    let mut cmd = Command::new(java_path);
    cmd.arg("-jar")
        .arg(jar_path)
        .arg(port.to_string())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        cmd.process_group(0);
    }

    let child = cmd.spawn().map_err(AutoServiceError::Launch)?;

    register_jvm_pid(child.id());

    Ok((child, socket_addr, endpoint))
}

/// Result of a single readiness poll against a starting service.
pub enum Readiness {
    Ready,
    Waiting,
    /// The process exited, or the startup deadline elapsed.
    Failed(AutoServiceError),
}

/// One spawn-and-poll attempt to start a service. A trait so that tests can fake a lost port
/// race ("fails, then binds").
pub trait StartupAttempt: Sized {
    type Output;

    /// Port this attempt asked the service to bind.
    fn port(&self) -> u16;

    fn poll(&mut self) -> Readiness;

    fn into_ready(self) -> Self::Output;
}

/// Spawned service that has not accepted a connection yet. Dropping it reaps the child process
/// through the [`JavaExpansionServer`] drop.
struct PendingServer {
    server: JavaExpansionServer,
    socket_addr: SocketAddr,
    deadline: Instant,
}

impl PendingServer {
    fn spawn(jar_path: &Path) -> Result<Self, AutoServiceError> {
        let port = reserve_ephemeral_port()?;
        let (child, socket_addr, endpoint) = spawn_expansion_process(jar_path, port)?;
        Ok(Self {
            server: JavaExpansionServer {
                endpoint,
                port,
                jar_path: jar_path.to_path_buf(),
                child: Some(child),
            },
            socket_addr,
            deadline: Instant::now() + STARTUP_TIMEOUT,
        })
    }
}

impl StartupAttempt for PendingServer {
    type Output = JavaExpansionServer;

    fn port(&self) -> u16 {
        self.server.port
    }

    /// An early exit means a lost port race, so it is [`Readiness::Failed`] (retry on a new
    /// port), not an error.
    fn poll(&mut self) -> Readiness {
        if let Some(child) = self.server.child.as_mut()
            && let Ok(Some(status)) = child.try_wait()
        {
            return Readiness::Failed(AutoServiceError::SpawnFailed(format!(
                "Java expansion service on {} exited during startup with status: {status}",
                self.server.endpoint
            )));
        }

        if is_endpoint_ready(&self.socket_addr) {
            Readiness::Ready
        } else if Instant::now() >= self.deadline {
            Readiness::Failed(AutoServiceError::Timeout(self.server.endpoint.clone()))
        } else {
            Readiness::Waiting
        }
    }

    fn into_ready(self) -> JavaExpansionServer {
        info!(
            "Java Expansion Service is ready on {}",
            self.server.endpoint
        );
        self.server
    }
}

/// Bounded retry budget for service startup.
struct StartupAttempts {
    remaining: u32,
}

impl StartupAttempts {
    fn new() -> Self {
        Self {
            remaining: MAX_STARTUP_ATTEMPTS,
        }
    }

    /// Consumes one attempt and returns the last failure once the budget is spent. The warning
    /// logs `port`, so repeated warnings with the same port show that retries do not re-pick.
    fn record(&mut self, port: u16, failure: AutoServiceError) -> Result<(), AutoServiceError> {
        self.remaining -= 1;
        if self.remaining == 0 {
            return Err(failure);
        }
        warn!(
            port,
            attempts_left = self.remaining,
            "Java Expansion Service failed to start ({failure}); retrying on a fresh port"
        );
        Ok(())
    }
}

/// Spawns attempts until one is ready, up to [`MAX_STARTUP_ATTEMPTS`]. Drops (and so reaps) each
/// failed attempt before the next. An error from `spawn` returns at once, without a retry.
pub async fn drive_startup<A: StartupAttempt>(
    mut spawn: impl FnMut() -> Result<A, AutoServiceError>,
) -> Result<A::Output, AutoServiceError> {
    let mut attempts = StartupAttempts::new();

    loop {
        let mut pending = spawn()?;
        let failure = loop {
            match pending.poll() {
                Readiness::Ready => return Ok(pending.into_ready()),
                Readiness::Failed(err) => break err,
                Readiness::Waiting => tokio::time::sleep(READINESS_POLL_INTERVAL).await,
            }
        };
        attempts.record(pending.port(), failure)?;
    }
}

/// Blocking [`drive_startup`]: sleeps the calling thread between polls.
pub fn drive_startup_blocking<A: StartupAttempt>(
    mut spawn: impl FnMut() -> Result<A, AutoServiceError>,
) -> Result<A::Output, AutoServiceError> {
    let mut attempts = StartupAttempts::new();

    loop {
        let mut pending = spawn()?;
        let failure = loop {
            match pending.poll() {
                Readiness::Ready => return Ok(pending.into_ready()),
                Readiness::Failed(err) => break err,
                Readiness::Waiting => std::thread::sleep(READINESS_POLL_INTERVAL),
            }
        };
        attempts.record(pending.port(), failure)?;
    }
}

/// Manages the lifecycle of a locally spawned Java expansion service process.
#[derive(Debug)]
pub struct JavaExpansionServer {
    endpoint: String,
    port: u16,
    jar_path: PathBuf,
    child: Option<Child>,
}

impl JavaExpansionServer {
    /// Resolves the target to a JAR, then calls [`start_with_jar`](Self::start_with_jar).
    ///
    /// Resolution runs on a blocking worker: a synchronous download of 100 MB or more would stall
    /// the runtime. Polling stays async, so dropping this future still reaps a half-started JVM.
    pub async fn start(target: &str) -> Result<Self, AutoServiceError> {
        let artifact = JavaExpansionArtifact::from_target(target)?;
        let jar_path = tokio::task::spawn_blocking(move || artifact.resolve_jar())
            .await
            .map_err(AutoServiceError::ResolutionTask)??;
        Self::start_with_jar(&jar_path).await
    }

    /// Makes up to [`MAX_STARTUP_ATTEMPTS`] attempts, each on a new port, so a lost port race
    /// recovers.
    pub async fn start_with_jar(jar_path: &Path) -> Result<Self, AutoServiceError> {
        drive_startup(|| PendingServer::spawn(jar_path)).await
    }

    /// Blocking [`start`](Self::start), including JAR resolution.
    pub fn start_blocking(target: &str) -> Result<Self, AutoServiceError> {
        let jar_path = JavaExpansionArtifact::from_target(target)?.resolve_jar()?;
        Self::start_blocking_with_jar(&jar_path)
    }

    /// Blocking [`start_with_jar`](Self::start_with_jar).
    pub fn start_blocking_with_jar(jar_path: &Path) -> Result<Self, AutoServiceError> {
        drive_startup_blocking(|| PendingServer::spawn(jar_path))
    }

    /// `host:port` this service listens on.
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    pub fn jar_path(&self) -> &Path {
        &self.jar_path
    }
}

static ACTIVE_JVM_PIDS: std::sync::Mutex<Vec<u32>> = std::sync::Mutex::new(Vec::new());

#[cfg(unix)]
extern "C" fn cleanup_orphaned_jvms() {
    if let Ok(pids) = ACTIVE_JVM_PIDS.lock() {
        for &pid in pids.iter() {
            unsafe {
                // Kill process group (negative PID) then direct PID
                libc::kill(-(pid as i32), libc::SIGKILL);
                libc::kill(pid as i32, libc::SIGKILL);
            }
        }
    }
}

fn register_jvm_pid(pid: u32) {
    #[cfg(unix)]
    {
        static ATEXIT_INIT: std::sync::Once = std::sync::Once::new();
        ATEXIT_INIT.call_once(|| unsafe {
            libc::atexit(cleanup_orphaned_jvms);
        });
    }
    if let Ok(mut pids) = ACTIVE_JVM_PIDS.lock() {
        pids.push(pid);
    }
}

fn deregister_jvm_pid(pid: u32) {
    if let Ok(mut pids) = ACTIVE_JVM_PIDS.lock() {
        pids.retain(|&p| p != pid);
    }
}

impl Drop for JavaExpansionServer {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let pid = child.id();
            deregister_jvm_pid(pid);
            debug!("Terminating Java Expansion Service process group (pid: {pid})");
            #[cfg(unix)]
            unsafe {
                // Kill process group to reap child JVM processes and threads
                libc::kill(-(pid as i32), libc::SIGKILL);
            }
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}
