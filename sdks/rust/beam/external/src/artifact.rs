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

//! Artifact fetching and cache logic for Java expansion services.

use std::fs::{self, File};
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use thiserror::Error;

use tracing::{debug, info, warn};

/// Default Maven Central repository base URL.
pub const MAVEN_CENTRAL_REPO: &str = "https://repo.maven.apache.org/maven2";

/// Canonical group ID for Apache Beam artifacts.
pub const BEAM_GROUP_ID: &str = "org.apache.beam";

/// Beam release downloaded for automated JARs; `BEAM_EXPANSION_SERVICE_VERSION` overrides it at
/// run time. This is the release that core resolves at build time from Gradle (see
/// [`beam::pipeline::BEAM_RELEASE_VERSION`]).
pub const DEFAULT_BEAM_RELEASE_VERSION: &str = beam::pipeline::BEAM_RELEASE_VERSION;

/// Canonical prefix for automated Java expansion service targets.
pub const AUTO_JAVA_PREFIX: &str = "autojava:";

/// Generic automated service prefix.
pub const AUTO_PREFIX: &str = "auto:";

/// Canonical Gradle target for the Java GCP Expansion Service (BigQuery, Spanner, PubSub).
pub const GCP_EXPANSION_SERVICE_TARGET: &str =
    ":sdks:java:io:google-cloud-platform:expansion-service:runExpansionService";

/// Canonical Gradle target for the Java Standard I/O Expansion Service (Kafka, Iceberg, JMS).
pub const IO_EXPANSION_SERVICE_TARGET: &str = ":sdks:java:io:expansion-service:runExpansionService";

/// Canonical Gradle target for the SchemaIO Expansion Service.
pub const SCHEMAIO_EXPANSION_SERVICE_TARGET: &str =
    ":sdks:java:extensions:schemaio-expansion-service:runExpansionService";

/// Errors during automated expansion service resolution and execution.
#[derive(Error, Debug)]
pub enum AutoServiceError {
    #[error("I/O error during automated service operation: {0}")]
    Io(#[from] io::Error),

    #[error("HTTP error downloading expansion service JAR from {url}: {source}")]
    Download {
        url: String,
        #[source]
        source: Box<ureq::Error>,
    },

    #[error("Java executable not found. Please install Java (JRE/JDK >= 11) or set JAVA_HOME.")]
    JavaNotFound,

    #[error("Failed to launch the Java process: {0}")]
    Launch(#[source] io::Error),

    #[error("JAR resolution task did not finish: {0}")]
    ResolutionTask(#[source] tokio::task::JoinError),

    #[error("Failed to start Java expansion service: {0}")]
    SpawnFailed(String),

    #[error("Timed out waiting for Java expansion service to become ready on {0}")]
    Timeout(String),

    #[error("Unsupported automated expansion service target: '{0}'")]
    InvalidTarget(String),
}

/// Metadata for a downloadable Beam Java expansion service artifact.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JavaExpansionArtifact {
    /// Gradle target or descriptor string.
    pub target: String,
    /// Maven artifact ID (e.g. `beam-sdks-java-io-google-cloud-platform-expansion-service`).
    pub artifact_id: String,
    /// Maven group ID (e.g. `org.apache.beam`).
    pub group_id: String,
    /// Beam version to fetch.
    pub version: String,
    /// Maven repository base URL.
    pub repository_url: String,
    /// Relative path within the Beam source repository to the module directory.
    pub gradle_module_dir: Option<String>,
    /// Optional cache directory override.
    pub cache_dir: Option<PathBuf>,
}

impl JavaExpansionArtifact {
    /// Resolves an expansion service target string into a [`JavaExpansionArtifact`].
    pub fn from_target(target: &str) -> Result<Self, AutoServiceError> {
        let version = std::env::var("BEAM_EXPANSION_SERVICE_VERSION")
            .unwrap_or_else(|_| DEFAULT_BEAM_RELEASE_VERSION.to_string());
        Self::from_target_and_version(target, version)
    }

    /// Resolves an expansion service target string with an explicit Beam version.
    pub fn from_target_and_version(
        target: &str,
        version: impl Into<String>,
    ) -> Result<Self, AutoServiceError> {
        let clean = target
            .strip_prefix(AUTO_JAVA_PREFIX)
            .or_else(|| target.strip_prefix(AUTO_PREFIX))
            .unwrap_or(target)
            .trim();

        let fallback_version = version.into();

        let (group_id, artifact_id, target_version, gradle_module_dir) = match clean {
            "gcp"
            | "bigquery"
            | "spanner"
            | "beam:expansion:service:java:gcp"
            | ":sdks:java:io:google-cloud-platform:expansion-service:runExpansionService"
            | ":sdks:java:io:google-cloud-platform:expansion-service:shadowJar"
            | "sdks:java:io:google-cloud-platform:expansion-service:shadowJar"
            | "sdks:java:io:google-cloud-platform:expansion-service" => (
                BEAM_GROUP_ID.to_string(),
                "beam-sdks-java-io-google-cloud-platform-expansion-service".to_string(),
                None,
                Some("sdks/java/io/google-cloud-platform/expansion-service".to_string()),
            ),
            "io"
            | "kafka"
            | "iceberg"
            | "beam:expansion:service:java:io"
            | ":sdks:java:io:expansion-service:runExpansionService"
            | ":sdks:java:io:expansion-service:shadowJar"
            | "sdks:java:io:expansion-service:shadowJar"
            | "sdks:java:io:expansion-service" => (
                BEAM_GROUP_ID.to_string(),
                "beam-sdks-java-io-expansion-service".to_string(),
                None,
                Some("sdks/java/io/expansion-service".to_string()),
            ),
            "schemaio"
            | ":sdks:java:extensions:schemaio-expansion-service:runExpansionService"
            | ":sdks:java:extensions:schemaio-expansion-service:shadowJar"
            | "sdks:java:extensions:schemaio-expansion-service:shadowJar"
            | "sdks:java:extensions:schemaio-expansion-service" => (
                BEAM_GROUP_ID.to_string(),
                "beam-sdks-java-extensions-schemaio-expansion-service".to_string(),
                None,
                Some("sdks/java/extensions/schemaio-expansion-service".to_string()),
            ),
            other => parse_arbitrary_target(other)?,
        };

        let resolved_version = target_version.unwrap_or(fallback_version);

        Ok(Self {
            target: target.to_string(),
            artifact_id,
            group_id,
            version: resolved_version,
            repository_url: MAVEN_CENTRAL_REPO.to_string(),
            gradle_module_dir,
            cache_dir: None,
        })
    }

    #[must_use]
    pub fn with_version(mut self, version: impl Into<String>) -> Self {
        self.version = version.into();
        self
    }

    #[must_use]
    pub fn with_repository_url(mut self, repository_url: impl Into<String>) -> Self {
        self.repository_url = repository_url.into();
        self
    }

    #[must_use]
    pub fn with_cache_dir(mut self, cache_dir: impl Into<PathBuf>) -> Self {
        self.cache_dir = Some(cache_dir.into());
        self
    }

    pub fn cache_dir(&self) -> PathBuf {
        self.cache_dir
            .clone()
            .unwrap_or_else(get_beam_jar_cache_dir)
    }

    /// Returns the standard JAR file name (e.g. `beam-sdks-java-io-google-cloud-platform-expansion-service-2.64.0.jar`).
    pub fn jar_name(&self) -> String {
        format!("{}-{}.jar", self.artifact_id, self.version)
    }

    /// Returns the Maven Central URL for downloading this JAR.
    pub fn maven_url(&self) -> String {
        let group_path = self.group_id.replace('.', "/");
        format!(
            "{}/{}/{}/{}/{}",
            self.repository_url.trim_end_matches('/'),
            group_path,
            self.artifact_id,
            self.version,
            self.jar_name()
        )
    }

    /// Returns the JAR path: local build, then cache, then a Maven Central download.
    pub fn resolve_jar(&self) -> Result<PathBuf, AutoServiceError> {
        self.find_prebuilt_jar()
            .or_else(|| self.find_cached_jar())
            .map(Ok)
            .unwrap_or_else(|| self.download_and_cache_jar())
    }

    /// Returns the name of the JAR a local Gradle build of this SDK version produces
    /// (e.g. `beam-sdks-java-io-expansion-service-2.78.0-SNAPSHOT.jar`).
    pub fn dev_jar_name(&self) -> String {
        let version = beam::pipeline::BEAM_SDK_VERSION.replace(".dev", "-SNAPSHOT");
        format!("{}-{version}.jar", self.artifact_id)
    }

    /// Locates the JAR built for this SDK version in the local Beam repository, if any.
    fn find_prebuilt_jar(&self) -> Option<PathBuf> {
        self.gradle_module_dir
            .as_deref()
            .zip(find_beam_repo_root())
            .map(|(mod_dir, root)| {
                root.join(mod_dir)
                    .join("build/libs")
                    .join(self.dev_jar_name())
            })
            .filter(|path| path.is_file())
            .inspect(|path| {
                info!(
                    "Using locally built expansion service JAR: {}",
                    path.display()
                );
            })
    }

    /// Locates a cached JAR in the local Beam cache directory.
    fn find_cached_jar(&self) -> Option<PathBuf> {
        let cached = self.cache_dir().join(self.jar_name());
        Some(cached)
            .filter(|path| path.is_file() && fs::metadata(path).is_ok_and(|m| m.len() > 0))
            .inspect(|path| {
                debug!("Found cached expansion service JAR: {}", path.display());
            })
    }

    /// Downloads the expansion service JAR from Maven Central into the local cache.
    fn download_and_cache_jar(&self) -> Result<PathBuf, AutoServiceError> {
        let cache_dir = self.cache_dir();
        fs::create_dir_all(&cache_dir)?;

        let target_jar = cache_dir.join(self.jar_name());
        // Unique per download so concurrent threads/processes never share a temp file.
        static DOWNLOAD_SEQ: AtomicU64 = AtomicU64::new(0);
        let temp_jar = cache_dir.join(format!(
            "{}.{}.{}.tmp",
            self.jar_name(),
            std::process::id(),
            DOWNLOAD_SEQ.fetch_add(1, Ordering::Relaxed)
        ));
        let url = self.maven_url();

        info!(
            "Downloading Java expansion service JAR from Maven Central: {} -> {}",
            url,
            target_jar.display()
        );
        warn!(
            "Downloading JAR from public Maven Central ({}). Consider pre-staging dependencies for production environments.",
            url
        );

        // ureq reports non-2xx statuses as errors, so reaching the body means success.
        let mut resp = ureq::get(&url)
            .call()
            .map_err(|e| AutoServiceError::Download {
                url: url.clone(),
                source: Box::new(e),
            })?;

        // No size limit; a JAR can have any size, so stream it to disk.
        let written = File::create(&temp_jar).and_then(|f| {
            let mut file = BufWriter::new(f);
            io::copy(&mut resp.body_mut().as_reader(), &mut file)?;
            file.flush()
        });
        if let Err(e) = written {
            let _ = fs::remove_file(&temp_jar);
            return Err(e.into());
        }

        if let Err(e) = fs::rename(&temp_jar, &target_jar) {
            let _ = fs::remove_file(&temp_jar);
            // A concurrent download may have already published a valid JAR.
            if fs::metadata(&target_jar).is_ok_and(|m| m.is_file() && m.len() > 0) {
                debug!(
                    "Concurrent download already cached JAR: {}",
                    target_jar.display()
                );
                return Ok(target_jar);
            }
            return Err(e.into());
        }
        info!(
            "Successfully cached expansion service JAR: {}",
            target_jar.display()
        );

        Ok(target_jar)
    }
}

/// Parses Maven coordinates (`group:artifact:version`) or Gradle targets (`:sdks:java:...`).
fn parse_arbitrary_target(
    target: &str,
) -> Result<(String, String, Option<String>, Option<String>), AutoServiceError> {
    let parts: Vec<&str> = target.split(':').collect();
    if parts.len() == 3 && !target.starts_with(':') {
        return Ok((
            parts[0].to_string(),
            parts[1].to_string(),
            Some(parts[2].to_string()),
            None,
        ));
    }

    let stripped = target
        .trim_start_matches(':')
        .trim_end_matches(":runExpansionService")
        .trim_end_matches(":shadowJar");

    if stripped.is_empty() {
        return Err(AutoServiceError::InvalidTarget(target.to_string()));
    }

    let artifact_id = format!("beam-{}", stripped.replace(':', "-"));
    let module_dir = stripped.replace(':', "/");
    Ok((
        BEAM_GROUP_ID.to_string(),
        artifact_id,
        None,
        Some(module_dir),
    ))
}

/// Creates an automated Java expansion service target string for a Gradle target.
pub fn use_automated_java_expansion_service(gradle_target: &str) -> String {
    format!("{AUTO_JAVA_PREFIX}{gradle_target}")
}

/// Checks whether an expansion service string requests an automated service.
pub fn is_automated_expansion_service(target: &str) -> bool {
    let t = target.trim();
    t.starts_with(AUTO_JAVA_PREFIX)
        || t.starts_with(AUTO_PREFIX)
        || matches!(t, "auto" | "gcp" | "bigquery" | "kafka" | "io" | "schemaio")
        || t.starts_with(":sdks:java:")
        || t.starts_with("sdks:java:")
}

fn find_beam_repo_root() -> Option<PathBuf> {
    std::iter::successors(std::env::current_dir().ok(), |path| {
        path.parent().map(Path::to_path_buf)
    })
    .find(|dir| dir.join("settings.gradle").is_file() || dir.join("settings.gradle.kts").is_file())
}

/// Resolves the Beam JAR cache directory with optional overrides.
pub fn resolve_beam_jar_cache_dir(
    beam_cache_dir: Option<&str>,
    home_dir: Option<&Path>,
) -> PathBuf {
    beam_cache_dir
        .map(|dir| PathBuf::from(dir).join("jars"))
        .or_else(|| home_dir.map(|home| home.join(".apache_beam/cache/jars")))
        .unwrap_or_else(|| PathBuf::from(".beam_cache/jars"))
}

/// Returns the default cache directory for downloaded expansion service JARs
/// (`~/.apache_beam/cache/jars`).
pub fn get_beam_jar_cache_dir() -> PathBuf {
    resolve_beam_jar_cache_dir(
        std::env::var("BEAM_CACHE_DIR").ok().as_deref(),
        std::env::var_os("HOME").as_deref().map(Path::new),
    )
}
