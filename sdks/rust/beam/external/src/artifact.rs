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

use beam::pipeline::BEAM_RELEASE_VERSION;
use std::env;
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

struct ResolvedTarget {
    group_id: String,
    artifact_id: String,
    version: Option<String>,
    gradle_module_dir: Option<String>,
}

/// Metadata for a downloadable Beam Java expansion service artifact.
#[doc(hidden)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JavaExpansionArtifact {
    /// Gradle target or descriptor string.
    target: String,
    /// Maven artifact ID (e.g. `beam-sdks-java-io-google-cloud-platform-expansion-service`).
    artifact_id: String,
    /// Maven group ID (e.g. `org.apache.beam`).
    group_id: String,
    /// Beam version to fetch.
    version: String,
    /// Maven repository base URL.
    repository_url: String,
    /// Relative path within the Beam source repository to the module directory.
    gradle_module_dir: Option<String>,
    /// Optional cache directory override.
    cache_dir: Option<PathBuf>,
}

impl JavaExpansionArtifact {
    /// Resolves an expansion service target string into a [`JavaExpansionArtifact`].
    pub fn from_target(target: &str) -> Result<Self, AutoServiceError> {
        let clean = target
            .strip_prefix(AUTO_JAVA_PREFIX)
            .or_else(|| target.strip_prefix(AUTO_PREFIX))
            .unwrap_or(target)
            .trim();

        let resolved = if let Some(known) = resolve_known_target(clean) {
            ResolvedTarget {
                group_id: BEAM_GROUP_ID.to_string(),
                artifact_id: known.artifact_id.to_string(),
                version: None,
                gradle_module_dir: Some(known.gradle_module_dir.to_string()),
            }
        } else {
            parse_arbitrary_target(clean)?
        };

        let version = resolved
            .version
            .unwrap_or_else(|| BEAM_RELEASE_VERSION.to_string());

        Ok(Self {
            target: target.to_string(),
            artifact_id: resolved.artifact_id,
            group_id: resolved.group_id,
            version,
            repository_url: MAVEN_CENTRAL_REPO.to_string(),
            gradle_module_dir: resolved.gradle_module_dir,
            cache_dir: None,
        })
    }

    pub fn target(&self) -> &str {
        &self.target
    }

    pub fn artifact_id(&self) -> &str {
        &self.artifact_id
    }

    pub fn group_id(&self) -> &str {
        &self.group_id
    }

    pub fn version(&self) -> &str {
        &self.version
    }

    pub fn repository_url(&self) -> &str {
        &self.repository_url
    }

    pub fn gradle_module_dir(&self) -> Option<&str> {
        self.gradle_module_dir.as_deref()
    }

    /// Overrides the artifact version.
    #[must_use]
    pub fn with_version(mut self, version: impl Into<String>) -> Self {
        self.version = version.into();
        self
    }

    /// Overrides the repository URL for artifact resolution.
    #[must_use]
    pub fn with_repository_url(mut self, repository_url: impl Into<String>) -> Self {
        self.repository_url = repository_url.into();
        self
    }

    /// Overrides the cache directory where JARs are stored.
    #[must_use]
    pub fn with_cache_dir(mut self, cache_dir: impl Into<PathBuf>) -> Self {
        self.cache_dir = Some(cache_dir.into());
        self
    }

    /// Returns the cache directory where JARs are stored.
    pub fn cache_dir(&self) -> PathBuf {
        self.cache_dir.clone().unwrap_or_else(beam_jar_cache_dir)
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
fn parse_arbitrary_target(target: &str) -> Result<ResolvedTarget, AutoServiceError> {
    let parts: Vec<&str> = target.split(':').collect();
    if parts.len() == 3 && !target.starts_with(':') {
        return Ok(ResolvedTarget {
            group_id: parts[0].to_string(),
            artifact_id: parts[1].to_string(),
            version: Some(parts[2].to_string()),
            gradle_module_dir: None,
        });
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
    Ok(ResolvedTarget {
        group_id: BEAM_GROUP_ID.to_string(),
        artifact_id,
        version: None,
        gradle_module_dir: Some(module_dir),
    })
}

struct KnownExpansionTarget {
    artifact_id: &'static str,
    gradle_module_dir: &'static str,
}

const GCP_TARGET: KnownExpansionTarget = KnownExpansionTarget {
    artifact_id: "beam-sdks-java-io-google-cloud-platform-expansion-service",
    gradle_module_dir: "sdks/java/io/google-cloud-platform/expansion-service",
};

const IO_TARGET: KnownExpansionTarget = KnownExpansionTarget {
    artifact_id: "beam-sdks-java-io-expansion-service",
    gradle_module_dir: "sdks/java/io/expansion-service",
};

const SCHEMAIO_TARGET: KnownExpansionTarget = KnownExpansionTarget {
    artifact_id: "beam-sdks-java-extensions-schemaio-expansion-service",
    gradle_module_dir: "sdks/java/extensions/schemaio-expansion-service",
};

/// Resolves the canonical Gradle target or the service URN of a known expansion service.
/// Other Gradle spellings resolve to the same artifact through `parse_arbitrary_target`.
fn resolve_known_target(clean: &str) -> Option<&'static KnownExpansionTarget> {
    match clean {
        GCP_EXPANSION_SERVICE_TARGET | "beam:expansion:service:java:gcp" => Some(&GCP_TARGET),
        IO_EXPANSION_SERVICE_TARGET | "beam:expansion:service:java:io" => Some(&IO_TARGET),
        SCHEMAIO_EXPANSION_SERVICE_TARGET => Some(&SCHEMAIO_TARGET),
        _ => None,
    }
}

/// Creates an automated Java expansion service target string for a Gradle target.
#[doc(hidden)]
pub fn automated_java_target(gradle_target: &str) -> String {
    format!("{AUTO_JAVA_PREFIX}{gradle_target}")
}

/// Checks whether an expansion service string requests an automated service.
#[doc(hidden)]
pub fn is_automated_expansion_service(target: &str) -> bool {
    let t = target.trim();
    if t.starts_with(AUTO_JAVA_PREFIX) || t.starts_with(AUTO_PREFIX) {
        return true;
    }
    let clean = t
        .strip_prefix(AUTO_JAVA_PREFIX)
        .or_else(|| t.strip_prefix(AUTO_PREFIX))
        .unwrap_or(t)
        .trim();
    clean == "auto"
        || resolve_known_target(clean).is_some()
        || clean.starts_with(":sdks:java:")
        || clean.starts_with("sdks:java:")
}

fn find_beam_repo_root() -> Option<PathBuf> {
    std::iter::successors(env::current_dir().ok(), |path| {
        path.parent().map(Path::to_path_buf)
    })
    .find(|dir| dir.join("settings.gradle").is_file() || dir.join("settings.gradle.kts").is_file())
}

/// Resolves the Beam JAR cache directory with optional overrides.
#[doc(hidden)]
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
#[doc(hidden)]
pub fn beam_jar_cache_dir() -> PathBuf {
    resolve_beam_jar_cache_dir(
        env::var("BEAM_CACHE_DIR").ok().as_deref(),
        env::var_os("HOME").as_deref().map(Path::new),
    )
}
