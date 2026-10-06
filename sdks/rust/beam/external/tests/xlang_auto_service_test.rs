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

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use external::ExpansionClient;
use external::expansionx::*;
use tracing::field::{Field, Visit};
use tracing_subscriber::Layer;
use tracing_subscriber::layer::{Context, SubscriberExt};

#[test]
fn test_is_automated_expansion_service() {
    assert!(is_automated_expansion_service(
        "autojava::sdks:java:io:google-cloud-platform:expansion-service:runExpansionService"
    ));
    assert!(is_automated_expansion_service("auto:gcp"));
    assert!(is_automated_expansion_service("auto:bigquery"));
    assert!(is_automated_expansion_service("auto:kafka"));
    assert!(is_automated_expansion_service(
        ":sdks:java:io:google-cloud-platform:expansion-service:runExpansionService"
    ));
    assert!(is_automated_expansion_service("gcp"));
    assert!(is_automated_expansion_service("bigquery"));

    assert!(!is_automated_expansion_service("localhost:8097"));
    assert!(!is_automated_expansion_service("127.0.0.1:8097"));
    assert!(!is_automated_expansion_service("http://localhost:8097"));
    assert!(!is_automated_expansion_service("https://10.0.0.1:8097"));
}

#[test]
fn test_use_automated_java_expansion_service_helper() {
    let target = use_automated_java_expansion_service(GCP_EXPANSION_SERVICE_TARGET);
    assert_eq!(
        target,
        "autojava::sdks:java:io:google-cloud-platform:expansion-service:runExpansionService"
    );
    assert!(is_automated_expansion_service(&target));
}

#[test]
fn test_gcp_artifact_resolution() {
    let artifact = JavaExpansionArtifact::from_target(
        "autojava::sdks:java:io:google-cloud-platform:expansion-service:runExpansionService",
    )
    .unwrap();

    assert_eq!(
        artifact.artifact_id,
        "beam-sdks-java-io-google-cloud-platform-expansion-service"
    );
    assert_eq!(artifact.group_id, "org.apache.beam");
    assert_eq!(
        artifact.gradle_module_dir,
        Some("sdks/java/io/google-cloud-platform/expansion-service".to_string())
    );
    assert_eq!(
        artifact.jar_name(),
        format!("{}-{}.jar", artifact.artifact_id, artifact.version)
    );
    assert!(artifact.maven_url().contains("repo.maven.apache.org"));
    assert!(artifact.maven_url().contains("org/apache/beam"));
}

#[test]
fn test_shorthand_targets() {
    let gcp = JavaExpansionArtifact::from_target("auto:gcp").unwrap();
    assert_eq!(
        gcp.artifact_id,
        "beam-sdks-java-io-google-cloud-platform-expansion-service"
    );

    let bq = JavaExpansionArtifact::from_target("bigquery").unwrap();
    assert_eq!(
        bq.artifact_id,
        "beam-sdks-java-io-google-cloud-platform-expansion-service"
    );

    let io = JavaExpansionArtifact::from_target("auto:io").unwrap();
    assert_eq!(io.artifact_id, "beam-sdks-java-io-expansion-service");

    let kafka = JavaExpansionArtifact::from_target("kafka").unwrap();
    assert_eq!(kafka.artifact_id, "beam-sdks-java-io-expansion-service");

    let schemaio = JavaExpansionArtifact::from_target("schemaio").unwrap();
    assert_eq!(
        schemaio.artifact_id,
        "beam-sdks-java-extensions-schemaio-expansion-service"
    );
}

#[test]
fn test_custom_maven_coordinate() {
    let artifact =
        JavaExpansionArtifact::from_target("com.example.beam:custom-expansion:1.2.3").unwrap();

    assert_eq!(artifact.group_id, "com.example.beam");
    assert_eq!(artifact.artifact_id, "custom-expansion");
    assert_eq!(artifact.version, "1.2.3");
    assert_eq!(artifact.jar_name(), "custom-expansion-1.2.3.jar");
    assert_eq!(
        artifact.maven_url(),
        "https://repo.maven.apache.org/maven2/com/example/beam/custom-expansion/1.2.3/custom-expansion-1.2.3.jar"
    );
}

/// Only the JAR of this SDK version is used, never another version left in `build/libs`.
#[test]
fn test_dev_jar_name_matches_the_local_gradle_build() {
    let io = JavaExpansionArtifact::from_target("io").unwrap();
    let version = beam::pipeline::BEAM_SDK_VERSION.replace(".dev", "-SNAPSHOT");
    assert_eq!(
        io.dev_jar_name(),
        format!("beam-sdks-java-io-expansion-service-{version}.jar")
    );
    assert!(!io.dev_jar_name().contains(".dev"), "{}", io.dev_jar_name());
}

#[test]
fn test_find_java_executable() {
    let java = find_java_executable();
    assert!(
        java.is_ok(),
        "Java executable must be discoverable on PATH or JAVA_HOME"
    );
}

#[test]
fn test_beam_jar_cache_dir() {
    let cache_dir = get_beam_jar_cache_dir();
    assert!(cache_dir.ends_with("jars"));
}

#[test]
fn test_download_and_cache_jar() {
    let artifact = JavaExpansionArtifact::from_target("auto:gcp").unwrap();
    let jar_path = artifact
        .resolve_jar()
        .expect("Failed to resolve or download JAR");
    assert!(jar_path.exists());
    assert!(std::fs::metadata(&jar_path).unwrap().len() > 10_000_000);
}

#[tokio::test]
async fn test_start_java_expansion_server() {
    let server = JavaExpansionServer::start("auto:gcp")
        .await
        .expect("Failed to start server");
    assert!(!server.endpoint().is_empty());
    assert!(server.port() > 0);
    drop(server);
}

#[tokio::test]
async fn test_expansion_client_with_auto_service() {
    let mut client = ExpansionClient::connect("auto:gcp")
        .await
        .expect("Failed to connect through automated expansion service");
    let resp = client
        .discover_schema_transforms()
        .await
        .expect("Failed to discover schema transforms");
    assert!(!resp.schema_transform_configs.is_empty());
    assert!(
        resp.schema_transform_configs
            .contains_key("beam:schematransform:org.apache.beam:bigquery_storage_read:v1")
    );
    assert!(
        resp.schema_transform_configs
            .contains_key("beam:schematransform:org.apache.beam:bigquery_storage_write:v2")
    );
    assert!(
        resp.schema_transform_configs
            .contains_key("beam:schematransform:org.apache.beam:generate_sequence:v1")
    );
}

#[test]
fn test_java_expansion_server_start_blocking() {
    let server = JavaExpansionServer::start_blocking("auto:gcp")
        .expect("Failed to start Java expansion server synchronously");
    assert!(!server.endpoint().is_empty());
    assert!(server.port() > 0);
    assert!(server.jar_path().exists());
    drop(server);
}

/// Collects the `port` field of every retry warning emitted on the current thread.
///
/// The retry decision is internal to the startup loop. Its warning is the only visible record
/// of the number of attempts and the port of each attempt.
#[derive(Clone, Default)]
struct RetryPorts(Arc<Mutex<Vec<u64>>>);

impl Visit for RetryPorts {
    fn record_u64(&mut self, field: &Field, value: u64) {
        if field.name() == "port" {
            self.0.lock().expect("retry port lock").push(value);
        }
    }

    fn record_debug(&mut self, _field: &Field, _value: &dyn std::fmt::Debug) {}
}

impl<S: tracing::Subscriber> Layer<S> for RetryPorts {
    fn on_event(&self, event: &tracing::Event<'_>, _ctx: Context<'_, S>) {
        event.record(&mut self.clone());
    }
}

/// Drives the startup retry loop to exhaustion and checks it re-picks a port each time.
///
/// A JVM given a non-JAR file exits at once, like a lost port race, so every attempt fails
/// deterministically. `service_test.rs` covers recovery with scripted attempts.
#[test]
fn test_startup_retries_on_a_fresh_port_until_the_budget_is_spent() {
    let jar = std::env::temp_dir().join("beam_rust_sdk_not_a_real_expansion_service.jar");
    std::fs::write(&jar, b"this is not a JAR").expect("write bogus jar");

    let ports = RetryPorts::default();
    let subscriber = tracing_subscriber::registry().with(ports.clone());

    let err = tracing::subscriber::with_default(subscriber, || {
        JavaExpansionServer::start_blocking_with_jar(&jar)
            .expect_err("a JVM handed a non-JAR can never report ready")
    });
    let _ = std::fs::remove_file(&jar);

    assert!(
        matches!(err, AutoServiceError::SpawnFailed(_)),
        "a process that dies during startup must surface as SpawnFailed, got: {err:?}"
    );

    let ports = ports.0.lock().expect("retry port lock").clone();
    assert_eq!(
        u32::try_from(ports.len()).expect("attempt count fits in u32"),
        MAX_STARTUP_ATTEMPTS - 1,
        "every attempt but the last should log a retry, saw ports {ports:?}"
    );

    let distinct: HashSet<u64> = ports.iter().copied().collect();
    assert_eq!(
        distinct.len(),
        ports.len(),
        "each retry must pick a fresh port rather than reuse the dead one, saw {ports:?}"
    );
}

#[test]
fn test_version_override() {
    let artifact = JavaExpansionArtifact::from_target_and_version("auto:gcp", "2.99.0").unwrap();
    assert_eq!(artifact.version, "2.99.0");
    assert_eq!(
        artifact.jar_name(),
        "beam-sdks-java-io-google-cloud-platform-expansion-service-2.99.0.jar"
    );

    let artifact_builder = JavaExpansionArtifact::from_target("auto:gcp")
        .unwrap()
        .with_version("2.98.0");
    assert_eq!(artifact_builder.version, "2.98.0");
    assert_eq!(
        artifact_builder.jar_name(),
        "beam-sdks-java-io-google-cloud-platform-expansion-service-2.98.0.jar"
    );
}

#[test]
fn test_custom_beam_cache_dir_resolution() {
    let default_dir = resolve_beam_jar_cache_dir(None, Some(Path::new("/Users/test")));
    assert_eq!(
        default_dir,
        PathBuf::from("/Users/test/.apache_beam/cache/jars")
    );

    let custom = resolve_beam_jar_cache_dir(Some("/tmp/test_beam_cache_123"), None);
    assert_eq!(custom, PathBuf::from("/tmp/test_beam_cache_123/jars"));

    let fallback = resolve_beam_jar_cache_dir(None, None);
    assert_eq!(fallback, PathBuf::from(".beam_cache/jars"));

    let artifact = JavaExpansionArtifact::from_target("auto:gcp")
        .unwrap()
        .with_cache_dir(PathBuf::from("/tmp/custom_jars"));
    assert_eq!(artifact.cache_dir(), PathBuf::from("/tmp/custom_jars"));
}

#[tokio::test]
async fn test_client_clone_drop_lifecycle() {
    let client1 = ExpansionClient::connect("auto:gcp")
        .await
        .expect("Failed to connect");
    let mut client2 = client1.clone();

    // Dropping client1 must not stop the server while client2 holds a reference.
    drop(client1);

    let resp = client2
        .discover_schema_transforms()
        .await
        .expect("Client clone should still function after original was dropped");
    assert!(!resp.schema_transform_configs.is_empty());
}
