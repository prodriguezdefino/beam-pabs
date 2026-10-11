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

//! Tests for typed secret references and resolution.

use beam::options::{Secret, SecretError, SecretProvider};

#[test]
fn references_parse_by_provider() {
    let env: Secret = "env:API_KEY".parse().expect("env reference");
    assert_eq!(env.provider(), SecretProvider::Env);
    assert_eq!(env.reference(), "API_KEY");

    let file: Secret = "file:/var/run/secrets/key".parse().expect("file reference");
    assert_eq!(file.provider(), SecretProvider::File);
    assert_eq!(file.reference(), "/var/run/secrets/key");

    let gcp: Secret = "gcp:projects/p/secrets/s/versions/latest"
        .parse()
        .expect("gcp reference");
    assert_eq!(gcp.provider(), SecretProvider::Gcp);
    assert_eq!(gcp.reference(), "projects/p/secrets/s/versions/latest");
}

#[test]
fn plaintext_is_rejected() {
    assert!(matches!(
        "AIzaSyExampleKey".parse::<Secret>(),
        Err(SecretError::Unrecognized(_))
    ));
    assert!(matches!(
        "env:".parse::<Secret>(),
        Err(SecretError::Empty(SecretProvider::Env))
    ));
}

#[test]
fn secrets_serialize_as_their_reference() {
    let secret: Secret = "env:API_KEY".parse().expect("env reference");

    let json = serde_json::to_string(&secret).expect("serializes");
    assert_eq!(json, "\"env:API_KEY\"");
    assert_eq!(
        serde_json::from_str::<Secret>(&json).expect("deserializes"),
        secret
    );
    assert!(serde_json::from_str::<Secret>("\"plaintext\"").is_err());
}

#[tokio::test]
async fn file_secrets_resolve_without_the_trailing_newline() {
    let path = std::env::temp_dir().join(format!("beam_secret_test_{}", std::process::id()));
    std::fs::write(&path, "s3cr3t\n").expect("writable");
    let secret: Secret = format!("file:{}", path.display())
        .parse()
        .expect("file reference");

    let value = secret.resolve().await.expect("resolves");

    assert_eq!(value.expose(), "s3cr3t");
    assert_eq!(format!("{value:?}"), "SecretValue(<redacted>)");
}

#[tokio::test]
async fn env_secrets_resolve_from_the_process_environment() {
    // PATH is set in every test environment; no variable has to be modified.
    let secret: Secret = "env:PATH".parse().expect("env reference");

    let value = secret.resolve().await.expect("resolves");

    assert_eq!(value.expose(), std::env::var("PATH").expect("PATH is set"));
}

#[tokio::test]
async fn unavailable_secrets_name_the_reference() {
    let secret: Secret = "env:BEAM_SECRET_TEST_UNSET_VARIABLE"
        .parse()
        .expect("env reference");

    let err = secret.resolve().await.expect_err("unset variable");

    assert!(matches!(
        err,
        SecretError::Unavailable {
            provider: SecretProvider::Env,
            ..
        }
    ));
    assert!(err.to_string().contains("BEAM_SECRET_TEST_UNSET_VARIABLE"));
}
