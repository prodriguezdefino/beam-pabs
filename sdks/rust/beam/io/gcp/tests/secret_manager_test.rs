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

//! Tests for Google Cloud Secret Manager resolution.

use beam::options::{Secret, SecretError, SecretProvider};

#[tokio::test]
async fn gcp_references_must_name_a_secret_version() {
    // Linking the crate registers the resolver; bad names fail before any request.
    let _ = gcp::secret_manager::validate_resource_name;
    let secret: Secret = "gcp:projects/p/secrets/s".parse().expect("gcp reference");

    let err = secret.resolve().await.expect_err("not a secret version");

    assert!(matches!(
        &err,
        SecretError::Unavailable { provider: SecretProvider::Gcp, message, .. }
            if message.contains("projects/<project>/secrets/<secret>/versions/<version>")
    ));
}

#[test]
fn validate_resource_name_accepts_only_secret_versions() {
    use gcp::secret_manager::validate_resource_name;

    for name in [
        "projects/p/secrets/s/versions/latest",
        "projects/my-project/secrets/api-key/versions/3",
    ] {
        assert_eq!(validate_resource_name(name), Ok(name));
    }
    for name in [
        "",
        "projects/p/secrets/s",
        "projects//secrets/s/versions/1",
        "projects/p/secrets//versions/1",
        "projects/p/secrets/s/versions/",
        "projects/p/secrets/s/versions/1/extra",
        "/projects/p/secrets/s/versions/1",
        "project/p/secrets/s/versions/1",
        "projects/p/secret/s/versions/1",
        "projects/p/secrets/s/version/1",
    ] {
        let err = validate_resource_name(name).expect_err(name);
        assert_eq!(
            err,
            format!(
                "'{name}' is not a secret version: expected \
                 projects/<project>/secrets/<secret>/versions/<version>"
            )
        );
    }
}
