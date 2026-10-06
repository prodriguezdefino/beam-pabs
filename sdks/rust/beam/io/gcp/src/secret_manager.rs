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

//! Resolves `gcp:` [`Secret`](beam::options::Secret) references against Google Cloud
//! Secret Manager.
//!
//! A reference names a secret version, such as
//! `gcp:projects/my-project/secrets/api-key/versions/latest`. The resolver reads it with
//! Application Default Credentials (the worker service account on Dataflow). Linking this
//! crate registers the resolver.

use beam::options::{SecretFuture, SecretProvider, SecretResolverRegistration, SecretValue};
use google_cloud_secretmanager_v1::client::SecretManagerService;
use tokio::sync::OnceCell;

use crate::runtime::client_runtime;

static CLIENT: OnceCell<SecretManagerService> = OnceCell::const_new();

/// Checks that `name` is a secret version resource name.
#[doc(hidden)]
pub fn validate_resource_name(name: &str) -> Result<&str, String> {
    let segments: Vec<&str> = name.split('/').collect();
    match segments.as_slice() {
        ["projects", project, "secrets", secret, "versions", version]
            if [project, secret, version].iter().all(|s| !s.is_empty()) =>
        {
            Ok(name)
        }
        _ => Err(format!(
            "'{name}' is not a secret version: expected \
             projects/<project>/secrets/<secret>/versions/<version>"
        )),
    }
}

/// Reads the secret version `name` from Secret Manager.
pub async fn access_secret_version(name: String) -> Result<SecretValue, String> {
    validate_resource_name(&name)?;
    // The client runs on the runtime that built it, which is not the runtime of the caller.
    let task = client_runtime().spawn(async move {
        let client = CLIENT
            .get_or_try_init(|| SecretManagerService::builder().build())
            .await
            .map_err(|e| e.to_string())?;
        let response = client
            .access_secret_version()
            .set_name(name)
            .send()
            .await
            .map_err(|e| e.to_string())?;
        let data = response.payload.map(|p| p.data).unwrap_or_default();
        String::from_utf8(data.to_vec())
            .map(SecretValue::new)
            .map_err(|_| "The secret payload is not UTF-8".to_string())
    });
    task.await.map_err(|e| e.to_string())?
}

fn resolve(name: String) -> SecretFuture {
    Box::pin(access_secret_version(name))
}

inventory::submit! {
    SecretResolverRegistration { provider: SecretProvider::Gcp, resolve }
}
