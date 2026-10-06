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

//! Typed references to credentials held outside the pipeline.
//!
//! A [`Secret`] option names *where* a credential is, never the credential itself:
//!
//! ```text
//! --api_key=env:GEMINI_API_KEY
//! --api_key=file:/var/run/secrets/gemini
//! --api_key=gcp:projects/my-project/secrets/gemini/versions/latest
//! ```
//!
//! No variant can hold plaintext, so a credential cannot reach a command line, worker
//! options, job metadata or logs. Resolve the reference where it is used, usually in `DoFn`
//! setup on the worker. This crate resolves `env` and `file`; other providers register a
//! [`SecretResolverRegistration`] (the GCP I/O crate provides `gcp`).

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

/// The system that holds a [`Secret`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum SecretProvider {
    /// An environment variable of the process that resolves the secret.
    Env,
    /// A file on the machine that resolves the secret, for example a mounted Kubernetes secret.
    File,
    /// A Google Cloud Secret Manager secret version.
    Gcp,
}

impl SecretProvider {
    const ALL: [Self; 3] = [Self::Env, Self::File, Self::Gcp];

    /// Returns the scheme that names this provider in a secret reference.
    pub const fn scheme(self) -> &'static str {
        match self {
            Self::Env => "env",
            Self::File => "file",
            Self::Gcp => "gcp",
        }
    }
}

impl fmt::Display for SecretProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.scheme())
    }
}

/// A reference to a credential, written `<provider>:<reference>`. It serializes as its
/// reference string, so it is safe to display and to send to workers.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct Secret {
    provider: SecretProvider,
    reference: String,
}

/// The reason that a [`Secret`] did not parse or resolve.
#[derive(thiserror::Error, Debug, Clone, PartialEq, Eq)]
pub enum SecretError {
    /// The reference does not start with a known `<provider>:` scheme.
    #[error(
        "'{0}' is not a secret reference: expected env:<VAR>, file:<PATH> or \
         gcp:projects/<p>/secrets/<s>/versions/<v>. Plaintext secrets are not accepted"
    )]
    Unrecognized(String),
    /// The reference has a scheme but no text after it.
    #[error("secret reference '{0}:' names no {0} secret")]
    Empty(SecretProvider),
    /// No crate in this binary resolves the provider.
    #[error("no resolver for '{0}' secrets is linked into this binary")]
    NoResolver(SecretProvider),
    /// The provider did not return the secret.
    #[error("could not resolve {provider} secret '{reference}': {message}")]
    Unavailable {
        provider: SecretProvider,
        reference: String,
        message: String,
    },
}

impl Secret {
    /// Returns [`SecretError::Empty`] if `reference` is empty.
    pub fn new(
        provider: SecretProvider,
        reference: impl Into<String>,
    ) -> Result<Self, SecretError> {
        let reference = reference.into();
        (!reference.is_empty())
            .then_some(Self {
                provider,
                reference,
            })
            .ok_or(SecretError::Empty(provider))
    }

    pub fn provider(&self) -> SecretProvider {
        self.provider
    }

    /// Returns the name of the secret in the provider: a variable name, a path or a resource name.
    pub fn reference(&self) -> &str {
        &self.reference
    }

    /// Fetches the credential. A `file` secret loses its trailing whitespace. Returns
    /// [`SecretError::NoResolver`] if no linked crate resolves the provider.
    pub async fn resolve(&self) -> Result<SecretValue, SecretError> {
        match self.provider {
            SecretProvider::Env => std::env::var(&self.reference)
                .map(SecretValue::new)
                .map_err(|e| self.unavailable(e)),
            SecretProvider::File => tokio::fs::read_to_string(&self.reference)
                .await
                .map(|contents| SecretValue::new(contents.trim_end().to_string()))
                .map_err(|e| self.unavailable(e)),
            provider => {
                let registration = inventory::iter::<SecretResolverRegistration>
                    .into_iter()
                    .find(|registration| registration.provider == provider)
                    .ok_or(SecretError::NoResolver(provider))?;
                (registration.resolve)(self.reference.clone())
                    .await
                    .map_err(|message| self.unavailable(message))
            }
        }
    }

    fn unavailable(&self, cause: impl fmt::Display) -> SecretError {
        SecretError::Unavailable {
            provider: self.provider,
            reference: self.reference.clone(),
            message: cause.to_string(),
        }
    }
}

impl fmt::Display for Secret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.provider, self.reference)
    }
}

impl FromStr for Secret {
    type Err = SecretError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        SecretProvider::ALL
            .into_iter()
            .find_map(|provider| {
                s.strip_prefix(provider.scheme())
                    .and_then(|rest| rest.strip_prefix(':'))
                    .map(|reference| Self::new(provider, reference))
            })
            .unwrap_or_else(|| Err(SecretError::Unrecognized(s.to_string())))
    }
}

impl TryFrom<String> for Secret {
    type Error = SecretError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        value.parse()
    }
}

impl From<Secret> for String {
    fn from(secret: Secret) -> Self {
        secret.to_string()
    }
}

/// A resolved credential. It implements neither `Display` nor `Serialize` and its `Debug`
/// output is redacted, so only [`expose`](Self::expose) gives the plaintext.
#[derive(Clone, PartialEq, Eq)]
pub struct SecretValue(String);

impl SecretValue {
    pub fn new(plaintext: String) -> Self {
        Self(plaintext)
    }

    /// Returns the plaintext credential.
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for SecretValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SecretValue(<redacted>)")
    }
}

/// The future that a secret resolver returns: the plaintext, or the reason for the failure.
pub type SecretFuture = Pin<Box<dyn Future<Output = Result<SecretValue, String>> + Send>>;

/// Registers the resolver for a [`SecretProvider`] that this crate does not resolve.
///
/// ```ignore
/// inventory::submit! {
///     SecretResolverRegistration { provider: SecretProvider::Gcp, resolve: access_secret_version }
/// }
/// ```
pub struct SecretResolverRegistration {
    pub provider: SecretProvider,
    pub resolve: fn(String) -> SecretFuture,
}

inventory::collect!(SecretResolverRegistration);
