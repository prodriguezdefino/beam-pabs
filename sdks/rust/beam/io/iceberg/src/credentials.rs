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

//! Google credentials from a vended access token.

use std::fmt;

use google_cloud_auth::credentials::{CacheableResource, CredentialsProvider, EntityTag};
use google_cloud_auth::errors::CredentialsError;
use http::header::{AUTHORIZATION, HeaderValue};
use http::{Extensions, HeaderMap};
use iceberg::{Error, ErrorKind};

/// Sends a fixed access token as `Authorization: Bearer <token>`.
pub struct BearerToken {
    headers: HeaderMap,
    entity_tag: EntityTag,
}

impl BearerToken {
    /// # Errors
    ///
    /// Fails if the token is not a valid HTTP header value.
    pub fn new(token: &str) -> iceberg::Result<Self> {
        let mut value = HeaderValue::from_str(&format!("Bearer {token}")).map_err(|e| {
            Error::new(ErrorKind::DataInvalid, "The vended GCS token is not valid").with_source(e)
        })?;
        value.set_sensitive(true);
        Ok(Self {
            headers: HeaderMap::from_iter([(AUTHORIZATION, value)]),
            entity_tag: EntityTag::new(),
        })
    }
}

/// Hides the token, so that logs and errors do not show it.
impl fmt::Debug for BearerToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("BearerToken(..)")
    }
}

impl CredentialsProvider for BearerToken {
    async fn headers(
        &self,
        extensions: Extensions,
    ) -> Result<CacheableResource<HeaderMap>, CredentialsError> {
        match extensions.get::<EntityTag>() {
            Some(tag) if *tag == self.entity_tag => Ok(CacheableResource::NotModified),
            _ => Ok(CacheableResource::New {
                entity_tag: self.entity_tag.clone(),
                data: self.headers.clone(),
            }),
        }
    }

    async fn universe_domain(&self) -> Option<String> {
        None
    }
}
