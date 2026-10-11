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

use thiserror::Error;

/// Errors during cross-language transform expansion and execution.
#[derive(Debug, Error)]
pub enum ExpansionError {
    #[error("Failed to connect to expansion service at '{endpoint}': {source}")]
    Connection {
        endpoint: String,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[error("Expansion RPC failed: {0}")]
    Rpc(String),

    #[error("Expansion service returned error: {0}")]
    ExpansionFailed(String),

    #[error("Invalid expansion response: {0}")]
    InvalidResponse(String),

    #[error("Invalid configuration: {0}")]
    InvalidConfig(String),

    #[error("Payload encoding error: {0}")]
    Encoding(String),

    #[error("Component resolution error: {0}")]
    ComponentNotFound(String),
}

impl PartialEq for ExpansionError {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (
                Self::Connection {
                    endpoint: e1,
                    source: s1,
                },
                Self::Connection {
                    endpoint: e2,
                    source: s2,
                },
            ) => e1 == e2 && s1.to_string() == s2.to_string(),
            (Self::Rpc(a), Self::Rpc(b)) => a == b,
            (Self::ExpansionFailed(a), Self::ExpansionFailed(b)) => a == b,
            (Self::InvalidResponse(a), Self::InvalidResponse(b)) => a == b,
            (Self::InvalidConfig(a), Self::InvalidConfig(b)) => a == b,
            (Self::Encoding(a), Self::Encoding(b)) => a == b,
            (Self::ComponentNotFound(a), Self::ComponentNotFound(b)) => a == b,
            _ => false,
        }
    }
}

impl Eq for ExpansionError {}
