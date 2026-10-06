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

//! Errors raised while bridging Beam schemas and rows to Arrow.

use arrow_schema::ArrowError;
use beam::schema::SchemaError;
use thiserror::Error;

/// Errors raised while converting between Beam and Arrow representations.
#[derive(Debug, Error)]
pub enum ArrowBridgeError {
    /// An error reported by arrow-rs itself, typically array validation.
    #[error("Arrow error: {0}")]
    Arrow(#[from] ArrowError),

    /// An error reported by the Beam schema model.
    #[error("Beam schema error: {0}")]
    Schema(#[from] SchemaError),

    /// The type has no mapping between the two type systems.
    #[error("Unsupported type for field '{field}': {detail}")]
    UnsupportedType { field: String, detail: String },

    /// A value did not match the declared field type.
    #[error("Type mismatch for field '{field}': expected {expected}, found {actual}")]
    TypeMismatch {
        field: String,
        expected: String,
        actual: String,
    },

    /// A null was found where the schema forbids it.
    #[error("Null value for non-nullable field '{field}'")]
    UnexpectedNull { field: String },

    /// A value does not fit the target type.
    #[error("Value {value} of field '{field}' is out of range for {target}")]
    OutOfRange {
        field: String,
        value: String,
        target: String,
    },

    /// A field required by the Beam schema is absent from the Arrow data.
    #[error("Field '{0}' is missing from the Arrow data")]
    MissingField(String),
}

/// Result alias for Arrow bridge operations.
pub type Result<T> = std::result::Result<T, ArrowBridgeError>;
