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

//! Errors raised by the Avro connector outside of pipeline execution.

use arrow_avro::errors::AvroError;
use arrow_io::ArrowBridgeError;
use thiserror::Error;

/// Errors raised when inspecting or converting Avro files.
#[derive(Debug, Error)]
pub enum AvroIoError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Avro error: {0}")]
    Avro(#[from] AvroError),

    #[error("{0}")]
    Arrow(#[from] ArrowBridgeError),
}

impl From<arrow_schema::ArrowError> for AvroIoError {
    fn from(err: arrow_schema::ArrowError) -> Self {
        Self::Arrow(ArrowBridgeError::Arrow(err))
    }
}
