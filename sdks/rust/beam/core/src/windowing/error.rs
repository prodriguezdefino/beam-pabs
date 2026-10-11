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

//! Errors that can occur during windowing strategy and trigger operations.

use thiserror::Error;

/// Error returned when decoding or validating windowing strategies and triggers.
#[derive(Clone, Error, Debug, PartialEq, Eq)]
pub enum WindowingError {
    #[error("WindowingStrategy missing window_fn FunctionSpec")]
    MissingWindowFn,

    #[error("Failed to decode window fn payload: {0}")]
    DecodePayload(String),

    #[error("Unsupported window fn URN: '{0}'")]
    UnsupportedWindowFn(String),

    #[error("Unknown accumulation mode value: {0}")]
    UnknownAccumulationMode(i32),

    #[error("Unknown output time value: {0}")]
    UnknownOutputTime(i32),

    #[error("Unknown closing behavior value: {0}")]
    UnknownClosingBehavior(i32),

    #[error("Unknown on time behavior value: {0}")]
    UnknownOnTimeBehavior(i32),

    #[error("Unknown merge status value: {0}")]
    UnknownMergeStatus(i32),

    #[error("Invalid trigger count {0}: must be at least 1")]
    InvalidTriggerCount(i32),

    #[error("Invalid trigger: {0}")]
    InvalidTrigger(String),
}
