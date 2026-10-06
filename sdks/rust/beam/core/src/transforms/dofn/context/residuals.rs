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

//! Residual roots of splittable `DoFn`s. When one checkpoints or is split mid-bundle, the
//! runner gets the unprocessed rest of the restriction as a [`ResidualApplication`] and
//! reschedules it.

/// A delayed bundle application that holds residual work.
#[derive(Clone, Debug, PartialEq)]
pub struct ResidualApplication {
    pub transform_id: String,
    pub input_id: String,
    pub element: Vec<u8>,
    /// The lowest timestamp that the residual can output, or `None` if the SDF does not
    /// know it. The harness gives it to every output of the transform.
    pub output_watermark: Option<crate::Instant>,
    pub delay: Option<std::time::Duration>,
}

/// Thread-safe collector for residual roots and their output watermarks in a bundle.
#[derive(Default, Debug)]
pub struct ResidualCollector {
    residuals: std::sync::Mutex<Vec<ResidualApplication>>,
}

impl ResidualCollector {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add(&self, application: ResidualApplication) {
        let mut guard = self
            .residuals
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        guard.push(application);
    }

    pub fn drain(&self) -> Vec<ResidualApplication> {
        let mut guard = self
            .residuals
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        std::mem::take(&mut *guard)
    }
}
