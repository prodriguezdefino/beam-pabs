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

use super::ProcessContext;

/// A delayed bundle application that holds residual work.
#[derive(Clone, Debug, PartialEq)]
pub struct ResidualApplication {
    pub transform_id: String,
    pub input_id: String,
    pub element: Vec<u8>,
    pub output_watermarks: std::collections::HashMap<String, i64>,
    pub is_bounded: bool,
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
        if let Ok(mut guard) = self.residuals.lock() {
            guard.push(application);
        }
    }

    pub fn drain(&self) -> Vec<ResidualApplication> {
        if let Ok(mut guard) = self.residuals.lock() {
            std::mem::take(&mut *guard)
        } else {
            Vec::new()
        }
    }
}

impl<T> ProcessContext<'_, T> {
    /// Adds a residual application for the runner to reschedule.
    pub fn add_residual(&self, application: ResidualApplication) {
        if let Some(c) = self.residual_collector {
            c.add(application);
        }
    }

    /// Adds a residual with an explicit output watermark and delay. The residual uses input id
    /// `in` and output name `out`.
    pub fn add_residual_with_watermark(
        &self,
        element: Vec<u8>,
        watermark_millis: i64,
        delay: Option<std::time::Duration>,
        is_bounded: bool,
    ) {
        let mut output_watermarks = std::collections::HashMap::new();
        output_watermarks.insert("out".to_string(), watermark_millis);
        self.add_residual(ResidualApplication {
            transform_id: self.transform_id.to_string(),
            input_id: "in".to_string(),
            element,
            output_watermarks,
            is_bounded,
            delay,
        });
    }
}
