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

//! Configuration options for Cloud Bigtable [`WriteMutations`](super::WriteMutations)
//! transforms.

use std::time::Duration;

use beam::transforms::display_data::DisplayDataBuilder;
use serde::{Deserialize, Serialize};

use super::batch::{
    DEFAULT_MAX_BYTES_PER_BATCH, DEFAULT_MAX_ELEMENTS_PER_BATCH, DEFAULT_MAX_IN_FLIGHT_BYTES,
    DEFAULT_MAX_IN_FLIGHT_REQUESTS,
};
use crate::bigtable::target::WriteTarget;

/// Default per-attempt timeout (`60s`) for `MutateRows` RPCs.
pub const DEFAULT_WRITE_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(60);

/// Configuration for a Cloud Bigtable [`WriteMutations`](super::WriteMutations) transform.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WriteOptions {
    pub(crate) target: WriteTarget,
    pub(crate) app_profile_id: Option<String>,
    pub(crate) max_elements_per_batch: usize,
    pub(crate) max_bytes_per_batch: usize,
    pub(crate) max_in_flight_requests: usize,
    pub(crate) max_in_flight_bytes: usize,
    pub(crate) attempt_timeout: Duration,
    pub(crate) enable_flow_control: bool,
    pub(crate) endpoint: Option<String>,
}

impl WriteOptions {
    /// Creates default write options for `target`.
    pub fn new(target: WriteTarget) -> Self {
        Self {
            target,
            app_profile_id: None,
            max_elements_per_batch: DEFAULT_MAX_ELEMENTS_PER_BATCH,
            max_bytes_per_batch: DEFAULT_MAX_BYTES_PER_BATCH,
            max_in_flight_requests: DEFAULT_MAX_IN_FLIGHT_REQUESTS,
            max_in_flight_bytes: DEFAULT_MAX_IN_FLIGHT_BYTES,
            attempt_timeout: DEFAULT_WRITE_ATTEMPT_TIMEOUT,
            enable_flow_control: true,
            endpoint: None,
        }
    }

    /// Creates write options for a Cloud Bigtable table.
    pub fn from_table(
        project: impl Into<String>,
        instance: impl Into<String>,
        table: impl Into<String>,
    ) -> Self {
        Self::new(WriteTarget::table(project, instance, table))
    }

    /// Creates write options for a Cloud Bigtable authorized view.
    pub fn from_authorized_view(
        project: impl Into<String>,
        instance: impl Into<String>,
        table: impl Into<String>,
        view: impl Into<String>,
    ) -> Self {
        Self::new(WriteTarget::authorized_view(project, instance, table, view))
    }

    /// Sets the Bigtable App Profile ID.
    pub fn with_app_profile_id(mut self, app_profile_id: impl Into<String>) -> Self {
        let id = app_profile_id.into();
        self.app_profile_id = (!id.is_empty()).then_some(id);
        self
    }

    /// Sets the maximum number of row entries per `MutateRows` batch (`1..=100_000`).
    pub fn with_max_elements_per_batch(mut self, max_elements: usize) -> Self {
        self.max_elements_per_batch = max_elements;
        self
    }

    /// Sets the maximum byte size per `MutateRows` batch.
    pub fn with_max_bytes_per_batch(mut self, max_bytes: usize) -> Self {
        self.max_bytes_per_batch = max_bytes;
        self
    }

    /// Sets the maximum number of concurrent in-flight `MutateRows` RPCs per bundle.
    pub fn with_max_in_flight_requests(mut self, max_in_flight: usize) -> Self {
        self.max_in_flight_requests = max_in_flight;
        self
    }

    /// Sets the per-attempt timeout for `MutateRows` RPCs.
    pub fn with_attempt_timeout(mut self, timeout: Duration) -> Self {
        self.attempt_timeout = timeout;
        self
    }

    /// Enables or disables Bigtable `RateLimitInfo` flow-control backoff.
    pub fn with_flow_control(mut self, enabled: bool) -> Self {
        self.enable_flow_control = enabled;
        self
    }

    /// Overrides the gRPC endpoint (for example, when connecting to a local emulator).
    pub fn with_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        let ep = endpoint.into();
        self.endpoint = (!ep.is_empty()).then_some(ep);
        self
    }

    /// Checks the write configuration without calling Bigtable.
    pub fn validate(&self) -> beam::Result {
        self.target.validate()?;
        if self.max_elements_per_batch == 0 || self.max_elements_per_batch > 100_000 {
            return Err(beam::Error::msg(format!(
                "max_elements_per_batch ({}) must be in 1..=100_000",
                self.max_elements_per_batch
            )));
        }
        if self.max_bytes_per_batch == 0 {
            return Err(beam::Error::msg(
                "max_bytes_per_batch must be greater than 0",
            ));
        }
        if self.max_in_flight_requests == 0 {
            return Err(beam::Error::msg(
                "max_in_flight_requests must be greater than 0",
            ));
        }
        Ok(())
    }

    pub(super) fn populate_display_data(&self, builder: &mut DisplayDataBuilder) {
        builder.add_text("target", self.target.resource_name());
        if let Some(profile) = &self.app_profile_id {
            builder.add_text("app_profile_id", profile);
        }
        builder.add_integer(
            "max_elements_per_batch",
            i64::try_from(self.max_elements_per_batch).unwrap_or(i64::MAX),
        );
        builder.add_integer(
            "max_bytes_per_batch",
            i64::try_from(self.max_bytes_per_batch).unwrap_or(i64::MAX),
        );
        builder.add_boolean("flow_control", self.enable_flow_control);
    }
}
