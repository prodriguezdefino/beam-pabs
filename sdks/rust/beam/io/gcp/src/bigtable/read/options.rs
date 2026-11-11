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

//! Configuration options for Cloud Bigtable [`ReadRows`](super::ReadRows) transforms.

use std::sync::Arc;
use std::time::Duration;

use beam::schema::Schema;
use beam::transforms::display_data::DisplayDataBuilder;
use prost::Message as _;
use serde::{Deserialize, Serialize};

use super::sample::DEFAULT_MAX_INITIAL_BUNDLES;
use crate::bigtable::key::is_valid_range;
use crate::bigtable::proto::RowFilter;
use crate::bigtable::target::ReadTarget;
use crate::bigtable::{BigtableReadShape, flattened_row_schema, nested_row_schema};

/// Default duration of silent server-side filtering (`60s`) before
/// [`ReadRangesFn`](super::reader::ReadRangesFn) checkpoints through
/// [`ProcessContinuation::resume`](beam::transforms::sdf::ProcessContinuation::resume).
pub const DEFAULT_MAX_SILENT_SCAN_DURATION: Duration = Duration::from_secs(60);

/// Default per-attempt gRPC timeout (`60s`) for `SampleRowKeys` and `ReadRows` stream messages.
pub const DEFAULT_READ_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(60);

/// Configuration for a Cloud Bigtable [`ReadRows`](super::ReadRows) transform.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReadOptions {
    pub(crate) target: ReadTarget,
    pub(crate) app_profile_id: Option<String>,
    pub(crate) shape: BigtableReadShape,
    pub(crate) start_key: Vec<u8>,
    pub(crate) end_key: Vec<u8>,
    pub(crate) filter_bytes: Vec<u8>,
    pub(crate) max_initial_bundles: usize,
    pub(crate) sub_split_tablets: bool,
    pub(crate) max_silent_scan_duration: Duration,
    pub(crate) attempt_timeout: Duration,
    pub(crate) endpoint: Option<String>,
}

impl ReadOptions {
    /// Creates default read options for `target`.
    pub fn new(target: ReadTarget) -> Self {
        Self {
            target,
            app_profile_id: None,
            shape: BigtableReadShape::Flattened,
            start_key: Vec::new(),
            end_key: Vec::new(),
            filter_bytes: Vec::new(),
            max_initial_bundles: DEFAULT_MAX_INITIAL_BUNDLES,
            sub_split_tablets: true,
            max_silent_scan_duration: DEFAULT_MAX_SILENT_SCAN_DURATION,
            attempt_timeout: DEFAULT_READ_ATTEMPT_TIMEOUT,
            endpoint: None,
        }
    }

    /// Creates read options for a Cloud Bigtable table.
    pub fn from_table(
        project: impl Into<String>,
        instance: impl Into<String>,
        table: impl Into<String>,
    ) -> Self {
        Self::new(ReadTarget::table(project, instance, table))
    }

    /// Creates read options for a Cloud Bigtable materialized view.
    pub fn from_materialized_view(
        project: impl Into<String>,
        instance: impl Into<String>,
        view: impl Into<String>,
    ) -> Self {
        Self::new(ReadTarget::materialized_view(project, instance, view))
    }

    /// Creates read options for a Cloud Bigtable authorized view.
    pub fn from_authorized_view(
        project: impl Into<String>,
        instance: impl Into<String>,
        table: impl Into<String>,
        view: impl Into<String>,
    ) -> Self {
        Self::new(ReadTarget::authorized_view(project, instance, table, view))
    }

    /// Sets the Bigtable App Profile ID (used for standard routing or Data Boost).
    pub fn with_app_profile_id(mut self, app_profile_id: impl Into<String>) -> Self {
        let id = app_profile_id.into();
        self.app_profile_id = (!id.is_empty()).then_some(id);
        self
    }

    /// Sets the output row shape (`Flattened` or `Nested`).
    pub fn with_shape(mut self, shape: BigtableReadShape) -> Self {
        self.shape = shape;
        self
    }

    /// Restricts the scan to the half-open key range `[start_key, end_key)`.
    pub fn with_key_range(
        mut self,
        start_key: impl Into<Vec<u8>>,
        end_key: impl Into<Vec<u8>>,
    ) -> Self {
        self.start_key = start_key.into();
        self.end_key = end_key.into();
        self
    }

    /// Sets a server-side [`RowFilter`].
    pub fn with_row_filter(mut self, filter: &RowFilter) -> Self {
        self.filter_bytes = filter.encode_to_vec();
        self
    }

    /// Sets a raw protobuf-encoded `google.bigtable.v2.RowFilter`.
    pub fn with_row_filter_bytes(mut self, filter_bytes: impl Into<Vec<u8>>) -> Self {
        self.filter_bytes = filter_bytes.into();
        self
    }

    /// Sets the maximum number of super-range bundles emitted by `SampleKeyRangesFn`.
    pub fn with_max_initial_bundles(mut self, max_initial_bundles: usize) -> Self {
        self.max_initial_bundles = max_initial_bundles.max(1);
        self
    }

    /// Sets whether super-ranges expand into per-tablet restrictions during `split_restriction`.
    pub fn with_sub_split_tablets(mut self, sub_split_tablets: bool) -> Self {
        self.sub_split_tablets = sub_split_tablets;
        self
    }

    /// Sets the maximum duration of silent server-side scanning (`last_scanned_row_key`
    /// heartbeats without matching rows) before proactive SDF self-checkpointing.
    pub fn with_max_silent_scan_duration(mut self, duration: Duration) -> Self {
        self.max_silent_scan_duration = duration;
        self
    }

    /// Sets the per-attempt timeout for `SampleRowKeys` and `ReadRows` messages.
    pub fn with_attempt_timeout(mut self, timeout: Duration) -> Self {
        self.attempt_timeout = timeout;
        self
    }

    /// Overrides the gRPC endpoint (for example, when connecting to a local emulator).
    pub fn with_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        let ep = endpoint.into();
        self.endpoint = (!ep.is_empty()).then_some(ep);
        self
    }

    /// Returns the Bigtable read target.
    pub fn target(&self) -> &ReadTarget {
        &self.target
    }

    /// Returns the configured application profile ID, if any.
    pub fn app_profile_id(&self) -> Option<&str> {
        self.app_profile_id.as_deref()
    }

    /// Returns the output row shape.
    pub fn shape(&self) -> BigtableReadShape {
        self.shape
    }

    /// Returns the maximum number of initial key-range bundles.
    pub fn max_initial_bundles(&self) -> usize {
        self.max_initial_bundles
    }

    /// Returns whether sampled tablets are split further.
    pub fn sub_split_tablets(&self) -> bool {
        self.sub_split_tablets
    }

    /// Returns the output [`Schema`] for this read configuration.
    pub fn output_schema(&self) -> Arc<Schema> {
        match self.shape {
            BigtableReadShape::Flattened => flattened_row_schema(),
            BigtableReadShape::Nested => nested_row_schema(),
        }
    }

    /// Decodes the configured [`RowFilter`], if any.
    pub fn decoded_filter(&self) -> beam::Result<Option<RowFilter>> {
        if self.filter_bytes.is_empty() {
            return Ok(None);
        }
        RowFilter::decode(self.filter_bytes.as_slice())
            .map(Some)
            .map_err(|e| {
                beam::Error::msg(format!("Invalid Bigtable RowFilter protobuf bytes: {e}"))
            })
    }

    /// Checks the read configuration without calling Bigtable.
    pub fn validate(&self) -> beam::Result {
        self.target.validate()?;
        if !is_valid_range(&self.start_key, &self.end_key) {
            return Err(beam::Error::msg(format!(
                "Invalid Bigtable key range: start_key ({:?}) >= end_key ({:?})",
                self.start_key, self.end_key
            )));
        }
        if self.max_initial_bundles == 0 {
            return Err(beam::Error::msg(
                "max_initial_bundles must be greater than 0",
            ));
        }
        let _ = self.decoded_filter()?;
        Ok(())
    }

    pub(super) fn populate_display_data(&self, builder: &mut DisplayDataBuilder) {
        builder.add_text("target", self.target.resource_name());
        if let Some(profile) = &self.app_profile_id {
            builder.add_text("app_profile_id", profile);
        }
        builder.add_text("shape", self.shape.as_str());
        builder.add_integer(
            "max_initial_bundles",
            i64::try_from(self.max_initial_bundles).unwrap_or(i64::MAX),
        );
    }
}
