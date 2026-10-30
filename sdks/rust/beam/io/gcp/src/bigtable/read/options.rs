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

//! Configuration options for native Cloud Bigtable [`ReadRows`](super::ReadRows) transforms.

use std::sync::Arc;
use std::time::Duration;

use beam::schema::Schema;
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

/// Configuration for a native Cloud Bigtable [`ReadRows`](super::ReadRows) transform.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReadOptions {
    /// Target table, materialized view, or authorized view.
    pub target: ReadTarget,
    /// Optional Bigtable App Profile ID (such as a serverless Data Boost read profile).
    pub app_profile_id: Option<String>,
    /// Output schema shape (`Flattened` columns or `Nested` rows).
    pub shape: BigtableReadShape,
    /// Inclusive lower bound row key (empty means start of table).
    pub start_key: Vec<u8>,
    /// Exclusive upper bound row key (empty means end of table).
    pub end_key: Vec<u8>,
    /// Protobuf-encoded `google.bigtable.v2.RowFilter` pushed down to Bigtable.
    pub filter_bytes: Vec<u8>,
    /// Maximum number of super-range bundles emitted by `SampleKeyRangesFn`.
    pub max_initial_bundles: usize,
    /// Whether `split_restriction` expands coalesced super-ranges along tablet boundaries.
    pub sub_split_tablets: bool,
    /// Maximum silent scan duration before proactive SDF self-checkpointing.
    pub max_silent_scan_duration: Duration,
    /// Per-attempt timeout for `SampleRowKeys` and `ReadRows` messages.
    pub attempt_timeout: Duration,
    /// Optional custom gRPC endpoint override (or `BIGTABLE_EMULATOR_HOST`).
    pub endpoint: Option<String>,
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

    /// Sets whether output rows are flattened (`true`) or nested (`false`).
    pub fn with_flattened(mut self, flattened: bool) -> Self {
        self.shape = if flattened {
            BigtableReadShape::Flattened
        } else {
            BigtableReadShape::Nested
        };
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

    /// Returns the output [`Schema`] for this read configuration.
    pub fn output_schema(&self) -> Arc<Schema> {
        match self.shape {
            BigtableReadShape::Flattened => flattened_row_schema(),
            BigtableReadShape::Nested => nested_row_schema(),
        }
    }

    /// Decodes the configured [`RowFilter`], if any.
    pub fn decoded_filter(&self) -> Result<Option<RowFilter>, String> {
        if self.filter_bytes.is_empty() {
            return Ok(None);
        }
        RowFilter::decode(self.filter_bytes.as_slice())
            .map(Some)
            .map_err(|e| format!("Invalid Bigtable RowFilter protobuf bytes: {e}"))
    }

    /// Validates the read configuration.
    pub fn validate(&self) -> Result<(), String> {
        self.target.validate()?;
        if !is_valid_range(&self.start_key, &self.end_key) {
            return Err(format!(
                "Invalid Bigtable key range: start_key ({:?}) >= end_key ({:?})",
                self.start_key, self.end_key
            ));
        }
        if self.max_initial_bundles == 0 {
            return Err("max_initial_bundles must be greater than 0".to_string());
        }
        let _ = self.decoded_filter()?;
        Ok(())
    }
}
