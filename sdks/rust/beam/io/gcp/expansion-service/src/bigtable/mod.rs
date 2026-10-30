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

//! SchemaTransform providers for the native Cloud Bigtable read and write connectors.

mod filter;

use std::collections::HashMap;
use std::time::Duration;

use beam::io::gcp::bigtable::read::{ReadOptions, ReadRows};
use beam::io::gcp::bigtable::target::{ReadTarget, WriteTarget};
use beam::io::gcp::bigtable::write::{WriteMutations, WriteOptions};
use beam::pipeline::Pipeline;
use beam::schema::{BeamRow, Row, Schema};
use beam::transforms::{DoFn, ParDo, ProcessContext};
use bytes::Bytes;
use expansion::{
    PCollectionId, SchemaTransformProvider, TransformInputs, register_schema_transform,
};

use crate::ErrorHandling;

/// Identifier of [`BigtableReadProvider`].
pub const URN_BIGTABLE_READ: &str = "beam:schematransform:org.apache.beam:rust:bigtable_read:v1";

/// Identifier of [`BigtableWriteProvider`].
pub const URN_BIGTABLE_WRITE: &str = "beam:schematransform:org.apache.beam:rust:bigtable_write:v1";

/// Configuration for [`BigtableReadProvider`].
///
/// Matches the fields of the Java `BigtableReadSchemaTransformConfiguration` (`project_id`,
/// `instance_id`, `table_id`, `flatten`), plus optional Rust extensions for Materialized Views,
/// Authorized Views, Data Boost (`app_profile_id`), key ranges, `RowFilter` bytes, a native
/// value bitmask filter, and silent-scan self-checkpointing.
///
/// `value_bitmask` alone keeps the cells whose value has every bit of the mask set. With
/// `value_bitmask_family` and `value_bitmask_qualifier`, it keeps the whole rows where that
/// column matches. With `row_filter`, the read applies `row_filter` and then the bitmask filter.
#[derive(Clone, Debug, Default, PartialEq, BeamRow)]
pub struct BigtableReadConfig {
    pub project_id: String,
    pub instance_id: String,
    pub table_id: Option<String>,
    pub materialized_view_id: Option<String>,
    pub authorized_view_id: Option<String>,
    pub app_profile_id: Option<String>,
    pub flatten: Option<bool>,
    pub start_key: Option<Bytes>,
    pub end_key: Option<Bytes>,
    pub row_filter: Option<Bytes>,
    pub value_bitmask: Option<Bytes>,
    pub value_bitmask_family: Option<String>,
    pub value_bitmask_qualifier: Option<Bytes>,
    pub max_initial_bundles: Option<i64>,
    pub sub_split_tablets: Option<bool>,
    pub max_silent_scan_seconds: Option<i64>,
    pub endpoint: Option<String>,
    pub error_handling: Option<ErrorHandling>,
}

impl BigtableReadConfig {
    /// Converts this configuration into validated [`ReadOptions`].
    pub fn read_options(self) -> Result<ReadOptions, String> {
        let non_empty = |opt: Option<String>| opt.filter(|s| !s.is_empty());
        let target = match (
            non_empty(self.table_id),
            non_empty(self.materialized_view_id),
            non_empty(self.authorized_view_id),
        ) {
            (Some(table), None, None) => {
                ReadTarget::table(self.project_id, self.instance_id, table)
            }
            (None, Some(view), None) => {
                ReadTarget::materialized_view(self.project_id, self.instance_id, view)
            }
            (Some(table), None, Some(view)) => {
                ReadTarget::authorized_view(self.project_id, self.instance_id, table, view)
            }
            _ => {
                return Err(
                    "Specify either table_id, materialized_view_id, or both table_id and authorized_view_id"
                        .to_string(),
                );
            }
        };

        let mut options = ReadOptions::new(target).with_flattened(self.flatten.unwrap_or(true));
        if let Some(profile) = non_empty(self.app_profile_id) {
            options = options.with_app_profile_id(profile);
        }
        if self.start_key.is_some() || self.end_key.is_some() {
            options = options.with_key_range(
                self.start_key.unwrap_or_default().to_vec(),
                self.end_key.unwrap_or_default().to_vec(),
            );
        }
        options = filter::with_filters(
            options,
            self.row_filter,
            filter::BitmaskConfig {
                mask: self.value_bitmask,
                family: non_empty(self.value_bitmask_family),
                qualifier: self.value_bitmask_qualifier,
            },
        )?;
        if let Some(max_bundles) = self.max_initial_bundles {
            let max_bundles = usize::try_from(max_bundles)
                .ok()
                .filter(|&n| n > 0)
                .ok_or_else(|| format!("max_initial_bundles ({max_bundles}) must be positive"))?;
            options = options.with_max_initial_bundles(max_bundles);
        }
        if let Some(sub_split) = self.sub_split_tablets {
            options = options.with_sub_split_tablets(sub_split);
        }
        if let Some(secs) = self.max_silent_scan_seconds {
            let secs = u64::try_from(secs)
                .ok()
                .filter(|&s| s > 0)
                .ok_or_else(|| format!("max_silent_scan_seconds ({secs}) must be positive"))?;
            options = options.with_max_silent_scan_duration(Duration::from_secs(secs));
        }
        if let Some(endpoint) = non_empty(self.endpoint) {
            options = options.with_endpoint(endpoint);
        }
        options.validate()?;
        Ok(options)
    }
}

/// SchemaTransform provider for native Cloud Bigtable reads.
#[derive(Default)]
pub struct BigtableReadProvider;

impl SchemaTransformProvider for BigtableReadProvider {
    fn identifier(&self) -> &'static str {
        URN_BIGTABLE_READ
    }

    fn description(&self) -> &'static str {
        "Reads rows from a Cloud Bigtable table, materialized view, or authorized view into Beam rows."
    }

    fn config_schema(&self) -> Schema {
        (**BigtableReadConfig::beam_schema()).clone()
    }

    fn input_tags(&self) -> Vec<String> {
        Vec::new()
    }

    fn build_transform(
        &self,
        config: Row,
        _inputs: TransformInputs,
        pipeline: &mut Pipeline,
    ) -> beam::Result<HashMap<String, PCollectionId>> {
        let mut config = BigtableReadConfig::from_row(&config).map_err(beam::Error::msg)?;
        let errors_tag = config
            .error_handling
            .take()
            .map(|h| h.output)
            .filter(|out| !out.is_empty());
        let options = config.read_options().map_err(beam::Error::msg)?;
        let read = ReadRows::new(options).with_encoded_rows();
        let begin = pipeline.begin();
        match errors_tag {
            Some(tag) => {
                let outputs = begin.try_apply("ReadFromBigtable", read.with_failures())?;
                Ok(HashMap::from([
                    ("output".to_string(), outputs.output.id().into()),
                    (tag, outputs.failures.id().into()),
                ]))
            }
            None => {
                let rows = begin.try_apply("ReadFromBigtable", read)?;
                Ok(HashMap::from([("output".to_string(), rows.id().into())]))
            }
        }
    }
}

register_schema_transform!(BigtableReadProvider);

/// Configuration for [`BigtableWriteProvider`].
///
/// Matches the fields of the Java `BigtableWriteSchemaTransformConfiguration` (`project_id`,
/// `instance_id`, `table_id`), plus optional Rust extensions for Authorized Views,
/// `app_profile_id`, batching, and error handling.
#[derive(Clone, Debug, Default, PartialEq, BeamRow)]
pub struct BigtableWriteConfig {
    pub project_id: String,
    pub instance_id: String,
    pub table_id: String,
    pub authorized_view_id: Option<String>,
    pub app_profile_id: Option<String>,
    pub max_elements_per_batch: Option<i64>,
    pub max_bytes_per_batch: Option<i64>,
    pub max_in_flight_requests: Option<i64>,
    pub enable_flow_control: Option<bool>,
    pub endpoint: Option<String>,
    pub error_handling: Option<ErrorHandling>,
}

impl BigtableWriteConfig {
    /// Converts this configuration into validated [`WriteOptions`].
    pub fn write_options(self) -> Result<WriteOptions, String> {
        let non_empty = |opt: Option<String>| opt.filter(|s| !s.is_empty());
        let target = match non_empty(self.authorized_view_id) {
            Some(view) => {
                WriteTarget::authorized_view(self.project_id, self.instance_id, self.table_id, view)
            }
            None => WriteTarget::table(self.project_id, self.instance_id, self.table_id),
        };

        let mut options = WriteOptions::new(target);
        if let Some(profile) = non_empty(self.app_profile_id) {
            options = options.with_app_profile_id(profile);
        }
        if let Some(max_elems) = self.max_elements_per_batch {
            let max_elems = usize::try_from(max_elems)
                .ok()
                .filter(|&n| (1..=100_000).contains(&n))
                .ok_or_else(|| {
                    format!("max_elements_per_batch ({max_elems}) must be in 1..=100_000")
                })?;
            options = options.with_max_elements_per_batch(max_elems);
        }
        if let Some(max_bytes) = self.max_bytes_per_batch {
            let max_bytes = usize::try_from(max_bytes)
                .ok()
                .filter(|&n| n > 0)
                .ok_or_else(|| format!("max_bytes_per_batch ({max_bytes}) must be positive"))?;
            options = options.with_max_bytes_per_batch(max_bytes);
        }
        if let Some(max_in_flight) = self.max_in_flight_requests {
            let max_in_flight = usize::try_from(max_in_flight)
                .ok()
                .filter(|&n| n > 0)
                .ok_or_else(|| {
                    format!("max_in_flight_requests ({max_in_flight}) must be positive")
                })?;
            options = options.with_max_in_flight_requests(max_in_flight);
        }
        if let Some(flow) = self.enable_flow_control {
            options = options.with_flow_control(flow);
        }
        if let Some(endpoint) = non_empty(self.endpoint) {
            options = options.with_endpoint(endpoint);
        }
        options.validate()?;
        Ok(options)
    }
}

/// SchemaTransform provider for native Cloud Bigtable writes.
#[derive(Default)]
pub struct BigtableWriteProvider;

impl SchemaTransformProvider for BigtableWriteProvider {
    fn identifier(&self) -> &'static str {
        URN_BIGTABLE_WRITE
    }

    fn description(&self) -> &'static str {
        "Writes mutation Beam rows to a Cloud Bigtable table or authorized view using MutateRows."
    }

    fn config_schema(&self) -> Schema {
        (**BigtableWriteConfig::beam_schema()).clone()
    }

    fn input_tags(&self) -> Vec<String> {
        vec!["input".to_string()]
    }

    fn build_transform(
        &self,
        config: Row,
        inputs: TransformInputs,
        pipeline: &mut Pipeline,
    ) -> beam::Result<HashMap<String, PCollectionId>> {
        let mut config = BigtableWriteConfig::from_row(&config).map_err(beam::Error::msg)?;
        let errors_tag = config
            .error_handling
            .take()
            .map(|h| h.output)
            .filter(|out| !out.is_empty());
        let rows = inputs
            .get::<Row>(pipeline, "input")
            .map_err(beam::Error::msg)?;
        let options = config.write_options().map_err(beam::Error::msg)?;
        let errors = rows.try_apply("WriteToBigtable", WriteMutations::new(options))?;
        match errors_tag {
            Some(tag) => Ok(HashMap::from([(tag, errors.id().into())])),
            None => {
                let done = errors.apply("FailOnError", ParDo::new(FailOnBigtableWriteErrorFn));
                Ok(HashMap::from([(
                    "post_write".to_string(),
                    done.id().into(),
                )]))
            }
        }
    }
}

#[derive(Clone)]
struct FailOnBigtableWriteErrorFn;

impl DoFn for FailOnBigtableWriteErrorFn {
    type In = Row;
    type Out = i32;

    fn process_element(&mut self, row: Row, _ctx: &mut ProcessContext<'_, i32>) -> beam::Result {
        let reason = row
            .get_string("error_message")
            .ok()
            .flatten()
            .unwrap_or_default();
        Err(beam::Error::msg(format!(
            "Bigtable rejected a mutation row, and the write has no error_handling output: {reason}"
        )))
    }
}

register_schema_transform!(BigtableWriteProvider);
