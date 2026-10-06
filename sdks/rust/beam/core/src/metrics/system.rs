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

//! Builders for the system metrics that the worker harness measures and reports.

use std::collections::HashMap;

use model::pipeline::MonitoringInfo;

use super::{
    LABEL_PCOLLECTION, LABEL_PTRANSFORM, TYPE_DISTRIBUTION_INT64, TYPE_PROGRESS, TYPE_SUM_INT64,
    URN_DATA_CHANNEL_READ_INDEX, URN_ELEMENT_COUNT, URN_PROCESS_BUNDLE_MSECS,
    URN_SAMPLED_BYTE_SIZE, URN_WORK_COMPLETED, URN_WORK_REMAINING,
};
use crate::coders::{CoderError, VarIntCoder};

/// Builds an int64 sum `MonitoringInfo` with one label.
fn int64_counter(
    urn: &str,
    label_key: &str,
    label_val: &str,
    value: i64,
) -> Result<MonitoringInfo, CoderError> {
    let mut payload = Vec::new();
    VarIntCoder::encode_varint(value, &mut payload)?;

    Ok(MonitoringInfo {
        urn: urn.to_string(),
        r#type: TYPE_SUM_INT64.to_string(),
        payload,
        labels: HashMap::from([(label_key.to_string(), label_val.to_string())]),
        ..Default::default()
    })
}

/// Builds a `beam:metric:element_count:v1` `MonitoringInfo` for a PCollection.
pub fn element_count(pcollection_id: &str, count: i64) -> Result<MonitoringInfo, CoderError> {
    int64_counter(URN_ELEMENT_COUNT, LABEL_PCOLLECTION, pcollection_id, count)
}

/// Builds a `beam:metric:data_channel:read_index:v1` `MonitoringInfo` for a source PTransform.
/// The read index is the total number of elements read from the data channel. Some runners,
/// for example Dataflow Runner v2, use it to detect dropped or duplicated data.
pub fn data_channel_read_index(
    ptransform_id: &str,
    index: i64,
) -> Result<MonitoringInfo, CoderError> {
    int64_counter(
        URN_DATA_CHANNEL_READ_INDEX,
        LABEL_PTRANSFORM,
        ptransform_id,
        index,
    )
}

/// Builds a `beam:metric:pardo_execution_time:process_bundle_msecs:v1` `MonitoringInfo`.
pub fn process_bundle_msecs(ptransform_id: &str, msecs: i64) -> Result<MonitoringInfo, CoderError> {
    int64_counter(
        URN_PROCESS_BUNDLE_MSECS,
        LABEL_PTRANSFORM,
        ptransform_id,
        msecs,
    )
}

/// Builds a `beam:metric:sampled_byte_size:v1` `MonitoringInfo` for a PCollection.
/// The payload is an int64 distribution: `<count><sum><min><max>` VarInts.
pub fn sampled_byte_size(
    pcollection_id: &str,
    count: i64,
    sum: i64,
    min: i64,
    max: i64,
) -> Result<MonitoringInfo, CoderError> {
    let mut payload = Vec::new();
    VarIntCoder::encode_varint(count, &mut payload)?;
    VarIntCoder::encode_varint(sum, &mut payload)?;
    VarIntCoder::encode_varint(min, &mut payload)?;
    VarIntCoder::encode_varint(max, &mut payload)?;

    Ok(MonitoringInfo {
        urn: URN_SAMPLED_BYTE_SIZE.to_string(),
        r#type: TYPE_DISTRIBUTION_INT64.to_string(),
        payload,
        labels: HashMap::from([(LABEL_PCOLLECTION.to_string(), pcollection_id.to_string())]),
        ..Default::default()
    })
}

/// Builds a `beam:metrics:progress:v1` `MonitoringInfo` for a PTransform. The payload is an
/// iterable of doubles: a big-endian `i32` count, then big-endian `f64` values.
fn progress(urn: &str, ptransform_id: &str, value: f64) -> MonitoringInfo {
    let mut payload = 1_i32.to_be_bytes().to_vec();
    payload.extend_from_slice(&value.to_be_bytes());
    MonitoringInfo {
        urn: urn.to_string(),
        r#type: TYPE_PROGRESS.to_string(),
        payload,
        labels: HashMap::from([(LABEL_PTRANSFORM.to_string(), ptransform_id.to_string())]),
        ..Default::default()
    }
}

/// Builds a `beam:metric:ptransform_progress:completed:v1` `MonitoringInfo`: the work that the
/// current element of a splittable DoFn has completed.
pub fn work_completed(ptransform_id: &str, work: f64) -> MonitoringInfo {
    progress(URN_WORK_COMPLETED, ptransform_id, work)
}

/// Builds a `beam:metric:ptransform_progress:remaining:v1` `MonitoringInfo`: the work that is
/// left in the current element of a splittable DoFn. Runners such as Dataflow estimate the
/// backlog of a stage from it.
pub fn work_remaining(ptransform_id: &str, work: f64) -> MonitoringInfo {
    progress(URN_WORK_REMAINING, ptransform_id, work)
}
