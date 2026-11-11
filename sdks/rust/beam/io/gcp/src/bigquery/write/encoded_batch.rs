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

//! Rows in Arrow IPC format, and the append requests that hold them.

use beam::metrics::Metrics;
use beam_io_arrow::EncodedBatch;
use bytes::Bytes;
use google_cloud_bigquery::model::ArrowRecordBatch;

use crate::bigquery::METRICS;

/// The rows of one append request. A clone shares the bytes, so a writer keeps the rows of a
/// request that it sent with no copy.
#[derive(Clone, Debug)]
pub(super) struct Request {
    pub(super) rows: ArrowRecordBatch,
    pub(super) count: i64,
}

impl Request {
    /// Reports `rows_written` and `bytes_written` for this accepted append batch.
    pub(super) fn record_written(&self) {
        let bytes = i64::try_from(self.rows.serialized_record_batch.len()).unwrap_or(i64::MAX);
        Metrics::counter(METRICS, "rows_written").inc_by(self.count);
        Metrics::counter(METRICS, "bytes_written").inc_by(bytes);
    }
}

impl From<EncodedBatch> for Request {
    /// Moves the bytes into the request, with no copy.
    fn from(batch: EncodedBatch) -> Self {
        Self {
            rows: ArrowRecordBatch::new().set_serialized_record_batch(Bytes::from(batch.bytes)),
            count: batch.rows,
        }
    }
}
