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

//! Column-wise conversion between Beam [`Row`](beam::schema::Row)s and Arrow
//! [`RecordBatch`](arrow_array::RecordBatch)es.
//!
//! Rows to batch and encoded rows to batch fill the same Arrow builders, one value at a time,
//! with lists, maps and structs as child builders. The first reads the values of Beam rows,
//! the second reads the row coder bytes and builds no Beam row.
//!
//! Batch to rows follows the requested Beam schema, such as that of a `#[derive(BeamRow)]`
//! struct, and accepts Arrow types that convert losslessly: any integer width (range
//! checked), `Float32` for `DOUBLE`, large/view strings, binaries and lists, dictionary
//! columns, any timestamp unit and `Date64`. Columns and struct children match **by name**.

mod columns;
mod encoded;
mod from_arrow;
mod projection;
mod readers;
mod skip;
mod to_arrow;

use crate::error::ArrowBridgeError;

pub use encoded::{
    RowBatchBuilder, encoded_rows_to_projected_batch, encoded_rows_to_record_batch,
    record_batch_to_encoded_rows,
};
pub use from_arrow::{array_to_values, record_batch_to_beam_rows, record_batch_to_rows};
pub use projection::Projection;
pub use to_arrow::{beam_rows_to_record_batch, rows_to_projected_batch, rows_to_record_batch};

const MICROS_PER_SECOND: i64 = 1_000_000;

fn out_of_range(name: &str, value: impl ToString, target: &str) -> ArrowBridgeError {
    ArrowBridgeError::OutOfRange {
        field: name.to_string(),
        value: value.to_string(),
        target: target.to_string(),
    }
}
