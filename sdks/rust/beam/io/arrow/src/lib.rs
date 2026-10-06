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

//! Bridge between Beam schemas/rows and [Apache Arrow](https://arrow.apache.org/).
//!
//! Columnar connectors (Parquet, Avro) exchange data as Arrow
//! [`RecordBatch`](arrow_array::RecordBatch)es. The [`schema`] module has the type table;
//! [`RowCodec`] abstracts over a connector's element type. The arrow-rs crates are
//! re-exported so downstream code does not need its own, possibly mismatched, dependency.

pub mod batch;
pub mod codec;
pub mod convert;
pub mod error;
pub mod schema;

pub use batch::{
    ArrowBeamRowBatchConverter, ArrowRecordBatch, ArrowRecordBatchCoder, ArrowRowBatchConverter,
};
pub use codec::{BeamRowCodec, RowCodec, SchemaRowCodec};
pub use convert::{
    array_to_values, beam_rows_to_record_batch, record_batch_to_beam_rows, record_batch_to_rows,
    rows_to_record_batch,
};
pub use error::{ArrowBridgeError, Result};
pub use schema::{
    arrow_data_type, arrow_to_beam_field, arrow_to_beam_schema, beam_to_arrow_field,
    beam_to_arrow_fields, beam_to_arrow_schema, beam_to_arrow_type, field_type_to_arrow,
};

pub use arrow_array;
pub use arrow_buffer;
pub use arrow_ipc;
pub use arrow_schema;
