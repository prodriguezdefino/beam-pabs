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

//! Failure output of a BigQuery read for [`Read::with_failures`] and
//! [`ReadRows::with_failures`].
//!
//! The stream reader emits `Result<T, Row>`: a converted value goes to the main output, and a
//! failed row goes to `failures`.
//!
//! [`Read::with_failures`]: super::Read::with_failures
//! [`ReadRows::with_failures`]: super::ReadRows::with_failures

use std::marker::PhantomData;
use std::sync::{Arc, LazyLock};

use beam::coders::DefaultCoder;
use beam::schema::{Field, FieldType, FieldValue, Row, Schema};
use beam::transforms::ProcessContext;
use beam_io_arrow::arrow_array::RecordBatch;
use beam_io_arrow::{RowCodec, convert_rows, render_row};

use super::reader::BatchOutput;
use crate::bigquery::schema::beam_batch;

static FAILURE_SCHEMA: LazyLock<Arc<Schema>> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("failed_row", FieldType::string()),
        Field::new("error_message", FieldType::string()),
        Field::new("stream", FieldType::string()),
        Field::new("offset", FieldType::int64()),
    ]))
});

/// Schema of the failure output of a BigQuery read.
///
/// Holds `failed_row` (text), `error_message`, `stream` and `offset`.
pub fn read_failure_schema() -> &'static Arc<Schema> {
    &FAILURE_SCHEMA
}

/// Builds the failure row for `row`, a batch with one row at `offset` in `stream`.
pub fn failure_row(
    row: &RecordBatch,
    error: &beam::Error,
    (stream, offset): (&str, i64),
) -> beam::Result<Row> {
    let values = vec![
        Some(FieldValue::String(render_row(row))),
        Some(FieldValue::String(error.to_string())),
        Some(FieldValue::String(stream.to_string())),
        Some(FieldValue::Int64(offset)),
    ];
    Row::new(Arc::clone(&FAILURE_SCHEMA), values).map_err(beam::Error::msg)
}

/// Converts `batch` with `codec`, where the first row is at `offset` in `stream`. A failed
/// batch converts one row at a time, and each bad row becomes a failure row.
pub fn read_outputs<T>(
    codec: &impl RowCodec<T>,
    batch: &RecordBatch,
    (stream, offset): (&str, i64),
) -> beam::Result<Vec<Result<T, Row>>> {
    convert_rows(batch, |rows| {
        let rows = beam_batch(rows)?;
        codec.decode_batch(&rows).map_err(beam::Error::msg)
    })
    .into_iter()
    .map(|converted| match converted {
        Ok(value) => Ok(Ok(value)),
        Err((index, row, error)) => {
            let offset = offset + i64::try_from(index).unwrap_or(i64::MAX);
            failure_row(&row, &error, (stream, offset)).map(Err)
        }
    })
    .collect()
}

/// Emits converted elements, and a failure row for each row that does not convert.
pub(super) struct WithErrors<C, T> {
    codec: C,
    _marker: PhantomData<fn() -> T>,
}

impl<C, T> WithErrors<C, T> {
    pub(super) fn new(codec: C) -> Self {
        Self {
            codec,
            _marker: PhantomData,
        }
    }
}

impl<C: Clone, T> Clone for WithErrors<C, T> {
    fn clone(&self) -> Self {
        Self::new(self.codec.clone())
    }
}

impl<C, T> BatchOutput for WithErrors<C, T>
where
    C: RowCodec<T> + Clone,
    T: DefaultCoder,
{
    type Out = Result<T, Row>;

    fn emit_batch(
        &self,
        batch: &RecordBatch,
        first_row: (&str, i64),
        ctx: &mut ProcessContext<'_, Self::Out>,
    ) -> beam::Result {
        read_outputs(&self.codec, batch, first_row)?
            .into_iter()
            .try_for_each(|output| ctx.emit(output))
    }
}
