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

//! The error output of a read. Only [`ReadRows::with_failures`] uses this module.
//! Its stream reader emits `Result<RowElement, Row>`: a row goes to the main output, and an
//! error row to the error output.
//!
//! [`ReadRows::with_failures`]: super::ReadRows::with_failures

use std::sync::{Arc, LazyLock};

use arrow_io::RowOutput;
use arrow_io::arrow_array::RecordBatch;
use beam::schema::{Field, FieldType, FieldValue, Row, RowElement, Schema};
use beam::transforms::ProcessContext;

use super::reader::StreamOutput;
use crate::bigquery::storage::schema::{batch_to_encoded_rows, batch_to_rows};

static ERROR_SCHEMA: LazyLock<Arc<Schema>> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("failed_row", FieldType::string()),
        Field::new("error_message", FieldType::string()),
        Field::new("stream", FieldType::string()),
        Field::new("offset", FieldType::int64()),
    ]))
});

/// The schema of the error output: the row as text, the reason, and the stream and offset of
/// the row.
pub fn error_schema() -> &'static Arc<Schema> {
    &ERROR_SCHEMA
}

/// Converts the rows of `batch` into rows of `schema` in the form that `output` names.
fn convert(
    batch: &RecordBatch,
    schema: &Arc<Schema>,
    output: RowOutput,
) -> beam::Result<Vec<RowElement>> {
    Ok(match output {
        RowOutput::Encoded => batch_to_encoded_rows(batch, schema)?
            .into_iter()
            .map(RowElement::Encoded)
            .collect(),
        RowOutput::Values => batch_to_rows(batch, schema)?
            .into_iter()
            .map(RowElement::Row)
            .collect(),
    })
}

/// The values of a one-row batch as `name: value` pairs, in the debug format of Arrow.
fn render(row: &RecordBatch) -> String {
    let schema = row.schema();
    let fields = schema
        .fields()
        .iter()
        .zip(row.columns())
        .map(|(field, column)| format!("{}: {column:?}", field.name()))
        .collect::<Vec<_>>()
        .join(", ");
    // Arrow prints an array on several lines.
    format!(
        "{{{}}}",
        fields.split_whitespace().collect::<Vec<_>>().join(" ")
    )
}

/// The error row for `row`, a batch with one row at `offset` in `stream`.
pub fn error_row(
    row: &RecordBatch,
    error: &beam::Error,
    (stream, offset): (&str, i64),
) -> beam::Result<Row> {
    let values = vec![
        Some(FieldValue::String(render(row))),
        Some(FieldValue::String(error.to_string())),
        Some(FieldValue::String(stream.to_string())),
        Some(FieldValue::Int64(offset)),
    ];
    Row::new(Arc::clone(&ERROR_SCHEMA), values).map_err(beam::Error::msg)
}

/// The rows of `batch`, whose first row is at `offset` in `stream`. A batch that does not
/// convert converts again one row at a time, and each row that fails becomes an error row.
pub fn read_outputs(
    batch: &RecordBatch,
    schema: &Arc<Schema>,
    output: RowOutput,
    (stream, offset): (&str, i64),
) -> beam::Result<Vec<Result<RowElement, Row>>> {
    if let Ok(rows) = convert(batch, schema, output) {
        return Ok(rows.into_iter().map(Ok).collect());
    }
    (offset..)
        .zip(0..batch.num_rows())
        .map(|(offset, index)| {
            let row = batch.slice(index, 1);
            match convert(&row, schema, output) {
                Ok(rows) => rows
                    .into_iter()
                    .next()
                    .map(Ok)
                    .ok_or_else(|| beam::Error::msg("A batch with one row gave no row")),
                Err(error) => error_row(&row, &error, (stream, offset)).map(Err),
            }
        })
        .collect()
}

impl StreamOutput for Result<RowElement, Row> {
    fn emit_batch(
        batch: &RecordBatch,
        schema: &Arc<Schema>,
        output: RowOutput,
        first_row: (&str, i64),
        ctx: &mut ProcessContext<'_, Self>,
    ) -> beam::Result {
        read_outputs(batch, schema, output, first_row)?
            .into_iter()
            .try_for_each(|output| ctx.emit(output))
    }
}
