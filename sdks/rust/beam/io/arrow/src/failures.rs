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

//! The rows of a batch that do not convert, for the error output of a read.

use arrow_array::RecordBatch;

/// A row of a batch that did not convert: its index in the batch, the batch of that one row,
/// and the reason.
pub type FailedRow<E> = (usize, RecordBatch, E);

/// Converts `batch` with `convert`. If the batch does not convert, converts each row alone,
/// so only the rows that fail become errors. Only a batch with a bad row takes this path.
pub fn convert_rows<T, E>(
    batch: &RecordBatch,
    convert: impl Fn(&RecordBatch) -> Result<Vec<T>, E>,
) -> Vec<Result<T, FailedRow<E>>> {
    if let Ok(rows) = convert(batch) {
        return rows.into_iter().map(Ok).collect();
    }
    (0..batch.num_rows())
        .filter_map(|index| {
            let row = batch.slice(index, 1);
            match convert(&row) {
                Ok(rows) => rows.into_iter().next().map(Ok),
                Err(error) => Some(Err((index, row, error))),
            }
        })
        .collect()
}

/// The values of a one-row batch as `name: value` pairs, in the debug format of Arrow.
pub fn render_row(row: &RecordBatch) -> String {
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
