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

//! [`ReadOutput`] error routing of [`ReadRangesFn`](super::ReadRangesFn) outputs.

use std::sync::{Arc, LazyLock};

use beam::schema::{Field, FieldType, FieldValue, Row, RowElement, Schema};
use beam::transforms::{Outputs, ProcessContext};

use super::assembler::RawRow;
use super::encode::{encode_flattened, encode_nested, into_flattened_rows, into_nested_row};
use crate::bigtable::BigtableReadShape;
use crate::bigtable::target::ReadTarget;

static READ_ERROR_SCHEMA: LazyLock<Arc<Schema>> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("target", FieldType::string()),
        Field::new("row_key", FieldType::bytes()),
        Field::new("error_message", FieldType::string()),
    ]))
});

/// Schema of the error output [`PCollection<Row>`](beam::values::PCollection) produced by
/// [`ReadRows::with_failures`](super::ReadRows::with_failures).
pub fn read_error_schema() -> &'static Arc<Schema> {
    &READ_ERROR_SCHEMA
}

/// Output dispatch trait for [`ReadRangesFn`](super::ReadRangesFn): either [`RowElement`]
/// (fail-fast) or `Result<RowElement, Row>` (routing conversion/stream errors to the
/// `failures` output).
pub trait ReadOutput: Outputs + Sized {
    fn emit_raw_row(
        raw_row: RawRow,
        shape: BigtableReadShape,
        encoded_rows: bool,
        target: &ReadTarget,
        ctx: &mut ProcessContext<'_, Self>,
    ) -> beam::Result;

    /// Routes a conversion or stream error at `row_key`.
    ///
    /// When `emit_failure` returns `Ok` for a stream error, the reader does not drop the rest
    /// of the key range. If the reader claimed a key, it resumes after the last claimed key.
    /// Before the first claimed key, a resume makes no progress. Then the reader stops, and
    /// the error message names the unread key range.
    fn emit_failure(
        target: &ReadTarget,
        row_key: &[u8],
        error: String,
        ctx: &mut ProcessContext<'_, Self>,
    ) -> beam::Result;
}

/// Elements of one converted Bigtable row: one for the nested shape, one per column for the
/// flattened shape.
enum RowElements {
    One(Option<RowElement>),
    Encoded(std::vec::IntoIter<Vec<u8>>),
    Rows(std::vec::IntoIter<Row>),
}

impl Iterator for RowElements {
    type Item = RowElement;

    fn next(&mut self) -> Option<RowElement> {
        match self {
            Self::One(element) => element.take(),
            Self::Encoded(columns) => columns.next().map(RowElement::Encoded),
            Self::Rows(rows) => rows.next().map(RowElement::Row),
        }
    }
}

fn convert(
    raw_row: RawRow,
    shape: BigtableReadShape,
    encoded_rows: bool,
) -> beam::Result<RowElements> {
    Ok(match (encoded_rows, shape) {
        (true, BigtableReadShape::Nested) => {
            RowElements::One(Some(RowElement::Encoded(encode_nested(&raw_row)?)))
        }
        (true, BigtableReadShape::Flattened) => {
            RowElements::Encoded(encode_flattened(&raw_row)?.into_iter())
        }
        (false, BigtableReadShape::Nested) => {
            RowElements::One(Some(RowElement::Row(into_nested_row(raw_row)?)))
        }
        (false, BigtableReadShape::Flattened) => {
            RowElements::Rows(into_flattened_rows(raw_row)?.into_iter())
        }
    })
}

impl ReadOutput for RowElement {
    fn emit_raw_row(
        raw_row: RawRow,
        shape: BigtableReadShape,
        encoded_rows: bool,
        _target: &ReadTarget,
        ctx: &mut ProcessContext<'_, Self>,
    ) -> beam::Result {
        convert(raw_row, shape, encoded_rows)?.try_for_each(|element| ctx.emit(element))
    }

    fn emit_failure(
        target: &ReadTarget,
        _row_key: &[u8],
        error: String,
        _ctx: &mut ProcessContext<'_, Self>,
    ) -> beam::Result {
        Err(beam::Error::msg(format!(
            "Bigtable ReadRows failed on {}: {}",
            target.resource_name(),
            error
        )))
    }
}

impl ReadOutput for Result<RowElement, Row> {
    fn emit_raw_row(
        raw_row: RawRow,
        shape: BigtableReadShape,
        encoded_rows: bool,
        target: &ReadTarget,
        ctx: &mut ProcessContext<'_, Self>,
    ) -> beam::Result {
        // A reference count increment: the conversion consumes the row, and a failure needs
        // the key.
        let row_key = raw_row.key.clone();
        match convert(raw_row, shape, encoded_rows) {
            Ok(elements) => elements.map(Ok).try_for_each(|element| ctx.emit(element)),
            Err(err) => Self::emit_failure(target, &row_key, err.to_string(), ctx),
        }
    }

    fn emit_failure(
        target: &ReadTarget,
        row_key: &[u8],
        error: String,
        ctx: &mut ProcessContext<'_, Self>,
    ) -> beam::Result {
        let err_row = Row::new(
            Arc::clone(&READ_ERROR_SCHEMA),
            vec![
                Some(FieldValue::String(target.resource_name())),
                Some(FieldValue::Bytes(row_key.to_vec())),
                Some(FieldValue::String(error)),
            ],
        )
        .map_err(beam::Error::from)?;
        ctx.emit(Err(err_row))
    }
}
