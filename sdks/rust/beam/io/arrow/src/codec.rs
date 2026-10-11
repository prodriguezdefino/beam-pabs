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

//! Element codecs: how a connector turns its element type into Arrow rows and back.
//!
//! A [`RowCodec`] gives the Beam schema of `T` and both conversions, so one reader and one
//! writer serve `#[derive(BeamRow)]` structs ([`BeamRowCodec`]) and dynamically typed rows
//! ([`SchemaRowCodec`]).

use std::fmt;
use std::marker::PhantomData;
use std::sync::Arc;

use arrow_array::RecordBatch;
use beam::schema::{BeamRow, RowElement, Schema};

use crate::convert::{
    RowBatchBuilder, record_batch_to_beam_rows, record_batch_to_encoded_rows, record_batch_to_rows,
};
use crate::error::{ArrowBridgeError, Result};

/// Converts elements of type `T` to and from Arrow rows of a fixed Beam schema.
pub trait RowCodec<T>: Send + Sync + 'static {
    /// The Beam schema every element maps to.
    fn schema(&self) -> &Arc<Schema>;

    /// Adds `value` to `batch` as one row of [`schema`](Self::schema).
    fn append(&self, value: &T, batch: &mut RowBatchBuilder) -> Result<()>;

    /// Converts a record batch into elements, matching columns by name.
    fn decode_batch(&self, batch: &RecordBatch) -> Result<Vec<T>>;
}

/// [`RowCodec`] for `#[derive(BeamRow)]` types, using the derived schema.
pub struct BeamRowCodec<T>(PhantomData<fn() -> T>);

impl<T> BeamRowCodec<T> {
    pub fn new() -> Self {
        Self(PhantomData)
    }
}

impl<T> Default for BeamRowCodec<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> Clone for BeamRowCodec<T> {
    fn clone(&self) -> Self {
        Self::new()
    }
}

impl<T> fmt::Debug for BeamRowCodec<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("BeamRowCodec")
    }
}

impl<T: BeamRow + 'static> RowCodec<T> for BeamRowCodec<T> {
    fn schema(&self) -> &Arc<Schema> {
        T::beam_schema()
    }

    fn append(&self, value: &T, batch: &mut RowBatchBuilder) -> Result<()> {
        batch.append_row(&value.to_row().map_err(ArrowBridgeError::from)?)
    }

    fn decode_batch(&self, batch: &RecordBatch) -> Result<Vec<T>> {
        record_batch_to_beam_rows(batch)
    }
}

/// The form of each row that [`SchemaRowCodec`] reads from a record batch.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum RowOutput {
    /// [`RowElement::Row`]: a fused Rust step takes the row with no decode.
    #[default]
    Values,
    /// [`RowElement::Encoded`]: the row coder bytes, with no row between. Another SDK, or a
    /// step after a fusion break, takes the bytes with no encode.
    Encoded,
}

/// [`RowCodec`] for dynamically typed rows of an explicit schema.
///
/// It writes both forms of a [`RowElement`]: row coder bytes go to Arrow with no row
/// between. A written row must have as many values as the schema has fields.
#[derive(Clone, Debug)]
pub struct SchemaRowCodec {
    schema: Arc<Schema>,
    output: RowOutput,
}

impl SchemaRowCodec {
    pub fn new(schema: impl Into<Arc<Schema>>) -> Self {
        Self {
            schema: schema.into(),
            output: RowOutput::default(),
        }
    }

    /// Sets the form of each row read from a batch (default [`RowOutput::Values`]).
    pub fn with_output(mut self, output: RowOutput) -> Self {
        self.output = output;
        self
    }
}

impl RowCodec<RowElement> for SchemaRowCodec {
    fn schema(&self) -> &Arc<Schema> {
        &self.schema
    }

    fn append(&self, value: &RowElement, batch: &mut RowBatchBuilder) -> Result<()> {
        batch.append(value)
    }

    fn decode_batch(&self, batch: &RecordBatch) -> Result<Vec<RowElement>> {
        Ok(match self.output {
            RowOutput::Values => record_batch_to_rows(batch, &self.schema)?
                .into_iter()
                .map(RowElement::Row)
                .collect(),
            RowOutput::Encoded => record_batch_to_encoded_rows(batch, &self.schema)?
                .into_iter()
                .map(RowElement::Encoded)
                .collect(),
        })
    }
}
