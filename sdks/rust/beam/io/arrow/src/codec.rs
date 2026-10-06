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

//! Element codecs: how a connector turns its element type into Beam rows and back.
//!
//! A [`RowCodec`] gives the Beam schema of `T` and both conversions, so one reader and
//! writer serve `#[derive(BeamRow)]` structs ([`BeamRowCodec`]) and [`Row`]s
//! ([`SchemaRowCodec`]).

use std::fmt;
use std::marker::PhantomData;
use std::sync::Arc;

use arrow_array::RecordBatch;
use beam::schema::{BeamRow, Row, Schema};

use crate::convert::{record_batch_to_beam_rows, record_batch_to_rows};
use crate::error::{ArrowBridgeError, Result};

/// Converts elements of type `T` to and from Beam rows of a fixed schema.
pub trait RowCodec<T>: Send + Sync + 'static {
    /// The Beam schema every element maps to.
    fn schema(&self) -> &Arc<Schema>;

    /// Converts one element into a row of [`schema`](Self::schema).
    fn to_row(&self, value: &T) -> Result<Row>;

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

    fn to_row(&self, value: &T) -> Result<Row> {
        value.to_row().map_err(ArrowBridgeError::from)
    }

    fn decode_batch(&self, batch: &RecordBatch) -> Result<Vec<T>> {
        record_batch_to_beam_rows(batch)
    }
}

/// [`RowCodec`] for dynamically typed [`Row`]s of an explicit schema.
///
/// Written rows must have as many values as the schema has fields.
#[derive(Clone, Debug)]
pub struct SchemaRowCodec {
    schema: Arc<Schema>,
}

impl SchemaRowCodec {
    pub fn new(schema: impl Into<Arc<Schema>>) -> Self {
        Self {
            schema: schema.into(),
        }
    }
}

impl RowCodec<Row> for SchemaRowCodec {
    fn schema(&self) -> &Arc<Schema> {
        &self.schema
    }

    fn to_row(&self, value: &Row) -> Result<Row> {
        Ok(value.clone())
    }

    fn decode_batch(&self, batch: &RecordBatch) -> Result<Vec<Row>> {
        record_batch_to_rows(batch, &self.schema)
    }
}
