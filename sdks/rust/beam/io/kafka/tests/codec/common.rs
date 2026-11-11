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

//! Helpers of the codec tests.

use std::sync::Arc;

use beam::Instant;
use beam::prelude::*;
use beam::schema::{FieldValue, Row, Schema};
use beam_io_kafka::codec::{CodecError, Decoder, Encoder, RecordRef};

pub(crate) const NO_HEADERS: &[(&str, Option<&[u8]>)] = &[];

pub(crate) fn record(value: Option<&[u8]>) -> RecordRef<'_> {
    RecordRef {
        topic: "events",
        partition: 3,
        offset: 42,
        timestamp: Some(Instant::from_millis(1_000)),
        key: None,
        value,
        headers: NO_HEADERS,
    }
}

pub(crate) fn decode<D: Decoder>(
    decoder: &D,
    record: &RecordRef<'_>,
) -> std::result::Result<D::Element, CodecError> {
    decoder.decode(&mut D::State::default(), record)
}

/// Gives `encoder` the schema of rows of `schema`, as the write does.
pub(crate) fn bind<E: Encoder<Input = Row>>(encoder: &mut E, schema: &Arc<Schema>) -> Result {
    let pipeline = Pipeline::new();
    let rows = pipeline
        .apply("Create", Create::new(Vec::<Row>::new()))
        .with_row_schema(schema);
    encoder.elements(&rows).map(|_| ())
}

pub(crate) fn make_row(schema: &Arc<Schema>, values: Vec<Option<FieldValue>>) -> Row {
    Row::new(Arc::clone(schema), values).expect("values of the schema")
}
