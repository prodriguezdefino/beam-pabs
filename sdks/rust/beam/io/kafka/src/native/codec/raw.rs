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

//! Values as they are ([`Raw`]) and UTF-8 values ([`Utf8`]).

use beam::schema::{FieldType, FieldValue};
use beam::values::PCollection;

use super::{CodecError, Decoder, Encoder, Outgoing, RecordRef};

/// Record values as they are, as `Vec<u8>`: the default codec of the read and the write.
///
/// The read copies each value once, out of the buffer of librdkafka. A record with no value
/// (a tombstone) does not convert; [`WithMetadata`](super::WithMetadata) reads it as a
/// [`KafkaRecord`](super::KafkaRecord) with no value.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Raw;

impl Decoder for Raw {
    type Element = Vec<u8>;
    type Output = Vec<u8>;

    fn decode(&self, record: &RecordRef<'_>) -> Result<Vec<u8>, CodecError> {
        record.value_or_err().map(<[u8]>::to_vec)
    }

    fn output(&self, elements: PCollection<Vec<u8>>) -> PCollection<Vec<u8>> {
        elements
    }
}

impl Encoder for Raw {
    type Input = Vec<u8>;
    type Element = Vec<u8>;

    fn elements(&mut self, input: &PCollection<Vec<u8>>) -> beam::Result<PCollection<Vec<u8>>> {
        Ok(input.clone())
    }

    fn encode<'a>(
        &self,
        element: &'a Vec<u8>,
        _buffer: &'a mut Vec<u8>,
    ) -> Result<Outgoing<'a>, CodecError> {
        Ok(Outgoing::value(element))
    }

    fn failed_type(&self) -> FieldType {
        FieldType::bytes()
    }

    fn failed_value(&self, element: Vec<u8>) -> Result<FieldValue, CodecError> {
        Ok(FieldValue::Bytes(element))
    }
}

/// Record values that are UTF-8 text, as `String`. A value that is not UTF-8 does not
/// convert.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Utf8;

impl Decoder for Utf8 {
    type Element = String;
    type Output = String;

    fn decode(&self, record: &RecordRef<'_>) -> Result<String, CodecError> {
        let value = record.value_or_err()?;
        std::str::from_utf8(value)
            .map(str::to_string)
            .map_err(|e| CodecError::new(format!("The value is not UTF-8: {e}")))
    }

    fn output(&self, elements: PCollection<String>) -> PCollection<String> {
        elements
    }
}

impl Encoder for Utf8 {
    type Input = String;
    type Element = String;

    fn elements(&mut self, input: &PCollection<String>) -> beam::Result<PCollection<String>> {
        Ok(input.clone())
    }

    fn encode<'a>(
        &self,
        element: &'a String,
        _buffer: &'a mut Vec<u8>,
    ) -> Result<Outgoing<'a>, CodecError> {
        Ok(Outgoing::value(element.as_bytes()))
    }

    fn failed_type(&self) -> FieldType {
        FieldType::string()
    }

    fn failed_value(&self, element: String) -> Result<FieldValue, CodecError> {
        Ok(FieldValue::String(element))
    }
}
