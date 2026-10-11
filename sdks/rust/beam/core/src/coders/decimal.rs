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

//! Coder for [`Decimal`] values.

use rust_decimal::Decimal;

use super::URN_BYTES;
use super::standard::{VarIntCoder, read_bytes, write_bytes};
use super::traits::{Coder, CoderError, CoderRegistry, Context, DefaultCoder};
use crate::schema::logical::{from_minimal_twos_complement, to_minimal_twos_complement};

/// Coder for [`Decimal`], registered as `beam:coder:bytes:v1`.
///
/// Beam has no standard decimal coder. The bytes payload is a varint scale, then the
/// minimal big-endian two's-complement unscaled value. This payload is the whole-stream
/// encoding of the Java `BigDecimalCoder`. Runners know the bytes coder, so they add no
/// length prefix of their own.
#[derive(Clone, Copy, Debug, Default)]
pub struct DecimalCoder;

fn decimal_payload(value: &Decimal) -> Result<Vec<u8>, CoderError> {
    let unscaled = to_minimal_twos_complement(value.mantissa());
    let mut payload = Vec::with_capacity(unscaled.len() + 1);
    VarIntCoder::encode_varint(i64::from(value.scale()), &mut payload)?;
    payload.extend_from_slice(&unscaled);
    Ok(payload)
}

fn decimal_from_payload(payload: &[u8]) -> Result<Decimal, CoderError> {
    let mut rest = payload;
    let scale = VarIntCoder::decode_varint(&mut rest)?;
    let scale = u32::try_from(scale)
        .map_err(|_| CoderError::Format(format!("Invalid decimal scale: {scale}")))?;
    let mantissa = from_minimal_twos_complement(rest)?;
    Decimal::try_from_i128_with_scale(mantissa, scale).map_err(|e| {
        CoderError::Format(format!("Decimal {mantissa}e-{scale} is out of range: {e}"))
    })
}

impl Coder<Decimal> for DecimalCoder {
    fn urn(&self) -> &'static str {
        URN_BYTES
    }

    fn encode(
        &self,
        value: &Decimal,
        writer: &mut Vec<u8>,
        context: Context,
    ) -> Result<(), CoderError> {
        write_bytes(&decimal_payload(value)?, writer, context)
    }

    fn decode(&self, reader: &mut &[u8], context: Context) -> Result<Decimal, CoderError> {
        decimal_from_payload(&read_bytes(reader, context)?)
    }
}

impl DefaultCoder for Decimal {
    type Coder = DecimalCoder;

    fn coder() -> Self::Coder {
        DecimalCoder
    }

    fn encode_element(&self, writer: &mut Vec<u8>) -> Result<(), CoderError> {
        DecimalCoder.encode(self, writer, Context::Nested)
    }

    fn decode_element(reader: &mut &[u8]) -> Result<Self, CoderError> {
        DecimalCoder.decode(reader, Context::Nested)
    }

    fn register_coder<R: CoderRegistry + ?Sized>(registry: &R) -> String {
        registry.register_coder(URN_BYTES, Vec::new())
    }
}
