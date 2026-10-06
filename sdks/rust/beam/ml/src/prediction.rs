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

//! Data types for inference predictions.

use std::io::{Read, Write};

use beam::coders::{Coder, CoderError, CoderRegistry, Context, DefaultCoder, URN_KV};
use serde::{Deserialize, Serialize};

/// Output record emitted by [`RunInference`](crate::RunInference), pairing input with prediction.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PredictionResult<In, Out> {
    /// Input element submitted for inference.
    pub input: In,
    /// Model prediction output.
    pub output: Out,
}

impl<In, Out> PredictionResult<In, Out> {
    /// Creates a new `PredictionResult`.
    pub fn new(input: In, output: Out) -> Self {
        Self { input, output }
    }
}

/// Coder for `PredictionResult<In, Out>` that uses the standard Beam KV encoding.
#[derive(Clone, Debug)]
pub struct PredictionResultCoder<IC, OC> {
    input_coder: IC,
    output_coder: OC,
}

impl<IC, OC> PredictionResultCoder<IC, OC> {
    /// Creates a coder from component input and output coders.
    pub fn new(input_coder: IC, output_coder: OC) -> Self {
        Self {
            input_coder,
            output_coder,
        }
    }
}

impl<In: Send + Sync + 'static, Out: Send + Sync + 'static, IC: Coder<In>, OC: Coder<Out>>
    Coder<PredictionResult<In, Out>> for PredictionResultCoder<IC, OC>
{
    fn urn(&self) -> &'static str {
        URN_KV
    }

    fn encode(
        &self,
        value: &PredictionResult<In, Out>,
        writer: &mut dyn Write,
        context: Context,
    ) -> Result<(), CoderError> {
        self.input_coder
            .encode(&value.input, writer, Context::Nested)?;
        self.output_coder.encode(&value.output, writer, context)?;
        Ok(())
    }

    fn decode(
        &self,
        reader: &mut dyn Read,
        context: Context,
    ) -> Result<PredictionResult<In, Out>, CoderError> {
        let input = self.input_coder.decode(reader, Context::Nested)?;
        let output = self.output_coder.decode(reader, context)?;
        Ok(PredictionResult::new(input, output))
    }
}

impl<In: DefaultCoder, Out: DefaultCoder> DefaultCoder for PredictionResult<In, Out> {
    type Coder = PredictionResultCoder<In::Coder, Out::Coder>;

    fn coder() -> Self::Coder {
        PredictionResultCoder::new(In::coder(), Out::coder())
    }

    fn encode_element(&self, writer: &mut dyn Write) -> Result<(), CoderError> {
        self.input.encode_element(writer)?;
        self.output.encode_element(writer)?;
        Ok(())
    }

    fn decode_element(reader: &mut dyn Read) -> Result<Self, CoderError> {
        let input = In::decode_element(reader)?;
        let output = Out::decode_element(reader)?;
        Ok(PredictionResult::new(input, output))
    }

    fn register_coder<R: CoderRegistry + ?Sized>(registry: &R) -> String {
        let in_id = In::register_coder(registry);
        let out_id = Out::register_coder(registry);
        registry.register_coder(URN_KV, vec![in_id, out_id])
    }
}
