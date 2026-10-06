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

//! Prompt and response records for remote LLM inference, with their coders.

use beam::coders::{Coder, CoderError, CoderRegistry, Context, DefaultCoder, URN_KV};
use serde::{Deserialize, Serialize};

/// Input prompt record sent for remote LLM inference.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PromptRequest {
    /// Unique identifier for tracking and joining downstream.
    pub request_id: String,
    /// Text prompt provided to the language model.
    pub prompt: String,
}

/// Wire coder for [`PromptRequest`].
#[derive(Clone, Debug)]
pub struct PromptRequestCoder {
    id_coder: <String as DefaultCoder>::Coder,
    prompt_coder: <String as DefaultCoder>::Coder,
}

impl Coder<PromptRequest> for PromptRequestCoder {
    fn urn(&self) -> &'static str {
        URN_KV
    }

    fn encode(
        &self,
        element: &PromptRequest,
        writer: &mut dyn std::io::Write,
        context: Context,
    ) -> Result<(), CoderError> {
        self.id_coder
            .encode(&element.request_id, writer, Context::Nested)?;
        self.prompt_coder.encode(&element.prompt, writer, context)?;
        Ok(())
    }

    fn decode(
        &self,
        reader: &mut dyn std::io::Read,
        context: Context,
    ) -> Result<PromptRequest, CoderError> {
        let request_id = self.id_coder.decode(reader, Context::Nested)?;
        let prompt = self.prompt_coder.decode(reader, context)?;
        Ok(PromptRequest { request_id, prompt })
    }
}

impl DefaultCoder for PromptRequest {
    type Coder = PromptRequestCoder;

    fn coder() -> Self::Coder {
        PromptRequestCoder {
            id_coder: String::coder(),
            prompt_coder: String::coder(),
        }
    }

    fn encode_element(&self, writer: &mut dyn std::io::Write) -> Result<(), CoderError> {
        self.request_id.encode_element(writer)?;
        self.prompt.encode_element(writer)?;
        Ok(())
    }

    fn decode_element(reader: &mut dyn std::io::Read) -> Result<Self, CoderError> {
        let request_id = String::decode_element(reader)?;
        let prompt = String::decode_element(reader)?;
        Ok(Self { request_id, prompt })
    }

    fn register_coder<R: CoderRegistry + ?Sized>(registry: &R) -> String {
        let str_id = String::register_coder(registry);
        registry.register_coder(URN_KV, vec![str_id.clone(), str_id])
    }
}

/// Output response record returned by the remote language model.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LLMResponse {
    /// Unique identifier corresponding to the request.
    pub request_id: String,
    /// Generated output text from the model.
    pub response_text: String,
    /// Total token count consumed (prompt + response).
    pub token_count: usize,
}

/// Wire coder for [`LLMResponse`].
#[derive(Clone, Debug)]
pub struct LLMResponseCoder {
    id_coder: <String as DefaultCoder>::Coder,
    resp_coder: <String as DefaultCoder>::Coder,
    count_coder: <i64 as DefaultCoder>::Coder,
}

impl Coder<LLMResponse> for LLMResponseCoder {
    fn urn(&self) -> &'static str {
        URN_KV
    }

    fn encode(
        &self,
        element: &LLMResponse,
        writer: &mut dyn std::io::Write,
        context: Context,
    ) -> Result<(), CoderError> {
        self.id_coder
            .encode(&element.request_id, writer, Context::Nested)?;
        self.resp_coder
            .encode(&element.response_text, writer, Context::Nested)?;
        let count = element.token_count as i64;
        self.count_coder.encode(&count, writer, context)?;
        Ok(())
    }

    fn decode(
        &self,
        reader: &mut dyn std::io::Read,
        context: Context,
    ) -> Result<LLMResponse, CoderError> {
        let request_id = self.id_coder.decode(reader, Context::Nested)?;
        let response_text = self.resp_coder.decode(reader, Context::Nested)?;
        let count: i64 = self.count_coder.decode(reader, context)?;
        let token_count = count as usize;
        Ok(LLMResponse {
            request_id,
            response_text,
            token_count,
        })
    }
}

impl DefaultCoder for LLMResponse {
    type Coder = LLMResponseCoder;

    fn coder() -> Self::Coder {
        LLMResponseCoder {
            id_coder: String::coder(),
            resp_coder: String::coder(),
            count_coder: i64::coder(),
        }
    }

    fn encode_element(&self, writer: &mut dyn std::io::Write) -> Result<(), CoderError> {
        self.request_id.encode_element(writer)?;
        self.response_text.encode_element(writer)?;
        (self.token_count as i64).encode_element(writer)?;
        Ok(())
    }

    fn decode_element(reader: &mut dyn std::io::Read) -> Result<Self, CoderError> {
        let request_id = String::decode_element(reader)?;
        let response_text = String::decode_element(reader)?;
        let token_count = i64::decode_element(reader)? as usize;
        Ok(Self {
            request_id,
            response_text,
            token_count,
        })
    }

    fn register_coder<R: CoderRegistry + ?Sized>(registry: &R) -> String {
        let str_id = String::register_coder(registry);
        let int_id = i64::register_coder(registry);
        let inner_kv = registry.register_coder(URN_KV, vec![str_id.clone(), int_id]);
        registry.register_coder(URN_KV, vec![str_id, inner_kv])
    }
}
