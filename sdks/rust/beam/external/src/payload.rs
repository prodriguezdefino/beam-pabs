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

//! Encodes cross-language payloads (`SchemaTransformPayload`, `ExternalConfigurationPayload`,
//! `JavaClassLookupPayload`). Rows use the `beam:coder:row:v1` wire format.

use beam::coders::RowCoder;
use beam::schema::Row;
use model::pipeline as proto;
use prost::Message;

use super::error::ExpansionError;

/// Standard expansion payload URN for schema transforms.
pub const URN_EXPANSION_SCHEMA_TRANSFORM: &str = "beam:expansion:payload:schematransform:v1";

/// Standard expansion payload URN for Java class lookup transforms.
pub const URN_EXPANSION_JAVA_CLASS_LOOKUP: &str = "beam:expansion:payload:java_class_lookup:v1";

fn encode_row_with_schema(
    row: &Row,
    context: &str,
) -> Result<(proto::Schema, Vec<u8>), ExpansionError> {
    let mut row_bytes = Vec::new();
    RowCoder::encode_row(row, &mut row_bytes)
        .map_err(|e| ExpansionError::Encoding(format!("{context}: {e}")))?;
    let schema_proto: proto::Schema = (**row.schema()).clone().into();
    Ok((schema_proto, row_bytes))
}

/// Encodes a [`Row`] configuration into a serialized `SchemaTransformPayload` protobuf message.
pub fn encode_schema_transform_payload(
    identifier: impl Into<String>,
    config_row: &Row,
) -> Result<Vec<u8>, ExpansionError> {
    let (schema_proto, row_bytes) =
        encode_row_with_schema(config_row, "Failed to encode config row")?;

    let payload = proto::SchemaTransformPayload {
        identifier: identifier.into(),
        configuration_schema: Some(schema_proto),
        configuration_row: row_bytes,
    };

    Ok(payload.encode_to_vec())
}

/// Encodes a [`Row`] into an `ExternalConfigurationPayload` protobuf message.
pub fn encode_external_configuration_payload(config_row: &Row) -> Result<Vec<u8>, ExpansionError> {
    let (schema_proto, row_bytes) =
        encode_row_with_schema(config_row, "Failed to encode config row")?;

    let payload = proto::ExternalConfigurationPayload {
        schema: Some(schema_proto),
        payload: row_bytes,
    };

    Ok(payload.encode_to_vec())
}

/// Builder method invocation for `JavaClassLookupPayload`.
#[derive(Debug, Clone)]
pub struct JavaBuilderMethodCall {
    pub method_name: String,
    pub params: Row,
}

impl JavaBuilderMethodCall {
    pub fn new(method_name: impl Into<String>, params: Row) -> Self {
        Self {
            method_name: method_name.into(),
            params,
        }
    }
}

/// Encodes a `JavaClassLookupPayload` protobuf message.
pub fn encode_java_class_lookup_payload(
    class_name: impl Into<String>,
    constructor_method: Option<String>,
    constructor_args: Option<&Row>,
    builder_methods: Vec<JavaBuilderMethodCall>,
) -> Result<Vec<u8>, ExpansionError> {
    let (constructor_schema, constructor_payload) = match constructor_args {
        Some(args) => {
            let (schema_proto, bytes) = encode_row_with_schema(args, "Constructor args encode")?;
            (Some(schema_proto), bytes)
        }
        None => (None, Vec::new()),
    };

    let proto_builders = builder_methods
        .into_iter()
        .map(|b| {
            let (schema_proto, bytes) = encode_row_with_schema(
                &b.params,
                &format!("Builder method '{}' args encode", b.method_name),
            )?;
            Ok(proto::BuilderMethod {
                name: b.method_name,
                schema: Some(schema_proto),
                payload: bytes,
            })
        })
        .collect::<Result<Vec<_>, ExpansionError>>()?;

    let payload = proto::JavaClassLookupPayload {
        class_name: class_name.into(),
        constructor_method: constructor_method.unwrap_or_default(),
        constructor_schema,
        constructor_payload,
        builder_methods: proto_builders,
    };

    Ok(payload.encode_to_vec())
}
