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

//! The record format of a SchemaTransform configuration: its name and its schema.

use beam::io::kafka::codec::{Avro, Decoder, Json, Payload, Proto, Raw, Utf8};

/// The codec of the `format` of a configuration.
#[derive(Clone)]
pub enum Format {
    Raw(Payload<Raw>),
    String(Payload<Utf8>),
    Json(Json),
    Avro(Avro),
    Proto(Proto),
}

impl std::fmt::Debug for Format {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Raw(_) => "RAW",
            Self::String(_) => "STRING",
            Self::Json(_) => "JSON",
            Self::Avro(_) => "AVRO",
            Self::Proto(_) => "PROTO",
        })
    }
}

impl Format {
    /// The codec of the format `name` (any case). `JSON` and `AVRO` reads need
    /// `schema`: a JSON Schema or an Avro schema.
    ///
    /// # Errors
    ///
    /// Fails for an unknown format, for `PROTO` (see [`Format::proto`]), or for an invalid
    /// schema.
    pub fn parse(name: &str, schema: Option<&str>) -> beam::Result<Self> {
        match (name.to_ascii_uppercase().as_str(), schema) {
            ("RAW", _) => Ok(Self::Raw(Payload::raw())),
            ("STRING", _) => Ok(Self::String(Payload::string())),
            ("JSON", Some(schema)) => Json::schema(schema).map(Self::Json),
            ("JSON", None) => Ok(Self::Json(Json::new())),
            ("AVRO", Some(schema)) => Avro::schema(schema).map(Self::Avro),
            ("AVRO", None) => Ok(Self::Avro(Avro::new())),
            ("PROTO", _) => Err(beam::Error::msg(
                "The PROTO format needs a file_descriptor_path and a message_name",
            )),
            _ => Err(beam::Error::msg(format!(
                "Unknown format '{name}': expected RAW, STRING, JSON, AVRO or PROTO"
            ))),
        }
    }

    /// The `PROTO` format: the message `message_name` of the file descriptor set in the file
    /// `file_descriptor_path`.
    ///
    /// # Errors
    ///
    /// Fails as [`Proto::from_file`] fails.
    pub fn proto(file_descriptor_path: &str, message_name: &str) -> beam::Result<Self> {
        Proto::from_file(file_descriptor_path, message_name).map(Self::Proto)
    }

    /// Fails if a read cannot use this format: `JSON` and `AVRO` reads need a schema.
    ///
    /// # Errors
    ///
    /// Fails with the reason.
    pub fn validate_read(&self) -> beam::Result {
        match self {
            Self::Raw(codec) => codec.validate(),
            Self::String(codec) => codec.validate(),
            Self::Json(codec) => codec.validate(),
            Self::Avro(codec) => codec.validate(),
            Self::Proto(codec) => codec.validate(),
        }
    }
}
