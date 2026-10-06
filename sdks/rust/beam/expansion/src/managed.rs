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

//! The `beam:transform:managed:v1` request: finds the provider of the transform identifier and
//! converts the YAML config to its config row. The replay entry names that provider, so the
//! worker never reads YAML.

use beam::schema::Schema;
use beam_io_managed::{URN_MANAGED, managed_config_schema};
use beam_model::expansion::SchemaTransformConfig;
use beam_model::pipeline as proto;
use prost::Message;

use crate::config::decode_config;
use crate::discovery::find_managed_provider;
use crate::request::Resolved;
use crate::yaml;

/// Annotation keys of `ExternalTransforms.Annotations` for an expanded Managed transform.
const ANNOTATION_SCHEMATRANSFORM_URN: &str = "schematransform_urn";
const ANNOTATION_UNDERLYING_URN: &str = "managed_underlying_transform_urn";
const ANNOTATION_CONFIG_ROW_SCHEMA: &str = "config_row_schema";
const ANNOTATION_CONFIG_ROW: &str = "config_row";

/// Resolves a Managed request. The annotations give the Managed URN, the requested
/// identifier and the Managed config row, as the Java provider does.
pub(crate) fn resolve(
    caller_schema: Option<proto::Schema>,
    row_bytes: &[u8],
) -> Result<Resolved, String> {
    let schema = managed_config_schema();
    let managed = decode_config(Schema::clone(&schema), caller_schema, row_bytes)
        .map_err(|e| format!("Failed to decode config row for '{URN_MANAGED}': {e}"))?;
    let text = |name: &str| {
        managed
            .get_string(name)
            .map(|value| value.filter(|s| !s.is_empty()))
            .map_err(|e| e.to_string())
    };
    let identifier = text("transform_identifier")?
        .ok_or("The Managed transform has no 'transform_identifier'")?;
    let config = match (text("config")?, text("config_url")?) {
        (Some(config), None) => config.to_string(),
        (None, Some(url)) => read_config_url(url)?,
        (Some(_), Some(_)) | (None, None) => {
            return Err("Please specify a config or a config URL, but not both.".to_string());
        }
    };
    let provider = find_managed_provider(identifier)?;
    let row = yaml::to_row(
        &config,
        provider.config_schema(),
        provider.managed_key_aliases(),
    )
    .map_err(|e| format!("Invalid config for '{identifier}': {e}"))?;
    let annotations = [
        (
            ANNOTATION_SCHEMATRANSFORM_URN,
            URN_MANAGED.as_bytes().to_vec(),
        ),
        (ANNOTATION_UNDERLYING_URN, identifier.as_bytes().to_vec()),
        (
            ANNOTATION_CONFIG_ROW_SCHEMA,
            proto::Schema::from(Schema::clone(&schema)).encode_to_vec(),
        ),
        (
            ANNOTATION_CONFIG_ROW,
            managed.to_row_bytes().map_err(|e| e.to_string())?,
        ),
    ];
    Ok(Resolved {
        provider,
        config: row,
        annotations: annotations
            .into_iter()
            .map(|(key, value)| (key.to_string(), value))
            .collect(),
    })
}

/// Limit on the size of a YAML config file read from `config_url`: 1 MiB.
const MAX_CONFIG_BYTES: usize = 1024 * 1024;

/// Reads the YAML file at `url`: a local path, a `file://` URL, or a URL of a linked file
/// system, such as `gs://` in the GCP expansion service.
fn read_config_url(url: &str) -> Result<String, String> {
    use std::io::Read;

    let fs = beam_io_file::get_filesystem(url)
        .map_err(|e| format!("Cannot read the config URL '{url}': {e}"))?;
    let reader = fs
        .open_read(url)
        .map_err(|e| format!("Cannot read the config URL '{url}': {e}"))?;
    let mut bytes = Vec::new();
    reader
        .take((MAX_CONFIG_BYTES + 1) as u64)
        .read_to_end(&mut bytes)
        .map_err(|e| format!("Cannot read the config URL '{url}': {e}"))?;
    if bytes.len() > MAX_CONFIG_BYTES {
        return Err(format!(
            "The config URL '{url}' exceeds the {MAX_CONFIG_BYTES}-byte limit"
        ));
    }
    String::from_utf8(bytes).map_err(|e| format!("The config URL '{url}' is not UTF-8: {e}"))
}

/// The discovery entry of the Managed transform: the `ManagedConfig` schema and one output.
pub(crate) fn discovery_entry() -> (String, SchemaTransformConfig) {
    let config = SchemaTransformConfig {
        config_schema: Some(Schema::clone(&managed_config_schema()).into()),
        input_pcollection_names: Vec::new(),
        output_pcollection_names: vec!["output".to_string()],
        description: "Expands the Rust provider of a Managed transform identifier.".to_string(),
    };
    (URN_MANAGED.to_string(), config)
}
